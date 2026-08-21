(ns ring-chez.zlib
  "gzip compression over zlib, bound through `jolt.ffi`.

   jolt has no `java.util.zip`, so no existing Ring compression middleware can
   run here; Igropyr answers the same problem the same way — zlib through the
   FFI, `deflateInit2` with window-bits 31 for the gzip wrapper. The pump below
   is ported from jolt-lang/http-client's `jolt.http.zlib` (EPL-2.0, same
   license as this repo), narrowed to compression and made degradable.

   `available?` says whether zlib was found; `gzip` returns nil when it was
   not, which callers read as \"send this uncompressed\" (Igropyr's
   `gzip-compress` returns #f for exactly that)."
  (:require [jolt.ffi :as ffi]))

;; z_stream layout (LP64): the four fields the pump drives, by byte offset.
(def ^:private ZS 112)            ; sizeof(z_stream)
(def ^:private O-next-in 0)
(def ^:private O-avail-in 8)
(def ^:private O-next-out 24)
(def ^:private O-avail-out 32)
(def ^:private CHUNK 65536)
(def ^:private Z-STREAM-END 1)
(def ^:private Z-FINISH 4)
(def ^:private GZIP-WINDOW-BITS 31)   ; 15 (max window) + 16 (gzip wrapper)

(defn- bind-zlib []
  {:version      (ffi/foreign-fn "zlibVersion"   [] :pointer)
   :deflate-init (ffi/foreign-fn "deflateInit2_"
                                 [:pointer :int :int :int :int :int :pointer :int] :int)
   :deflate      (ffi/foreign-fn "deflate"       [:pointer :int] :int)
   :deflate-end  (ffi/foreign-fn "deflateEnd"    [:pointer] :int)})

;; Resolve from the RUNNING PROCESS first, and only load a shared object if
;; that fails. The Chez runtime links a zlib of its own (it compresses fasl
;; files with it), so on a normal jolt these symbols are already here — and
;; putting a second zlib with the same global names into the process is a
;; known way to corrupt deflate: Igropyr documents a FreeBSD build where
;; deflateInit2_ reported success but left a state whose sym_buf held only the
;; low 32 bits of pending_buf, and deflate() then faulted once the C heap sat
;; above 4 GB. Never loading a second copy avoids that class of failure
;; entirely, and the candidates below are only for a build that exports none.
;;
;; Failure is not an error: it means no compression, which is always a valid
;; way to answer a request.
(def ^:private zlib
  (delay
    (or (try (bind-zlib) (catch Throwable _ nil))
        (some (fn [lib]
                (try (ffi/load-library lib) (bind-zlib) (catch Throwable _ nil)))
              ["libz.so.1" "libz.so" "libz.dylib"]))))

(defn available?
  "True when zlib could be bound. False means `gzip` returns nil and callers
  should send bodies as they are."
  []
  (some? @zlib))

(defn gzip
  "Compress `src` (a byte array) into gzip format at `level` (1..9, default 6),
   or nil if zlib is unavailable or refuses the input — nil means \"send it
   uncompressed\"."
  ([^bytes src] (gzip src 6))
  ([^bytes src level]
   (when-let [{:keys [version deflate-init deflate deflate-end]} @zlib]
     (let [n       (alength src)
           strm    (ffi/alloc ZS)
           src-buf (ffi/alloc (max 1 n))
           out-buf (ffi/alloc CHUNK)]
       (try
         (dotimes [i ZS] (ffi/write strm :uint8 i 0))
         (ffi/write-array src-buf src)
         (ffi/write strm :pointer O-next-in src-buf)
         (ffi/write strm :uint O-avail-in n)
         (when-not (zero? (deflate-init strm level 8 GZIP-WINDOW-BITS 8 0 (version) ZS))
           (throw (ex-info "zlib: deflateInit2 failed" {:level level})))
         (let [chunks (loop [acc []]
                        (ffi/write strm :pointer O-next-out out-buf)
                        (ffi/write strm :uint O-avail-out CHUNK)
                        (let [r        (deflate strm Z-FINISH)
                              produced (- CHUNK (ffi/read strm :uint O-avail-out))
                              acc      (if (pos? produced)
                                         (conj acc (ffi/read-array out-buf produced))
                                         acc)]
                          (cond
                            (neg? r) (do (deflate-end strm)
                                         (throw (ex-info "zlib: deflate failed" {:rc r})))
                            (= r Z-STREAM-END) acc
                            :else (recur acc))))]
           (deflate-end strm)
           (byte-array (mapcat seq chunks)))
         ;; a compressor that throws must not take the response with it: the
         ;; uncompressed body is still a correct answer
         (catch Throwable _ nil)
         (finally (ffi/free strm) (ffi/free src-buf) (ffi/free out-buf)))))))
