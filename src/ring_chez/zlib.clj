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
;; jolt 0.8.9+ registers its zlib under private jolt_z_* names instead, so on
;; Linux nothing here resolves from the process; deps.edn declares libz as an
;; optional :jolt/native, and a native jolt loaded is where the symbols come
;; from. No name clash is possible with the runtime's copy any more.
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

(def ^:private Z-OK 0)
(def ^:private Z-BUF-ERROR -5)
(def ^:private Z-SYNC-FLUSH 2)

(defn- init-stream!
  "A zeroed z_stream set up for gzip output, or a throw. window-bits 9..15 and
  mem-level 1..9 trade compression for the state's size (zlib holds about
  2^(window-bits+2) + 2^(mem-level+9) octets per stream, ~256 KB at the
  defaults)."
  [{:keys [version deflate-init]} level window-bits mem-level]
  (let [strm (ffi/alloc ZS)]
    (dotimes [i ZS] (ffi/write strm :uint8 0 i))
    (when-not (zero? (deflate-init strm level 8 (+ 16 window-bits) mem-level 0 (version) ZS))
      (ffi/free strm)
      (throw (ex-info "zlib: deflateInit2 failed" {:level level})))
    strm))

(defn- pump!
  "Feed n octets already in src-buf through deflate with flush mode `flush`,
  draining every octet it produces through out-buf. Returns the output as a
  byte array. A sync flush is done when deflate leaves output space unused; a
  finish when it reports the stream end."
  [{:keys [deflate]} strm src-buf n out-buf flush]
  (ffi/write strm :pointer src-buf O-next-in)
  (ffi/write strm :uint n O-avail-in)
  (let [chunks (loop [acc []]
                 (ffi/write strm :pointer out-buf O-next-out)
                 (ffi/write strm :uint CHUNK O-avail-out)
                 (let [r        (deflate strm flush)
                       left     (ffi/read strm :uint O-avail-out)
                       produced (- CHUNK left)
                       acc      (if (pos? produced)
                                  (conj acc (ffi/read-array out-buf produced))
                                  acc)]
                   (cond
                     (= r Z-STREAM-END) acc
                     ;; Z_BUF_ERROR only means "no progress possible", which a
                     ;; flush with nothing pending can answer; anything else
                     ;; negative is a real failure
                     (and (neg? r) (not= r Z-BUF-ERROR))
                     (throw (ex-info "zlib: deflate failed" {:rc r}))
                     (and (= flush Z-SYNC-FLUSH) (pos? left)) acc
                     (= r Z-BUF-ERROR) acc
                     :else (recur acc))))]
    (if (= 1 (count chunks))
      (first chunks)
      (byte-array (mapcat seq chunks)))))

(defn gzip
  "Compress `src` (a byte array) into gzip format at `level` (1..9, default 6),
   or nil if zlib is unavailable or refuses the input — nil means \"send it
   uncompressed\"."
  ([^bytes src] (gzip src 6))
  ([^bytes src level]
   (when-let [zl @zlib]
     (let [n       (alength src)
           src-buf (ffi/alloc (max 1 n))
           out-buf (ffi/alloc CHUNK)
           strm    (try (init-stream! zl level 15 8) (catch Throwable _ nil))]
       (try
         (when strm
           (ffi/write-array src-buf src)
           (pump! zl strm src-buf n out-buf Z-FINISH))
         ;; a compressor that throws must not take the response with it: the
         ;; uncompressed body is still a correct answer
         (catch Throwable _ nil)
         (finally
           (when strm ((:deflate-end zl) strm) (ffi/free strm))
           (ffi/free src-buf)
           (ffi/free out-buf)))))))

;; --- streaming ---------------------------------------------------------------
;; One gzip member spread over many writes. Each chunk is deflated with
;; Z_SYNC_FLUSH, so everything written so far is decodable as soon as it
;; arrives (a server-sent event must not wait for the next one to be seen),
;; and the compressor keeps its window between chunks: a chunk that repeats
;; what came before, as consecutive renders of one page mostly do, costs a few
;; octets rather than its own size.

(defn deflater
  "A streaming gzip compressor, or nil when zlib is unavailable. Feed it with
  `deflate-chunk!`, end the member with `deflate-finish!`, and release it with
  `deflate-close!` (finish releases it too). Not safe for concurrent use: one
  stream, one writer. opts:

    :level        1..9 (default 6)
    :window-bits  9..15 (default 15) — how far back a match may reach
    :mem-level    1..9 (default 8)"
  ([] (deflater {}))
  ([{:keys [level window-bits mem-level] :or {level 6 window-bits 15 mem-level 8}}]
   (when-let [zl @zlib]
     (let [strm (init-stream! zl level window-bits mem-level)]
       {:zl      zl
        :strm    strm
        :out-buf (ffi/alloc CHUNK)
        ;; the input buffer grows to the largest chunk seen and is reused
        :in      (atom {:buf (ffi/alloc 4096) :cap 4096})
        :open    (atom true)}))))

(defn- stage-input!
  "Copy src into the deflater's input buffer, growing it when src is larger."
  [{:keys [in]} ^bytes src]
  (let [n (alength src)
        {:keys [buf cap]} @in]
    (if (<= n cap)
      (do (when (pos? n) (ffi/write-array buf src)) buf)
      (let [cap' (max n (* 2 cap))
            buf' (ffi/alloc cap')]
        (ffi/free buf)
        (reset! in {:buf buf' :cap cap'})
        (ffi/write-array buf' src)
        buf'))))

(defn deflate-close!
  "Release the deflater's native state without finishing the member.
  Idempotent."
  [{:keys [zl strm out-buf in open]}]
  (when (compare-and-set! open true false)
    ((:deflate-end zl) strm)
    (ffi/free strm)
    (ffi/free out-buf)
    (ffi/free (:buf @in)))
  nil)

(defn deflate-chunk!
  "Compress src (a byte array) as the next part of the stream, flushed so the
  receiver can decode everything up to here. Returns the compressed octets."
  ^bytes [{:keys [zl strm out-buf open] :as d} ^bytes src]
  (when-not @open
    (throw (ex-info "zlib: deflater is closed" {})))
  (pump! zl strm (stage-input! d src) (alength src) out-buf Z-SYNC-FLUSH))

(defn deflate-finish!
  "End the gzip member: returns the final octets (the last block and the
  trailer) and releases the deflater."
  ^bytes [{:keys [zl strm out-buf open] :as d}]
  (when-not @open
    (throw (ex-info "zlib: deflater is closed" {})))
  (try
    (pump! zl strm (:buf @(:in d)) 0 out-buf Z-FINISH)
    (finally (deflate-close! d))))
