(ns ring-chez.zstd
  "zstd compression (RFC 8878, `Content-Encoding: zstd`) over the system
   libzstd, bound through `jolt.ffi`.

   Why it is here beside gzip: deflate can only look 32 KB back and match 258
   octets at a time, so on a stream it barely helps once a render outgrows
   that. zstd keeps a window as large as you give it; a page re-rendered over
   one event stream costs a few dozen octets per render instead of its own
   compressed size.

   Same contract as `ring-chez.zlib`: `available?` says whether libzstd was
   found, and `compress` returns nil when it was not, which callers read as
   \"send this uncompressed\".

   Window: RFC 9659 caps what an HTTP encoder may use at 8 MB (window-log 23),
   since browsers refuse anything larger. For a stream the window wants to be
   3-4x a typical uncompressed message, so repeats across messages are found.
   The default, 2^19 = 512 KB, covers renders up to ~150 KB."
  (:require [jolt.ffi :as ffi]))

(def default-level
  "1: fast, and on a stream the window does most of the work anyway. zstd
  accepts negative levels (faster still) down to its own minimum."
  1)

(def default-window-log
  "2^19 = 512 KB. A stream's compressor holds ~1.4 MB of native memory at
  this window and level 1."
  19)

(def ^:private max-level 19)        ; 20+ are \"ultra\" levels needing > 8 MB windows
(def ^:private min-window-log 10)
(def ^:private max-window-log 23)   ; RFC 9659: 8 MB is the HTTP limit

;; enum values from zstd.h (stable API)
(def ^:private C-LEVEL 100)
(def ^:private C-WINDOW-LOG 101)
(def ^:private D-WINDOW-LOG-MAX 100)
(def ^:private E-FLUSH 1)
(def ^:private E-END 2)

;; ZSTD_inBuffer / ZSTD_outBuffer: {ptr; size_t size; size_t pos}
(def ^:private BUF 24)
(def ^:private CHUNK 65536)

(defn- bind-zstd []
  {:min-level   (ffi/foreign-fn "ZSTD_minCLevel" [] :int)
   :is-error    (ffi/foreign-fn "ZSTD_isError" [:size_t] :uint)
   :error-name  (ffi/foreign-fn "ZSTD_getErrorName" [:size_t] :string)
   :bound       (ffi/foreign-fn "ZSTD_compressBound" [:size_t] :size_t)
   :create-c    (ffi/foreign-fn "ZSTD_createCCtx" [] :pointer)
   :free-c      (ffi/foreign-fn "ZSTD_freeCCtx" [:pointer] :size_t)
   :set-c       (ffi/foreign-fn "ZSTD_CCtx_setParameter" [:pointer :int :int] :size_t)
   :compress2   (ffi/foreign-fn "ZSTD_compress2" [:pointer :pointer :size_t :pointer :size_t] :size_t)
   :stream2     (ffi/foreign-fn "ZSTD_compressStream2" [:pointer :pointer :pointer :int] :size_t)
   :create-d    (ffi/foreign-fn "ZSTD_createDCtx" [] :pointer)
   :free-d      (ffi/foreign-fn "ZSTD_freeDCtx" [:pointer] :size_t)
   :set-d       (ffi/foreign-fn "ZSTD_DCtx_setParameter" [:pointer :int :int] :size_t)
   :dstream     (ffi/foreign-fn "ZSTD_decompressStream" [:pointer :pointer :pointer] :size_t)})

;; The process first (a jolt that loaded the declared :jolt/native, or one
;; linked against libzstd), then the usual install locations. dlopen on macOS
;; does not search Homebrew's prefix, hence the absolute paths. Failure means
;; no zstd, never an error.
(def ^:private zstd
  (delay
    (or (try (bind-zstd) (catch Throwable _ nil))
        (some (fn [lib]
                (try (ffi/load-library lib) (bind-zstd) (catch Throwable _ nil)))
              ["libzstd.so.1" "libzstd.so"
               "libzstd.1.dylib" "/opt/homebrew/lib/libzstd.1.dylib"
               "/usr/local/lib/libzstd.1.dylib" "/opt/local/lib/libzstd.1.dylib"
               "libzstd.dll" "zstd.dll"]))))

(defn available?
  "True when libzstd could be bound. False means `compress` returns nil and
  `compressor` returns nil."
  []
  (some? @zstd))

(defn- check! [{:keys [is-error error-name]} what rc]
  (if (zero? (is-error rc))
    rc
    (throw (ex-info (str "zstd: " what " failed: " (error-name rc)) {:rc rc}))))

(defn- validate! [{:keys [min-level]} level window-log]
  (when-not (and (int? level) (<= (min-level) level max-level))
    (throw (ex-info (str "zstd: level must be " (min-level) ".." max-level) {:level level})))
  (when-not (and (int? window-log) (<= min-window-log window-log max-window-log))
    (throw (ex-info (str "zstd: window-log must be " min-window-log ".." max-window-log
                         " (HTTP decoders accept at most 8 MB)")
                    {:window-log window-log}))))

(defn- make-cctx [{:keys [create-c free-c set-c] :as z} level window-log]
  (validate! z level window-log)
  (let [cctx (create-c)]
    (when (ffi/null? cctx) (throw (ex-info "zstd: ZSTD_createCCtx failed" {})))
    (try
      (check! z "set level" (set-c cctx C-LEVEL level))
      (check! z "set window" (set-c cctx C-WINDOW-LOG window-log))
      cctx
      (catch Throwable t (free-c cctx) (throw t)))))

(defn compress
  "Compress `src` (a byte array) as one zstd frame, or nil when libzstd is
  unavailable or the compressor fails — nil means \"send it uncompressed\".
  opts: :level, :window-log (the frame's window shrinks to the input anyway)."
  ([^bytes src] (compress src {}))
  ([^bytes src {:keys [level window-log]
                :or {level default-level window-log default-window-log}}]
   (when-let [{:keys [bound compress2 free-c] :as z} @zstd]
     (let [n    (alength src)
           cap  (bound n)
           sbuf (ffi/alloc (max 1 n))
           dbuf (ffi/alloc cap)
           cctx (try (make-cctx z level window-log) (catch Throwable _ nil))]
       (try
         (when cctx
           (when (pos? n) (ffi/write-array sbuf src))
           (let [m (check! z "compress" (compress2 cctx dbuf cap sbuf n))]
             (ffi/read-array dbuf m)))
         (catch Throwable _ nil)
         (finally
           (when cctx (free-c cctx))
           (ffi/free sbuf)
           (ffi/free dbuf)))))))

;; --- streaming ---------------------------------------------------------------
;; One frame spread over many writes, each flushed (ZSTD_e_flush) so it can be
;; decoded on arrival; the window carries across them.

(defn compressor
  "A streaming zstd compressor, or nil when libzstd is unavailable. Feed it
  with `compress-chunk!`, end the frame with `compress-finish!`, release it
  with `compress-close!` (finish releases it too). One stream, one writer.
  Throws on an invalid option. opts:

    :level       zstd level, negative (faster) up to 19 (default 1)
    :window-log  10..23, how far back a match may reach as a power of two
                 (default 19 = 512 KB; aim for 3-4x a typical message)"
  ([] (compressor {}))
  ([{:keys [level window-log] :or {level default-level window-log default-window-log}}]
   (when-let [z @zstd]
     {:z      z
      :cctx   (make-cctx z level window-log)
      :inb    (ffi/alloc BUF)
      :outb   (ffi/alloc BUF)
      :out    (ffi/alloc CHUNK)
      :in     (atom {:buf (ffi/alloc 4096) :cap 4096})
      :open   (atom true)})))

(defn- stage-input! [{:keys [in]} ^bytes src]
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

(defn- pump!
  "Run n staged octets through ZSTD_compressStream2 with end-op `mode` until
  the input is consumed and everything is flushed. Returns the output."
  [{:keys [z cctx inb outb out]} src-buf n mode]
  (ffi/write inb :pointer src-buf 0)
  (ffi/write inb :size_t n 8)
  (ffi/write inb :size_t 0 16)
  (let [chunks (loop [acc []]
                 (ffi/write outb :pointer out 0)
                 (ffi/write outb :size_t CHUNK 8)
                 (ffi/write outb :size_t 0 16)
                 (let [left     (check! z "compressStream2" ((:stream2 z) cctx outb inb mode))
                       produced (ffi/read outb :size_t 16)
                       acc      (if (pos? produced) (conj acc (ffi/read-array out produced)) acc)]
                   ;; 0 = flushed completely (input is then consumed too)
                   (if (zero? left) acc (recur acc))))]
    (if (= 1 (count chunks))
      (first chunks)
      (byte-array (mapcat seq chunks)))))

(defn compress-close!
  "Release the compressor without ending the frame. Idempotent."
  [{:keys [z cctx inb outb out in open]}]
  (when (compare-and-set! open true false)
    ((:free-c z) cctx)
    (ffi/free inb)
    (ffi/free outb)
    (ffi/free out)
    (ffi/free (:buf @in)))
  nil)

(defn compress-chunk!
  "Compress src (a byte array) as the next part of the frame, flushed so the
  receiver can decode everything up to here. Returns the compressed octets."
  ^bytes [{:keys [open] :as c} ^bytes src]
  (when-not @open (throw (ex-info "zstd: compressor is closed" {})))
  (pump! c (stage-input! c src) (alength src) E-FLUSH))

(defn compress-finish!
  "End the frame: returns the final octets and releases the compressor."
  ^bytes [{:keys [open in] :as c}]
  (when-not @open (throw (ex-info "zstd: compressor is closed" {})))
  (try
    (pump! c (:buf @in) 0 E-END)
    (finally (compress-close! c))))

;; --- decoding ----------------------------------------------------------------

(defn decompress
  "Decode `src` (one or more zstd frames, or a prefix of a flushed stream) and
  return every octet it yields. Windows over 8 MB are refused, as an HTTP
  client would. Throws when libzstd is unavailable or the data is corrupt."
  ^bytes [^bytes src]
  (let [{:keys [create-d free-d set-d dstream] :as z}
        (or @zstd (throw (ex-info "zstd: libzstd unavailable" {})))
        n    (alength src)
        sbuf (ffi/alloc (max 1 n))
        inb  (ffi/alloc BUF)
        outb (ffi/alloc BUF)
        out  (ffi/alloc CHUNK)
        dctx (create-d)]
    (try
      (check! z "set window max" (set-d dctx D-WINDOW-LOG-MAX max-window-log))
      (when (pos? n) (ffi/write-array sbuf src))
      (ffi/write inb :pointer sbuf 0)
      (ffi/write inb :size_t n 8)
      (ffi/write inb :size_t 0 16)
      (let [chunks (loop [acc []]
                     (ffi/write outb :pointer out 0)
                     (ffi/write outb :size_t CHUNK 8)
                     (ffi/write outb :size_t 0 16)
                     (check! z "decompressStream" (dstream dctx outb inb))
                     (let [produced (ffi/read outb :size_t 16)
                           acc      (if (pos? produced) (conj acc (ffi/read-array out produced)) acc)]
                       ;; a full output buffer may mean more is held back
                       (if (or (< (ffi/read inb :size_t 16) n) (= produced CHUNK))
                         (recur acc)
                         acc)))]
        (byte-array (mapcat seq chunks)))
      (finally
        (free-d dctx)
        (ffi/free sbuf) (ffi/free inb) (ffi/free outb) (ffi/free out)))))
