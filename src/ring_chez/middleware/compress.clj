(ns ring-chez.middleware.compress
  "Response compression with zstd and gzip, negotiated per request.

       (require '[ring-chez.middleware.compress :as compress])
       (def app (compress/wrap-compress handler))
       (def app (compress/wrap-compress handler {:zstd {:level -1 :window-log 18}}))

   The coding is the one the client weighs highest among those the server
   offers and can load (`:encodings`, default zstd then gzip); on a tie the
   server's order wins, so a browser sending `gzip, deflate, br, zstd` gets
   zstd. A coding whose library is missing is skipped, and with none left the
   response goes out as it is.

   Everything else is `ring-chez.middleware.gzip`'s policy: `:min-size`,
   compressible content types, a handler's own Content-Encoding is left
   alone, `Vary: Accept-Encoding`, a distinct ETag (`-zst` / `-gz`), and a
   `core.async` channel body compressed as one stream, each chunk flushed so
   it decodes on arrival. On a stream zstd is the one to want: its window
   (512 KB by default) reaches back across whole messages, where deflate's
   32 KB does not, so a re-rendered page costs a few dozen octets."
  (:require [clojure.core.async :as async]
            [clojure.string :as str]
            [ring-chez.zlib :as zlib]
            [ring-chez.zstd :as zstd]))

(def default-min-size
  "Below this the framing costs more than the compression saves (Igropyr
  gzip-min-size)."
  1024)

(def default-compressible-types
  "Content-type prefixes worth compressing. Anything already compressed —
  images, video, zip, gzip — only gets bigger and costs CPU for it."
  ["text/" "application/json" "application/javascript" "application/xml"
   "application/x-javascript" "application/edn" "application/ld+json"
   "image/svg+xml"])

(def default-encodings
  "Server preference, best first."
  ["zstd" "gzip"])

;; --- codings ------------------------------------------------------------------
;; :whole compresses a byte array (nil = leave it), :stream makes a
;; {:chunk :finish :close} encoder (nil = leave it).

(defn- gzip-opts [{:keys [level window-bits mem-level] :or {level 6}}]
  (cond-> {:level level}
    window-bits (assoc :window-bits window-bits)
    mem-level   (assoc :mem-level mem-level)))

(def ^:private codings
  {"zstd" {:available? zstd/available?
           :etag       "-zst"
           :whole      (fn [bs opts] (zstd/compress bs (:zstd opts {})))
           :stream     (fn [opts]
                         (when-let [c (zstd/compressor (:zstd opts {}))]
                           {:chunk  #(zstd/compress-chunk! c %)
                            :finish #(zstd/compress-finish! c)
                            :close  #(zstd/compress-close! c)}))}
   "gzip" {:available? zlib/available?
           :etag       "-gz"
           :whole      (fn [bs opts] (zlib/gzip bs (:level (gzip-opts opts))))
           :stream     (fn [opts]
                         (when-let [d (zlib/deflater (gzip-opts opts))]
                           {:chunk  #(zlib/deflate-chunk! d %)
                            :finish #(zlib/deflate-finish! d)
                            :close  #(zlib/deflate-close! d)}))}})

(defn- coding-name [c]
  (let [n (str/lower-case (name c))]
    (if (= n "x-gzip") "gzip" n)))

;; --- negotiation --------------------------------------------------------------

(defn- q-value
  "The q of one Accept-Encoding entry's parameters: 1 when absent, and 1 when
  unparseable (a client that garbled its q did not mean to refuse)."
  [params]
  (or (some (fn [p]
              (let [p (str/trim p)]
                (when (str/starts-with? (str/lower-case p) "q=")
                  (try (Double/parseDouble (str/trim (subs p 2)))
                       (catch Throwable _ 1.0)))))
            params)
      1.0))

(defn accepted-codings
  "Accept-Encoding parsed to {coding q}, `*` included; nil for no header.
  RFC 9110: q=0 means NOT acceptable."
  [accept-encoding]
  (when accept-encoding
    (reduce (fn [m entry]
              (let [parts (str/split entry #";")
                    nm    (str/trim (or (first parts) ""))]
                (if (str/blank? nm)
                  m
                  (assoc m (coding-name nm) (q-value (rest parts))))))
            {}
            (str/split (str accept-encoding) #","))))

(defn choose-coding
  "The coding to use for this Accept-Encoding, from `offered` (server
  preference, best first), or nil. An entry naming the coding beats the
  wildcard; the highest q wins and ties go to the server's order."
  [accept-encoding offered]
  (when-let [acc (accepted-codings accept-encoding)]
    (let [scored (keep (fn [c]
                         (let [q (get acc c (get acc "*"))]
                           (when (and q (pos? q)) [c q])))
                       offered)]
      (when (seq scored)
        (first (reduce (fn [[_ bq :as best] [_ q :as cand]] (if (> q bq) cand best))
                       scored))))))

;; --- response rewriting --------------------------------------------------------

(defn- header
  "Case-insensitive header lookup: a handler may have written any casing."
  [headers name]
  (some (fn [[k v]] (when (.equalsIgnoreCase ^String (str k) name) v)) headers))

(defn- compressible-type? [types content-type]
  (let [ct (str/lower-case (str (or content-type "")))]
    (boolean (some #(str/starts-with? ct %) types))))

(defn- body->bytes
  "The body's octets, or nil for a body whose octets are not in memory (a
  File, an InputStream, a channel) — those pass through uncompressed."
  [body]
  (cond
    (string? body) (.getBytes ^String body "UTF-8")
    (bytes? body)  body
    (or (seq? body) (vector? body))
    (let [parts (map body->bytes body)]
      (when (every? some? parts)
        (let [total (reduce + 0 (map #(alength ^bytes %) parts))
              out   (byte-array total)]
          (loop [off 0 parts parts]
            (if-let [^bytes p (first parts)]
              (do (System/arraycopy p 0 out off (alength p))
                  (recur (+ off (alength p)) (rest parts)))
              out)))))
    :else nil))

(defn- encoded-etag
  "An encoded body is a different entity and must not share the plain one's
  validator, or a cache can serve one for the other (Igropyr gzip-etag)."
  [etag suffix]
  (when etag
    (if (str/ends-with? etag "\"")
      (str (subs etag 0 (dec (count etag))) suffix "\"")
      (str etag suffix))))

(defn- assoc-vary
  "Vary: Accept-Encoding, added to whatever the handler already varies on."
  [headers]
  (let [current (header headers "vary")]
    (cond
      (nil? current) (assoc headers "Vary" "Accept-Encoding")
      (some #(.equalsIgnoreCase ^String (str/trim %) "accept-encoding")
            (str/split current #",")) headers
      :else (assoc headers "Vary" (str current ", Accept-Encoding")))))

(defn- encoded-headers [headers coding]
  (-> headers
      (assoc "Content-Encoding" coding)
      assoc-vary
      (as-> h (if-let [e (header h "etag")]
                (assoc h "ETag" (encoded-etag e (get-in codings [coding :etag])))
                h))
      ;; the adapter frames from the octets it actually writes, so a stale
      ;; length cannot desynchronise the connection — but a wrong one in the
      ;; map would still be a lie to any middleware between here and it
      (dissoc "Content-Length" "content-length")))

(defn- chunk-bytes ^bytes [v]
  (if (string? v) (.getBytes ^String v "UTF-8") v))

(defn- encode-channel
  "A channel carrying src's chunks as one compressed stream, or nil when no
  encoder could be made. Each chunk is flushed on its own so it can be
  decoded on arrival. src closing (or an empty chunk, which the adapter reads
  as the end) finishes the stream. The adapter closes the returned channel
  when the client goes away; the next chunk then finds it closed and src is
  closed in turn, so the producer's following put returns false as it would
  have without us."
  [src {:keys [chunk finish close]}]
  (let [out (async/chan)]
    (async/go
      (try
        (loop []
          (let [v (async/<! src)
                ^bytes bs (some-> v chunk-bytes)]
            (if (or (nil? bs) (zero? (alength bs)))
              (do (async/>! out (finish))
                  (async/close! out))
              (if (async/>! out (chunk bs))
                (recur)
                (async/close! src)))))
        (catch Throwable _
          (async/close! out)
          (async/close! src))
        (finally (close))))
    out))

(defn- offered
  "The codings from opts that are known and loadable, in preference order."
  [{:keys [encodings] :or {encodings default-encodings}}]
  (filterv (fn [c] (when-let [{:keys [available?]} (codings c)] (available?)))
           (map coding-name encodings)))

(defn compress-response
  "Compress one response for one request, or return it unchanged. opts as in
  `wrap-compress`."
  [request response {:keys [min-size types stream?]
                     :or   {min-size default-min-size
                            types    default-compressible-types
                            stream?  true}
                     :as   opts}]
  (let [headers (:headers response)
        body    (:body response)
        coding  (when (and (map? response)
                           ;; a handler that encoded its own body owns that decision
                           (nil? (header headers "content-encoding"))
                           (compressible-type? types (header headers "content-type")))
                  (choose-coding (get-in request [:headers "accept-encoding"])
                                 (offered opts)))]
    (cond
      (nil? coding) response

      (async/chan? body)
      (if-let [enc (and stream?
                        (try ((get-in codings [coding :stream]) opts)
                             (catch Throwable _ nil)))]
        (assoc response :body (encode-channel body enc)
               :headers (encoded-headers headers coding))
        response)

      :else
      (let [^bytes raw (body->bytes body)]
        (if-not (and raw (> (alength raw) min-size))
          response
          (if-let [^bytes out ((get-in codings [coding :whole]) raw opts)]
            (assoc response :body out :headers (encoded-headers headers coding))
            response))))))

(defn wrap-compress
  "Middleware compressing responses with the best coding the client accepts.
  opts:

    :encodings  codings to offer, best first (default [\"zstd\" \"gzip\"])
    :min-size   smallest body worth compressing (default 1024)
    :types      content-type prefixes to compress
                (default `default-compressible-types`)
    :stream?    compress channel bodies as a flushed stream (default true)
    :zstd       {:level :window-log} — level from zstd's minimum (negative is
                faster) up to 19, default 1; window-log 10..23, default 19
                (512 KB). For streams aim the window at 3-4x a typical
                message so repeats across messages are found. Each open
                stream holds ~1.4 MB at the defaults.
    :level, :window-bits, :mem-level
                gzip's: zlib level 1..9 (default 6), window 9..15, memory 1..9

  An invalid :zstd setting throws here rather than on the first request.
  Place it outermost, so it sees the finished response."
  ([handler] (wrap-compress handler {}))
  ([handler opts]
   (when (and (some #{"zstd"} (offered opts)) (:zstd opts))
     (zstd/compress-close! (zstd/compressor (:zstd opts))))
   (fn
     ([request] (compress-response request (handler request) opts))
     ([request respond raise]
      (handler request #(respond (compress-response request % opts)) raise)))))
