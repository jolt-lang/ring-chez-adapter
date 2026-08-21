(ns ring-chez.middleware.gzip
  "gzip response compression.

   jolt has no `java.util.zip`, so the usual Ring gzip middleware cannot load
   here. This is Igropyr's policy (express.sc `finish!`) over
   `ring-chez.zlib`:

       (require '[ring-chez.middleware.gzip :as gzip])
       (def app (gzip/wrap-gzip handler))

   A response is compressed when the client accepts gzip, the body is over
   `:min-size` (default 1024 — below that the gzip header costs more than it
   saves), and the content type is worth compressing. `Content-Encoding: gzip`
   and `Vary: Accept-Encoding` are set, and an `ETag` is given a distinct
   value, since a gzipped body is a different entity.

   In-memory bodies only: a string, a byte array, or a seq/vector of those.
   A `File`, an `InputStream` or a `core.async` channel passes through
   uncompressed — compressing a stream means framing deflate output chunk by
   chunk, and the one case where it clearly pays, static files, is better
   served by caching the compressed copy (see `ring-chez.middleware.static`).

   Where zlib is unavailable the middleware is a no-op, which is what
   Igropyr's `gzip-compress` returning #f means: uncompressed is always a
   correct answer."
  (:require [clojure.string :as str]
            [ring-chez.zlib :as zlib]))

(def default-min-size
  "Below this the gzip header and trailer cost more than the compression
  saves (Igropyr gzip-min-size)."
  1024)

(def default-compressible-types
  "Content-type prefixes worth compressing. Anything already compressed —
  images, video, zip, gzip — only gets bigger and costs CPU for it."
  ["text/" "application/json" "application/javascript" "application/xml"
   "application/x-javascript" "application/edn" "application/ld+json"
   "image/svg+xml"])

(defn- q-zero?
  "true when the parameters of one Accept-Encoding entry say q=0. RFC 9110:
  q=0 means NOT acceptable, and clients send it precisely for codings they
  cannot decode."
  [params]
  (boolean
   (some (fn [p]
           (let [p (str/trim p)]
             (and (str/starts-with? (str/lower-case p) "q=")
                  (try (zero? (Double/parseDouble (str/trim (subs p 2))))
                       (catch Throwable _ false)))))
         params)))

(defn gzip-acceptable?
  "Does this Accept-Encoding value allow gzip? (Igropyr `gzip-acceptable?`.)

   Not a substring search: that would compress for a client that said
   `gzip;q=0` — which it sends precisely because it cannot decode it — and
   would fire on any unrelated coding containing those letters. The most
   specific verdict wins, so `*;q=0, gzip` still compresses, and the wildcard
   only decides when no entry names gzip."
  [accept-encoding]
  (when accept-encoding
    (loop [entries  (str/split (str accept-encoding) #",")
           explicit nil
           star     nil]
      (if-let [entry (first entries)]
        (let [parts (str/split entry #";")
              nm    (str/lower-case (str/trim (or (first parts) "")))
              ok    (not (q-zero? (rest parts)))]
          (cond
            (or (= nm "gzip") (= nm "x-gzip")) (recur (rest entries) ok star)
            (= nm "*")                         (recur (rest entries) explicit ok)
            :else                              (recur (rest entries) explicit star)))
        (if (nil? explicit) (true? star) explicit)))))

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

(defn- gzip-etag
  "A gzipped body is a different entity and must not share the plain one's
  validator, or a cache can serve one for the other (Igropyr gzip-etag)."
  [etag]
  (when etag
    (if (str/ends-with? etag "\"")
      (str (subs etag 0 (dec (count etag))) "-gz\"")
      (str etag "-gz"))))

(defn- assoc-vary
  "Vary: Accept-Encoding, added to whatever the handler already varies on."
  [headers]
  (let [current (header headers "vary")]
    (cond
      (nil? current) (assoc headers "Vary" "Accept-Encoding")
      (some #(.equalsIgnoreCase ^String (str/trim %) "accept-encoding")
            (str/split current #",")) headers
      :else (assoc headers "Vary" (str current ", Accept-Encoding")))))

(defn gzip-response
  "Compress one response for one request, or return it unchanged. opts as in
  `wrap-gzip`."
  [request response {:keys [min-size types level]
                     :or   {min-size default-min-size
                            types    default-compressible-types
                            level    6}}]
  (let [headers (:headers response)
        body    (:body response)]
    (if-not (and (map? response)
                 (gzip-acceptable? (get-in request [:headers "accept-encoding"]))
                 ;; a handler that encoded its own body owns that decision
                 (nil? (header headers "content-encoding"))
                 (compressible-type? types (header headers "content-type"))
                 (zlib/available?))
      response
      (let [^bytes raw (body->bytes body)]
        (if-not (and raw (> (alength raw) min-size))
          response
          (if-let [^bytes gz (zlib/gzip raw level)]
            (-> response
                (assoc :body gz)
                (assoc :headers (-> headers
                                    (assoc "Content-Encoding" "gzip")
                                    assoc-vary
                                    (as-> h (if-let [e (header h "etag")]
                                              (assoc h "ETag" (gzip-etag e))
                                              h))))
                ;; the adapter frames from the octets it actually writes, so a
                ;; stale length cannot desynchronise the connection — but a
                ;; wrong one in the map would still be a lie to any middleware
                ;; between here and it
                (update :headers dissoc "Content-Length" "content-length"))
            response))))))

(defn wrap-gzip
  "Middleware compressing responses the client will accept. opts:

     :min-size  smallest body worth compressing (default 1024)
     :types     content-type prefixes to compress (default
                `default-compressible-types`)
     :level     zlib compression level 1..9 (default 6)

   Place it outermost, so it sees the finished response."
  ([handler] (wrap-gzip handler {}))
  ([handler opts]
   (fn
     ([request] (gzip-response request (handler request) opts))
     ([request respond raise]
      (handler request #(respond (gzip-response request % opts)) raise)))))
