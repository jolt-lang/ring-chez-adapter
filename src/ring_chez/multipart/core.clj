(ns ring-chez.multipart.core
  "High-level `multipart/form-data` parsing for ring-style request maps, and
  the whole public surface of the parser.

  `ring-chez.middleware.multipart` is the Ring-shaped wrapper over this, and is
  what an app normally wants. Reach for this namespace directly when the
  middleware's request shape isn't enough — to stream a body chunk by chunk, or
  to read a part's `:charset` / `:headerlist`.

  The parser is split by function:

  - `ring-chez.multipart.errors`  — error constructors and classification predicates
  - `ring-chez.multipart.bytes`   — low-level byte/ASCII/charset helpers
  - `ring-chez.multipart.headers` — RFC 7230/7578 header value parsing and quoting
  - `ring-chez.multipart.parser`  — incremental SansIO parser (make-parser/parse-chunk/...)
  - this namespace                — parse-multipart/parse-form-data/is-form-request?,
                                    re-exporting the public API of the four above

  Ported from [defnull/multipart](https://github.com/defnull/multipart) and
  Apache-2.0, unlike the rest of this repo — see LICENSE-multipart."
  (:require [clojure.string :as str]
            [ring-chez.multipart.bytes :as bytes]
            [ring-chez.multipart.headers :as headers]
            [ring-chez.multipart.parser :as parser]
            [ring-chez.multipart.errors :as errors]))

;; Re-exports, so requiring this namespace alone gives the whole parser API.
(def make-parser parser/make-parser)
(def close-parser parser/close-parser)
(def parse-chunk parser/parse-chunk)
(def parse-stream parser/parse-stream)
(def parse-content-disposition headers/parse-content-disposition)
(def parse-options-header headers/parse-options-header)
(def header-quote headers/header-quote)
(def header-unquote headers/header-unquote)
(def content-disposition-quote headers/content-disposition-quote)
(def content-disposition-unquote headers/content-disposition-unquote)
(def multipart-error? errors/multipart-error?)
(def parser-error? errors/parser-error?)
(def strict-parser-error? errors/strict-parser-error?)
(def parser-limit-reached? errors/parser-limit-reached?)
(def parser-state-error? errors/parser-state-error?)

;;; ---------------------------------------------------------------------------
;;; High-level API
;;; ---------------------------------------------------------------------------

(defn- assoc-conj [m k v]
  (assoc m k (if-let [cur (get m k)]
               (if (vector? cur) (conj cur v) [cur v])
               v)))

(defn- reduce-part-events
  "Fold parser events into part maps. flush-pending? closes a part still open
  when the events run out — which happens when the stream ended after that
  part's closing delimiter but before the two bytes that say whether another
  part follows. Its body is complete; only the terminator is missing. Strict
  parsing never reaches that case, because close-parser raises first."
  [events memory-limit flush-pending?]
  (loop [events (seq events)
         current nil
         parts []
         total 0]
    (if events
      (let [event (first events)]
        (case (first event)
          :segment (recur (next events) (assoc (second event) :bytes (byte-array 0)) parts total)
          :body (recur (next events)
                       (update current :bytes bytes/ba-concat (second event))
                       parts total)
          :end (do
                 (when (and memory-limit (> (+ total (alength (:bytes current))) memory-limit))
                   (throw (errors/parser-limit-reached "Memory limit reached")))
                 (recur (next events) nil
                        (conj parts current)
                        (+ total (alength (:bytes current)))))))
      (if (and flush-pending? current)
        (do (when (and memory-limit (> (+ total (alength (:bytes current))) memory-limit))
              (throw (errors/parser-limit-reached "Memory limit reached")))
            (conj parts current))
        parts))))

;; ring hands the request body as a java.io.InputStream, and this library exists
;; to be used by ring-core, so accept one wherever a body is taken. Wrapping it as
;; a read-fn rather than slurping it keeps the streaming path: the parser asks for
;; the chunk size it wants and the stream is read no further ahead than that.
(defn- stream->read-fn [in]
  (fn [size]
    (let [buf (byte-array size)
          n (.read in buf 0 size)]
      (cond
        (neg? n) nil
        (= n size) buf
        :else (bytes/ba-slice buf 0 n)))))

(defn- coerce-body [body]
  (if (instance? java.io.InputStream body) (stream->read-fn body) body))

;; The urlencoded branch wants the whole body at once, so a stream is drained.
(defn- urlencoded-body [body]
  (if (instance? java.io.InputStream body) (.readAllBytes body) body))

(defn parse-multipart
  "Parse a multipart/form-data body into a vector of part maps, each with
  :name, :filename, :content-type, :charset, :headerlist and :bytes keys.

  body is either a byte array holding the complete body, or a read-fn of
  a size argument returning byte arrays (nil at EOF) for streaming input.
  Options are the same as for `make-parser`, plus :memory-limit (nil =
  unlimited) capping the total payload bytes across all parts, and
  :check-complete (default true) requiring the closing --boundary-- delimiter.

  RFC 7578 §4.1 requires that delimiter, so a body without one is malformed and
  the default rejects it. Some servers and clients truncate it, and some parsers
  accept that; :check-complete false returns the parts that did arrive complete
  instead of raising."
  ([boundary body] (parse-multipart boundary body {}))
  ([boundary body {:keys [memory-limit check-complete] :or {check-complete true} :as opts}]
   (let [body (coerce-body body)
         p (apply parser/make-parser boundary (mapcat identity opts))]
     (if (fn? body)
       (reduce-part-events (parser/parse-stream p body) memory-limit (not check-complete))
       (let [[p events] (parser/parse-chunk p body)]
         (parser/close-parser p check-complete)
         (reduce-part-events events memory-limit (not check-complete)))))))

(defn is-form-request?
  "True if content-type represents a form request that can be parsed with
  `parse-form-data`."
  [content-type]
  (contains? #{"multipart/form-data" "application/x-www-form-urlencoded"
               "application/x-url-encoded"}
             (str/lower-case
               (str/trim (first (str/split (or content-type "") #";" 2))))))

(defn- percent-decode
  "Decode percent-encoded bytes to a string. '+' decodes to space; invalid
  escape sequences are passed through literally."
  [^bytes ba charset]
  (let [n (alength ba)
        hex-val (fn [b]
                  (let [c (int b)]
                    (cond (<= 48 c 57) (- c 48)
                          (<= 65 c 70) (- c 55)
                          (<= 97 c 102) (- c 87)
                          :else -1)))]
    (loop [i 0 out []]
      (if (>= i n)
        (bytes/ba->str (byte-array out) charset)
        (let [c (int (aget ba i))]
          (cond
            (= c 43)
            (recur (inc i) (conj out 32))

            (and (= c 37) (< (+ i 2) n)
                 (>= (hex-val (aget ba (inc i))) 0)
                 (>= (hex-val (aget ba (+ i 2))) 0))
            (recur (+ i 3)
                   (conj out (+ (* 16 (hex-val (aget ba (inc i))))
                                (hex-val (aget ba (+ i 2))))))

            :else
            (recur (inc i) (conj out c))))))))

(defn- split-on-first [s sep]
  (let [i (str/index-of s sep)]
    (if (nil? i) [s nil] [(subs s 0 i) (subs s (inc i))])))

(defn- parse-urlencoded [^bytes body charset opts]
  (let [mem-limit (or (:memory-limit opts) (* 1024 64 128))
        part-limit (or (:part-limit opts) 128)]
    (when (> (alength body) mem-limit)
      (throw (errors/parser-limit-reached "Memory limit exceeded")))
    (let [fields (str/split (bytes/ba->str body charset) #"&" (inc part-limit))]
      (when (> (count fields) part-limit)
        (throw (errors/parser-limit-reached "Memory limit exceeded")))
      (reduce
        (fn [params field]
          (let [[name value] (split-on-first field "=")]
            (if (str/blank? name)
              params
              (assoc-conj params
                          (percent-decode (bytes/str->ba name charset) charset)
                          (percent-decode (bytes/str->ba (or value "") charset) charset)))))
        {} fields))))

(defn parse-form-data
  "Parse form data from a ring-style request map (:headers with
  \"content-type\", :body with a byte array or read-fn) into
  {:params {...} :files {...}}.

  Text fields are decoded to strings; file uploads are returned as part
  maps (see `parse-multipart`) keyed by field name. Multiple values for
  the same name are collected into vectors.

  Options:
    :charset       default charset for headers and text fields (default \"utf-8\")
    :strict        raise on unusual or unsupported input
    :ignore-errors if true, suppress all exceptions and return empty
                   results; if false, never suppress; nil (default)
                   suppresses in non-strict mode
    plus the `parse-multipart` options."
  ([request] (parse-form-data request {}))
  ([request {:keys [charset strict ignore-errors] :or {charset "utf-8"} :as opts}]
   (let [content-type (or (get-in request [:headers "content-type"])
                          (get request :content-type))]
     (try
       (if (nil? content-type)
         (if strict
           (throw (errors/strict-parser-error "Missing Content-Type header"))
           {:params {} :files {}})
         (let [[ctype options] (headers/parse-options-header content-type)
               charset (or (get options "charset") charset)]
           (when-not (bytes/is-valid-charset? charset)
             (throw (errors/parser-error (str "Invalid charset: " (pr-str charset)))))
           (cond
             (= ctype "multipart/form-data")
             (let [boundary (get options "boundary")]
               (when (str/blank? boundary)
                 (throw (errors/parser-error "Missing boundary for multipart/form-data")))
               (reduce
                 (fn [acc part]
                   (if (:filename part)
                     (update acc :files assoc-conj (:name part) part)
                     (update acc :params assoc-conj (:name part)
                             (bytes/ba->str (:bytes part) (or (:charset part) charset)))))
                 {:params {} :files {}}
                 (parse-multipart boundary (:body request) (dissoc opts :charset))))

             (contains? #{"application/x-www-form-urlencoded" "application/x-url-encoded"} ctype)
             {:params (parse-urlencoded (urlencoded-body (:body request)) charset opts) :files {}}

             strict
             (throw (errors/strict-parser-error "Unsupported Content-Type"))

             :else
             {:params {} :files {}})))
       (catch Throwable e
         (if (errors/multipart-error? e)
           (let [ignore (if (nil? ignore-errors) (not strict) ignore-errors)]
             (if ignore {:params {} :files {}} (throw e)))
           (throw e)))))))
