(ns ring-chez.multipart.headers
  "RFC 7230/7578 header value parsing and quoting."
  (:require [clojure.string :as str]))

(def known-headers
  #{"Content-Disposition" "Content-Type" "Content-Length" "Content-Range"})
(def re-token #"[a-zA-Z0-9\-!#$%&'*+.^_`|~]+")
(def re-hname #"[a-zA-Z0-9\-_]+")
(def re-value #"(?:\"[^\\\"]*\"|[a-zA-Z0-9\-!#$%&'*+.^_`|~]+|\"(?:\\\\.|[^\\\"])*\")")
(def re-option #"; *([a-zA-Z0-9\-_]+) *= *(\"[^\\\"]*\"|[a-zA-Z0-9\-!#$%&'*+.^_`|~]+|\"(?:\\\\.|[^\\\"])*\")")
;;; ---------------------------------------------------------------------------
;;; Header parsing
;;; ---------------------------------------------------------------------------

(defn- rpartition-last [s sep]
  (let [i (str/last-index-of s sep)]
    (if (nil? i) s (subs s (inc i)))))

(defn header-quote
  "Quote header option values if necessary."
  [val]
  (if (re-matches re-token val)
    val
    (str "\"" (-> val (str/replace "\\" "\\\\") (str/replace "\"" "\\\"")) "\"")))

(defn header-unquote
  "Unquote header option values."
  ([val] (header-unquote val false))
  ([val filename]
   (if (and val (= \" (first val)) (= \" (last val)))
     (let [val (subs val 1 (dec (count val)))]
       (if (and filename
                (or (and (>= (count val) 3) (= (subs val 1 3) ":\\"))
                    (str/starts-with? val "\\\\")))
         (rpartition-last val "\\")
         (-> val (str/replace "\\\\" "\\") (str/replace "\\\"" "\""))))
     val)))

(defn content-disposition-quote
  "Quote field names or filenames for Content-Disposition headers the same
  way modern browsers do it (WHATWG HTML5 limited percent-encoding)."
  [val]
  (str "\"" (-> val (str/replace "\r" "%0D") (str/replace "\n" "%0A") (str/replace "\"" "%22")) "\""))

(defn content-disposition-unquote
  "Unquote field names or filenames from Content-Disposition headers.
  Detects legacy backslash-escaped quoted strings and modern (HTML5)
  limited percent-encoding. If filename is true, additional windows/ie6
  legacy workarounds are applied."
  ([val] (content-disposition-unquote val false))
  ([val filename]
   (let [val (cond
               (and val (= \" (first val)) (= \" (last val)))
               (let [val (subs val 1 (dec (count val)))]
                 (if (str/includes? val "\\\"")
                   (-> val (str/replace "\\\\" "\\") (str/replace "\\\"" "\""))
                   (if (str/includes? val "%")
                     (-> val (str/replace "%0D" "\r") (str/replace "%0A" "\n") (str/replace "%22" "\""))
                     val)))
               (str/includes? val "%")
               (-> val (str/replace "%0D" "\r") (str/replace "%0A" "\n") (str/replace "%22" "\""))
               :else val)]
     (if (and filename
              (or (and (>= (count val) 3) (= (subs val 1 3) ":\\"))
                  (str/starts-with? val "\\\\")))
       (rpartition-last val "\\")
       val))))

(defn parse-options-header
  "Parse Content-Type (or similar) headers into [primary-value options-map].
  Option values are unquoted with header-unquote by default; pass
  content-disposition-unquote for Content-Disposition headers."
  ([header] (parse-options-header header nil header-unquote))
  ([header options unquote]
   (let [i (str/index-of header ";")]
     (if (nil? i)
       [(str/lower-case (str/trim header)) (or options {})]
       [(str/lower-case (str/trim (subs header 0 i)))
        (reduce (fn [m [_ k v]]
                  (let [k (str/lower-case k)]
                    (assoc m k (unquote v (= k "filename")))))
                (or options {})
                (re-seq re-option (subs header i)))]))))

(defn- split-on-quote
  "Split s on the quote char, at most max-splits times, preserving trailing
  empty strings (Python str.split('\"', maxsplit) semantics)."
  [s max-splits]
  (loop [parts [] start 0 splits 0]
    (if (or (>= splits max-splits) (>= start (count s)))
      (conj parts (subs s start))
      (if-let [i (str/index-of s "\"" start)]
        (recur (conj parts (subs s start i)) (inc i) (inc splits))
        (conj parts (subs s start))))))

(defn- parse-cd-slow [value]
  (let [[dtype opts] (parse-options-header value nil content-disposition-unquote)]
    [dtype (get opts "name") (get opts "filename")]))

(defn parse-content-disposition
  "Parse a standard multipart Content-Disposition header value into
  [disposition name filename]. name and filename are nil when the
  corresponding header parameter is missing. Additional parameters are
  ignored."
  [value]
  (let [parts (split-on-quote value 4)]
    (if (and (= (nth parts 0) "form-data; name=") (= (peek parts) ""))
      (cond
        (= (count parts) 3)
        (let [name (nth parts 1)]
          ["form-data"
           (if (str/includes? name "%") (content-disposition-unquote name) name)
           nil])
        (and (= (count parts) 5) (= (nth parts 2) "; filename="))
        (let [name (nth parts 1)
              filename (nth parts 3)]
          ["form-data" name
           (if (or (str/includes? filename "%") (str/includes? filename "\\"))
             (content-disposition-unquote filename true)
             filename)])
        :else
        (parse-cd-slow value))
      (parse-cd-slow value))))

