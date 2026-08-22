(ns ring-chez.multipart.parser
  "Incremental SansIO multipart parser (port of PushMultipartParser)."
  (:require [clojure.string :as str]
            [ring-chez.multipart.bytes :refer [ba= ba-concat ba-slice ba-eq-at? ba-index-of
                                               ba-rindex-of ba-starts-with? str->ba ba->str
                                               title-case is-valid-charset? utf8-valid?
                                               crlf cr-byte lf-byte dbl-dash]]
            [ring-chez.multipart.headers :refer [known-headers re-hname
                                                 parse-content-disposition
                                                 parse-options-header]]
            [ring-chez.multipart.errors :refer [parser-error strict-parser-error
                                                parser-limit-reached parser-state-error
                                                multipart-error?]]))
;;; ---------------------------------------------------------------------------
;;; Segment and parser state
;;; ---------------------------------------------------------------------------

(defn- make-segment
  "Build a segment map from a parsed header list."
  [headerlist]
  (reduce
    (fn [seg [name value]]
      (case name
        "Content-Disposition"
        (let [[d n f] (parse-content-disposition value)]
          (assoc seg :disposition d :name n :filename f))
        "Content-Type"
        (let [[ct args] (parse-options-header value)]
          (assoc seg :content-type ct :charset (get args "charset")))
        seg))
    {:headerlist headerlist
     :disposition nil :name nil :filename nil
     :content-type nil :charset nil
     :bytes-received 0 :complete false}
    headerlist))

(defn make-parser
  "Create a new incremental multipart parser.

  Options (keyword arguments):
    :content-length    expected input size in bytes, -1 (default) if unknown
    :max-header-size   max length of a single header line (default 4224)
    :max-header-count  max number of headers per segment (default 8)
    :max-segment-size  max size of a single segment body (nil = unlimited)
    :max-segment-count max number of segments (nil = unlimited)
    :header-charset    charset for header names and values (default \"utf-8\")
    :strict            enable additional format and sanity checks

  Returns a parser state map to be fed to `parse-chunk`."
  [boundary & {:keys [content-length max-header-size max-header-count
                      max-segment-size max-segment-count header-charset strict]
               :or {content-length -1
                    max-header-size 4224
                    max-header-count 8
                    header-charset "utf-8"
                    strict false}}]
  (let [boundary (if (string? boundary) (str->ba boundary "UTF-8") boundary)]
    (when (zero? (alength boundary)) (throw (parser-error "Empty boundary")))
    (when (ba-index-of boundary lf-byte 0)
      (throw (parser-error "Invalid characters in boundary")))
    (when (and strict (> (alength boundary) 1024))
      (throw (strict-parser-error "Boundary too long")))
    (when-not (is-valid-charset? header-charset)
      (throw (parser-error (str "Invalid charset: " (pr-str header-charset)))))
    {:boundary boundary
     :delimiter (ba-concat crlf dbl-dash boundary)
     :first-delim (ba-concat dbl-dash boundary)
     :content-length content-length
     :max-header-size max-header-size
     :max-header-count max-header-count
     :max-segment-size max-segment-size
     :max-segment-count max-segment-count
     :header-charset header-charset
     :strict strict
     :state :preamble
     :buffer (byte-array 0)
     :parsed 0
     :segment nil
     :segment-headerlist []
     :segment-limit -1
     :segment-count 0
     :error nil
     :closed false}))

(defn- on-segment-start [state]
  (let [count (inc (:segment-count state))]
    (when (and (:max-segment-count state) (> count (:max-segment-count state)))
      (throw (parser-limit-reached "Maximum segment count exceeded")))
    (assoc state :segment-count count :segment nil :segment-headerlist [] :segment-limit -1)))

(defn- on-segment-headerline [state line]
  (let [[line hlist]
        (if (and (pos? (alength line))
                 (let [b (int (aget line 0))] (or (= b 32) (= b 9))))
          (do
            (when (or (:strict state) (empty? (:segment-headerlist state)))
              (throw (strict-parser-error "Unexpected segment header continuation")))
            (let [[name value] (peek (:segment-headerlist state))
                  prev (str name ": " value)
                  hlist (pop (:segment-headerlist state))]
              [(ba-concat (str->ba prev (:header-charset state))
                          (str->ba " " (:header-charset state))
                          (str->ba (str/trim (ba->str line (:header-charset state)))
                                   (:header-charset state)))
               hlist]))
          [line (:segment-headerlist state)])]
    (when (> (alength line) (:max-header-size state))
      (throw (parser-limit-reached "Maximum segment header length exceeded")))
    (when (>= (count hlist) (:max-header-count state))
      (throw (parser-limit-reached "Maximum segment header count exceeded")))
    (when (and (contains? #{"utf8" "utf-8" "ascii" "us-ascii"}
                          (str/lower-case (:header-charset state)))
               (not (utf8-valid? line)))
      (throw (parser-error "Segment header failed to decode")))
    (let [line-str (try
                     (ba->str line (:header-charset state))
                     (catch Throwable _
                       (throw (parser-error "Segment header failed to decode"))))
          i (str/index-of line-str ":")]
      (when (nil? i)
        (throw (parser-error "Malformed segment header")))
      (let [name (subs line-str 0 i)
            value (str/trim (subs line-str (inc i)))
            name (if (contains? known-headers name)
                   name
                   (let [name (title-case (str/trim name))]
                     (when (and (not (contains? known-headers name))
                                (not (re-matches re-hname name)))
                       (throw (parser-error "Invalid segment header name")))
                     name))]
        (if (= name "Content-Length")
          (let [cl (try (parse-long value) (catch Throwable _ nil))]
            (when (or (nil? cl) (neg? cl) (not= (str cl) value))
              (throw (parser-error "Invalid segment Content-Length header value")))
            (when (>= (:segment-limit state) 0)
              (throw (parser-error "Multiple segment Content-Length headers")))
            (when (and (:max-segment-size state) (> cl (:max-segment-size state)))
              (throw (parser-limit-reached
                      "Segment Content-Length larger than maximum segment size")))
            (assoc state :segment-limit cl))
          (assoc state :segment-headerlist
                 (conj hlist [name value])))))))

(defn- create-segment [state]
  (let [segment (make-segment (:segment-headerlist state))]
    (when-not (= "form-data" (:disposition segment))
      (if (nil? (:disposition segment))
        (throw (parser-error "Missing Content-Disposition segment header"))
        (throw (parser-error "Invalid Content-Disposition segment header: Wrong type"))))
    (if (nil? (:name segment))
      (do
        (when (:strict state)
          (throw (strict-parser-error
                  "Invalid Content-Disposition segment header: Missing name option")))
        [(assoc segment :name "") state])
      [segment state])))

(defn- on-segment-payload [state chunk]
  (let [segment (:segment state)
        received (+ (:bytes-received segment) (alength chunk))]
    (when (and (:max-segment-size state) (> received (:max-segment-size state)))
      (throw (parser-limit-reached "Maximum segment size exceeded")))
    (when (and (>= (:segment-limit state) 0) (> received (:segment-limit state)))
      (throw (parser-error "Segment Content-Length exceeded")))
    (assoc state :segment (assoc segment :bytes-received received))))

(defn- on-segment-complete [state]
  (let [segment (:segment state)]
    (when (and (>= (:segment-limit state) 0)
               (< (:bytes-received segment) (:segment-limit state)))
      (throw (parser-error "Segment size does not match Content-Length header")))
    (assoc state :segment (assoc segment :complete true))))

;;; ---------------------------------------------------------------------------
;;; Incremental parser
;;; ---------------------------------------------------------------------------

(defn- parse-buffer
  "Process one accumulated buffer. Returns [state offset events]."
  [parser buffer]
  (let [bufferlen (alength buffer)
        delimiter (:delimiter parser)
        d-len (alength delimiter)]
    (loop [state parser offset 0 events []]
      (if (>= offset bufferlen)
        [state offset events]
        (case (:state state)
          :preamble
          (if (< bufferlen d-len)
            [state offset events]
            (let [idx (ba-index-of buffer (:first-delim state) offset)]
              (if idx
                (do
                  (when (and (pos? idx)
                             (not (ba= (ba-slice buffer (- idx 2) idx) crlf)))
                    (throw (parser-error "Unexpected byte in front of first boundary")))
                  (let [next-start (+ idx d-len)
                        tail (ba-slice buffer (- next-start 2) next-start)]
                    (cond
                      (ba= tail crlf)
                      (recur (assoc (on-segment-start state) :state :header)
                             next-start events)

                      (ba= tail dbl-dash)
                      [(assoc state :state :complete) next-start events]

                      (and (pos? (alength tail)) (= (int (aget tail 0)) 10))
                      (throw (parser-error "Invalid line break after first boundary"))

                      (<= next-start bufferlen)
                      (throw (parser-error "Unexpected byte after first boundary"))

                      :else
                      [state (max 0 (- idx 2)) events])))
                (do
                  (when (and (:strict state) (>= bufferlen d-len))
                    (throw (strict-parser-error "Boundary not found in first chunk")))
                  (let [i (ba-rindex-of buffer cr-byte (- bufferlen (dec d-len)))]
                    [state (if (nil? i) bufferlen i) events])))))

          :header
          (let [nl (ba-index-of buffer crlf offset)]
            (cond
              (and nl (> nl offset))
              (recur (on-segment-headerline state (ba-slice buffer offset nl))
                     (+ nl 2) events)

              (and nl (= nl offset))
              (let [[segment state] (create-segment state)]
                (recur (-> state (assoc :state :body) (assoc :segment segment))
                       (+ offset 2)
                       (conj events [:segment segment])))

              :else
              (do
                (when (ba-index-of buffer lf-byte offset)
                  (throw (parser-error "Invalid line break in segment header")))
                (when (> (- bufferlen offset) (:max-header-size state))
                  (throw (parser-limit-reached "Maximum segment header length exceeded")))
                [state offset events])))

          :body
          (let [idx (ba-index-of buffer delimiter offset)]
            (if idx
              (let [[state events] (if (> idx offset)
                                     (let [chunk (ba-slice buffer offset idx)]
                                       [(on-segment-payload state chunk)
                                        (conj events [:body chunk])])
                                     [state events])
                    next-start (+ idx d-len 2)
                    tail (ba-slice buffer (- next-start 2) next-start)]
                (cond
                  (ba= tail crlf)
                  (do (on-segment-complete state)
                      (recur (assoc (on-segment-start state) :state :header)
                             (+ idx d-len 2)
                             (conj events [:end])))

                  (ba= tail dbl-dash)
                  (do (on-segment-complete state)
                      [(assoc state :state :complete) (+ idx d-len 2)
                       (conj events [:end])])

                  (> next-start bufferlen)
                  [state idx events]

                  :else
                  (throw (parser-error "Unexpected bytes after boundary"))))
              ;; Boundary not found. Emit as much as possible, but keep any
              ;; bytes that may belong to a partial boundary at the end.
              (let [i (ba-rindex-of buffer cr-byte (max offset (- bufferlen (dec d-len))))]
                (cond
                  (or (nil? i) (not (ba-starts-with? delimiter (ba-slice buffer i (alength buffer)))))
                  (let [chunk (if (zero? offset) buffer (ba-slice buffer offset (alength buffer)))
                        state (on-segment-payload state chunk)]
                    [state bufferlen (conj events [:body chunk])])

                  (> i offset)
                  (let [chunk (ba-slice buffer offset i)
                        state (on-segment-payload state chunk)]
                    [state i (conj events [:body chunk])])

                  :else
                  [state offset events]))))

          :complete
          [state offset events])))))

(defn close-parser
  "Close the parser. If check-complete is true (default) and the parser has
  not reached the end of the multipart stream, a ParserError is raised."
  ([parser] (close-parser parser true))
  ([parser check-complete]
   (let [parser (assoc parser :closed true :buffer (byte-array 0))]
     (if (and check-complete (not= (:state parser) :complete))
       (throw (parser-error "Unexpected end of multipart stream (parser closed)"))
       parser))))

(defn parse-chunk
  "Feed a chunk of bytes to the parser and return [parser events].

  events is a vector of parser events: [:segment segment-map], [:body
  byte-array] and [:end]. For each part the parser emits one [:segment]
  event with the parsed headers, zero or more [:body] events with payload
  data, and one [:end] event.

  An empty chunk signals the end of input: the parser is closed, and a
  ParserError is raised if the multipart stream was incomplete."
  [parser chunk]
  (try
    (cond
      (zero? (alength chunk))
      (do (close-parser parser) [parser []])

      (:closed parser)
      (throw (parser-state-error "Parser closed"))

      :else
      (let [clen (:content-length parser)
            available (+ (:parsed parser) (alength (:buffer parser)) (alength chunk))]
        (when (and (>= clen 0) (< clen available))
          (throw (parser-error "Content-Length limit exceeded")))
        (if (= (:state parser) :complete)
          (do
            (when (:strict parser)
              (throw (strict-parser-error "Unexpected data after end of multipart stream")))
            [parser []])
          (let [buffer (ba-concat (:buffer parser) chunk)
                [state offset events] (parse-buffer parser buffer)]
            [(assoc state :parsed (+ (:parsed state) offset)
                    :buffer (ba-slice buffer offset (alength buffer)))
             events]))))
    (catch Throwable e
      (if (multipart-error? e)
        (do (close-parser (assoc parser :error e) false)
            (throw e))
        (throw e)))))

(defn- event-seq [parser read-fn chunk-size]
  (lazy-seq
    (if (:closed parser)
      nil
      (let [clen (:content-length parser)
            remaining (when (>= clen 0) (- clen (:parsed parser) (alength (:buffer parser))))]
        (if (and remaining (<= remaining 0))
          (do (close-parser parser) nil)
          (let [chunk (read-fn (if (and remaining (< remaining chunk-size)) remaining chunk-size))]
            (if (or (nil? chunk) (zero? (alength chunk)))
              (do (close-parser parser) nil)
              (let [[parser events] (parse-chunk parser chunk)]
                (concat events (event-seq parser read-fn chunk-size))))))))))

(defn parse-stream
  "Parse an entire multipart stream by reading chunks from read-fn, a
  function of a size argument that returns a byte array (or nil at EOF).
  Returns a lazy seq of parser events, closing the parser at end of input;
  an incomplete stream raises a ParserError."
  ([parser read-fn]
   (parse-stream parser read-fn 65536))
  ([parser read-fn chunk-size]
   (event-seq parser read-fn chunk-size)))
