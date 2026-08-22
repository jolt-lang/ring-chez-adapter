(ns ring-chez.multipart.errors
  "Multipart parser error types and classification predicates."
)
;;; ---------------------------------------------------------------------------
;;; Errors
;;; ---------------------------------------------------------------------------

(defn make-error [type msg]
  (ex-info msg {:type type}))

(defn parser-error [msg] (make-error :multipart/parser-error msg))
(defn strict-parser-error [msg] (make-error :multipart/strict-parser-error msg))
(defn parser-limit-reached [msg] (make-error :multipart/limit-reached msg))
(defn parser-state-error [msg] (make-error :multipart/state-error msg))

(defn multipart-error?
  "True if the exception is a multipart parser error."
  [e]
  (boolean (some? (ex-data e))))

(defn parser-error?
  "True if the exception is a ParserError (invalid input)."
  [e]
  (= :multipart/parser-error (:type (ex-data e))))

(defn strict-parser-error?
  "True if the exception is a StrictParserError (unusual input)."
  [e]
  (= :multipart/strict-parser-error (:type (ex-data e))))

(defn parser-limit-reached?
  "True if the exception is a ParserLimitReached (one of the configured
  limits was exceeded)."
  [e]
  (= :multipart/limit-reached (:type (ex-data e))))

(defn parser-state-error?
  "True if the exception is a ParserStateError (parser used incorrectly)."
  [e]
  (= :multipart/state-error (:type (ex-data e))))
