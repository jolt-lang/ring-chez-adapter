(ns ring-chez.multipart.core
  "Deprecated: the multipart parser lives in
  [jolt-lang/multipart](https://github.com/jolt-lang/multipart), which this
  library depends on. Require `multipart.core` instead; this namespace only
  re-exports its public API so existing requires keep working."
  (:require [multipart.core :as mp]))

(def make-parser mp/make-parser)
(def close-parser mp/close-parser)
(def parse-chunk mp/parse-chunk)
(def parse-stream mp/parse-stream)
(def parse-multipart mp/parse-multipart)
(def is-form-request? mp/is-form-request?)
(def parse-form-data mp/parse-form-data)
(def parse-content-disposition mp/parse-content-disposition)
(def parse-options-header mp/parse-options-header)
(def header-quote mp/header-quote)
(def header-unquote mp/header-unquote)
(def content-disposition-quote mp/content-disposition-quote)
(def content-disposition-unquote mp/content-disposition-unquote)
(def multipart-error? mp/multipart-error?)
(def parser-error? mp/parser-error?)
(def strict-parser-error? mp/strict-parser-error?)
(def parser-limit-reached? mp/parser-limit-reached?)
(def parser-state-error? mp/parser-state-error?)
