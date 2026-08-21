(ns ring-chez.middleware.multipart
  "`multipart/form-data` request parsing, in Ring's shape.

   Ring's own `ring.middleware.multipart-params` is written against Apache
   commons-fileupload, which cannot load under jolt, so a Ring stack here has
   no way to accept a file upload. This is the same middleware over
   [jolt-lang/multipart](https://github.com/jolt-lang/multipart), an RFC 7578
   parser in pure Clojure:

       (require '[ring-chez.middleware.multipart :as multipart])
       (def app (multipart/wrap-multipart-params handler))

   `:multipart-params` is added to the request, and merged into `:params`, as
   Ring specifies. A text field's value is a string; an upload is

       {:filename \"avatar.png\" :content-type \"image/png\"
        :bytes #object[\"[B\"] :size 20481}

   which is `ring.middleware.multipart-params.byte-array/byte-array-store`'s
   shape plus `:size`. There is no temp-file store: the parser buffers in
   memory, so bound uploads with the adapter's `:max-request-bytes` rather
   than assuming a large one will spool to disk.

   Repeated field names collect into a vector, as in Ring.

   A request that is not `multipart/form-data` passes through untouched —
   including its `:body`, which this reads only when it is going to parse it."
  (:require [clojure.string :as str]
            [multipart.core :as multipart]))

(defn- multipart-form?
  "Ring's own test: the content type names multipart/form-data. Parameters
  (`; boundary=...`) follow, so this is a prefix match on the media type."
  [request]
  (some-> (or (get-in request [:headers "content-type"])
              (:content-type request))
          (str/lower-case)
          (str/starts-with? "multipart/form-data")))

(defn- ring-file
  "One parsed part in the shape Ring's byte-array store produces. The parser's
  own extras (:name, :headerlist, :charset) are dropped: a middleware's job
  here is to speak Ring, and code that wants the parser's richer part map can
  call the parser."
  [{:keys [filename content-type ^bytes bytes]}]
  {:filename     filename
   :content-type content-type
   :bytes        bytes
   :size         (if bytes (alength bytes) 0)})

(defn- assoc-conj
  "Ring's assoc-conj: a repeated name collects into a vector rather than
  overwriting (ring.util.codec)."
  [m k v]
  (assoc m k (if-let [cur (get m k)]
               (if (vector? cur) (conj cur v) [cur v])
               v)))

(defn- one-or-many [f v]
  (if (vector? v) (mapv f v) (f v)))

(defn multipart-params-request
  "Add `:multipart-params` and `:params` to a multipart request; return any
   other request unchanged. opts are passed to `multipart.core/parse-form-data`
   (`:charset`, `:strict`, `:max-segment-size`, `:memory-limit`, …)."
  ([request] (multipart-params-request request {}))
  ([request opts]
   (if-not (multipart-form? request)
     request
     (let [{:keys [params files]} (multipart/parse-form-data request opts)
           ;; params and files are keyed the same way, so a form with a text
           ;; field and an upload under one name has to conj rather than let
           ;; merge drop one of them
           all (reduce-kv (fn [m k v] (assoc-conj m k (one-or-many ring-file v)))
                          (or params {})
                          (or files {}))]
       (merge-with merge request
                   {:multipart-params all}
                   {:params all})))))

(defn wrap-multipart-params
  "Middleware parsing `multipart/form-data` bodies into `:multipart-params`,
   merged into `:params`. See the namespace docstring for the shape and
   `multipart.core/parse-form-data` for opts.

   Supports Ring's three-arity async handlers as well as the synchronous
   two-arity ones, though this adapter only calls the synchronous shape."
  ([handler] (wrap-multipart-params handler {}))
  ([handler opts]
   (fn
     ([request] (handler (multipart-params-request request opts)))
     ([request respond raise]
      (handler (multipart-params-request request opts) respond raise)))))
