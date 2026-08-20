(ns bench.server
  (:require [clojure.string :as str]
            [ring.adapter.jetty :refer [run-jetty]]
            [ring.core.protocols :as protocols]
            [ring.websocket :as ws])
  (:import [java.io OutputStream IOException]))

(def ^:const plaintext-body "Hello, World!")
(def ^:const json-body "{\"message\":\"Hello, World!\"}")

(defn- event-bytes [i]
  (.getBytes (format "id: %d\nevent: tick\ndata: {\"tick\":%d}\n\n" i i) "UTF-8"))

;; SSE over the portable Ring streaming protocol. The adapter hands the body an
;; OutputStream and does not flush, so each event is flushed here — without that
;; small events sit in Jetty's write buffer until it fills.
(defrecord SSEBody [interval-ms total]
  protocols/StreamableResponseBody
  (write-body-to-stream [_ _response out]
    (let [^OutputStream os out]
      (try
        (dotimes [i total]
          (when (pos? i) (Thread/sleep interval-ms))
          (.write os ^bytes (event-bytes i))
          (.flush os))
        (catch IOException _)
        (finally (.close os))))))

;; The benchmark runs no middleware (see benchmark/README.md), so the query
;; string is parsed here rather than by wrap-params.
(defn- query-param [query-string k]
  (when query-string
    (some (fn [pair]
            (let [[pk pv] (str/split pair #"=" 2)]
              (when (= pk k) pv)))
          (str/split query-string #"&"))))

(defn- long-param [query-string k default]
  (or (some-> (query-param query-string k) parse-long) default))

(defn ws-handler
  [_request]
  {:ring.websocket/listener
   {:on-open    (fn [_socket])
    :on-message (fn [socket message] (ws/send socket message))
    :on-close   (fn [_socket _code _reason])
    :on-error   (fn [_socket _error])}})

(defn app
  [{:keys [uri request-method query-string] :as request}]
  (case [request-method uri]
    [:get "/plaintext"] {:status 200
                         :headers {"Content-Type" "text/plain"}
                         :body plaintext-body}
    [:get "/json"] {:status 200
                    :headers {"Content-Type" "application/json"}
                    :body json-body}
    [:get "/sse"] {:status 200
                   :headers {"Content-Type" "text/event-stream"
                             "Cache-Control" "no-cache"}
                   :body (->SSEBody (long-param query-string "interval" 1000)
                                    (long-param query-string "events" 10))}
    [:get "/ws"] (ws-handler request)
    {:status 404
     :headers {"Content-Type" "text/plain"}
     :body "not found"}))

(defn -main
  [& _args]
  (let [port (Long/parseLong (or (System/getenv "PORT") "8082"))]
    (println "listening on 0.0.0.0:" port)
    (run-jetty app {:host "0.0.0.0" :port port :join? true})))
