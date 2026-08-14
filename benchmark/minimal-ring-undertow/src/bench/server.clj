(ns bench.server
  (:require [ring.adapter.undertow :refer [run-undertow]]
            [ring.adapter.undertow.response :as ures]
            [ring.websocket :as ws]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]])
  (:import [java.io OutputStream IOException]))

(def ^:const plaintext-body "Hello, World!")
(def ^:const json-body "{\"message\":\"Hello, World!\"}")

(defn- event-bytes [i]
  (.getBytes (format "id: %d\nevent: tick\ndata: {\"tick\":%d}\n\n" i i) "UTF-8"))

;; The adapter streams InputStream bodies with io/copy and never flushes, so
;; small events sit in Undertow's write buffer until it fills. Flushing after
;; each event is what makes SSE actually stream.
(defrecord SSEBody [interval-ms total]
  ures/RespondBody
  (respond [_ exchange]
    (if (.isInIoThread exchange)
      (.dispatch exchange ^Runnable (fn [] (let [body (->SSEBody interval-ms total)]
                                             (ures/respond body exchange))))
      (do
        (when-not (.isBlocking exchange) (.startBlocking exchange))
        (let [^OutputStream os (.getOutputStream exchange)]
          (try
            (dotimes [i total]
              (when (pos? i) (Thread/sleep interval-ms))
              (.write os (event-bytes i))
              (.flush os))
            (catch IOException _)
            (finally (.endExchange exchange))))))))

(defn- long-param
  [params k default]
  (or (some-> (get params k) parse-long) default))

(defn ws-handler
  [_request]
  {:ring.websocket/listener
   {:on-open    (fn [_socket])
    :on-message (fn [socket message] (ws/send socket message))
    :on-close   (fn [_socket _code _reason])
    :on-error   (fn [_socket _error])}})

(defn app-handler
  [{:keys [uri request-method query-params] :as _request}]
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
                   :body (->SSEBody (long-param query-params "interval" 1000)
                                    (long-param query-params "events" 10))}
    [:get "/ws"] (ws-handler _request)
    {:status 404
     :headers {"Content-Type" "text/plain"}
     :body "not found"}))

(def app
  (wrap-defaults app-handler api-defaults))

(defn -main
  [& _args]
  (let [port (Long/parseLong (or (System/getenv "PORT") "8080"))]
    (println "listening on 0.0.0.0:" port)
    (run-undertow app {:host "0.0.0.0" :port port})))
