(ns ring-chez.bench
  "Benchmark server mirroring minimal-ring-undertow's endpoints so the two
  adapters measure the same handler work under ab.

  Run: jolt -M:bench   (PORT=8081 STRATEGY=threads|fibers to override)

  HANDLER_TIMEOUT_MS overrides :handler-timeout-ms, which is what the
  threads strategy's per-request thread handoff costs: 0 runs the handler
  inline, any positive value runs it under a deadline."
  (:require [ring-chez.adapter :as adapter]))

(defn app [request]
  (case [(:request-method request) (:uri request)]
    [:get "/plaintext"] {:status 200
                         :headers {"Content-Type" "text/plain"}
                         :body "Hello, World!"}
    [:get "/json"] {:status 200
                    :headers {"Content-Type" "application/json"}
                    :body "{\"message\":\"Hello, World!\"}"}
    {:status 404
     :headers {"Content-Type" "text/plain"}
     :body "not found"}))

(defn -main [& _]
  (let [port (Long/parseLong (or (System/getenv "PORT") "8081"))
        strategy (keyword (or (System/getenv "STRATEGY") "threads"))
        workers (when-let [w (some-> (System/getenv "WORKERS") not-empty)]
                  (Long/parseLong w))
        handler-timeout (when-let [t (some-> (System/getenv "HANDLER_TIMEOUT_MS") not-empty)]
                          (Long/parseLong t))]
    (println "listening on 127.0.0.1:" port "strategy:" strategy "workers:" workers
             "handler-timeout-ms:" handler-timeout)
    (adapter/run-server app (merge {:port port :strategy strategy}
                                   (when workers {:worker-threads workers})
                                   (when handler-timeout
                                     {:handler-timeout-ms handler-timeout})))
    ;; -main returning exits the process; park forever
    @(promise)))
