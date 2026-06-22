(ns ring-chez.adapter-test
  (:require [ring-chez.adapter :as adapter]
            [jolt.http-client :as http]
            [clojure.string :as str]))

(def failures (atom 0))
(defn check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "— expected" (pr-str expected) "got" (pr-str actual)))))
(defn check-has [label needle haystack]
  (if (and (string? haystack) (str/includes? haystack needle))
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "— no" (pr-str needle) "in" (pr-str haystack)))))

(defn handler [{:keys [uri request-method query-string headers]}]
  (cond
    (= uri "/")     {:status 200 :headers {"Content-Type" "text/plain"}
                     :body (str "hello " (name request-method))}
    (= uri "/echo") {:status 200 :headers {"Content-Type" "text/plain"}
                     :body (str "q=" query-string " ua=" (get headers "user-agent" "?"))}
    :else           {:status 404 :headers {"Content-Type" "text/plain"} :body "not found"}))

(defn -main [& _]
  (println "ring adapter over jolt.ffi sockets")
  (let [server (adapter/run-server handler {:port 8399})]
    (Thread/sleep 250)
    (try
      (let [r    (http/get "http://127.0.0.1:8399/")
            ;; clj-http-lite throws on 4xx/5xx unless :throw-exceptions false
            r404 (http/get "http://127.0.0.1:8399/nope" {:throw-exceptions false})
            rq   (http/get "http://127.0.0.1:8399/echo?a=1&b=2")]
        (check "GET / status 200" 200 (:status r))
        (check-has "GET / body" "hello get" (:body r))
        (check-has "content-type header" "text/plain" (get (:headers r) "content-type" ""))
        (check "unknown route 404" 404 (:status r404))
        (check-has "query string reaches handler" "q=a=1&b=2" (:body rq)))
      (finally
        (adapter/stop-server server))))
  (println (str "\n" (if (zero? @failures) "all passed" (str @failures " FAILED"))))
  (when (pos? @failures) (throw (ex-info "test failures" {:n @failures}))))
