(ns ring-janet.adapter-test
  (:require [ring-janet.adapter :as adapter]
            [clojure.string :as str]))

(def failures (atom 0))

(defn check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "— expected" (pr-str expected) "got" (pr-str actual)))))

(defn -main [& _]
  (println "ring-janet.adapter — live round trips")
  (let [seen (atom nil)
        handler (fn [req]
                  (reset! seen (dissoc req :body))
                  {:status 200
                   :headers {"X-From" "adapter-test"}
                   :body (str "m=" (name (:request-method req))
                              " uri=" (:uri req)
                              " q=" (:query-string req)
                              " body=" (some-> (:body req) slurp))})
        server (adapter/run-server handler {:port 8399})]
    (janet.ev/sleep 0.1)
    (let [r (janet.spork.http/request "GET" "http://127.0.0.1:8399/x/y?a=1&b=2")
          body (str (janet.spork.http/read-body r))]
      (check "status" 200 (janet/get r :status))
      (check "request map pieces" "m=get uri=/x/y q=a=1&b=2 body=" body)
      (check "response header" "adapter-test"
             (str (janet/get (janet/get r :headers) "x-from")))
      (check "ring SPEC keys present" true
             (every? #(contains? @seen %)
                     [:server-port :server-name :uri :query-string :scheme
                      :request-method :protocol :headers])))
    (let [r (janet.spork.http/request "POST" "http://127.0.0.1:8399/post"
                                      :body "hello=1")
          body (str (janet.spork.http/read-body r))]
      (check "post body drains" "m=post uri=/post q=null body=hello=1"
             (str/replace body "q= " "q=null ")))
    (let [boom (fn [_] (throw (ex-info "boom" {})))
          s2 (adapter/run-server boom {:port 8398})
          _ (janet.ev/sleep 0.1)
          r (janet.spork.http/request "GET" "http://127.0.0.1:8398/")]
      (check "throwing handler answers 500" 500 (janet/get r :status))
      (adapter/stop-server s2))
    (adapter/stop-server server))
  (if (pos? @failures)
    (do (println @failures "failing check(s)") (janet.os/exit 1))
    (println "all checks passed")))
