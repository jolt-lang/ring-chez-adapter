(ns bench.server-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [ring.websocket.protocols :as wsp]
            [bench.server :as server]))

(deftest plaintext-route
  (let [resp (server/app {:request-method :get :uri "/plaintext"})]
    (is (= 200 (:status resp)))
    (is (= "Hello, World!" (:body resp)))
    (is (str/starts-with? (get-in resp [:headers "Content-Type"]) "text/plain"))))

(deftest json-route
  (let [resp (server/app {:request-method :get :uri "/json"})]
    (is (= 200 (:status resp)))
    (is (= "{\"message\":\"Hello, World!\"}" (:body resp)))
    (is (= "application/json" (get-in resp [:headers "Content-Type"])))))

(deftest not-found
  (let [resp (server/app {:request-method :get :uri "/nope"})]
    (is (= 404 (:status resp)))))

(deftest sse-response-shape
  (let [resp (server/app {:request-method :get :uri "/sse"
                          :query-string "interval=10&events=3"})]
    (is (= 200 (:status resp)))
    (is (str/starts-with? (get-in resp [:headers "Content-Type"]) "text/event-stream"))
    (is (instance? bench.server.SSEBody (:body resp)))))

(deftest sse-defaults-applied
  (let [resp (server/app {:request-method :get :uri "/sse"})]
    (is (= 200 (:status resp)))
    (is (instance? bench.server.SSEBody (:body resp)))))

(deftest ws-route-returns-listener-map
  (let [resp (server/app {:request-method :get :uri "/ws"})]
    (is (map? (:ring.websocket/listener resp)))
    (is (fn? (get-in resp [:ring.websocket/listener :on-message])))))

(deftest ws-echo-text
  (let [sent (atom [])
        listener (:ring.websocket/listener (server/app {:request-method :get :uri "/ws"}))
        socket (reify wsp/Socket
                 (-open? [_] true)
                 (-send [_ msg] (swap! sent conj msg)))]
    ((:on-message listener) socket "hello")
    (is (= ["hello"] @sent))))

(deftest ws-echo-binary
  (let [sent (atom [])
        listener (:ring.websocket/listener (server/app {:request-method :get :uri "/ws"}))
        payload (java.nio.ByteBuffer/wrap (byte-array [1 2 3]))
        socket (reify wsp/Socket
                 (-open? [_] true)
                 (-send [_ msg] (swap! sent conj msg)))]
    ((:on-message listener) socket payload)
    (is (= [payload] (map identity @sent)))
    (is (instance? java.nio.ByteBuffer (first @sent)))))
