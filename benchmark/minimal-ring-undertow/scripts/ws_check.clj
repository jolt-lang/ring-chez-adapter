;; WS echo smoke check against a running server: clojure -M scripts/ws_check.clj
(require '[clojure.string :as str])
(import '(java.net URI)
        '(java.net.http HttpClient WebSocket$Listener WebSocket)
        '(java.util.concurrent LinkedBlockingQueue TimeUnit))

(def got (LinkedBlockingQueue.))

(def ws (.. (HttpClient/newHttpClient)
            (newWebSocketBuilder)
            (buildAsync (URI. "ws://localhost:8080/ws")
                        (reify WebSocket$Listener
                          (onOpen [_ w] (.request w 1) nil)
                          (onText [_ w part _last]
                            (.offer got (.toString part))
                            (.request w 1)
                            nil)
                          (onClose [_ w c r]
                            (.offer got (str "CLOSE " c " " r))
                            nil)))
            join))

(.sendText ws "hello ws" true)
(.sendText ws "second msg" true)
(Thread/sleep 500)
(.sendClose ws WebSocket/NORMAL_CLOSURE "bye")

(let [a (.poll got 2 TimeUnit/SECONDS)
      b (.poll got 2 TimeUnit/SECONDS)
      c (.poll got 2 TimeUnit/SECONDS)]
  (println "echo1:" a)
  (println "echo2:" b)
  (println "close:" c)
  (if (and (= a "hello ws") (= b "second msg") (str/starts-with? (str c) "CLOSE 1000"))
    (println "WS-ECHO-OK")
    (System/exit 1)))
