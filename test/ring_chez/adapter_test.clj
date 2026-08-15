(ns ring-chez.adapter-test
  (:require [ring-chez.adapter :as adapter]
            [ring-chez.sse :as sse]
            [ring-chez.websocket :as ws]
            [jolt.http-client :as http]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [jolt.ffi :as ffi]
            [jolt.io-poller :as poller]))

;; --- raw socket test client (keep-alive & later SSE/WS need wire control) ---

(ffi/defcfn t-socket    "socket"    [:int :int :int] :int)
(ffi/defcfn t-connect   "connect"   [:int :pointer :int] :int)
(ffi/defcfn t-close     "close"     [:int] :int)
(ffi/defcfn t-recv      "recv"      [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn t-send      "send"      [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn t-setsockopt "setsockopt" [:int :int :int :pointer :int] :int)

(def ^:private t-macos?
  (str/includes? (str/lower-case (or (System/getProperty "os.name") "")) "mac"))
(def ^:private t-sol-socket (if t-macos? 0xffff 1))
(def ^:private t-so-rcvtimeo (if t-macos? 0x1006 20))

(defn- t-sockaddr [port]
  (let [sa (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write sa :uint8 i 0))
    (if t-macos?
      (do (ffi/write sa :uint8 0 16) (ffi/write sa :uint8 1 2))
      (ffi/write sa :uint8 0 2))
    (ffi/write sa :uint8 2 (bit-and (bit-shift-right port 8) 0xff))
    (ffi/write sa :uint8 3 (bit-and port 0xff))
    (ffi/write sa :uint8 4 127) (ffi/write sa :uint8 5 0)
    (ffi/write sa :uint8 6 0)   (ffi/write sa :uint8 7 1)
    sa))

(defn- t-set-rcvtimeo! [fd ms]
  ;; struct timeval: tv_sec (8 bytes), tv_usec (4 bytes macOS / 8 linux)
  (let [tv (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write tv :uint8 i 0))
    (ffi/write tv :uint64 0 (quot ms 1000))
    (if t-macos?
      (ffi/write tv :uint 8 (long (rem ms 1000)))
      (ffi/write tv :uint64 8 (long (rem ms 1000))))
    (let [r (t-setsockopt fd t-sol-socket t-so-rcvtimeo tv 16)]
      (ffi/free tv)
      r)))

(defn client-connect
  "Open a raw TCP connection to 127.0.0.1:port; returns fd. recv times out
  after rcvtimeo-ms so tests can't hang forever."
  [port & [rcvtimeo-ms]]
  (let [fd (t-socket 2 1 0)]
    (when (neg? fd) (throw (ex-info "client socket() failed" {})))
    (when rcvtimeo-ms (t-set-rcvtimeo! fd rcvtimeo-ms))
    (let [sa (t-sockaddr port)]
      (when (neg? (t-connect fd sa 16))
        (t-close fd) (ffi/free sa) (throw (ex-info "connect() failed" {})))
      (ffi/free sa))
    fd))

(defn client-send [fd ^String s]
  (let [buf (ffi/alloc (max 1 (* 4 (count s))))
        n (ffi/write-bytes buf s)]
    (loop [off 0]
      (when (< off n)
        (let [sent (t-send fd (+ buf off) (- n off) 0)]
          (when (pos? sent) (recur (+ off sent))))))
    (ffi/free buf)))

(defn client-recv
  "Read until response looks complete: headers + Content-Length body, or
  connection closed / recv timeout. Returns the accumulated string
  (\"\" when peer closed immediately, nil on timeout with nothing)."
  [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      (loop [acc ""]
        (if-let [hdr-end (str/index-of acc "\r\n\r\n")]
          (let [hdrs (str/lower-case (subs acc 0 hdr-end))
                i    (str/index-of hdrs "content-length:")
                need (if i
                       (let [s (+ i (count "content-length:"))
                             e (loop [j s] (if (or (>= j (count hdrs))
                                                   (= \return (nth hdrs j))
                                                   (= \newline (nth hdrs j)))
                                             j (recur (inc j))))]
                         (or (parse-long (str/trim (subs hdrs s e))) 0))
                       0)]
            (if (>= (- (count acc) (+ hdr-end 4)) need)
              acc
              (let [n (t-recv fd buf 65536 0)]
                (cond (pos? n) (recur (str acc (ffi/read-bytes buf n)))
                      (zero? n) acc
                      :else (if (pos? (count acc)) acc nil)))))
          (let [n (t-recv fd buf 65536 0)]
            (cond (pos? n) (recur (str acc (ffi/read-bytes buf n)))
                  (zero? n) acc
                  :else (if (pos? (count acc)) acc nil)))))
      (finally (ffi/free buf)))))

(defn client-close [fd] (t-close fd))

;; --- ws wire helpers (client side; all payloads ASCII so string ops are safe
;; only for headers — frames go through t-send-bytes / t-recv-n byte paths) ---

(defn- utf8-bytes [^String s]
  (map #(bit-and 0xff (long %)) (.getBytes s "UTF-8")))

(defn- bytes->str [bs]
  (String. (byte-array (map byte bs)) "UTF-8"))

(defn t-send-bytes [fd bs]
  (let [n (count bs) buf (ffi/alloc (max 1 n))]
    (doseq [[i b] (map-indexed vector bs)] (ffi/write buf :uint8 i b))
    (loop [off 0]
      (when (< off n)
        (let [sent (t-send fd (+ buf off) (- n off) 0)]
          (when (pos? sent) (recur (+ off sent))))))
    (ffi/free buf)))

(def t-pending (atom {}))

(defn- t-fill! [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      (let [got (t-recv fd buf 65536 0)]
        (when (pos? got)
          (swap! t-pending update fd
                 (fn [p] (into (vec p) (map #(ffi/read buf :uint8 %) (range got)))))
          true))
      (finally (ffi/free buf)))))

(defn t-recv-n
  "Exactly n buffered bytes from fd (frames arrive as one packet; without the
  buffer a 2-byte header read would swallow the whole frame)."
  [fd n]
  (while (and (< (count (get @t-pending fd)) n) (t-fill! fd)))
  (let [out (vec (take n (get @t-pending fd [])))]
    (swap! t-pending update fd #(vec (drop n %)))
    out))

(defn ws-client-frame
  "Masked client frame (clients must mask). opcode 0x1 text, 0x9 ping, 0x8 close.
  Payload length < 126."
  [opcode payload-bytes]
  (let [mask [0x11 0x22 0x33 0x44]  ; all < 0x80 so masked ASCII stays < 0x80
        masked (map-indexed (fn [i b] (bit-xor b (nth mask (mod i 4)))) payload-bytes)]
    (concat [(bit-or 0x80 opcode) (bit-or 0x80 (count payload-bytes))] mask masked)))

(defn ws-read-server-frame [fd]
  (let [h (t-recv-n fd 2)]
    (when (= 2 (count h))
      (let [opcode (bit-and 0x0f (first h))
            masked? (pos? (bit-and 0x80 (second h)))
            len7 (bit-and 0x7f (second h))]
        (if (< len7 126)
          {:opcode opcode :masked masked? :payload (t-recv-n fd len7)}
          (let [ext (t-recv-n fd 2)
                len (if (= 2 (count ext))
                      (+ (bit-shift-left (first ext) 8) (second ext)) -1)]
            {:opcode opcode :masked masked? :payload (t-recv-n fd len)}))))))

(defn client-recv-until
  "Read until marker is seen in the accumulated bytes (returns the whole
  accumulation), or the connection closes / recv times out."
  [fd marker]
  (let [buf (ffi/alloc 65536)]
    (try
      (loop [acc ""]
        (if (str/includes? acc marker)
          acc
          (let [n (t-recv fd buf 65536 0)]
            (if (pos? n)
              (recur (str acc (ffi/read-bytes buf n)))
              acc))))
      (finally (ffi/free buf)))))

(defn client-recv-all
  "Read until the connection closes (returns everything) or recv times out
  (returns what arrived)."
  [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      (loop [acc ""]
        (let [n (t-recv fd buf 65536 0)]
          (if (pos? n)
            (recur (str acc (ffi/read-bytes buf n)))
            acc)))
      (finally (ffi/free buf)))))

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

(def stream-abort-noticed (atom false))

(defn handler [{:keys [uri request-method query-string headers]}]
  (cond
    (= uri "/")     {:status 200 :headers {"Content-Type" "text/plain"}
                     :body (str "hello " (name request-method))}
    (= uri "/echo") {:status 200 :headers {"Content-Type" "text/plain"}
                     :body (str "q=" query-string " ua=" (get headers "user-agent" "?"))}
    (= uri "/slow") (do (Thread/sleep 400)
                        {:status 200 :headers {"Content-Type" "text/plain"} :body "slow done"})
    (= uri "/stream")
    (let [ch (a/chan)]
      (a/go (a/>! ch "foo") (a/>! ch "barbaz") (a/close! ch))
      {:status 200 :headers {"Content-Type" "text/plain"} :body ch})
    (= uri "/stream-slow")
    (let [ch (a/chan)]
      (future
        ;; keep producing until a put fails (server closed the channel after
        ;; the client vanished) — bounded so a broken server can't hang us
        (loop [i 0]
          (when (and (< i 30) (a/>!! ch (str "chunk" i)))
            (Thread/sleep 150)
            (recur (inc i))))
        (reset! stream-abort-noticed true)
        (a/close! ch))
      {:status 200 :headers {"Content-Type" "text/plain"} :body ch})
    (= uri "/stream-204")
    (let [ch (a/chan)]
      (a/go (a/>! ch "should-not-appear") (a/close! ch))
      {:status 204 :body ch})
    (= uri "/sse")
    (let [ch (a/chan)]
      (future
        (sse/send! ch {:id 1 :event "greet" :data "hello\nworld"})
        (sse/send! ch {:data "bye"})
        (a/close! ch))
      (sse/event-response ch))
    (= uri "/teapot") {:status 418 :headers {"Content-Type" "text/plain"} :body "teapot"}
    (= uri "/close-hdr") {:status 200 :headers {"Content-Type" "text/plain"
                                                "Connection" "close"}
                          :body "bye"}
    (= uri "/multi-hdr") {:status 200 :headers {"Content-Type" "text/plain"
                                                "Set-Cookie" ["a=1" "b=2"]}
                          :body "x"}
    :else           {:status 404 :headers {"Content-Type" "text/plain"} :body "not found"}))

;; --- Protocol correctness (adopted from capra) --------------------------------

(defn test-status-reasons []
  (let [server (adapter/run-server handler {:port 8415 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8415 3000)]
        (client-send fd "GET /teapot HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "reason: 418 reason phrase" "418 I'm a teapot" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-connection-header-list []
  ;; Connection is a comma list, case-insensitive: "Keep-Alive, Close" means close
  (let [server (adapter/run-server handler {:port 8416 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8416 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\nConnection: Keep-Alive, Close\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "conn-list: response served" "200 OK" r)
          (check-has "conn-list: close honored" "connection: close" (str/lower-case r))
          (check "conn-list: server closed conn" "" (client-recv fd)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-handler-connection-close []
  ;; a handler that sets Connection: close must win: single header, conn closes
  (let [server (adapter/run-server handler {:port 8417 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8417 3000)]
        (client-send fd "GET /close-hdr HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "hdr-close: response served" "200 OK" r)
          (check "hdr-close: exactly one Connection header" 1
                 (count (re-seq #"(?im)^Connection:" r)))
          (check-has "hdr-close: it says close" "connection: close" (str/lower-case r))
          (check "hdr-close: server closed conn" "" (client-recv fd)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-vector-header-values []
  ;; vector header values emit one header line per element
  (let [server (adapter/run-server handler {:port 8418 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8418 3000)]
        (client-send fd "GET /multi-hdr HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "vec-hdr: first line" "Set-Cookie: a=1" r)
          (check-has "vec-hdr: second line" "Set-Cookie: b=2" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-bad-request-lines []
  ;; malformed start line / bad header line / missing Host / bad version
  (let [server (adapter/run-server handler {:port 8419 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8419 3000)]
        (client-send fd "GETONLY\r\n\r\n")
        (check-has "bad: garbage start line -> 400" " 400 Bad Request" (client-recv fd))
        (client-close fd))
      (let [fd (client-connect 8419 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\nnocolon\r\n\r\n")
        (check-has "bad: header without colon -> 400" " 400 Bad Request" (client-recv fd))
        (client-close fd))
      (let [fd (client-connect 8419 3000)]
        (client-send fd "GET / HTTP/1.1\r\n\r\n")
        (check-has "bad: missing Host on 1.1 -> 400" " 400 Bad Request" (client-recv fd))
        (client-close fd))
      (let [fd (client-connect 8419 3000)]
        (client-send fd "GET / HTTP/9.9\r\nHost: t\r\n\r\n")
        (check-has "bad: unknown version -> 505" " 505 " (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-header-cap-is-431 []
  ;; header-only overflow is 431 Request Header Fields Too Large, not 413
  (let [server (adapter/run-server handler {:port 8420 :worker-threads 1
                                            :max-request-bytes 1000})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8420 2000)]
        (client-send fd (str "GET / HTTP/1.1\r\nHost: t\r\nX-Big: "
                             (apply str (repeat 3000 "a")) "\r\n"))
        (check-has "431: run-on headers -> 431" "431" (client-recv-until fd "\r\n\r\n"))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- Phase 1: worker pool ----------------------------------------------------

;; two slow requests must overlap when the pool has >= 2 workers
(defn test-concurrent-slow-requests []
  (let [server (adapter/run-server handler {:port 8401 :worker-threads 4})
        c      (atom 0)]
    (try
      (Thread/sleep 250)
      (let [f1 (future (http/get "http://127.0.0.1:8401/slow"))
            f2 (future (http/get "http://127.0.0.1:8401/slow"))
            t0 (System/currentTimeMillis)
            _  (deref f1 10000 :timeout)
            _  (deref f2 10000 :timeout)
            dt (- (System/currentTimeMillis) t0)]
        ;; serial handling of two 400ms sleeps would be ~800ms
        (check "concurrent slow requests overlap" true (< dt 700))
        (check "slow response body 1" 200 (:status (deref f1 0 nil))))
      (finally (adapter/stop-server server)))))

;; with a single worker, requests queue (still both answered)
(defn test-single-worker-queues []
  (let [server (adapter/run-server handler {:port 8402 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [f1 (future (http/get "http://127.0.0.1:8402/slow"))
            f2 (future (http/get "http://127.0.0.1:8402/"))
            _  (deref f1 10000 :timeout)
            r2 (deref f2 10000 :timeout)]
        (check "single worker: both served" 200 (:status r2)))
      (finally (adapter/stop-server server)))))

;; stop-server must return promptly even with idle workers parked
(defn test-stop-is-prompt []
  (let [server (adapter/run-server handler {:port 8403 :worker-threads 3})
        t0     (System/currentTimeMillis)]
    (Thread/sleep 250)
    (adapter/stop-server server)
    (check "stop-server prompt (<2s)" true (< (- (System/currentTimeMillis) t0) 2000))))

;; --- Phase 2: keep-alive -----------------------------------------------------

(defn test-keep-alive-two-requests []
  (let [server (adapter/run-server handler {:port 8404 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8404 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r1 (client-recv fd)]
          (check-has "keep-alive: first response ok" "200 OK" r1)
          (check-has "keep-alive: response says keep-alive"
                     "connection: keep-alive" (str/lower-case r1))
          ;; same connection, second request
          (client-send fd "GET /echo?q=2 HTTP/1.1\r\nHost: t\r\n\r\n")
          (let [r2 (client-recv fd)]
            (check-has "keep-alive: second response on same conn" "q=2" r2)
            (client-close fd))))
      (finally (adapter/stop-server server)))))

(defn test-connection-close-honored []
  (let [server (adapter/run-server handler {:port 8405 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8405 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
        (let [r1 (client-recv fd)]
          (check-has "close: response served" "200 OK" r1)
          (check-has "close: response says close" "connection: close" (str/lower-case r1))
          ;; server must close: next recv returns 0 bytes
          (check "close: server closed conn" "" (client-recv fd))
          (client-close fd)))
      (finally (adapter/stop-server server)))))

(defn test-keep-alive-idle-timeout []
  (let [server (adapter/run-server handler {:port 8406 :worker-threads 1
                                            :keep-alive-timeout-ms 600})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8406 5000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "idle: first response ok" "200 OK" (client-recv fd))
        ;; now go idle; server should close after ~600ms
        (Thread/sleep 1500)
        (check "idle: server closed after timeout" "" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-pipelined-requests []
  (let [server (adapter/run-server handler {:port 8407 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8407 3000)]
        ;; two complete requests in a single write
        (client-send fd (str "GET / HTTP/1.1\r\nHost: t\r\n\r\n"
                             "GET /echo?q=p HTTP/1.1\r\nHost: t\r\n\r\n"))
        (let [r (client-recv-until fd "q=p")]
          (check-has "pipelined: first response" "hello get" r)
          (check-has "pipelined: second response" "q=p" r)
          (client-close fd)))
      (finally (adapter/stop-server server)))))

;; --- Phase 3: streaming (channel body -> chunked) ----------------------------

(defn test-stream-chunked []
  (let [server (adapter/run-server handler {:port 8408 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8408 5000)]
        (client-send fd "GET /stream HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv-until fd "0\r\n\r\n")]
          (check-has "stream: chunked header" "transfer-encoding: chunked" (str/lower-case r))
          (check-has "stream: chunk 1 framed" "3\r\nfoo\r\n" r)
          (check-has "stream: chunk 2 framed" "6\r\nbarbaz\r\n" r)
          (check-has "stream: terminator" "0\r\n\r\n" r))
        ;; keep-alive survives a cleanly finished stream
        (client-send fd "GET /echo?q=after HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "stream: conn reusable after stream" "q=after" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-stream-client-disconnect-aborts []
  (let [server (adapter/run-server handler {:port 8409 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (reset! stream-abort-noticed false)
      (let [fd (client-connect 8409 5000)]
        (client-send fd "GET /stream-slow HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "abort: first chunk arrived" "chunk0" (client-recv-until fd "chunk0"))
        (client-close fd)                       ; go away mid-stream
        (Thread/sleep 3500)                     ; producer keeps trying; should notice soon
        (check "abort: producer noticed closed channel" true @stream-abort-noticed))
      ;; server still healthy afterwards
      (let [r (http/get "http://127.0.0.1:8409/")]
        (check "abort: server still serves" 200 (:status r)))
      (finally (adapter/stop-server server)))))

(defn test-stream-http10-close-delimited []
  (let [server (adapter/run-server handler {:port 8410 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8410 5000)]
        (client-send fd "GET /stream HTTP/1.0\r\nHost: t\r\n\r\n")
        (let [r (client-recv-all fd)]
          (check "http10: no chunked framing" false (str/includes? (str/lower-case r) "transfer-encoding"))
          (check "http10: raw body bytes" true (str/includes? r "foobarbaz"))
          (check "http10: conn closed" true (str/ends-with? r "foobarbaz"))))
      (finally (adapter/stop-server server)))))

(defn test-stream-204-no-framing []
  (let [server (adapter/run-server handler {:port 8411 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8411 5000)]
        (client-send fd "GET /stream-204 HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv-until fd "\r\n\r\n")]
          (check "204: no transfer-encoding" false (str/includes? (str/lower-case r) "transfer-encoding"))
          (check "204: no body leaked" false (str/includes? r "should-not-appear")))
        ;; connection still usable
        (client-send fd "GET /echo?q=204 HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "204: conn reusable" "q=204" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- Phase 4: SSE ------------------------------------------------------------

(defn test-sse []
  (let [server (adapter/run-server handler {:port 8412 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8412 5000)]
        (client-send fd "GET /sse HTTP/1.1\r\nHost: t\r\n\r\n")
        ;; events precede the terminator on the wire, so waiting for the
        ;; terminator delivers everything (a separate read could race packet
        ;; coalescing and lose the terminator with the last event)
        (let [r (client-recv-until fd "0\r\n\r\n")]
          (check-has "sse: content-type" "content-type: text/event-stream" (str/lower-case r))
          (check-has "sse: no-cache" "cache-control: no-cache" (str/lower-case r))
          (check-has "sse: id field" "id: 1" r)
          (check-has "sse: event field" "event: greet" r)
          (check-has "sse: multi-line data split" "data: hello\r\ndata: world" r)
          (check-has "sse: second event" "data: bye" r)
          (check-has "sse: chunk terminator" "0\r\n\r\n" r))
        (client-send fd "GET /echo?q=sse HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "sse: conn reusable" "q=sse" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- Phase 5: WebSocket ------------------------------------------------------

(defn test-websocket []
  (let [server (adapter/run-server handler
                 {:port 8413 :worker-threads 2
                  :ws-handler
                  (fn [session]
                    (loop []
                      (let [m (ws/recv! session)]
                        (when (not= :close (:type m))
                          (ws/send! session (:data m))
                          (recur)))))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8413 5000)]
        ;; handshake — RFC 6455 §1.3 golden values
        (client-send fd (str "GET /ws HTTP/1.1\r\nHost: t\r\n"
                             "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                             "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                             "Sec-WebSocket-Version: 13\r\n\r\n"))
        (let [hs (client-recv-until fd "\r\n\r\n")]
          (check-has "ws: 101 switching protocols" "101" hs)
          (check-has "ws: accept token (golden)"
                     "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" hs))
        ;; echo
        (t-send-bytes fd (ws-client-frame 0x1 (utf8-bytes "hello")))
        (let [f (ws-read-server-frame fd)]
          (check "ws: echo opcode text" 1 (:opcode f))
          (check "ws: server frames unmasked" false (:masked f))
          (check "ws: echo payload" "hello" (bytes->str (:payload f))))
        ;; ping answered automatically
        (t-send-bytes fd (ws-client-frame 0x9 (utf8-bytes "hi")))
        (let [f (ws-read-server-frame fd)]
          (check "ws: pong opcode" 10 (:opcode f))
          (check "ws: pong payload" "hi" (bytes->str (:payload f))))
        ;; close handshake
        (t-send-bytes fd (ws-client-frame 0x8 (utf8-bytes "")))
        (let [f (ws-read-server-frame fd)]
          (check "ws: close echoed" 8 (:opcode f)))
        (check "ws: server closed conn" "" (client-recv fd))
        (client-close fd))
      ;; server healthy afterwards
      (let [r (http/get "http://127.0.0.1:8413/")]
        (check "ws: server still serves" 200 (:status r)))
      (finally (adapter/stop-server server)))))

;; --- Phase 6: hardening -----------------------------------------------------

(defn test-max-request-size []
  (let [server (adapter/run-server handler {:port 8414 :worker-threads 2
                                            :max-request-bytes 1000})]
    (try
      (Thread/sleep 250)
      ;; run-on headers (no \r\n\r\n within cap) -> 431, connection closed
      (let [fd (client-connect 8414 2000)]
        (client-send fd (str "GET / HTTP/1.1\r\nHost: t\r\nX-Big: "
                             (apply str (repeat 3000 "a")) "\r\n"))
        (let [r (client-recv-until fd "\r\n\r\n")]
          (check-has "cap: 431 for run-on headers" "431" r)
          (check-has "cap: connection closed" "close" (str/lower-case r)))
        (client-close fd))
      ;; declared body larger than cap -> 413 without reading the body
      (let [fd (client-connect 8414 2000)]
        (client-send fd (str "POST /echo HTTP/1.1\r\nHost: t\r\n"
                             "Content-Length: 5000\r\n\r\npartial"))
        (let [r (client-recv-until fd "\r\n\r\n")]
          (check-has "cap: 413 for oversized body" "413" r))
        (client-close fd))
      ;; normal-sized requests still fine
      (let [r (http/get "http://127.0.0.1:8414/")]
        (check "cap: normal request passes" 200 (:status r)))
      (finally (adapter/stop-server server)))))

;; --- Phase 7: no stall under pressure ----------------------------------------

(defn test-worker-survives-bad-chunk []
  ;; a channel body yielding a non-string chunk throws inside the worker; the
  ;; worker must catch it, close the conn, and keep serving instead of dying
  ;; (a dead worker shrinks the pool permanently and starves later conns)
  (let [bad-body (fn [] (doto (a/chan 1) (a/put! :boom)))
        server (adapter/run-server
                (fn [req]
                  (if (= "/bad" (:uri req))
                    {:status 200 :headers {"Content-Type" "text/plain"} :body (bad-body)}
                    {:status 200 :headers {"Content-Type" "text/plain"} :body "ok"}))
                {:port 8426 :worker-threads 1})]
    (Thread/sleep 250)
    (try
      (let [fd (client-connect 8426 3000)]
        (client-send fd "GET /bad HTTP/1.1\r\nHost: t\r\n\r\n")
        (client-recv fd)                       ; head or "" — the point is survival
        (client-close fd))
      (let [fd (client-connect 8426 4000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check "survival: worker alive after bad chunk" true (some? r))
          (check-has "survival: still serves 200" "200" (or r "")))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-keep-alive-fairness []
  ;; more keep-alive connections than workers: every connection must still be
  ;; answered. Without pressure-retirement the first conns pin all workers
  ;; idle and the rest starve until the client times out (the ab -k -c 100 stall)
  (let [server (adapter/run-server handler {:port 8427 :worker-threads 2
                                            :keep-alive-timeout-ms 60000})]
    (Thread/sleep 250)
    (try
      (let [fds (mapv (fn [_] (client-connect 8427 5000)) (range 5))]
        (Thread/sleep 600)                     ; let the acceptor hand off / park
        (doseq [fd fds]
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
        (doseq [[i fd] (map-indexed vector fds)]
          (let [r (client-recv fd)]
            (check (str "fairness: conn " i " answered") true (some? r))
            (check-has (str "fairness: conn " i " 200") "200" (or r ""))))
        (doseq [fd fds] (client-close fd)))
      (finally (adapter/stop-server server)))))

(defn test-pipelined-under-pressure []
  ;; the regression the claim-time/leftover fix addresses: while other
  ;; connections queue for the pool, a pipelined batch on a claimed
  ;; connection must never be split by a retirement close — responses 1..n-1
  ;; carry buffered leftover and must keep the connection alive
  (let [server (adapter/run-server handler {:port 8428 :worker-threads 2
                                            :keep-alive-timeout-ms 60000})]
    (Thread/sleep 250)
    (try
      (let [claimed (mapv (fn [_] (client-connect 8428 5000)) (range 2))
            queued  (mapv (fn [_] (client-connect 8428 5000)) (range 3))]
        (Thread/sleep 600)                     ; 2 claimed, 1 pending, 2 backlogged
        (client-send (first claimed)
                     (str "GET /echo?q=1 HTTP/1.1\r\nHost: t\r\n\r\n"
                          "GET /echo?q=2 HTTP/1.1\r\nHost: t\r\n\r\n"
                          "GET /echo?q=3 HTTP/1.1\r\nHost: t\r\n\r\n"))
        (let [r (client-recv-until (first claimed) "q=3")]
          (check-has "pipeline+pressure: 1st answered" "q=1" r)
          (check-has "pipeline+pressure: 2nd answered" "q=2" r)
          (check-has "pipeline+pressure: 3rd answered (not split by close)" "q=3" r))
        (client-close (first claimed))
        (client-close (second claimed))
        ;; the queued connections must still all be served (fairness outcome)
        (doseq [fd queued]
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
          (check-has "pipeline+pressure: queued conn answered" "200" (client-recv fd)))
        (doseq [fd queued] (client-close fd)))
      (finally (adapter/stop-server server)))))

;; --- fiber strategy (:strategy :fibers) --------------------------------------
;; Connections are served by core.async go blocks that park on a shared
;; poll(2) poller thread instead of pinning pool threads.

(defn test-rebind-same-port-after-stop []
  ;; stop-server must release the port immediately: close() alone does not wake
  ;; the acceptor parked in accept() on Linux, so the listen socket (and its
  ;; port binding) survives the syscall and the rebind fails with EADDRINUSE.
  (let [s1 (adapter/run-server handler {:port 8423})]
    (Thread/sleep 100)
    (adapter/stop-server s1))
  (let [server (adapter/run-server handler {:port 8423})]
    (try
      (let [r (http/get "http://127.0.0.1:8423/")]
        (check "rebind: same port serves right after stop" 200 (:status r)))
      (finally
        (adapter/stop-server server)))))

(defn test-fiber-basic []
  (let [server (adapter/run-server handler {:port 8420 :strategy :fibers})]
    (Thread/sleep 250)
    (try
      (let [r (http/get "http://127.0.0.1:8420/")]
        (check "fiber: GET / status" 200 (:status r))
        (check "fiber: GET / body" "hello get" (:body r)))
      (let [r (http/get "http://127.0.0.1:8420/echo?q=hi&ua=1")]
        (check-has "fiber: query string reaches handler" "q=hi" (:body r)))
      (finally (adapter/stop-server server)))))

(defn test-fiber-idle-connections-do-not-pin []
  ;; Two idle keep-alive connections must not stop a third request from being
  ;; served promptly. Under :threads with 2 workers the two idle conns pin both
  ;; workers in recv until the 8s keep-alive timeout and the third stalls.
  (let [server (adapter/run-server handler {:port 8421 :strategy :fibers
                                            :worker-threads 2
                                            :keep-alive-timeout-ms 8000})]
    (Thread/sleep 250)
    (try
      (let [idle1 (client-connect 8421)
            idle2 (client-connect 8421)]
        (client-send idle1 "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber: idle conn 1 served" "200" (client-recv idle1))
        (client-send idle2 "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber: idle conn 2 served" "200" (client-recv idle2))
        ;; both conns now idle keep-alive; a third must still get a fast answer
        (let [fd (client-connect 8421 2500)]
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
          (let [r (client-recv fd)]
            (check "fiber: 3rd request served despite 2 idle conns" true (some? r))
            (check-has "fiber: 3rd request 200" "200" (or r "")))
          (client-close fd))
        (client-close idle1)
        (client-close idle2))
      (finally (adapter/stop-server server)))))

(defn test-fiber-keep-alive-and-pipelining []
  (let [server (adapter/run-server handler {:port 8422 :strategy :fibers})]
    (Thread/sleep 250)
    (try
      ;; two sequential requests on one connection
      (let [fd (client-connect 8422 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber ka: first response" "hello get" (client-recv fd))
        (client-send fd "GET /echo?q=2 HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber ka: second response" "q=2" (client-recv fd))
        (client-close fd))
      ;; two pipelined requests in a single write
      (let [fd (client-connect 8422 3000)]
        (client-send fd (str "GET / HTTP/1.1\r\nHost: t\r\n\r\n"
                             "GET /echo?q=p HTTP/1.1\r\nHost: t\r\n\r\n"))
        (let [r (client-recv-until fd "q=p")]
          (check-has "fiber pipe: first answered" "hello get" r)
          (check-has "fiber pipe: second answered" "q=p" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-fiber-streaming []
  (let [server (adapter/run-server handler {:port 8423 :strategy :fibers})]
    (Thread/sleep 250)
    (try
      (let [r (http/get "http://127.0.0.1:8423/stream")]
        (check "fiber stream: status" 200 (:status r))
        (check "fiber stream: body" "foobarbaz" (:body r))
        (check-has "fiber stream: chunked framing"
                   "transfer-encoding" (->> (:headers r) (map str/lower-case) (apply str))))
      (finally (adapter/stop-server server)))))

(defn test-fiber-idle-timeout []
  ;; an idle keep-alive conn is closed by the server after the timeout
  (let [server (adapter/run-server handler {:port 8424 :strategy :fibers
                                            :keep-alive-timeout-ms 400})]
    (Thread/sleep 250)
    (try
      (let [fd (client-connect 8424 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber timeout: first response ok" "200" (client-recv fd))
        (check "fiber timeout: server closed idle conn" "" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-fiber-stop-wakes-parked-conns []
  ;; stop-server while fiber go blocks are parked on idle keep-alive conns:
  ;; it must not wait out the 60s keep-alive timeout — parked conns are woken
  ;; (or their fds torn down) so stop returns promptly and the port rebinds.
  (let [server (adapter/run-server handler {:port 8429 :strategy :fibers
                                            :keep-alive-timeout-ms 60000})
        t0 (System/currentTimeMillis)]
    (Thread/sleep 250)
    (let [fds (mapv (fn [_] (client-connect 8429 5000)) (range 5))]
      (doseq [fd fds]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber stop: conn served" "200" (client-recv fd)))
      (adapter/stop-server server)
      (check "fiber stop: stop returns promptly with parked conns"
             true (< (- (System/currentTimeMillis) t0) 5000))
      (doseq [fd fds] (client-close fd))
      (let [s2 (adapter/run-server handler {:port 8429 :strategy :fibers})]
        (Thread/sleep 100)
        (try
          (let [r (http/get "http://127.0.0.1:8429/")]
            (check "fiber stop: port rebindable after stop" 200 (:status r)))
          (finally (adapter/stop-server s2)))))))

(defn test-fiber-restart-leaves-poller-clean []
  ;; regression for the io-poller migration flake: an earlier per-read
  ;; alts![(go (wait-ready)) (timeout)] race leaked a parked waker when it
  ;; registered after close+forget — its stale poller entry then misdirected
  ;; wakeups for a REUSED fd number, so a restarted server on the same port
  ;; accepted but never responded (~10% of stop/restart rounds). The leak's
  ;; direct signature is a leftover entry in the global poller table; assert
  ;; the table is empty after every round.
  (dotimes [i 6]
    (let [server (adapter/run-server handler {:port 8430 :strategy :fibers
                                               :keep-alive-timeout-ms 60000})]
      (Thread/sleep 100)
      (let [fds (mapv (fn [_] (client-connect 8430 5000)) (range 3))]
        (doseq [fd fds]
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
          (check-has "fiber restart: conn served" "200" (client-recv fd)))
        (adapter/stop-server server)
        (Thread/sleep 100)
        (doseq [fd fds] (client-close fd))
        (check (str "fiber restart: poller table clean after round " i)
               {} (:fds (poller/debug-state)))
        (let [s2 (adapter/run-server handler {:port 8430 :strategy :fibers})]
          (Thread/sleep 100)
          (try
            (let [fd (client-connect 8430 5000)]
              (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
              (check-has "fiber restart: rebind serves fresh request"
                         "200" (client-recv fd))
              (client-close fd))
            (finally (adapter/stop-server s2))))))))

(defn test-bad-strategy-throws []
  (try
    (let [server (adapter/run-server handler {:port 8425 :strategy :magic})]
      (adapter/stop-server server)
      (check "bad strategy: run-server throws" :threw :did-not-throw))
    (catch Throwable t
      (check "bad strategy: run-server throws" :threw :threw)
      (check-has "bad strategy: message names :strategy" ":strategy" (ex-message t)))))

;; --- RFC-0001: errno-enriched FFI errors ---------------------------------------

(defn test-bind-failure-carries-errno []
  (let [server (adapter/run-server handler {:port 8431 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (try
        (adapter/run-server handler {:port 8431 :worker-threads 1})
        (check "errno: second bind throws" :threw :did-not-throw)
        (catch Throwable t
          (let [d (ex-data t)]
            (check "errno: throws ex-info" true (map? d))
            (check "errno: ex-data names syscall" "bind" (:syscall d))
            (check "errno: ex-data has positive :errno" true (pos? (:errno d)))
            (check "errno: ex-data has :strerror text"
                   true (and (string? (:strerror d)) (pos? (count (:strerror d)))))
            (check-has "errno: message carries strerror"
                       "address already in use" (str/lower-case (ex-message t)))
            ;; the ORIGINAL server must be unaffected by the failed boot
            (let [fd (client-connect 8431 3000)]
              (client-send fd "GET /echo?q=ok HTTP/1.1\r\nHost: t\r\n\r\n")
              (check-has "errno: original server still serves" "q=ok" (client-recv fd))
              (client-close fd)))))
      (finally (adapter/stop-server server)))))

;; --- RFC-0002: boot-time option validation -------------------------------------

(defn test-boot-validation []
  (doseq [[k v] [[:port "abc"] [:port 0] [:port 70000]
                 [:worker-threads 0] [:worker-threads -1]
                 [:keep-alive-timeout-ms 0] [:keep-alive-timeout-ms -5]
                 [:max-request-bytes 0]
                 [:on-failure :not-a-fn]
                 [:ws-guard 42]
                 [:write-timeout-ms -1]]]
    (try
      (adapter/run-server handler {k v})
      (check (str "validation: " k " " (pr-str v) " rejected") :threw :did-not-throw)
      (catch Throwable t
        (check (str "validation: " k " " (pr-str v) " rejected") :threw :threw)
        (check (str "validation: " k " names key in ex-data") k (:key (ex-data t)))
        (check (str "validation: " k " carries :given")
               true (contains? (ex-data t) :given)))))
  ;; a failed validation must never have bound a socket: a clean boot on a
  ;; fresh port with all keys present-and-valid still serves.
  (let [server (adapter/run-server handler {:port 8432 :worker-threads 1
                                            :keep-alive-timeout-ms 30000
                                            :max-request-bytes 1048576
                                            :write-timeout-ms 0})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8432 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "validation: valid opts still serve" "200 OK" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- RFC-0003: unified failure path --------------------------------------------

(defn test-on-failure-hook []
  (let [seen (atom [])
        hook (fn [req t] (swap! seen conj [req t])
                       {:status 503 :headers {"Content-Type" "text/plain"}
                        :body "hooked"})
        server (adapter/run-server (fn [_] (throw (ex-info "boom" {:x 1})))
                 {:port 8433 :worker-threads 1 :on-failure hook})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8433 3000)]
        (client-send fd "GET /echo?q=1 HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "on-failure: hook response served" "503" r)
          (check-has "on-failure: hook body" "hooked" r))
        ;; keep-alive survives the failure: hook answers via normal path
        (client-send fd "GET /echo?q=2 HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "on-failure: conn reusable after hook" "503" (client-recv fd))
        (client-close fd))
      (let [[_ t] (first @seen)]
        (check "on-failure: hook got the thrown ex-data" {:x 1} (ex-data t)))
      (finally (adapter/stop-server server)))))

(defn test-on-failure-hook-throw-falls-back []
  (let [server (adapter/run-server (fn [_] (throw (ex-info "boom" {})))
                 {:port 8434 :worker-threads 1
                  :on-failure (fn [_ _] (throw (ex-info "hook also boom" {})))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8434 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "hook-throw: falls back to plain 500"
                   "500" (client-recv fd))
        (client-close fd))
      ;; worker survives the hook throw: a fresh request still answers
      (let [fd (client-connect 8434 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "hook-throw: worker survives" "500" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-nil-response-is-500 []
  ;; no hook: nil gets the Ring/Jetty 500, not a dropped connection
  (let [server (adapter/run-server (fn [_] nil) {:port 8435 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8435 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "nil-resp: 500 served" "500" r)
          (check-has "nil-resp: complete response body" "Internal Server Error" r))
        (client-close fd))
      (finally (adapter/stop-server server))))
  ;; with a hook: the synthetic throwable is tagged
  (let [types (atom [])
        server (adapter/run-server (fn [_] nil)
                 {:port 8436 :worker-threads 1
                  :on-failure (fn [_ t] (swap! types conj (:type (ex-data t))))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8436 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "nil-resp: hook path serves 500" "500" (client-recv fd))
        (client-close fd))
      (check "nil-resp: hook sees :ring-chez/nil-response"
             [:ring-chez/nil-response] @types)
      (finally (adapter/stop-server server)))))

(defn test-ws-failure-notifies-hook []
  (let [seen (atom [])
        server (adapter/run-server handler
                 {:port 8437 :worker-threads 1
                  :ws-handler (fn [_] (throw (ex-info "ws boom" {:w 1})))
                  :on-failure (fn [req t] (swap! seen conj [(req :uri) (ex-data t)]))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8437 5000)]
        (client-send fd (str "GET /ws HTTP/1.1\r\nHost: t\r\n"
                             "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                             "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                             "Sec-WebSocket-Version: 13\r\n\r\n"))
        (check-has "ws-fail: 101 already sent" "101" (client-recv-until fd "\r\n\r\n"))
        ;; session throws -> server closes (close is the truncation signal)
        (check "ws-fail: server closed conn" "" (client-recv fd))
        (client-close fd))
      (check "ws-fail: hook saw the session throw" ["/ws" {:w 1}] (first @seen))
      ;; worker survives: plain request still served
      (let [fd (client-connect 8437 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "ws-fail: worker survives" "200 OK" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn -main [& _]
  (println "ring adapter over jolt.ffi sockets")

  ;; --- baseline suite (existing behavior) ---
  (let [server (adapter/run-server handler {:port 8399})]
    (Thread/sleep 250)
    (try
      (let [r (http/get "http://127.0.0.1:8399/")]
        (check "GET / status 200" 200 (:status r))
        (check-has "GET / body" "hello get" (:body r))
        (check-has "content-type header" "text/plain" (get-in r [:headers "content-type"] "")))
      (let [r (try (http/get "http://127.0.0.1:8399/nope")
                   (catch Throwable t (ex-data t)))]
        (check "unknown route 404" 404 (:status r)))
      (let [r (http/get "http://127.0.0.1:8399/echo?q=hi&ua=1")]
        (check-has "query string reaches handler" "q=hi" (:body r)))
      (finally (adapter/stop-server server))))

  ;; --- Protocol correctness (adopted from capra) ---
  (test-status-reasons)
  (test-connection-header-list)
  (test-handler-connection-close)
  (test-vector-header-values)
  (test-bad-request-lines)
  (test-header-cap-is-431)

  ;; --- Phase 1 ---
  (test-concurrent-slow-requests)
  (test-single-worker-queues)
  (test-stop-is-prompt)

  ;; --- Phase 2 ---
  (test-keep-alive-two-requests)
  (test-connection-close-honored)
  (test-keep-alive-idle-timeout)
  (test-pipelined-requests)

  ;; --- Phase 3 ---
  (test-stream-chunked)
  (test-stream-client-disconnect-aborts)
  (test-stream-http10-close-delimited)
  (test-stream-204-no-framing)

  ;; --- Phase 4 ---
  (test-sse)

  ;; --- Phase 5 ---
  (test-websocket)

  ;; --- Phase 6 ---
  (test-max-request-size)
  (test-worker-survives-bad-chunk)
  (test-keep-alive-fairness)
  (test-pipelined-under-pressure)

  ;; --- concurrency strategies ---
  (test-rebind-same-port-after-stop)
  (test-fiber-basic)
  (test-fiber-idle-connections-do-not-pin)
  (test-fiber-keep-alive-and-pipelining)
  (test-fiber-streaming)
  (test-fiber-idle-timeout)
  (test-fiber-stop-wakes-parked-conns)
  (test-fiber-restart-leaves-poller-clean)
  (test-bad-strategy-throws)
  (test-bind-failure-carries-errno)
  (test-boot-validation)
  (test-on-failure-hook)
  (test-on-failure-hook-throw-falls-back)
  (test-nil-response-is-500)
  (test-ws-failure-notifies-hook)

  (if (zero? @failures)
    (println "all passed")
    (println @failures "FAILED"))
  (when (pos? @failures) (throw (ex-info "test failures" {:n @failures}))))
