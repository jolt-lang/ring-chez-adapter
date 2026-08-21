(ns ring-chez.adapter-test
  (:require [ring-chez.adapter :as adapter]
            [ring-chez.sse :as sse]
            [ring-chez.websocket :as ws]
            [jolt.http-client :as http]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [clojure.java.io :as io]
            [jolt.ffi :as ffi]
            [ring-chez.socket :as socket]
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

;; frame bytes buffered per client fd — TCP hands over whatever arrived, not
;; what was asked for, so a 2-byte header read would otherwise swallow the
;; rest of the frame. Keyed by fd, which the kernel reuses.
(def t-pending (atom {}))

(defn client-connect
  "Open a raw TCP connection to 127.0.0.1:port; returns fd. recv times out
  after rcvtimeo-ms so tests can't hang forever."
  [port & [rcvtimeo-ms]]
  (let [fd (t-socket 2 1 0)]
    (when (neg? fd) (throw (ex-info "client socket() failed" {})))
    ;; the kernel reuses fd numbers, and t-pending is keyed by fd: anything a
    ;; previous connection left buffered would be read as this one's frames
    (swap! t-pending dissoc fd)
    (when rcvtimeo-ms (t-set-rcvtimeo! fd rcvtimeo-ms))
    (let [sa (t-sockaddr port)]
      (when (neg? (t-connect fd sa 16))
        (t-close fd) (ffi/free sa) (throw (ex-info "connect() failed" {})))
      (ffi/free sa))
    fd))

(defn- send-buf!
  "Write all n bytes of buf to fd. A short send is normal and gets retried, and
  so does EINTR — under a loaded suite send(2) is interrupted often enough to
  matter. The old loop stopped on any non-positive return, which silently sent
  a PARTIAL request (or none at all): the server then sat waiting for a body
  that never arrived and closed on its idle timeout, and the test saw an empty
  response with nothing to say why. Anything that is not a retry throws."
  [fd buf n]
  (loop [off 0]
    (when (< off n)
      (let [sent (t-send fd (+ buf off) (- n off) 0)]
        (cond
          (pos? sent) (recur (+ off sent))
          (and (neg? sent) (or (poller/eintr?) (poller/eagain?))) (recur off)
          :else (throw (ex-info "client send failed"
                                {:sent sent :offset off :length n})))))))

(defn client-send [fd ^String s]
  (let [buf (ffi/alloc (max 1 (* 4 (count s))))
        n (ffi/write-bytes buf s)]
    (try (send-buf! fd buf n)
         (finally (ffi/free buf)))))

;; Responses accumulate as raw bytes and decode once, at the end: Content-Length
;; is an octet count, and a multibyte codepoint can straddle two recv calls.
;; ISO-8859-1 is byte-transparent, so scanning that view keeps the index
;; arithmetic below equal to byte offsets.
(defn- latin1 [^bytes bs] (String. bs "ISO-8859-1"))

(defn- bcat [^bytes a ^bytes b]
  (let [out (byte-array (+ (alength a) (alength b)))]
    (System/arraycopy a 0 out 0 (alength a))
    (System/arraycopy b 0 out (alength a) (alength b))
    out))

(def ^:private no-bytes (byte-array 0))

(defn client-recv-raw
  "Read until response looks complete: headers + Content-Length body, or
  connection closed / recv timeout. Returns the raw response octets — a
  binary body does not survive a UTF-8 decode (nil on timeout with nothing)."
  [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      (loop [acc no-bytes]
        (let [view    (latin1 acc)
              hdr-end (str/index-of view "\r\n\r\n")
              done?   (when hdr-end
                        (let [hdrs (str/lower-case (subs view 0 hdr-end))
                              i    (str/index-of hdrs "content-length:")
                              need (if i
                                     (let [s (+ i (count "content-length:"))
                                           e (loop [j s] (if (or (>= j (count hdrs))
                                                                 (= \return (nth hdrs j))
                                                                 (= \newline (nth hdrs j)))
                                                           j (recur (inc j))))]
                                       (or (parse-long (str/trim (subs hdrs s e))) 0))
                                     0)]
                          (>= (- (alength acc) (+ hdr-end 4)) need)))]
          (if done?
            acc
            (let [n (t-recv fd buf 65536 0)]
              (cond (pos? n)  (recur (bcat acc (ffi/read-array buf n)))
                    (zero? n) acc
                    :else     (when (pos? (alength acc)) acc))))))
      (finally (ffi/free buf)))))

(defn client-recv
  "client-recv-raw decoded as UTF-8 (\"\" when the peer closed immediately,
  nil on timeout with nothing)."
  [fd]
  (some-> (client-recv-raw fd) (String. "UTF-8")))

(defn response-body-bytes
  "The body octets of a raw response, framed by its Content-Length."
  [^bytes raw]
  (let [view (latin1 raw)
        hdr-end (str/index-of view "\r\n\r\n")
        from (+ hdr-end 4)
        i (str/index-of (str/lower-case (subs view 0 hdr-end)) "content-length:")
        len (if i
              (let [s (+ i (count "content-length:"))
                    e (str/index-of view "\r\n" s)]
                (parse-long (str/trim (subs view s e))))
              (- (alength raw) from))]
    (java.util.Arrays/copyOfRange raw from (+ from len))))

(defn client-close [fd]
  (swap! t-pending dissoc fd)
  (t-close fd))

;; --- ws wire helpers (client side; all payloads ASCII so string ops are safe
;; only for headers — frames go through t-send-bytes / t-recv-n byte paths) ---

(defn- utf8-bytes [^String s]
  (map #(bit-and 0xff (long %)) (.getBytes s "UTF-8")))

(defn- bytes->str [bs]
  (String. (byte-array (map byte bs)) "UTF-8"))

(defn t-send-bytes [fd bs]
  (let [n (count bs) buf (ffi/alloc (max 1 n))]
    (doseq [[i b] (map-indexed vector bs)] (ffi/write buf :uint8 i b))
    (try (send-buf! fd buf n)
         (finally (ffi/free buf)))))


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

(defn ws-frame
  "A client frame, masked as RFC 6455 §5.3 requires of clients. opcode 0x0
  continuation, 0x1 text, 0x2 binary, 0x8 close, 0x9 ping. All three length
  encodings; fin?, mask? and rsv let a test build the invalid ones too."
  [opcode payload-bytes & {:keys [fin? mask? rsv declared-len]
                           :or {fin? true mask? true rsv 0}}]
  (let [mask [0x11 0x22 0x33 0x44]
        n (or declared-len (count payload-bytes))
        b0 (bit-or (if fin? 0x80 0x00) (bit-shift-left rsv 4) opcode)
        mbit (if mask? 0x80 0x00)
        len-bytes (cond
                    (< n 126) [(bit-or mbit n)]
                    (< n 65536) [(bit-or mbit 126)
                                 (bit-and 0xff (bit-shift-right n 8))
                                 (bit-and 0xff n)]
                    :else (into [(bit-or mbit 127)]
                                (map #(bit-and 0xff (bit-shift-right n (* 8 (- 7 %))))
                                     (range 8))))
        body (if mask?
               (map-indexed (fn [i b] (bit-xor b (nth mask (mod i 4)))) payload-bytes)
               payload-bytes)]
    (concat [b0] len-bytes (when mask? mask) body)))

(defn ws-client-frame
  "Masked, final client frame. Payload length < 126."
  [opcode payload-bytes]
  (ws-frame opcode payload-bytes))

(defn ws-read-server-frame [fd]
  (let [h (t-recv-n fd 2)]
    (when (= 2 (count h))
      (let [fin (pos? (bit-and 0x80 (first h)))
            opcode (bit-and 0x0f (first h))
            masked? (pos? (bit-and 0x80 (second h)))
            len7 (bit-and 0x7f (second h))
            len (cond
                  (< len7 126) len7
                  (= len7 126) (let [ext (t-recv-n fd 2)]
                                 (if (= 2 (count ext))
                                   (+ (bit-shift-left (first ext) 8) (second ext)) -1))
                  :else (let [ext (t-recv-n fd 8)]
                          (if (= 8 (count ext))
                            (reduce (fn [a b] (+ (* a 256) b)) 0 ext) -1)))]
        {:fin fin :opcode opcode :masked masked? :len len
         :payload (if (neg? len) [] (t-recv-n fd len))}))))

(defn ws-close-code
  "The status code carried by a close frame, or nil."
  [f]
  (when (and (= 8 (:opcode f)) (<= 2 (count (:payload f))))
    (+ (bit-shift-left (first (:payload f)) 8) (second (:payload f)))))

(defn client-recv-until-bytes
  "Read until marker is seen in the accumulated bytes (returns the whole
  accumulation as octets), or the connection closes / recv times out."
  [fd marker]
  (let [buf (ffi/alloc 65536)
        needle (latin1 (.getBytes ^String marker "UTF-8"))]
    (try
      (loop [acc no-bytes]
        (if (str/includes? (latin1 acc) needle)
          acc
          (let [n (t-recv fd buf 65536 0)]
            (if (pos? n)
              (recur (bcat acc (ffi/read-array buf n)))
              acc))))
      (finally (ffi/free buf)))))

(defn client-recv-until
  "client-recv-until-bytes decoded as UTF-8."
  [fd marker]
  (String. (client-recv-until-bytes fd marker) "UTF-8"))

(defn ws-read-handshake!
  "Read a response head off fd and hand whatever followed the blank line to the
  frame buffer. The server may put its 101 and its first frame in one segment
  — which is exactly what this suite tests the SERVER for — so a reader that
  drops the surplus desynchronises every frame read after it. Returns the head."
  [fd]
  (let [^bytes bs (client-recv-until-bytes fd "\r\n\r\n")
        view (latin1 bs)
        end (+ (or (str/index-of view "\r\n\r\n") (- (alength bs) 4)) 4)]
    (when (< end (alength bs))
      (let [surplus (mapv #(bit-and 0xff (aget bs %)) (range end (alength bs)))]
        (swap! t-pending update fd #(into surplus (vec %)))))
    (subs view 0 end)))

(defn dechunk
  "A chunked transfer body -> the octets it carries."
  [^bytes body]
  (let [view (latin1 body)
        out (java.io.ByteArrayOutputStream.)]
    (loop [i 0]
      (let [e (str/index-of view "\r\n" i)
            n (Long/parseLong (str/trim (subs view i e)) 16)]
        (when (pos? n)
          (.write out body (+ e 2) n)
          (recur (+ e 2 n 2)))))
    (.toByteArray out)))

(defn client-recv-all
  "Read until the connection closes (returns everything) or recv times out
  (returns what arrived)."
  [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      (loop [acc no-bytes]
        (let [n (t-recv fd buf 65536 0)]
          (if (pos? n)
            (recur (bcat acc (ffi/read-array buf n)))
            (String. acc "UTF-8"))))
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
        (let [hs (ws-read-handshake! fd)]
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

(defn test-string-content-length-keep-alive []
  ;; ring middleware (ring-defaults) sets Content-Length as a *string*. The
  ;; codec honored only numeric values, suppressed the handler's own header,
  ;; and then emitted no framing at all — an HTTP/1.1 persistent response
  ;; with no body terminator, so keep-alive clients hang until timeout.
  (let [server (adapter/run-server (fn [_] {:status 200
                                             :headers {"Content-Type" "text/plain"
                                                       "Content-Length" "5"}
                                             :body "hello"})
                                    {:port 8445 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8445 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "string CL: response framed with Content-Length"
                   "Content-Length: 5" (client-recv fd))
        ;; legal framing means the connection is reusable
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "string CL: connection reusable" "hello" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- RFC-0006: friendly port-in-use error ---------------------------------------

(defn test-bind-eaddrinuse-friendly []
  (let [server (adapter/run-server handler {:port 8444 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (try
        (adapter/run-server handler {:port 8444 :worker-threads 1})
        (check "eaddrinuse: second bind throws" :threw :did-not-throw)
        (catch Throwable t
          (let [d (ex-data t)]
            (check "eaddrinuse: ex-data has :errno-name" "EADDRINUSE" (:errno-name d))
            (check-has "eaddrinuse: message names the port" "8444" (ex-message t))
            (check-has "eaddrinuse: message says already in use"
                       "already in use" (ex-message t))
            (check-has "eaddrinuse: message suggests :port" ":port" (ex-message t)))))
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

;; --- RFC-0004: websocket upgrade guard -----------------------------------------

(defn- ws-handshake [fd path]
  (client-send fd (str "GET " path " HTTP/1.1\r\nHost: t\r\n"
                       "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                       "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                       "Sec-WebSocket-Version: 13\r\n\r\n")))

(defn test-ws-guard-accepts []
  ;; truthy non-map return upgrades as before (echo round-trip)
  (let [server (adapter/run-server handler
                  {:port 8438 :worker-threads 1
                   :ws-handler (fn [session]
                                 (let [m (ws/recv! session)]
                                   (when (not= :close (:type m))
                                     (ws/send! session (:data m)))))
                   :ws-guard (fn [_] true)})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8438 5000)]
        (ws-handshake fd "/ws")
        (check-has "guard-accept: 101 sent" "101" (client-recv-until fd "\r\n\r\n"))
        (t-send-bytes fd (ws-client-frame 0x1 (utf8-bytes "hi")))
        (let [f (ws-read-server-frame fd)]
          (check "guard-accept: session runs" "hi" (bytes->str (:payload f))))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-guard-rejects-with-response []
  ;; a response map is served instead of the 101 — unauthenticated peers
  ;; never get the socket — and the conn stays keep-alive-usable
  (let [server (adapter/run-server handler
                  {:port 8439 :worker-threads 1
                   :ws-handler (fn [_] (check "guard-reject: session must not run"
                                              :ran :ran))
                   :ws-guard (fn [req] (if (= "/open" (req :uri)) true
                                         {:status 401
                                          :headers {"Content-Type" "text/plain"}
                                          :body "no token"}))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8439 5000)]
        (ws-handshake fd "/secret")
        (let [r (client-recv-until fd "\r\n\r\n")]
          (check-has "guard-reject: 401 instead of 101" "401" r)
          (check "guard-reject: no upgrade headers on reject"
                 false (str/includes? (str/lower-case r) "sec-websocket-accept"))
          (check-has "guard-reject: body" "no token" r))
        ;; same connection, normal request still served (no takeover happened)
        (client-send fd "GET /echo?q=after HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "guard-reject: conn reusable" "q=after" (client-recv fd))
        (client-close fd))
      ;; the guard's uri check passes on another path: upgrade proceeds
      (let [fd (client-connect 8439 5000)]
        (ws-handshake fd "/open")
        (check-has "guard-reject: other uri upgrades" "101" (client-recv-until fd "\r\n\r\n"))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-guard-nil-is-403 []
  (let [server (adapter/run-server handler
                  {:port 8440 :worker-threads 1
                   :ws-handler (fn [_] (check "guard-403: session must not run"
                                              :ran :ran))
                   :ws-guard (fn [_] nil)})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8440 5000)]
        (ws-handshake fd "/ws")
        (let [r (client-recv-until fd "\r\n\r\n")]
          (check-has "guard-403: 403 plain reject" "403" r)
          (check-has "guard-403: body" "Forbidden" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-guard-throw-is-request-failure []
  (let [seen (atom nil)
        server (adapter/run-server handler
                  {:port 8441 :worker-threads 1
                   :ws-handler (fn [_] (check "guard-throw: session must not run"
                                              :ran :ran))
                   :ws-guard (fn [_] (throw (ex-info "guard boom" {:g 1})))
                   :on-failure (fn [_ t] (reset! seen (ex-data t)))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8441 5000)]
        (ws-handshake fd "/ws")
        (check-has "guard-throw: failure path answers 500" "500" (client-recv fd))
        (client-close fd))
      (check "guard-throw: on-failure saw guard throw" {:g 1} @seen)
      (finally (adapter/stop-server server)))))

;; --- RFC-0005: write timeout ----------------------------------------------------

(defn- client-connect-tiny-rcvbuf [port rcvtimeo-ms]
  ;; SO_RCVBUF 2048 before connect: this peer stops draining almost
  ;; immediately, so the server's send blocks once buffers fill
  (let [fd (t-socket 2 1 0)
        so-rcvbuf (if t-macos? 0x1002 8)
        v (ffi/alloc 4)]
    (ffi/write v :int 0 2048)
    (t-setsockopt fd t-sol-socket so-rcvbuf v 4)
    (ffi/free v)
    (t-set-rcvtimeo! fd rcvtimeo-ms)
    (let [sa (t-sockaddr port)]
      (when (neg? (t-connect fd sa 16))
        (t-close fd) (ffi/free sa) (throw (ex-info "connect() failed" {})))
      (ffi/free sa))
    fd))

(defn- t-recv-some
  "One recv: whatever has arrived, as a string. Blocks until there is
  something (or the read times out)."
  [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      (let [n (t-recv fd buf 65536 0)]
        (if (pos? n) (ffi/read-bytes buf n) ""))
      (finally (ffi/free buf)))))

(defn- drain-until-eof [fd]
  ;; accumulate until the server closes (recv 0) or a read timeout
  (let [buf (ffi/alloc 65536)]
    (try
      (loop [acc ""]
        (let [n (t-recv fd buf 65536 0)]
          (cond (pos? n) (recur (str acc (ffi/read-bytes buf n)))
                (zero? n) acc
                :else (if (pos? (count acc)) acc ""))))
      (finally (ffi/free buf)))))

(defn test-write-timeout-cuts-stalled-peer []
  ;; 16MB: bigger than any autotuned send buffer, so the send genuinely
  ;; blocks once the peer stops draining
  (let [big (apply str (repeat 16777216 "a"))
        server (adapter/run-server
                 (fn [req] (if (= "/big" (:uri req))
                             {:status 200 :body big}
                             {:status 200 :body "q=free"}))
                 {:port 8442 :worker-threads 1 :write-timeout-ms 300})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect-tiny-rcvbuf 8442 8000)]
        (client-send fd "GET /big HTTP/1.1\r\nHost: t\r\n\r\n")
        ;; Read just enough to know the send is under way, THEN stall. A fixed
        ;; sleep is not enough: building a 16MB body and its send buffer can
        ;; take longer than the stall window on a loaded machine, and a stall
        ;; that opens before the first byte moves proves nothing — the client
        ;; then drains at the server's pace and the whole body arrives. That
        ;; is what made this flaky on CI.
        (let [head (t-recv-some fd)]
          (check "write-timeout: response started" true (pos? (count head)))
          ;; now never read: the tiny rcvbuf and the server sndbuf fill, the
          ;; blocking send times out at ~300ms, and the server abandons the
          ;; response and closes
          (Thread/sleep 1000)
          (let [r (drain-until-eof fd)
                total (+ (count head) (count r))]
            (check "write-timeout: body truncated" true (< total 16777216))
            (check "write-timeout: some bytes delivered first" true (pos? total))))
        (client-close fd))
      ;; the worker is free again: a fresh request is served promptly
      (let [fd (client-connect 8442 3000)
            _ (client-send fd "GET /echo?q=free HTTP/1.1\r\nHost: t\r\n\r\n")]
        (check-has "write-timeout: worker freed after cut" "q=free" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-write-timeout-zero-disables []
  (let [big (apply str (repeat 262144 "b"))
        server (adapter/run-server (fn [_] {:status 200 :body big})
                 {:port 8443 :worker-threads 1 :write-timeout-ms 0})]
    (try
      (Thread/sleep 250)
      ;; Connection: close -> the server closes after the last byte, so
      ;; drain-until-eof returns without waiting out a read timeout
      (let [fd (client-connect 8443 8000)
            _ (client-send fd "GET /big HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            r (drain-until-eof fd)]
        (check "write-timeout off: full body delivered" true (<= 262144 (count r)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- Round 3: bind address and peer -----------------------------------------------

(defn test-bind-host-option []
  ;; make-sockaddr hardcoded 127.0.0.1, so the server could not be reached from
  ;; anywhere but the box it ran on. :host picks the interface; the default
  ;; stays loopback so an upgrade never exposes a server that was private.
  (let [server (adapter/run-server handler {:port 8490 :host "0.0.0.0"})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8490/")]
        (check "host: 0.0.0.0 binds and serves" 200 (:status r)))
      (finally (adapter/stop-server server))))
  (let [server (adapter/run-server handler {:port 8491 :host "127.0.0.1"})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8491/")]
        (check "host: explicit loopback still serves" 200 (:status r)))
      (finally (adapter/stop-server server))))
  (doseq [bad ["" "not.an.address" "999.1.1.1" 42]]
    (try
      (let [s (adapter/run-server handler {:port 8492 :host bad})]
        (adapter/stop-server s)
        (check (str "host: " (pr-str bad) " rejected at boot") :threw :did-not-throw))
      (catch Throwable t
        (check (str "host: " (pr-str bad) " rejected at boot") :threw :threw)
        (check-has (str "host: " (pr-str bad) " names :host") ":host" (ex-message t))))))

(defn test-peer-ip-formatting []
  ;; the conversion itself, fed a sockaddr_in built by hand: accept() fills one
  ;; of these, and reading it is what replaced the hardcoded "127.0.0.1"
  (let [sa (ffi/alloc 16)]
    (try
      (dotimes [i 16] (ffi/write sa :uint8 i 0))
      (doseq [[a b c d] [[127 0 0 1] [10 1 2 3] [192 168 250 17] [255 255 255 255]]]
        (ffi/write sa :uint8 4 a) (ffi/write sa :uint8 5 b)
        (ffi/write sa :uint8 6 c) (ffi/write sa :uint8 7 d)
        (check (str "peer-ip: " a "." b "." c "." d)
               (str a "." b "." c "." d) (socket/peer-ip sa)))
      (finally (ffi/free sa)))))

(defn test-request-addressing []
  ;; :remote-addr and :server-name were string literals. remote-addr now comes
  ;; off the accepted socket, and server-name off the Host header the client
  ;; actually sent (Ring: the resolved server name), port stripped.
  (let [seen (atom nil)
        server (adapter/run-server
                 (fn [req] (reset! seen (select-keys req [:remote-addr :server-name
                                                          :server-port]))
                   {:status 200 :body "ok"})
                 {:port 8493 :host "0.0.0.0"})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8493 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: example.test:8493\r\n\r\n")
        (client-recv fd)
        (check "addressing: remote-addr is the peer" "127.0.0.1" (:remote-addr @seen))
        (check "addressing: server-name from Host" "example.test" (:server-name @seen))
        (check "addressing: server-port is the listen port" 8493 (:server-port @seen))
        (client-close fd))
      ;; HTTP/1.0 without a Host header falls back to the bind address
      (let [fd (client-connect 8493 3000)]
        (client-send fd "GET / HTTP/1.0\r\n\r\n")
        (client-recv fd)
        (check "addressing: server-name falls back to the bind host"
               "0.0.0.0" (:server-name @seen))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-fiber-request-addressing []
  (let [seen (atom nil)
        server (adapter/run-server
                 (fn [req] (reset! seen (:remote-addr req)) {:status 200 :body "ok"})
                 {:port 8494 :strategy :fibers})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8494/")]
        (check "addressing (fibers): served" 200 (:status r)))
      (check "addressing (fibers): remote-addr is the peer" "127.0.0.1" @seen)
      (finally (adapter/stop-server server)))))

;; --- Round 2: RFC 6455 codec ----------------------------------------------------

(defn- ws-echo-server [port ws-handler]
  (adapter/run-server handler {:port port :worker-threads 2 :ws-handler ws-handler}))

(defn- ws-open [port]
  (let [fd (client-connect port 5000)]
    (client-send fd (str "GET /ws HTTP/1.1\r\nHost: t\r\n"
                         "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                         "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                         "Sec-WebSocket-Version: 13\r\n\r\n"))
    (ws-read-handshake! fd)
    fd))

(defn test-harness-handshake-surplus []
  ;; Harness self-test. ws-read-handshake! has to keep whatever followed the
  ;; blank line, because a server may put its 101 and its first frame in one
  ;; segment — which is the case test-ws-leftover-frame drives at the server.
  ;; A reader that dropped the surplus read every later frame misaligned; that
  ;; happened only where the segments coalesced, so it passed on macOS and
  ;; failed on Linux CI. A small keep-alive response is the deterministic
  ;; stand-in: head and body arrive together.
  (let [server (adapter/run-server (fn [_] {:status 200 :body "surplus-body"})
                                   {:port 8469 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8469 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [head (ws-read-handshake! fd)
              carried (apply str (map char (get @t-pending fd)))]
          (check "harness: head stops at the blank line"
                 true (str/ends-with? head "\r\n\r\n"))
          (check "harness: bytes past the head are carried, not dropped"
                 "surplus-body" carried))
        (client-close fd)
        (check "harness: buffer cleared on close" nil (get @t-pending fd)))
      (finally (adapter/stop-server server)))))

(defn test-ws-large-frame []
  ;; encode-frame had only the 7-bit and 16-bit length forms, so a payload of
  ;; 65536 or more built a 16-bit header out of bytes that did not fit in one
  ;; (70000 >> 8 = 273): ffi/write :uint8 rejected it, the session died and the
  ;; connection closed with nothing on the wire.
  (let [big (apply str (repeat 70000 \x))
        server (ws-echo-server 8470 (fn [s] (ws/send! s big) (Thread/sleep 400)))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8470)
            f (ws-read-server-frame fd)]
        (check "ws big: 64-bit length form" 70000 (:len f))
        (check "ws big: opcode text" 1 (:opcode f))
        (check "ws big: payload delivered whole" 70000 (count (:payload f)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-leftover-frame []
  ;; A frame arriving in the same TCP segment as the handshake was dropped: the
  ;; adapter built the session from the fd alone and discarded read-request's
  ;; leftover, so the session parked forever on bytes it had already been given.
  (let [got (atom :none)
        server (ws-echo-server 8471 (fn [s] (reset! got (ws/recv! s))))]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8471 3000)]
        (t-send-bytes fd (concat (map int (str "GET /ws HTTP/1.1\r\nHost: t\r\n"
                                               "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                               "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                                               "Sec-WebSocket-Version: 13\r\n\r\n"))
                                 (ws-client-frame 0x1 (utf8-bytes "hello"))))
        (Thread/sleep 600)
        (check "ws leftover: frame pipelined with the handshake is seen"
               {:type :text :data "hello"} @got)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-fragmented-message []
  ;; recv-frame required FIN, so a fragmented message read as a peer close and
  ;; the connection was dropped mid-conversation.
  (let [got (atom :none)
        server (ws-echo-server 8472 (fn [s] (reset! got (ws/recv! s))))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8472)]
        (t-send-bytes fd (ws-frame 0x1 (utf8-bytes "hel") :fin? false))
        (t-send-bytes fd (ws-frame 0x0 (utf8-bytes "lo")))
        (Thread/sleep 500)
        (check "ws fragments: continuation frames reassembled"
               {:type :text :data "hello"} @got)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-unmasked-client-frame-rejected []
  ;; RFC 6455 §5.1: a server that receives an unmasked frame MUST fail the
  ;; connection. We delivered the payload to the handler instead.
  (let [got (atom :none)
        server (ws-echo-server 8473 (fn [s] (reset! got (ws/recv! s))))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8473)]
        (t-send-bytes fd (ws-frame 0x1 (utf8-bytes "hello") :mask? false))
        (let [f (ws-read-server-frame fd)]
          (check "ws unmasked: closed with 1002" 1002 (ws-close-code f)))
        (check "ws unmasked: handler saw a close, not the payload"
               :close (:type @got))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-rsv-bits-rejected []
  (let [server (ws-echo-server 8474 (fn [s] (ws/recv! s)))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8474)]
        (t-send-bytes fd (ws-frame 0x1 (utf8-bytes "hi") :rsv 4))
        (let [f (ws-read-server-frame fd)]
          (check "ws rsv: non-zero RSV closed with 1002" 1002 (ws-close-code f)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-invalid-utf8-is-1007 []
  ;; String. substitutes U+FFFD, so invalid UTF-8 in a text frame reached the
  ;; handler as mangled text instead of failing the connection with 1007.
  (let [server (ws-echo-server 8475 (fn [s] (ws/recv! s)))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8475)]
        ;; 0xC3 starts a 2-byte sequence; 0x28 is not a continuation byte
        (t-send-bytes fd (ws-frame 0x1 [0xC3 0x28]))
        (let [f (ws-read-server-frame fd)]
          (check "ws utf8: invalid text frame closed with 1007" 1007 (ws-close-code f)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-close-code-echoed []
  ;; RFC 6455 §5.5.1: a conforming peer expects its own code back. Replying
  ;; with an empty close made every clean shutdown look like "no status".
  (let [server (ws-echo-server 8476 (fn [s] (ws/recv! s)))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8476)]
        (t-send-bytes fd (ws-frame 0x8 [0x03 0xE8]))          ; 1000 normal
        (let [f (ws-read-server-frame fd)]
          (check "ws close: code echoed" 1000 (ws-close-code f)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-bad-close-code-is-1002 []
  (let [server (ws-echo-server 8477 (fn [s] (ws/recv! s)))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8477)]
        (t-send-bytes fd (ws-frame 0x8 [0x03 0xEC]))          ; 1004: reserved
        (let [f (ws-read-server-frame fd)]
          (check "ws close: reserved code answered 1002" 1002 (ws-close-code f)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-oversized-frame-rejected []
  ;; A declared length was taken at face value and buffered without limit, one
  ;; boxed Long per wire byte. A frame past the cap is now refused outright.
  (let [server (ws-echo-server 8478 (fn [s] (ws/recv! s)))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8478)]
        ;; declare 8 MiB, send nothing: the cap must fire on the header alone
        (t-send-bytes fd (ws-frame 0x2 [] :declared-len 8388608))
        (let [f (ws-read-server-frame fd)]
          (check "ws oversized: frame past the cap closed with 1009"
                 1009 (ws-close-code f)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-binary-round-trip []
  (let [payload [0x00 0xff 0x7f 0x80 0x01]
        server (ws-echo-server 8479
                 (fn [s] (let [m (ws/recv! s)]
                           (ws/send-binary! s (:data m)))))]
    (try
      (Thread/sleep 250)
      (let [fd (ws-open 8479)]
        (t-send-bytes fd (ws-frame 0x2 payload))
        (let [f (ws-read-server-frame fd)]
          (check "ws binary: opcode preserved" 2 (:opcode f))
          (check "ws binary: octets survive verbatim" payload (vec (:payload f))))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-handshake-validation []
  ;; Igropyr websocket-key: GET + HTTP/1.1 + Connection: upgrade + version 13 +
  ;; a 16-byte-decodable key, and an upgrade must not also declare a body.
  (let [server (ws-echo-server 8480 (fn [s] (ws/recv! s)))]
    (try
      (Thread/sleep 250)
      (doseq [[label req]
              [["missing Sec-WebSocket-Version"
                (str "GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")]
               ["wrong websocket version"
                (str "GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     "Sec-WebSocket-Version: 8\r\n\r\n")]
               ["key that is not 16 bytes"
                (str "GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
                     "Connection: Upgrade\r\nSec-WebSocket-Key: c2hvcnQ=\r\n"
                     "Sec-WebSocket-Version: 13\r\n\r\n")]
               ["POST instead of GET"
                (str "POST /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     "Sec-WebSocket-Version: 13\r\n\r\n")]
               ["upgrade that also declares a body"
                (str "GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
                     "Connection: Upgrade\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     "Sec-WebSocket-Version: 13\r\nContent-Length: 5\r\n\r\nhello")]
               ["no Connection: upgrade token"
                (str "GET /ws HTTP/1.1\r\nHost: t\r\nUpgrade: websocket\r\n"
                     "Connection: keep-alive\r\n"
                     "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                     "Sec-WebSocket-Version: 13\r\n\r\n")]]]
        (let [fd (client-connect 8480 3000)]
          (client-send fd req)
          (let [r (client-recv fd)]
            (check (str "ws handshake: " label " is refused")
                   false (str/includes? (or r "") "101")))
          (client-close fd)))
      (finally (adapter/stop-server server)))))

;; --- Round 1: correctness fixes ------------------------------------------------

(defn test-timeout-granularity []
  ;; set-rcvtimeo!/set-sndtimeo! build a struct timeval, whose second field is
  ;; MICROseconds. Writing (rem ms 1000) there made every timeout lose its
  ;; sub-second part: ka=900 became 900us (closed in ~1ms) and ka=1500 became
  ;; 1s. Only multiples of 1000 worked, which is why the defaults hid it.
  (let [server (adapter/run-server handler {:port 8460 :worker-threads 2
                                            :keep-alive-timeout-ms 900})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8460 10000)]
        ;; a PARTIAL head: the mid-request read is bounded by SO_RCVTIMEO, so
        ;; the server should sit on it for ~900ms before giving up with 400
        (client-send fd "GET / HTTP/1.1\r\nHos")
        (let [t0 (System/currentTimeMillis)
              r  (client-recv fd)
              dt (- (System/currentTimeMillis) t0)]
          (check-has "timeval: partial head eventually 400s" "400" (or r ""))
          (check (str "timeval: 900ms timeout waits ~900ms, not ~0 (got " dt "ms)")
                 true (<= 700 dt 1600)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-fiber-slow-handler-not-reaped []
  ;; The fibers sweeper armed one deadline per request and nothing refreshed
  ;; it while the handler ran, so any handler slower than the keep-alive
  ;; timeout had its connection closed under it and the client got nothing.
  (let [server (adapter/run-server
                 (fn [_] (Thread/sleep 1500) {:status 200 :body "slow-ok"})
                 {:port 8461 :strategy :fibers :keep-alive-timeout-ms 600})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8461 8000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "fiber slow handler: answered despite ka < handler time"
                   "slow-ok" (or (client-recv-until fd "slow-ok") ""))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-fiber-long-stream-not-reaped []
  ;; Same deadline bug on the streaming path: a stream outliving the
  ;; keep-alive timeout was cut mid-body, with no terminating chunk.
  (let [server (adapter/run-server
                 (fn [_] (let [ch (a/chan)]
                           (a/go (dotimes [i 5]
                                   (a/<! (a/timeout 250))
                                   (a/>! ch (str "tick" i "\n")))
                                 (a/close! ch))
                           {:status 200 :headers {"Content-Type" "text/plain"}
                            :body ch}))
                 {:port 8462 :strategy :fibers :keep-alive-timeout-ms 600})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8462 8000)]
        (client-send fd "GET /s HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv-until fd "0\r\n\r\n")]
          (check-has "fiber long stream: first chunk" "tick0" r)
          (check-has "fiber long stream: last chunk" "tick4" r)
          (check-has "fiber long stream: terminator sent" "0\r\n\r\n" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-header-crlf-is-dropped []
  ;; A header value carrying CRLF used to be written verbatim, so a handler
  ;; echoing user data into a header injected whole headers into the response
  ;; (Igropyr header-safe?, http.sc:677).
  (let [server (adapter/run-server
                 (fn [req] (if (= "/inject" (:uri req))
                             {:status 200
                              :headers {"X-Bad" "a\r\nSet-Cookie: pwned=1\r\nX-Tail: b"
                                        "X-Fine" "ok"}
                              :body "body"}
                             {:status 200 :body "plain"}))
                 {:port 8463 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8463 3000)]
        (client-send fd "GET /inject HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check "crlf header: injected header not on the wire"
                 false (str/includes? (str/lower-case r) "set-cookie"))
          (check "crlf header: unsafe header dropped entirely"
                 false (str/includes? r "X-Bad"))
          (check-has "crlf header: safe headers still sent" "X-Fine: ok" r))
        ;; framing survived, so the connection is still usable
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "crlf header: connection still framed" "plain" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-content-length-follows-body []
  ;; The codec preferred the handler's own Content-Length over the octets it
  ;; actually wrote, so a wrong declaration desynced keep-alive permanently.
  ;; Igropyr drops user framing headers and always emits its own.
  (let [server (adapter/run-server
                 (fn [req] (if (= "/lie" (:uri req))
                             {:status 200 :headers {"Content-Length" "3"}
                              :body "0123456789"}
                             {:status 200 :body "second"}))
                 {:port 8464 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8464 3000)]
        (client-send fd "GET /lie HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "lying CL: framed by actual octets" "Content-Length: 10" r)
          (check "lying CL: only one Content-Length" 1
                 (count (re-seq #"(?i)content-length" r))))
        ;; the desync signature: the next response must arrive intact
        (client-send fd "GET /x HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv fd)]
          (check-has "lying CL: next response starts at a status line" "HTTP/1.1 200" r)
          (check-has "lying CL: next response body intact" "second" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- UTF-8 request framing ----------------------------------------------------

(def ^:private em-dash "\u2014")            ; 1 character, 3 UTF-8 octets

(defn- echo-body-handler [req]
  (let [body (if-let [b (:body req)] (slurp b) "")]
    {:status 200
     :headers {"Content-Type" "text/plain; charset=utf-8"
               "X-Body-Octets" (str (alength (.getBytes ^String body "UTF-8")))
               "X-Body-Chars" (str (count body))}
     :body body}))

(defn- utf8-request [path body]
  (str "POST " path " HTTP/1.1\r\nHost: t\r\n"
       "Content-Type: application/json\r\n"
       "Content-Length: " (alength (.getBytes ^String body "UTF-8")) "\r\n\r\n"
       body))

(defn- byte-vals
  "wire[from,to) as unsigned ints, for t-send-bytes."
  [^bytes wire from to]
  (map #(bit-and 0xff (long (aget wire %))) (range from to)))

(defn test-utf8-request-body []
  ;; Content-Length is octets; framing the body by character count leaves the
  ;; server waiting for bytes that already arrived, and it 400s on timeout
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8446 :worker-threads 1
                                    :keep-alive-timeout-ms 1000})]
    (try
      (Thread/sleep 250)
      (let [body (str "{\"text\":\"" em-dash "\"}")
            fd (client-connect 8446 3000)
            _ (client-send fd (utf8-request "/" body))
            r (or (client-recv fd) "")]
        (check-has "utf8: handler ran" "200 OK" r)
        (check-has "utf8: body framed by octets" "X-Body-Octets: 14" r)
        (check-has "utf8: body decoded as 12 characters" "X-Body-Chars: 12" r)
        (check-has "utf8: body echoed intact" body r)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-utf8-split-across-reads []
  ;; the em dash's three octets land in two different recv() calls: decoding
  ;; each chunk on its own corrupts the codepoint
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8447 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [body (str "{\"text\":\"" em-dash "\"}")
            wire (.getBytes ^String (utf8-request "/" body) "UTF-8")
            cut (- (alength wire) 4)         ; after the em dash's first octet
            fd (client-connect 8447 3000)]
        (t-send-bytes fd (byte-vals wire 0 cut))
        (Thread/sleep 150)                   ; force the server to recv twice
        (t-send-bytes fd (byte-vals wire cut (alength wire)))
        (let [r (or (client-recv fd) "")]
          (check-has "utf8 split: handler ran" "200 OK" r)
          (check-has "utf8 split: octets intact" "X-Body-Octets: 14" r)
          (check-has "utf8 split: codepoint intact" body r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-utf8-pipelined []
  ;; leftover must be carried as bytes: slicing it by character count shifts
  ;; the second request's frame
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8448 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [b1 (str "{\"a\":\"" em-dash "\"}")
            b2 "{\"b\":\"\u65e5\u672c\u8a9e\"}"
            fd (client-connect 8448 3000)]
        (client-send fd (str (utf8-request "/one" b1) (utf8-request "/two" b2)))
        (let [r (client-recv-until fd b2)]
          (check-has "utf8 pipelined: first body" b1 r)
          (check-has "utf8 pipelined: second body" b2 r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-utf8-large-body []
  ;; a body spanning many recv buffers, every chunk boundary a coin flip on
  ;; landing mid-codepoint
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8449 :worker-threads 1
                                    :keep-alive-timeout-ms 5000})]
    (try
      (Thread/sleep 250)
      (let [body (apply str (repeat 40000 em-dash))    ; 120000 octets
            fd (client-connect 8449 8000)
            _ (client-send fd (utf8-request "/" body))
            r (or (client-recv fd) "")]
        (check-has "utf8 large: octet count" "X-Body-Octets: 120000" r)
        (check-has "utf8 large: char count" "X-Body-Chars: 40000" r)
        (check "utf8 large: body round-trips" true (str/includes? r body))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-utf8-fiber-request-body []
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8450 :strategy :fibers
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [body (str "{\"text\":\"" em-dash "\"}")
            fd (client-connect 8450 3000)
            _ (client-send fd (utf8-request "/" body))
            r (or (client-recv fd) "")]
        (check-has "utf8 fiber: handler ran" "200 OK" r)
        (check-has "utf8 fiber: body echoed intact" body r)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-unframeable-requests []
  ;; a head whose Content-Length cannot be trusted must not be framed by
  ;; guesswork: the leftover would be served as a second, forged request
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8451 :worker-threads 1
                                    :keep-alive-timeout-ms 1000})
        ask (fn [head]
              (let [fd (client-connect 8451 3000)]
                (client-send fd head)
                (let [r (or (client-recv-until fd "\r\n\r\n") "")]
                  (client-close fd)
                  r)))]
    (try
      (Thread/sleep 250)
      (check-has "framing: non-numeric Content-Length is 400"
                 "400" (ask "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: abc\r\n\r\n"))
      (check-has "framing: negative Content-Length is 400"
                 "400" (ask "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: -5\r\n\r\nhello"))
      (check-has "framing: conflicting Content-Lengths are 400"
                 "400" (ask (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                 "Content-Length: 5\r\nContent-Length: 7\r\n\r\nhello")))
      (check-has "framing: Transfer-Encoding is 501"
                 "501" (ask (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                 "Transfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n")))
      ;; a header whose *value* mentions content-length must not be mistaken
      ;; for framing — that request has no body at all
      (check-has "framing: content-length in a value is not framing"
                 "X-Body-Octets: 0"
                 (ask "GET / HTTP/1.1\r\nHost: t\r\nX-Note: content-length: 9\r\n\r\n"))
      (finally (adapter/stop-server server)))))

(defn- hex [bs]
  (apply str (map #(format "%02x" (bit-and 0xff (long %))) bs)))

;; bytes no UTF-8 decode survives: a lone continuation byte, a truncated
;; two-byte sequence, NUL, 0xff — plus an embedded CRLFCRLF, which a body
;; re-scanned as wire would mistake for the end of a head
(def ^:private binary-body
  [0x00 0xff 0xfe 0x41 0x0d 0x0a 0x0d 0x0a 0x80 0xc3 0x28 0x00])

(defn- drain
  "Every octet of a Ring body InputStream."
  [in]
  (.readAllBytes in))

(defn- binary-handler [req]
  (let [bs (if-let [b (:body req)] (drain b) (byte-array 0))]
    {:status 200
     :headers {"Content-Type" "text/plain"
               "X-Body-Hex" (hex bs)
               "X-Body-Octets" (str (alength bs))
               "X-Body-Nil" (str (nil? (:body req)))}
     :body "ok"}))

(defn test-binary-request-body []
  ;; a body is opaque octets; decoding it as UTF-8 replaces every byte that
  ;; is not valid UTF-8 with U+FFFD and the handler never sees what was sent
  (let [server (adapter/run-server binary-handler
                                   {:port 8452 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [head (str "POST /upload HTTP/1.1\r\nHost: t\r\n"
                      "Content-Type: application/octet-stream\r\n"
                      "Content-Length: " (count binary-body) "\r\n\r\n")
            fd (client-connect 8452 3000)]
        (t-send-bytes fd (concat (utf8-bytes head) binary-body))
        (let [r (or (client-recv fd) "")]
          (check-has "binary: octets delivered whole"
                     (str "X-Body-Octets: " (count binary-body)) r)
          (check-has "binary: bytes survive verbatim"
                     (str "X-Body-Hex: " (hex binary-body)) r))
        ;; the embedded CRLFCRLF must not have been read as a request
        ;; boundary — the connection is still framed where we left it
        (client-send fd "GET /after HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (or (client-recv fd) "")]
          (check-has "binary: connection still framed" "200 OK" r)
          (check-has "binary: bodyless request has nil :body" "X-Body-Nil: true" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- binary response bodies ---------------------------------------------------

;; PNG magic + IEND: real bytes no charset survives
(def ^:private png-bytes
  [0x89 0x50 0x4e 0x47 0x0d 0x0a 0x1a 0x0a 0x00 0x00 0x00 0x0d
   0xff 0xd8 0xff 0xe0 0x80 0xc3 0x28 0x00])

(defn- unsigned->bytes [vs]
  (byte-array (map #(byte (if (> % 127) (- % 256) %)) vs)))

(defn- binary-response-handler [req]
  (let [bs (unsigned->bytes png-bytes)]
    (case (:uri req)
      "/bytes"  {:status 200 :headers {"Content-Type" "image/png"} :body bs}
      "/stream" {:status 200 :headers {"Content-Type" "image/png"}
                 :body (java.io.ByteArrayInputStream. bs)}
      "/file"   (let [f (java.io.File/createTempFile "ring-chez" ".bin")]
                  (io/copy bs f)
                  (.deleteOnExit f)
                  {:status 200 :headers {"Content-Type" "application/octet-stream"} :body f})
      "/seq"    {:status 200 :headers {"Content-Type" "application/octet-stream"}
                 :body [(unsigned->bytes (take 10 png-bytes))
                        (unsigned->bytes (drop 10 png-bytes))]}
      "/chunks" (let [ch (a/chan)]
                  (a/go (a/>! ch (unsigned->bytes (take 10 png-bytes)))
                        (a/>! ch (unsigned->bytes (drop 10 png-bytes)))
                        (a/close! ch))
                  {:status 200 :headers {"Content-Type" "image/png"} :body ch})
      "/echo"   {:status 200 :headers {"Content-Type" "application/octet-stream"}
                 :body (drain (:body req))}
      {:status 404 :body "nope"})))

(defn test-binary-response-body []
  (let [server (adapter/run-server binary-response-handler
                                   {:port 8453 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})
        get-raw (fn [path]
                  (let [fd (client-connect 8453 3000)]
                    (client-send fd (str "GET " path " HTTP/1.1\r\nHost: t\r\n\r\n"))
                    (let [raw (client-recv-raw fd)] (client-close fd) raw)))]
    (try
      (Thread/sleep 250)
      (doseq [[path label] [["/bytes" "byte-array"] ["/stream" "InputStream"]
                            ["/file" "File"] ["/seq" "seq of byte-arrays"]]]
        (let [raw (get-raw path)]
          (check-has (str "binary resp (" label "): Content-Length is octets")
                     (str "Content-Length: " (count png-bytes)) (String. raw "ISO-8859-1"))
          (check (str "binary resp (" label "): body byte-for-byte")
                 (hex (unsigned->bytes png-bytes)) (hex (response-body-bytes raw)))))
      (finally (adapter/stop-server server)))))

(defn test-binary-response-chunks []
  ;; chunked framing sizes each chunk by its octets, and the chunk bytes go
  ;; out untouched
  (let [server (adapter/run-server binary-response-handler
                                   {:port 8454 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8454 3000)]
        (client-send fd "GET /chunks HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [raw (client-recv-until-bytes fd "0\r\n\r\n")
              view (latin1 raw)
              from (+ 4 (str/index-of view "\r\n\r\n"))
              body (java.util.Arrays/copyOfRange raw from (alength raw))]
          (check-has "binary chunks: chunked framing" "Transfer-Encoding: chunked" view)
          ;; wire: a CRLF <10 octets> CRLF a CRLF <10 octets> CRLF 0 CRLF CRLF
          (check "binary chunks: dechunked body byte-for-byte"
                 (hex (unsigned->bytes png-bytes)) (hex (dechunk body))))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-binary-round-trip []
  ;; request body in, same octets back out
  (let [server (adapter/run-server binary-response-handler
                                   {:port 8455 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8455 3000)
            head (str "POST /echo HTTP/1.1\r\nHost: t\r\n"
                      "Content-Type: application/octet-stream\r\n"
                      "Content-Length: " (count png-bytes) "\r\n\r\n")]
        (t-send-bytes fd (concat (utf8-bytes head) png-bytes))
        (let [raw (client-recv-raw fd)]
          (check "binary round-trip: octets unchanged"
                 (hex (unsigned->bytes png-bytes)) (hex (response-body-bytes raw))))
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
  (test-string-content-length-keep-alive)
  (test-bind-eaddrinuse-friendly)
  (test-boot-validation)
  (test-on-failure-hook)
  (test-on-failure-hook-throw-falls-back)
  (test-nil-response-is-500)
  (test-ws-failure-notifies-hook)
  (test-ws-guard-accepts)
  (test-ws-guard-rejects-with-response)
  (test-ws-guard-nil-is-403)
  (test-ws-guard-throw-is-request-failure)
  (test-write-timeout-cuts-stalled-peer)
  (test-write-timeout-zero-disables)

  ;; --- Round 3: bind address and peer ---
  (test-bind-host-option)
  (test-peer-ip-formatting)
  (test-request-addressing)
  (test-fiber-request-addressing)

  ;; --- Round 2: RFC 6455 codec ---
  (test-harness-handshake-surplus)
  (test-ws-large-frame)
  (test-ws-leftover-frame)
  (test-ws-fragmented-message)
  (test-ws-unmasked-client-frame-rejected)
  (test-ws-rsv-bits-rejected)
  (test-ws-invalid-utf8-is-1007)
  (test-ws-close-code-echoed)
  (test-ws-bad-close-code-is-1002)
  (test-ws-oversized-frame-rejected)
  (test-ws-binary-round-trip)
  (test-ws-handshake-validation)

  ;; --- Round 1: correctness fixes ---
  (test-timeout-granularity)
  (test-fiber-slow-handler-not-reaped)
  (test-fiber-long-stream-not-reaped)
  (test-header-crlf-is-dropped)
  (test-content-length-follows-body)

  ;; --- UTF-8 request framing ---
  (test-utf8-request-body)
  (test-utf8-split-across-reads)
  (test-utf8-pipelined)
  (test-utf8-large-body)
  (test-utf8-fiber-request-body)
  (test-unframeable-requests)
  (test-binary-request-body)
  (test-binary-response-body)
  (test-binary-response-chunks)
  (test-binary-round-trip)

  (if (zero? @failures)
    (println "all passed")
    (println @failures "FAILED"))
  (when (pos? @failures) (throw (ex-info "test failures" {:n @failures}))))
