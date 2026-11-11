(ns ring-chez.adapter-test
  (:require [ring-chez.adapter :as adapter]
            [ring-chez.sse :as sse]
            [ring-chez.fault :as fault]
            [ring-chez.middleware.multipart :as multipart]
            [ring-chez.multipart.core :as rc-mp]
            [ring-chez.middleware.gzip :as gzip]
            [ring-chez.middleware.compress :as compress]
            [ring-chez.zstd :as zstd]
            [ring-chez.middleware.static :as static]
            [ring-chez.middleware.proxy :as proxy]
            [ring-chez.websocket :as ws]
            [jolt.http-client :as http]
            [clojure.string :as str]
            [clojure.core.async :as a]
            [jolt.http.tls :as client-tls]
            [clojure.java.io :as io]
            [jolt.ffi :as ffi]
            [ring-chez.socket :as socket]
            [ring-chez.http]
            [ring-chez.tls]
            [jolt.socket.native :as native]
            [jolt.io-poller :as poller]
            [jolt.process]
            [clojure.java.shell]))

(defn- gunzip
  "Test oracle: the runtime's own java.util.zip, a separate implementation from
  ring-chez.zlib (RFC-0011)."
  [^bytes bs]
  (with-open [in (java.util.zip.GZIPInputStream. (java.io.ByteArrayInputStream. bs))]
    (.readAllBytes in)))

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
    (dotimes [i 16] (ffi/write sa :uint8 0 i))
    (if t-macos?
      (do (ffi/write sa :uint8 16 0) (ffi/write sa :uint8 2 1))
      (ffi/write sa :uint8 2 0))
    (ffi/write sa :uint8 (bit-and (bit-shift-right port 8) 0xff) 2)
    (ffi/write sa :uint8 (bit-and port 0xff) 3)
    (ffi/write sa :uint8 127 4) (ffi/write sa :uint8 0 5)
    (ffi/write sa :uint8 0 6)   (ffi/write sa :uint8 1 7)
    sa))

(defn- t-set-rcvtimeo! [fd ms]
  ;; struct timeval: tv_sec (8 bytes), tv_usec (4 bytes macOS / 8 linux)
  (let [tv (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write tv :uint8 0 i))
    (ffi/write tv :uint64 (quot ms 1000) 0)
    (if t-macos?
      (ffi/write tv :uint (long (rem ms 1000)) 8)
      (ffi/write tv :uint64 (long (rem ms 1000)) 8))
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

(defn client-connect-host
  "Raw TCP connection to host:port over whichever family host resolves to
  (RFC-0016 tests dial ::1). Uses the adapter's own resolver, so the client
  and the server agree on what a host means."
  ([host port] (client-connect-host host port nil))
  ([host port rcvtimeo-ms]
   (let [{:keys [addr addrlen family]} (first (socket/resolve-bind-sockaddrs host port))
         fd (t-socket family 1 0)]
     (when (neg? fd) (throw (ex-info "client socket() failed" {})))
     (swap! t-pending dissoc fd)
     (when rcvtimeo-ms (t-set-rcvtimeo! fd rcvtimeo-ms))
     (when (neg? (t-connect fd addr addrlen))
       (t-close fd) (ffi/free addr) (throw (ex-info (str "connect() to " host " failed") {})))
     (ffi/free addr)
     fd)))

(defn- send-buf!
  "Write all n bytes of buf to fd. A short send is normal and gets retried, and
  so does EINTR. The old loop stopped on any non-positive return, which
  silently sent a PARTIAL request (or none at all): the server then sat
  waiting for a body that never arrived and closed on its idle timeout, and
  the test saw an empty response with nothing to say why. Anything that is not
  a retry throws, with the errno, because that is the part the old loop threw
  away — it is what finally identified EPIPE here rather than a signal.

  The EINTR arm is defensive, not load-bearing: nothing in an automated run
  delivers a signal that runs a handler (every disposition but SIGINT/SIGQUIT
  is SIG_DFL, SIGPIPE is SIG_IGN), so it should never fire."
  [fd buf n]
  (loop [off 0]
    (when (< off n)
      (let [sent (t-send fd (+ buf off) (- n off) 0)]
        (cond
          (pos? sent) (recur (+ off sent))
          (and (neg? sent) (or (poller/eintr?) (poller/eagain?))) (recur off)
          :else (throw (ex-info (str "client send failed: errno " (ffi/errno)
                                     " " (ffi/errno-message (ffi/errno)))
                                {:sent sent :offset off :length n
                                 :errno (ffi/errno)
                                 :errno-message (ffi/errno-message (ffi/errno))})))))))

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
                    ;; a clean EOF with nothing read is "" — which is what
                    ;; several tests assert, so it is not worth announcing.
                    ;; An errno below is: that one distinguishes a peer that
                    ;; went away from a read that failed for another reason.
                    (zero? n) acc
                    ;; a signal is not the peer going away, and the three
                    ;; ways a recv can answer non-positive mean opposite
                    ;; things. Conflating them is what made `client-recv`
                    ;; answer nil where the test wanted "" (the peer closed).
                    ;; Defensive rather than load-bearing, like send-buf!'s:
                    ;; no signal in an automated run has a handler to
                    ;; interrupt anything. SO_RCVTIMEO's EAGAIN is NOT
                    ;; retried: that one really is "nothing came", and
                    ;; retrying it would hang the test.
                    (poller/eintr?) (recur acc)
                    :else     (do (when (zero? (alength acc))
                                    (println "  [recv: error errno" (ffi/errno)
                                             (ffi/errno-message (ffi/errno)) "fd" fd "]"))
                                  (when (pos? (alength acc)) acc)))))))
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
    (doseq [[i b] (map-indexed vector bs)] (ffi/write buf :uint8 b i))
    (try (send-buf! fd buf n)
         (finally (ffi/free buf)))))


(defn- t-fill! [fd]
  (let [buf (ffi/alloc 65536)]
    (try
      ;; EINTR retries here for the same reason it does in client-recv-raw: a
      ;; signal is not the end of the frame. Answering nil for one stopped
      ;; t-recv-n's fill loop short of the bytes it was asked for, and a
      ;; websocket frame parsed from a truncated buffer fails as a codec bug.
      (loop []
        (let [got (t-recv fd buf 65536 0)]
          (cond
            (pos? got)
            (do (swap! t-pending update fd
                       (fn [p] (into (vec p) (map #(ffi/read buf :uint8 %) (range got)))))
                true)
            (and (neg? got) (poller/eintr?)) (recur)
            :else nil)))
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
            (cond
              (pos? n) (recur (bcat acc (ffi/read-array buf n)))
              ;; EINTR is not the end of the response — same reasoning as
              ;; client-recv-raw. Returning acc here gave callers a partial (or
              ;; empty) response that reads as the server having answered
              ;; nothing: `uri: /a/b -> /a/b — no "..." in ""` was this.
              (and (neg? n) (poller/eintr?)) (recur acc)
              :else acc))))
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
(def idle-stream-channel (atom nil))
(def sse-quiet-channel (atom nil))

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
    (= uri "/stream-idle")
    ;; one chunk, then silence: the application has nothing more to say for
    ;; now, which is the normal state of an SSE stream between events
    (let [ch (a/chan)]
      (a/put! ch "first")
      (reset! idle-stream-channel ch)
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
    ;; the README's shape: event maps put straight on the channel
    (= uri "/sse-maps")
    (let [events (a/chan)]
      (a/go (a/>! events {:id 1 :event "greet" :data "hello\nworld"})
            (a/>! events {:data "bye"})
            (a/close! events))
      (sse/event-response events))
    (= uri "/sse-quiet")
    (let [events (a/chan)]
      (reset! sse-quiet-channel events)
      (a/put! events {:data "first"})
      (sse/event-response events))
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

(defn test-stream-idle-client-disconnect-reclaims []
  ;; A stream that is quiet cannot learn about its client from a failed write,
  ;; because there is no write. With one worker, a single such connection used
  ;; to take the whole server down with it.
  (let [server (adapter/run-server handler {:port 8425 :worker-threads 1})]
    (try
      (Thread/sleep 250)
      (reset! idle-stream-channel nil)
      (let [fd (client-connect 8425 5000)]
        (client-send fd "GET /stream-idle HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "idle stream: first chunk arrived" "first" (client-recv-until fd "first"))
        (client-close fd)                     ; go away, and stay quiet
        (Thread/sleep 2500)
        (check "idle stream: adapter closed the body channel" false
               (a/put! @idle-stream-channel "nobody is listening")))
      ;; the worker is back: with :worker-threads 1 this only answers if the
      ;; abandoned stream let go
      (let [r (http/get "http://127.0.0.1:8425/")]
        (check "idle stream: the only worker was reclaimed" 200 (:status r)))
      (finally (adapter/stop-server server)))))

(defn test-stream-idle-client-disconnect-reclaims-fibers []
  (let [server (adapter/run-server handler {:port 8426 :strategy :fibers})]
    (try
      (Thread/sleep 250)
      (reset! idle-stream-channel nil)
      (let [fd (client-connect 8426 5000)]
        (client-send fd "GET /stream-idle HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "idle stream (fibers): first chunk arrived" "first" (client-recv-until fd "first"))
        (client-close fd)
        (Thread/sleep 2500)
        (check "idle stream (fibers): adapter closed the body channel" false
               (a/put! @idle-stream-channel "nobody is listening")))
      (let [r (http/get "http://127.0.0.1:8426/")]
        (check "idle stream (fibers): server still serves" 200 (:status r)))
      (finally (adapter/stop-server server)))))

(defn test-stream-pipelined-request-is-not-a-disconnect []
  ;; readability is not the same as a hangup: a client that pipelines its next
  ;; request while a stream is running must not have the stream torn down
  (let [server (adapter/run-server handler {:port 8427 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (reset! idle-stream-channel nil)
      (let [fd (client-connect 8427 5000)]
        (client-send fd "GET /stream-idle HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "pipelined: first chunk arrived" "first" (client-recv-until fd "first"))
        ;; send the next request without reading the rest of this response
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (Thread/sleep 2500)
        (check "pipelined: stream still open" true
               (a/put! @idle-stream-channel "second"))
        (check-has "pipelined: and the chunk still arrives" "second"
                   (client-recv-until fd "second"))
        (client-close fd))
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
      ;; event maps put on the channel directly are formatted, not a dead stream
      (let [fd (client-connect 8412 5000)]
        (client-send fd "GET /sse-maps HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv-until fd "0\r\n\r\n")]
          (check-has "sse maps: id field" "id: 1" r)
          (check-has "sse maps: event field" "event: greet" r)
          (check-has "sse maps: multi-line data split" "data: hello\r\ndata: world" r)
          (check-has "sse maps: second event" "data: bye" r)
          (check-has "sse maps: chunk terminator" "0\r\n\r\n" r))
        (client-close fd))
      ;; a client that leaves a quiet stream still closes the producer's channel:
      ;; the next put is taken, the one after it finds the channel closed
      (let [fd (client-connect 8412 5000)]
        (client-send fd "GET /sse-quiet HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "sse quiet: first event arrived" "data: first" (client-recv-until fd "data: first"))
        (client-close fd)
        (Thread/sleep 2500)
        (let [ch @sse-quiet-channel]
          (a/>!! ch {:data "nobody"})
          (Thread/sleep 200)
          (check "sse quiet: producer's channel closed" false (a/>!! ch {:data "nobody"}))))
      (finally (adapter/stop-server server)))))

(defn test-stream-bad-chunk-reaches-on-failure []
  ;; a chunk that is neither a string nor bytes cannot be framed once the head
  ;; is out, so the connection closes; :on-failure is told why instead of the
  ;; stream just stopping
  (let [seen (promise)
        server (adapter/run-server
                (fn [_] {:status 200 :headers {"Content-Type" "text/plain"}
                         :body (doto (a/chan 1) (a/put! {:not "a chunk"}))})
                {:port 8599 :worker-threads 1
                 :on-failure (fn [req t]
                               (deliver seen [(:kind (:ring-chez/failure req)) (:type (ex-data t))])
                               nil)})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8599 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (client-recv fd)
        (client-close fd))
      (check "bad chunk: on-failure saw a :stream failure"
             [:stream :ring-chez/bad-chunk] (deref seen 3000 :never))
      (finally (adapter/stop-server server)))))

;; --- Phase 5: WebSocket ------------------------------------------------------

;; The handshake's SHA-1 is computed here rather than by jolt.crypto, so a server
;; never opens OpenSSL unless it serves :tls.
(defn test-ws-sha1 []
  (let [sha1 #'ring-chez.websocket/sha1
        hex (fn [s] (apply str (map #(format "%02x" %) (sha1 (.getBytes ^String s "UTF-8")))))]
    (check "sha1 of empty" "da39a3ee5e6b4b0d3255bfef95601890afd80709" (hex ""))
    (check "sha1 of abc" "a9993e364706816aba3e25717850c26c9cd0d89d" (hex "abc"))
    (check "sha1 of the two-block FIPS vector" "84983e441c3bd26ebaae4aa1f95129e5e54670f1"
           (hex "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))
    (check "sha1 of a million a's" "34aa973cd4c4daa4f61eeb2bdbad27316534016f"
           (hex (apply str (repeat 1000000 \a))))
    (check "RFC 6455 accept token" "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
           (ws/accept-token "dGhlIHNhbXBsZSBub25jZQ=="))))

(defn test-plain-server-does-not-load-crypto []
  (let [{:keys [out err exit]}
        (clojure.java.shell/sh
          "jolt" "-M" "-e"
          "(require 'ring-chez.adapter) (prn (some? (find-ns 'jolt.crypto)))"
          :dir (or (System/getenv "JOLT_PWD") (System/getProperty "user.dir")))]
    (check (str "requiring the adapter leaves jolt.crypto unloaded " (str/trim (str err)))
           [0 "false"] [exit (str/trim out)])))

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
  ;;
  ;; The read timeout is generous on purpose. What is asserted is fairness, not
  ;; latency: retiring an idle connection under pressure waits out a 2s grace
  ;; period first (never instantly, or a client mid-reuse races a reset), so
  ;; with 3 connections queued behind 2 workers the last one can legitimately
  ;; wait several seconds on a loaded machine. At 5s that tipped over on CI and
  ;; read as starvation. Real starvation still fails this: without retirement
  ;; the queued connections wait out the full 60s keep-alive timeout.
  (let [server (adapter/run-server handler {:port 8427 :worker-threads 2
                                            :keep-alive-timeout-ms 60000})]
    (Thread/sleep 250)
    (try
      (let [fds (mapv (fn [_] (client-connect 8427 20000)) (range 5))]
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

;; :fibers parks connections on jolt.io-poller, which has no Windows backend.
;; Where it has none, run-server refuses the strategy by name rather than
;; starting a server whose first read fails. Faked here so a POSIX run pins it.
(defn test-fibers-without-a-poller-throws []
  (with-redefs [poller/available? (constantly false)]
    (try
      (let [server (adapter/run-server handler {:port 8426 :strategy :fibers})]
        (adapter/stop-server server)
        (check "no poller: :fibers throws" :threw :did-not-throw))
      (catch Throwable t
        (check "no poller: :fibers throws" :threw :threw)
        (check-has "no poller: message says to use :threads" ":threads" (ex-message t))))))

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

;; --- port 0, and what a forked child inherits -------------------------------------

(defn test-port-zero-binds-a-free-port []
  (let [server (adapter/run-server handler {:port 0})
        port (:port server)]
    (try
      (check "port 0: the handle names the port the kernel picked" true (and (int? port) (pos? port)))
      (check "port 0: and the server answers on it" 200
             (:status (http/get (str "http://127.0.0.1:" port "/"))))
      (finally (adapter/stop-server server)))))

(defn test-listen-socket-is-close-on-exec []
  ;; Every process an application forks inherits open fds unless they are
  ;; marked. A child holding the listening socket keeps the port bound after
  ;; the server stops, so a restart fails with EADDRINUSE against a server
  ;; that is gone.
  (let [server (adapter/run-server handler {:port 0})
        port (:port server)
        child (jolt.process/process ["sleep" "20"] {})]
    (try
      (check "cloexec: the listening socket is marked" true (native/close-on-exec? (:socket server)))
      (adapter/stop-server server)
      (let [again (try (adapter/run-server handler {:port port})
                       (catch Throwable t {:error (ex-message t)}))]
        (check "cloexec: the port rebinds with a forked child still alive" nil (:error again))
        (when (:socket again) (adapter/stop-server again)))
      (finally (try (jolt.process/destroy-tree child) (catch Throwable _ nil))))))

;; --- RFC-0002: boot-time option validation -------------------------------------

(defn test-boot-validation []
  (doseq [[k v] [[:port "abc"] [:port -1] [:port 70000]
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

(defn test-ws-session-carries-request []
  ;; the handler sees the upgrade request the guard saw: path, query, headers
  (let [server (adapter/run-server handler
                  {:port 8497 :worker-threads 1
                   :ws-handler (fn [session]
                                 (let [req (:request session)]
                                   (ws/send! session (str (:uri req) "?" (:query-string req)
                                                          " " (get-in req [:headers "host"])))))})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8497 5000)]
        (ws-handshake fd "/ws?queue=q1")
        (check-has "session-request: 101 sent" "101" (ws-read-handshake! fd))
        (let [f (ws-read-server-frame fd)]
          (check "session-request: the upgrade request" "/ws?queue=q1 t"
                 (bytes->str (:payload f))))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-ws-close-with-status []
  ;; close! with a code (and reason) sends them in the close frame, as a
  ;; browser's CloseEvent reports them (RFC 6455 §5.5.1); without, the
  ;; frame stays empty
  (let [server (adapter/run-server handler
                  {:port 8598 :worker-threads 1
                   :ws-handler (fn [session]
                                 (let [m (ws/recv! session)]
                                   (case (:data m)
                                     "reason" (ws/close! session 1011 "cubi vanished")
                                     "code" (ws/close! session 1000)
                                     (ws/close! session))))})]
    (try
      (Thread/sleep 250)
      (doseq [[ask expect] [["reason" [3 243 "cubi vanished"]]
                            ["code" [3 232 ""]]
                            ["none" nil]]]
        (let [fd (client-connect 8598 5000)]
          (ws-handshake fd "/ws")
          (client-recv-until fd "\r\n\r\n")
          (t-send-bytes fd (ws-client-frame 0x1 (utf8-bytes ask)))
          (let [f (ws-read-server-frame fd)
                p (vec (:payload f))]
            (check (str "close-status " ask ": close opcode") 8 (:opcode f))
            (if expect
              (let [[hi lo reason] expect]
                (check (str "close-status " ask ": code") [hi lo] (subvec p 0 2))
                (check (str "close-status " ask ": reason") reason (bytes->str (subvec p 2))))
              (check (str "close-status " ask ": no status") 0 (count p))))
          (client-close fd)))
      (check "close-status: a code a peer may not see is refused" true
             (try (ws/close! {:closed? (atom false) :fd -1} 1005) false
                  (catch IllegalArgumentException _ true)))
      (check "close-status: a reason past 123 bytes is refused" true
             (try (ws/close! {:closed? (atom false) :fd -1} 1000 (apply str (repeat 124 "x"))) false
                  (catch IllegalArgumentException _ true)))
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
    (ffi/write v :int 2048 0)
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
                ;; a signal is not the close this is waiting for
                (poller/eintr?) (recur acc)
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

;; --- Wave 2 round 6: reuse-port and trusted-proxy addressing ----------------------

;; SO_REUSEPORT: several processes (here, several servers in one process) bind
;; the same port and the kernel spreads connections over them. Without it the
;; second bind must fail — that error is how you find out a server is already
;; running (RFC-0013).
(defn test-reuse-port []
  (let [h (fn [_] {:status 200 :headers {"Content-Type" "text/plain"} :body "ok"})
        s1 (adapter/run-server h {:port 8554 :reuse-port true})]
    (try
      (Thread/sleep 250)
      (let [s2 (try (adapter/run-server h {:port 8554 :reuse-port true})
                    (catch Throwable t t))]
        (check "reuse-port: second bind succeeds" false (instance? Throwable s2))
        (check "reuse-port: still serving" 200
               (:status (http/get "http://127.0.0.1:8554/")))
        (when-not (instance? Throwable s2) (adapter/stop-server s2)))
      (finally (adapter/stop-server s1))))
  ;; and without it, the second bind is refused with the message that names why
  (let [h  (fn [_] {:status 200 :body "ok"})
        s1 (adapter/run-server h {:port 8555})]
    (try
      (Thread/sleep 250)
      (let [t (try (adapter/run-server h {:port 8555}) nil (catch Throwable t t))]
        (check "no reuse-port: second bind refused" true (some? t))
        (check "no reuse-port: names EADDRINUSE" "EADDRINUSE"
               (:errno-name (ex-data t))))
      (finally (adapter/stop-server s1)))))

;; X-Forwarded-For is written by the client and appended to by each proxy, so
;; the left end is forgeable and the right end is not. ring's own middleware
;; takes the leftmost; Igropyr takes the Nth from the right, where N is a
;; deployment fact the operator states.
(defn test-trusted-proxy-addr []
  (let [seen (atom nil)
        h    (fn [req] (reset! seen (select-keys req [:remote-addr :ring-chez/peer-addr]))
               {:status 200 :headers {"Content-Type" "text/plain"}
                :body (str (:remote-addr req))})
        server (adapter/run-server
                 (proxy/wrap-forwarded-remote-addr h {:trust-proxy 1}) {:port 8556})]
    (try
      (Thread/sleep 250)
      ;; one trusted hop: take what OUR edge proxy saw, not what the client claimed
      (let [r (http/get "http://127.0.0.1:8556/"
                        {:headers {"X-Forwarded-For" "9.9.9.9, 10.0.0.9"}})]
        (check "proxy: takes the entry the trusted hop saw" "10.0.0.9" (:body r))
        (check "proxy: peer address kept" "127.0.0.1" (:ring-chez/peer-addr @seen)))
      ;; a client forging a whole chain cannot move the trusted position
      (let [r (http/get "http://127.0.0.1:8556/"
                        {:headers {"X-Forwarded-For" "1.1.1.1, 2.2.2.2, 3.3.3.3, 10.0.0.9"}})]
        (check "proxy: forged chain cannot move the pick" "10.0.0.9" (:body r)))
      ;; fewer entries than declared hops: fall back to the peer, never to a
      ;; client-supplied value
      (let [r (http/get "http://127.0.0.1:8556/" {:headers {"X-Forwarded-For" ""}})]
        (check "proxy: empty header falls back to peer" "127.0.0.1" (:body r)))
      (finally (adapter/stop-server server))))
  ;; trust nothing by default
  (let [h (fn [req] {:status 200 :headers {"Content-Type" "text/plain"}
                     :body (str (:remote-addr req))})
        server (adapter/run-server (proxy/wrap-forwarded-remote-addr h) {:port 8557})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8557/" {:headers {"X-Forwarded-For" "9.9.9.9"}})]
        (check "proxy: trust-proxy 0 ignores the header" "127.0.0.1" (:body r)))
      (finally (adapter/stop-server server))))
  ;; and the picker itself, at the boundaries
  (check "proxy: nth-from-right 1" "c" (proxy/nth-from-right "a, b, c" 1))
  (check "proxy: nth-from-right 3" "a" (proxy/nth-from-right "a, b, c" 3))
  (check "proxy: nth-from-right past the end" nil (proxy/nth-from-right "a, b" 3))
  (check "proxy: nth-from-right of nothing" nil (proxy/nth-from-right nil 1)))

;; --- Wave 2 round 5: static files -------------------------------------------------

;; wrap-file works under jolt but stats and re-opens on every request. This is
;; Igropyr's cache in Ring's shape: a hot file is a map lookup, the mtime is
;; re-checked at most once a second, the gzip copy is kept beside the plain one
;; and a file too big to cache is streamed from disk (RFC-0012).

(defn- static-root
  "A throwaway tree: index.html, a css file over the gzip floor, a binary file
  over the cache cap, a dotfile, and a symlink pointing out of the root."
  []
  (let [root (java.io.File. (str (System/getProperty "java.io.tmpdir")
                                 "/ring-chez-static-" (System/currentTimeMillis)))
        sub  (java.io.File. root "sub")
        _    (.mkdirs sub)
        spit! (fn [^java.io.File f ^String s]
                (with-open [o (java.io.FileOutputStream. f)]
                  (.write o (.getBytes s "UTF-8"))))]
    (spit! (java.io.File. root "index.html") "<h1>index</h1>")
    (spit! (java.io.File. root "app.css") (apply str (repeat 300 "body { margin: 0; } ")))
    (spit! (java.io.File. root ".env") "SECRET=hunter2")
    (spit! (java.io.File. sub "page.html") "<p>sub page</p>")
    (spit! (java.io.File. root "small.txt") "small")
    ;; over the 1 MiB cache cap on purpose: served as a File body, streamed
    (with-open [o (java.io.FileOutputStream. (java.io.File. root "big.bin"))]
      (.write o (byte-array (repeat 1500000 (byte 7)))))
    ;; a secret next to the root, and a symlink inside it pointing at it
    (spit! (java.io.File. (.getParentFile root) (str (.getName root) "-secret.txt"))
           "outside the root")
    (try
      (java.lang.Runtime/getRuntime)
      (let [p (.start (ProcessBuilder.
                        ["ln" "-s"
                         (str (.getPath (.getParentFile root)) "/" (.getName root) "-secret.txt")
                         (str (.getPath root) "/link.txt")]))]
        (.waitFor p))
      (catch Throwable _ nil))
    root))

(defn- static-get [fd path & [accept-encoding extra]]
  (client-send fd (str "GET " path " HTTP/1.1\r\nHost: t\r\n"
                       (when accept-encoding (str "Accept-Encoding: " accept-encoding "\r\n"))
                       (or extra "")
                       "\r\n"))
  (let [raw (client-recv-raw fd)]
    [(String. raw "ISO-8859-1") raw]))

(defn test-static-serves []
  (let [root   (static-root)
        server (adapter/run-server (fn [_] {:status 404 :headers {"Content-Type" "text/plain"}
                                            :body "handler"})
                                   {:port 8548})
        app    (static/wrap-static (fn [_] {:status 404 :headers {"Content-Type" "text/plain"}
                                            :body "handler"})
                                   (.getPath root)
                                   {:cache (static/new-cache)})]
    (adapter/swap-handler! server app)
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8548 5000)]
        (let [[view raw] (static-get fd "/index.html")]
          (check-has "static: served" "200 OK" view)
          (check-has "static: content-type" "Content-Type: text/html; charset=utf-8" view)
          (check-has "static: etag" "ETag: W/\"" view)
          (check "static: body" "<h1>index</h1>"
                 (String. ^bytes (response-body-bytes raw) "UTF-8")))
        ;; a directory serves its index
        (let [[view raw] (static-get fd "/")]
          (check-has "static: directory index" "200 OK" view)
          (check "static: index body" "<h1>index</h1>"
                 (String. ^bytes (response-body-bytes raw) "UTF-8")))
        (let [[view _] (static-get fd "/sub/page.html")]
          (check-has "static: nested file" "200 OK" view))
        ;; a miss falls through to the handler rather than answering itself
        (let [[view _] (static-get fd "/nope.html")]
          (check-has "static: miss falls through" "handler" view))
        ;; and so does a POST
        (client-send fd "POST /index.html HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n")
        (check-has "static: POST falls through" "handler" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-static-conditional []
  (let [root   (static-root)
        app    (static/wrap-static (fn [_] {:status 404 :body "no"}) (.getPath root)
                                   {:cache (static/new-cache)})
        server (adapter/run-server app {:port 8549})]
    (try
      (Thread/sleep 250)
      (let [fd   (client-connect 8549 5000)
            [view _] (static-get fd "/index.html")
            etag (second (re-find #"ETag: (\S+)" view))]
        (check "static: etag present" true (some? etag))
        (let [[view2 _] (static-get fd "/index.html" nil (str "If-None-Match: " etag "\r\n"))]
          (check-has "static: revalidation is 304" "304 Not Modified" view2)
          (check "static: 304 carries no body" true (not (str/includes? view2 "<h1>"))))
        ;; a stale validator gets the file
        (let [[view3 _] (static-get fd "/index.html" nil "If-None-Match: W/\"deadbeef\"\r\n")]
          (check-has "static: stale validator gets 200" "200 OK" view3))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-static-gzip []
  (let [root   (static-root)
        app    (static/wrap-static (fn [_] {:status 404 :body "no"}) (.getPath root)
                                   {:cache (static/new-cache)})
        server (adapter/run-server app {:port 8550})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8550 5000)
            [view raw] (static-get fd "/app.css" "gzip")]
        (check-has "static gzip: encoded" "Content-Encoding: gzip" view)
        (check-has "static gzip: vary" "Vary: Accept-Encoding" view)
        (check-has "static gzip: distinct etag" "-gz\"" view)
        (check "static gzip: round-trips" true
               (str/includes? (String. ^bytes (gunzip (response-body-bytes raw)) "UTF-8")
                              "body { margin: 0; }"))
        ;; served twice: the second comes from the cached compressed copy
        (let [[view2 raw2] (static-get fd "/app.css" "gzip")]
          (check-has "static gzip: second hit still encoded" "Content-Encoding: gzip" view2)
          (check "static gzip: second hit identical" true
                 (java.util.Arrays/equals ^bytes (response-body-bytes raw)
                                          ^bytes (response-body-bytes raw2))))
        ;; a client that does not accept it gets the plain file
        (let [[view3 _] (static-get fd "/app.css" nil)]
          (check "static gzip: plain when not accepted" false
                 (str/includes? view3 "Content-Encoding: gzip")))
        ;; under the floor: not worth compressing
        (let [[view4 _] (static-get fd "/small.txt" "gzip")]
          (check "static gzip: under floor untouched" false
                 (str/includes? view4 "Content-Encoding: gzip")))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-static-large-file []
  (let [root   (static-root)
        app    (static/wrap-static (fn [_] {:status 404 :body "no"}) (.getPath root)
                                   {:cache (static/new-cache)})
        server (adapter/run-server app {:port 8551})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8551 15000)
            [view raw] (static-get fd "/big.bin")
            body (response-body-bytes raw)]
        (check-has "static large: served" "200 OK" view)
        (check-has "static large: real content-length" "Content-Length: 1500000" view)
        (check "static large: full body streamed" 1500000 (alength ^bytes body))
        (check "static large: content intact" true
               (every? #(= (byte 7) %) (take 1000 (seq body))))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-static-path-safety []
  (let [root   (static-root)
        app    (static/wrap-static (fn [_] {:status 404 :headers {"Content-Type" "text/plain"}
                                            :body "handler"})
                                   (.getPath root)
                                   {:cache (static/new-cache)})
        server (adapter/run-server app {:port 8552})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8552 5000)
            refused (fn [path]
                      (let [[view _] (static-get fd path)]
                        (and (not (str/includes? view "SECRET=hunter2"))
                             (not (str/includes? view "outside the root")))))]
        ;; a dotfile is not servable: mounting a project directory must not
        ;; hand out .env or .git/config
        (check "static safety: dotfile refused" true (refused "/.env"))
        ;; the adapter normalizes .. out of :uri, and the encoded form must not
        ;; walk past the check either
        (check "static safety: encoded traversal refused" true
               (refused "/%2e%2e/%2e%2e/etc/passwd"))
        (check "static safety: encoded dotfile refused" true (refused "/%2Eenv"))
        ;; a symlink pointing out of the root is not followed
        (check "static safety: symlink out of root refused" true (refused "/link.txt"))
        ;; ordinary files still work after all that
        (let [[view _] (static-get fd "/index.html")]
          (check-has "static safety: normal file still served" "200 OK" view))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; the cache is the point: a file edited past the stat window is re-read, and
;; one edited inside it is not (that is what makes a hit free)
(defn test-static-cache-window []
  (let [root   (static-root)
        f      (java.io.File. root "index.html")
        app    (static/wrap-static (fn [_] {:status 404 :body "no"}) (.getPath root)
                                   {:cache (static/new-cache) :stat-window-ms 300})
        server (adapter/run-server app {:port 8553})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8553 5000)
            body-of (fn [] (let [[_ raw] (static-get fd "/index.html")]
                             (String. ^bytes (response-body-bytes raw) "UTF-8")))]
        (check "static cache: first read" "<h1>index</h1>" (body-of))
        (with-open [o (java.io.FileOutputStream. f)]
          (.write o (.getBytes "<h1>edited</h1>" "UTF-8")))
        ;; the mtime has to move for the re-read to notice, and mtime is
        ;; second-granular on some filesystems
        (.setLastModified f (+ (System/currentTimeMillis) 2000))
        (Thread/sleep 500)
        (check "static cache: re-read past the window" "<h1>edited</h1>" (body-of))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- Wave 2 round 4: gzip ---------------------------------------------------------

;; jolt has no java.util.zip, so no existing Ring gzip middleware can load.
;; This is Igropyr's policy over a zlib binding; the oracle for every check is
;; java.util.zip's GZIPInputStream, a separate implementation (RFC-0011).

(def ^:private path-big "/big")

(def ^:private gzip-text
  (apply str (repeat 400 "the quick brown fox jumps over the lazy dog. ")))

(def ^:private gzip-events
  ;; varied rows, so the first event does not already compress to nothing and
  ;; the saving on the repeats is the window's doing
  (let [rows (apply str (for [i (range 300)] (str "<li id=\"r" i "\">" (* i 7919 i) "</li>")))]
    (mapv #(str "event: patch\r\ndata: " rows " #" % "\r\n\r\n") (range 3))))

(def ^:private gzip-producer-stopped (atom false))

(defn- gzip-test-handler [{:keys [uri]}]
  (case uri
    "/big"    {:status 200 :headers {"Content-Type" "text/plain"} :body gzip-text}
    "/etag"   {:status 200 :headers {"Content-Type" "text/plain" "ETag" "\"abc123\""}
               :body gzip-text}
    "/small"  {:status 200 :headers {"Content-Type" "text/plain"} :body "tiny"}
    "/png"    {:status 200 :headers {"Content-Type" "image/png"} :body gzip-text}
    "/preenc" {:status 200 :headers {"Content-Type" "text/plain"
                                     "Content-Encoding" "br"} :body gzip-text}
    "/seq"    {:status 200 :headers {"Content-Type" "text/plain"}
               :body [gzip-text gzip-text]}
    "/events" {:status 200 :headers {"Content-Type" "text/event-stream"}
               :body (let [ch (a/chan)]
                       (a/go (doseq [e gzip-events] (a/>! ch e))
                             (a/close! ch))
                       ch)}
    "/events-forever"
    {:status 200 :headers {"Content-Type" "text/event-stream"}
     :body (let [ch (a/chan)]
             (a/go (loop []
                     (if (a/>! ch (first gzip-events))
                       (do (a/<! (a/timeout 50)) (recur))
                       (reset! gzip-producer-stopped true))))
             ch)}
    {:status 404 :headers {"Content-Type" "text/plain"} :body "no"}))

(defn- gzip-get
  "One request over a raw socket (the http client would transparently decode a
  gzip response and hide what we are testing). Returns [head-string body-bytes]."
  [fd path accept-encoding]
  (client-send fd (str "GET " path " HTTP/1.1\r\nHost: t\r\n"
                       (when accept-encoding (str "Accept-Encoding: " accept-encoding "\r\n"))
                       "\r\n"))
  (let [raw (client-recv-raw fd)]
    [(String. raw "ISO-8859-1") (response-body-bytes raw)]))

(defn test-gzip-compresses []
  (let [server (adapter/run-server (gzip/wrap-gzip gzip-test-handler) {:port 8544})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8544 5000)
            [view body] (gzip-get fd "/big" "gzip")]
        (check-has "gzip: Content-Encoding" "Content-Encoding: gzip" view)
        (check-has "gzip: Vary" "Vary: Accept-Encoding" view)
        (check "gzip: smaller than the original" true
               (< (alength body) (count (.getBytes gzip-text "UTF-8"))))
        (check "gzip: body round-trips" gzip-text
               (String. ^bytes (gunzip body) "UTF-8"))
        ;; the framing has to survive: Content-Length must count the COMPRESSED
        ;; octets, or the next response on this connection starts mid-body
        (let [[view2 body2] (gzip-get fd "/small" "gzip")]
          (check-has "gzip: connection still framed" "200 OK" view2)
          (check "gzip: next body intact" "tiny" (String. ^bytes body2 "UTF-8")))
        ;; a seq body is compressed as the octets it concatenates to
        (let [[_ body3] (gzip-get fd "/seq" "gzip")]
          (check "gzip: seq body round-trips" (str gzip-text gzip-text)
                 (String. ^bytes (gunzip body3) "UTF-8")))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn- chunk-parts
  "A chunked transfer body -> its chunks' octets, in order."
  [^bytes body]
  (let [view (latin1 body)]
    (loop [i 0 parts []]
      (let [e (str/index-of view "\r\n" i)
            n (Long/parseLong (str/trim (subs view i e)) 16)]
        (if (pos? n)
          (recur (+ e 2 n 2) (conj parts (java.util.Arrays/copyOfRange body (+ e 2) (+ e 2 n))))
          parts)))))

(defn test-gzip-stream []
  (let [server (adapter/run-server (gzip/wrap-gzip gzip-test-handler) {:port 8548})]
    (try
      (Thread/sleep 250)
      (let [fd  (client-connect 8548 5000)
            _   (client-send fd "GET /events HTTP/1.1\r\nHost: t\r\nAccept-Encoding: gzip\r\n\r\n")
            raw (client-recv-until-bytes fd "\r\n0\r\n\r\n")
            view (latin1 raw)
            body-start (+ 4 (str/index-of view "\r\n\r\n"))
            body (java.util.Arrays/copyOfRange raw body-start (alength raw))
            parts (chunk-parts body)
            plain (apply str gzip-events)]
        (check-has "gzip stream: Content-Encoding" "Content-Encoding: gzip" view)
        (check-has "gzip stream: still chunked" "Transfer-Encoding: chunked" view)
        (check "gzip stream: one chunk per event plus the trailer" 4 (count parts))
        ;; a sync flush ends on the empty stored block 00 00 ff ff: the client
        ;; can decode each event without waiting for the next
        (check "gzip stream: every event flushed" true
               (every? #(= [0 0 -1 -1] (vec (take-last 4 (seq %)))) (butlast parts)))
        ;; the window carries over, so a repeated event costs far less than
        ;; the first
        (check "gzip stream: repeats are cheap" true
               (< (* 4 (alength ^bytes (nth parts 1))) (alength ^bytes (first parts))))
        (check "gzip stream: round-trips" plain
               (String. ^bytes (gunzip (dechunk body)) "UTF-8"))
        (client-close fd))
      ;; a client that goes away still stops the producer
      (reset! gzip-producer-stopped false)
      (let [fd (client-connect 8548 5000)]
        (client-send fd "GET /events-forever HTTP/1.1\r\nHost: t\r\nAccept-Encoding: gzip\r\n\r\n")
        (client-recv-until-bytes fd "\r\n\r\n")
        (client-close fd)
        (Thread/sleep 1500)
        (check "gzip stream: producer told the client left" true @gzip-producer-stopped))
      ;; not accepted: the channel is streamed as it is
      (let [fd (client-connect 8548 5000)
            _  (client-send fd "GET /events HTTP/1.1\r\nHost: t\r\n\r\n")
            r  (client-recv-until fd "\r\n0\r\n\r\n")]
        (check "gzip stream: plain when not accepted" false (str/includes? r "Content-Encoding: gzip"))
        (check-has "gzip stream: plain events" "#2" r)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-gzip-negotiation []
  (let [server (adapter/run-server (gzip/wrap-gzip gzip-test-handler) {:port 8545})]
    (try
      (Thread/sleep 250)
      (let [gzipped? (fn [ae]
                       (let [fd (client-connect 8545 5000)
                             [view _] (gzip-get fd path-big ae)]
                         (client-close fd)
                         (str/includes? view "Content-Encoding: gzip")))]
        (check "gzip: no Accept-Encoding" false (gzipped? nil))
        (check "gzip: identity only" false (gzipped? "identity"))
        (check "gzip: plain gzip" true (gzipped? "gzip"))
        (check "gzip: deflate, gzip" true (gzipped? "deflate, gzip"))
        (check "gzip: q-value" true (gzipped? "gzip;q=0.5"))
        ;; q=0 means NOT acceptable — the client is saying it cannot decode it
        (check "gzip: gzip;q=0 refused" false (gzipped? "gzip;q=0"))
        (check "gzip: gzip;q=0.0 refused" false (gzipped? "gzip;q=0.0"))
        ;; the wildcard decides only when nothing names gzip
        (check "gzip: wildcard alone" true (gzipped? "*"))
        (check "gzip: explicit beats wildcard" true (gzipped? "*;q=0, gzip"))
        (check "gzip: explicit q=0 beats wildcard" false (gzipped? "*, gzip;q=0"))
        ;; a coding that merely contains the letters is not gzip
        (check "gzip: not a substring match" false (gzipped? "notgzip")))
      (finally (adapter/stop-server server)))))

(defn test-gzip-skips []
  (let [server (adapter/run-server (gzip/wrap-gzip gzip-test-handler) {:port 8546})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8546 5000)]
        (let [[view body] (gzip-get fd "/small" "gzip")]
          (check "gzip: under min-size untouched" false
                 (str/includes? view "Content-Encoding: gzip"))
          (check "gzip: under min-size body intact" "tiny" (String. ^bytes body "UTF-8")))
        (let [[view _] (gzip-get fd "/png" "gzip")]
          (check "gzip: incompressible type untouched" false
                 (str/includes? view "Content-Encoding: gzip")))
        (let [[view _] (gzip-get fd "/preenc" "gzip")]
          (check "gzip: already-encoded body left alone" false
                 (str/includes? view "Content-Encoding: gzip"))
          (check-has "gzip: its own encoding preserved" "Content-Encoding: br" view))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-gzip-etag []
  (let [server (adapter/run-server (gzip/wrap-gzip gzip-test-handler) {:port 8547})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8547 5000)
            [view _] (gzip-get fd "/etag" "gzip")]
        ;; a gzipped body is a different entity: sharing the plain validator
        ;; lets a cache serve one for the other
        (check-has "gzip: etag made distinct" "ETag: \"abc123-gz\"" view)
        (client-close fd))
      (let [fd (client-connect 8547 5000)
            [view _] (gzip-get fd "/etag" nil)]
        (check-has "gzip: plain etag unchanged" "ETag: \"abc123\"" view)
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- zstd ----------------------------------------------------------------------
;; The oracle is the zstd command-line tool reading a whole frame, a separate
;; program from ring-chez.zstd. It refuses to print anything from a truncated
;; frame, so the "decodable on arrival" check uses ring-chez.zstd's own
;; decoder over a prefix of the stream.

(defn- unzstd
  "Decode a complete zstd frame with the zstd CLI."
  [^bytes bs]
  (let [f (java.io.File/createTempFile "rc-zstd" ".zst")]
    (try
      (with-open [o (io/output-stream f)] (.write o bs))
      (let [{:keys [exit out err]} (clojure.java.shell/sh "zstd" "-dc" (str f))]
        (if (zero? exit) out (str "zstd failed: " err)))
      (finally (.delete f)))))

(def ^:private zstd-cli?
  (delay (try (zero? (:exit (clojure.java.shell/sh "zstd" "--version")))
              (catch Throwable _ false))))

(defn test-zstd-library []
  (check "zstd: library bound" true (zstd/available?))
  (let [raw (.getBytes ^String gzip-text "UTF-8")
        z   (zstd/compress raw)]
    (check "zstd: compresses" true (< (alength z) (quot (alength raw) 10)))
    (check "zstd: decompress round-trips" gzip-text
           (String. ^bytes (zstd/decompress z) "UTF-8"))
    (when @zstd-cli?
      (check "zstd: CLI decodes it" gzip-text (unzstd z))))
  ;; levels past 19 need windows beyond the 8 MB HTTP decoders must accept
  (check "zstd: ultra levels refused" :threw
         (try (zstd/compressor {:level 22}) (catch Throwable _ :threw)))
  (check "zstd: window over 2^23 refused" :threw
         (try (zstd/compressor {:window-log 24}) (catch Throwable _ :threw)))
  ;; the reason for zstd on a stream: a re-render that repeats a page bigger
  ;; than deflate's 32 KB window costs almost nothing
  (let [page (apply str (for [i (range 1500)]
                          (str "<li data-id=\"" (* i 7919) "\">row " i " " (hash i) "</li>")))
        c    (zstd/compressor {})
        a    (zstd/compress-chunk! c (.getBytes ^String page "UTF-8"))
        b    (zstd/compress-chunk! c (.getBytes ^String (str page "x") "UTF-8"))
        end  (zstd/compress-finish! c)]
    (check "zstd stream: second render under 100 octets" true (< (alength ^bytes b) 100))
    (check "zstd stream: first chunk decodable alone" page
           (String. ^bytes (zstd/decompress a) "UTF-8"))
    (check "zstd stream: whole stream round-trips" (str page page "x")
           (String. ^bytes (zstd/decompress (byte-array (concat a b end))) "UTF-8"))
    (check "zstd stream: finish released it" :threw
           (try (zstd/compress-chunk! c (byte-array 1)) (catch Throwable _ :threw)))))

(defn test-zstd-negotiation []
  (let [server (adapter/run-server (compress/wrap-compress gzip-test-handler) {:port 8610})]
    (try
      (Thread/sleep 250)
      (let [coding (fn [ae]
                     (let [fd (client-connect 8610 5000)
                           [view _] (gzip-get fd path-big ae)]
                       (client-close fd)
                       (second (re-find #"(?i)Content-Encoding: (\S+)" view))))]
        (check "compress: none asked" nil (coding nil))
        (check "compress: zstd" "zstd" (coding "zstd"))
        (check "compress: gzip only" "gzip" (coding "gzip"))
        ;; equal q: the server's preference, zstd first
        (check "compress: browser list prefers zstd" "zstd" (coding "gzip, deflate, br, zstd"))
        (check "compress: client q wins" "gzip" (coding "zstd;q=0.5, gzip"))
        (check "compress: zstd;q=0 refused" "gzip" (coding "zstd;q=0, gzip"))
        (check "compress: wildcard" "zstd" (coding "*"))
        (check "compress: explicit q=0 beats wildcard" "gzip" (coding "*, zstd;q=0"))
        (check "compress: nothing acceptable" nil (coding "br")))
      (let [fd (client-connect 8610 5000)
            [view body] (gzip-get fd "/etag" "zstd")]
        (check-has "compress: zstd etag distinct" "ETag: \"abc123-zst\"" view)
        (check-has "compress: zstd Vary" "Vary: Accept-Encoding" view)
        (check "compress: zstd body round-trips" gzip-text
               (String. ^bytes (zstd/decompress body) "UTF-8"))
        (when @zstd-cli?
          (check "compress: zstd body CLI-decodable" gzip-text (unzstd body)))
        ;; framing still right on the next response of this connection
        (let [[view2 body2] (gzip-get fd "/small" "zstd")]
          (check-has "compress: connection still framed" "200 OK" view2)
          (check "compress: next body intact" "tiny" (String. ^bytes body2 "UTF-8")))
        (client-close fd))
      (finally (adapter/stop-server server))))
  ;; wrap-gzip stays gzip-only
  (let [server (adapter/run-server (gzip/wrap-gzip gzip-test-handler) {:port 8611})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8611 5000)
            [view _] (gzip-get fd path-big "zstd")]
        (check "wrap-gzip: never zstd" false (str/includes? view "Content-Encoding"))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-zstd-stream []
  (let [server (adapter/run-server (compress/wrap-compress gzip-test-handler) {:port 8612})]
    (try
      (Thread/sleep 250)
      (let [fd  (client-connect 8612 5000)
            _   (client-send fd "GET /events HTTP/1.1\r\nHost: t\r\nAccept-Encoding: zstd\r\n\r\n")
            raw (client-recv-until-bytes fd "\r\n0\r\n\r\n")
            view (latin1 raw)
            body (java.util.Arrays/copyOfRange raw (+ 4 (str/index-of view "\r\n\r\n")) (alength raw))
            parts (chunk-parts body)
            plain (apply str gzip-events)]
        (check-has "zstd stream: Content-Encoding" "Content-Encoding: zstd" view)
        (check-has "zstd stream: still chunked" "Transfer-Encoding: chunked" view)
        (check "zstd stream: one chunk per event plus the epilogue" 4 (count parts))
        ;; flushed: the first chunk alone decodes to the first event
        (check "zstd stream: first event decodable on arrival" (first gzip-events)
               (String. ^bytes (zstd/decompress (first parts)) "UTF-8"))
        (check "zstd stream: repeats are cheap" true
               (< (* 10 (alength ^bytes (nth parts 1))) (alength ^bytes (first parts))))
        (check "zstd stream: round-trips" plain
               (String. ^bytes (zstd/decompress (dechunk body)) "UTF-8"))
        (when @zstd-cli?
          (check "zstd stream: CLI-decodable" plain (unzstd (dechunk body))))
        (client-close fd))
      (reset! gzip-producer-stopped false)
      (let [fd (client-connect 8612 5000)]
        (client-send fd "GET /events-forever HTTP/1.1\r\nHost: t\r\nAccept-Encoding: zstd\r\n\r\n")
        (client-recv-until-bytes fd "\r\n\r\n")
        (client-close fd)
        (Thread/sleep 1500)
        (check "zstd stream: producer told the client left" true @gzip-producer-stopped))
      (finally (adapter/stop-server server)))))

;; --- Wave 2 round 3: multipart uploads -------------------------------------------

;; ring's own wrap-multipart-params is commons-fileupload based and cannot load
;; under jolt. This is the same middleware over jolt-lang/multipart, proved
;; end-to-end: a real upload over the wire, through the adapter's own body
;; framing, with a payload that does not survive a UTF-8 round trip (RFC-0010).

(def ^:private upload-bytes
  ;; every byte value, so anything that decodes or re-encodes the payload
  ;; shows up as a mismatch rather than as a lucky pass
  (byte-array (map #(unchecked-byte %) (range 256))))

(defn- multipart-body
  "A two-part multipart/form-data body: one text field, one binary upload."
  [boundary ^bytes file-bytes]
  (let [head (str "--" boundary "\r\n"
                  "Content-Disposition: form-data; name=\"title\"\r\n\r\n"
                  "a picture\r\n"
                  "--" boundary "\r\n"
                  "Content-Disposition: form-data; name=\"file\"; filename=\"all.bin\"\r\n"
                  "Content-Type: application/octet-stream\r\n\r\n")
        tail (str "\r\n--" boundary "--\r\n")]
    (byte-array (concat (.getBytes head "UTF-8")
                        (seq file-bytes)
                        (.getBytes tail "UTF-8")))))

(defn- upload-handler [{:keys [multipart-params params]}]
  (let [file (get multipart-params "file")]
    {:status 200
     :headers {"Content-Type" "application/octet-stream"
               "X-Title"      (str (get multipart-params "title"))
               ;; :params must carry the same keys (Ring merges them)
               "X-Title-Params" (str (get params "title"))
               "X-Filename"   (str (:filename file))
               "X-File-Type"  (str (:content-type file))
               "X-Size"       (str (:size file))}
     ;; echo the octets back: the comparison is byte-for-byte
     :body (:bytes file)}))

(defn test-multipart-upload []
  (let [server (adapter/run-server (multipart/wrap-multipart-params upload-handler)
                                   {:port 8540})]
    (try
      (Thread/sleep 250)
      (let [boundary "----ringchez7Zf9"
            body     (multipart-body boundary upload-bytes)
            head     (str "POST /upload HTTP/1.1\r\nHost: t\r\n"
                          "Content-Type: multipart/form-data; boundary=" boundary "\r\n"
                          "Content-Length: " (alength body) "\r\n\r\n")
            fd       (client-connect 8540 5000)]
        (client-send fd head)
        (t-send-bytes fd (map #(bit-and 0xff (long %)) (seq body)))
        (let [raw  (client-recv-raw fd)
              view (String. raw "ISO-8859-1")
              got  (response-body-bytes raw)]
          (check-has "multipart: text field" "X-Title: a picture" view)
          (check-has "multipart: field also in :params" "X-Title-Params: a picture" view)
          (check-has "multipart: filename" "X-Filename: all.bin" view)
          (check-has "multipart: part content-type" "X-File-Type: application/octet-stream" view)
          (check-has "multipart: size" (str "X-Size: " (alength upload-bytes)) view)
          (check "multipart: upload survives byte-for-byte" true
                 (java.util.Arrays/equals ^bytes upload-bytes ^bytes got)))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; The interesting failures are not in the parser, they are at the seam: a body
;; big enough to span many recv calls, arriving chunked, with a boundary that
;; can land across a read. Repeated names ride along — Ring collects them into
;; a vector and so must this.
(defn test-multipart-large-chunked []
  (let [big (byte-array (map #(unchecked-byte (mod % 251)) (range 300000)))
        h   (fn [{:keys [multipart-params]}]
              (let [tags (get multipart-params "tag")
                    file (get multipart-params "file")]
                {:status 200
                 :headers {"Content-Type" "text/plain"
                           "X-Tags" (str/join "," (if (vector? tags) tags [tags]))
                           "X-Size" (str (:size file))
                           ;; a cheap checksum: the parser must not have
                           ;; dropped or reordered a read boundary
                           "X-Sum" (str (reduce + 0 (map #(bit-and 0xff (long %))
                                                         (seq (:bytes file)))))}
                 :body "ok"}))
        server (adapter/run-server (multipart/wrap-multipart-params h)
                                   {:port 8542 :max-request-bytes 2097152})]
    (try
      (Thread/sleep 250)
      (let [boundary "----ringchezBig"
            head-str (str "--" boundary "\r\n"
                          "Content-Disposition: form-data; name=\"tag\"\r\n\r\nred\r\n"
                          "--" boundary "\r\n"
                          "Content-Disposition: form-data; name=\"tag\"\r\n\r\nblue\r\n"
                          "--" boundary "\r\n"
                          "Content-Disposition: form-data; name=\"file\"; filename=\"big.bin\"\r\n"
                          "Content-Type: application/octet-stream\r\n\r\n")
            body (byte-array (concat (.getBytes head-str "UTF-8")
                                     (seq big)
                                     (.getBytes (str "\r\n--" boundary "--\r\n") "UTF-8")))
            fd   (client-connect 8542 15000)]
        (client-send fd (str "POST /upload HTTP/1.1\r\nHost: t\r\n"
                             "Content-Type: multipart/form-data; boundary=" boundary "\r\n"
                             "Transfer-Encoding: chunked\r\n\r\n"))
        ;; chunk it small enough that boundaries and part headers straddle reads
        (doseq [part (partition-all 8192 (seq body))]
          (client-send fd (str (Integer/toHexString (count part)) "\r\n"))
          (t-send-bytes fd (map #(bit-and 0xff (long %)) part))
          (client-send fd "\r\n"))
        (client-send fd "0\r\n\r\n")
        (let [view (client-recv fd)]
          (check-has "multipart chunked: repeated names collect" "X-Tags: red,blue" view)
          (check-has "multipart chunked: full size" (str "X-Size: " (alength big)) view)
          (check-has "multipart chunked: payload intact"
                     (str "X-Sum: " (reduce + 0 (map #(bit-and 0xff (long %)) (seq big))))
                     view))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; a truncated body must not take the server down with it
(defn test-multipart-truncated []
  (let [h (fn [{:keys [multipart-params]}]
            {:status 200 :headers {"Content-Type" "text/plain"}
             :body (str "params=" (pr-str (keys multipart-params)))})
        server (adapter/run-server (multipart/wrap-multipart-params h) {:port 8543})]
    (try
      (Thread/sleep 250)
      (let [boundary "----ringchezCut"
            ;; no closing --boundary--
            body (str "--" boundary "\r\n"
                      "Content-Disposition: form-data; name=\"a\"\r\n\r\nvalue\r\n")
            r (try (http/post "http://127.0.0.1:8543/upload"
                              {:body body
                               :content-type (str "multipart/form-data; boundary=" boundary)
                               :throw-exceptions false})
                   (catch Throwable t (ex-data t)))]
        (check "multipart truncated: answered, not hung" true (some? (:status r))))
      ;; and the server is still healthy
      (check "multipart truncated: server still serves" 200
             (:status (http/get "http://127.0.0.1:8543/x")))
      (finally (adapter/stop-server server)))))

;; the middleware must be invisible to everything that is not an upload
(defn test-multipart-passthrough []
  (let [seen (atom nil)
        h    (fn [req]
               (reset! seen (select-keys req [:multipart-params :params]))
               {:status 200 :headers {"Content-Type" "text/plain"}
                :body (str "body=" (if-let [b (:body req)] (slurp b) ""))})
        server (adapter/run-server (multipart/wrap-multipart-params h) {:port 8541})]
    (try
      (Thread/sleep 250)
      (let [r (http/post "http://127.0.0.1:8541/x"
                         {:body "name=value" :content-type "application/x-www-form-urlencoded"})]
        (check "multipart passthrough: served" 200 (:status r))
        (check-has "multipart passthrough: body untouched" "body=name=value" (:body r))
        (check "multipart passthrough: no :multipart-params" nil (:multipart-params @seen)))
      (finally (adapter/stop-server server)))))

;; The parser moved back out to jolt-lang/multipart; ring-chez.multipart.core
;; stays as a re-export so apps that required it directly keep working.
(defn test-multipart-core-reexport []
  (let [boundary "reexportBoundary"
        req {:headers {"content-type" (str "multipart/form-data; boundary=" boundary)}
             :body (multipart-body boundary upload-bytes)}
        {:keys [params files]} (rc-mp/parse-form-data req)]
    (check "multipart re-export: text field" "a picture" (get params "title"))
    (check "multipart re-export: upload intact" true
           (= (seq upload-bytes) (seq (:bytes (get files "file")))))
    (check "multipart re-export: predicates" true
           (rc-mp/is-form-request? "multipart/form-data; boundary=x"))))

;; stop-server used to close the listen fd itself and return without waiting
;; for the acceptor thread. An acceptor that had just handed a connection off
;; and not yet re-entered accept() — a preemption or a collection is enough —
;; then called accept() on a freed fd NUMBER, and the next run-server's
;; socket() had already taken that number for its own listener. The stale
;; acceptor won the race for the new server's first connection, read its own
;; running? as false and closed the fd unread: the client saw EOF with no
;; header. Seen once in CI on the test above this one; reproduced here by
;; holding the acceptor between two accepts for longer than the stop and the
;; next start take together.
(defn test-stale-acceptor-does-not-take-the-next-servers-connection []
  (let [real-accept native/c-accept
        gap (atom 0)]
    (with-redefs [native/c-accept (fn [fd sa salen]
                                    (when (pos? @gap) (Thread/sleep @gap))
                                    (real-accept fd sa salen))]
      (dotimes [i 8]
        (let [a (adapter/run-server handler {:port 8561})]
          (Thread/sleep 50)
          ;; the acceptor is waiting for a connection already; from here on every
          ;; accept it makes waits 30ms first
          (reset! gap 30)
          (http/get "http://127.0.0.1:8561/")
          (adapter/stop-server a)
          (let [b (adapter/run-server handler {:port 8562})]
            (try
              (Thread/sleep 100)
              (let [r (try (http/get "http://127.0.0.1:8562/")
                           (catch Throwable t {:error (str t)}))]
                (check (str "stale acceptor " i ": the next server answers its first connection")
                       200 (:status r)))
              (finally (reset! gap 0) (adapter/stop-server b)))))))))

;; --- server-level faults must not wedge the server -------------------------
;; Both of these used to end with the server holding the port and answering
;; nothing, which from outside is indistinguishable from a server under load:
;; in-flight requests finish, server-stats still answers, and connections that
;; are never accepted sit in the listen backlog where they cost the process no
;; fd and appear in no count. That is the shape a stall in the benchmarks takes,
;; so the shape is what these pin.

;; One bounded request. A wedged server would otherwise hang the whole suite
;; rather than fail one test, which is the failure these tests exist to catch.
(defn- get-within
  "GET / on port, or :timeout after ms. The response's first line, so a dead
  connection (:eof) reads differently from a dead server (:timeout)."
  [port ms]
  (let [ch (a/thread
             (try
               (let [sock (java.net.Socket. "127.0.0.1" (int port))]
                 (try
                   (let [out (.getOutputStream sock)]
                     (.write out (.getBytes "GET / HTTP/1.0\r\nHost: x\r\n\r\n" "UTF-8"))
                     (.flush out))
                   (let [buf (byte-array 256)
                         n (.read (.getInputStream sock) buf)]
                     (if (pos? n)
                       (first (str/split (String. buf 0 n "UTF-8") #"\r\n"))
                       :eof))
                   (finally (.close sock))))
               (catch Throwable t (str "ERR " (.getMessage t)))))
        [v p] (a/alts!! [ch (a/timeout ms)])]
    (if (= p ch) v :timeout)))

;; conn-release! runs in the worker's FINALLY, where a throw beats the catch
;; beside it and leaves the loop WITH THE WORKER. A worker that leaves was
;; never replaced, so the pool shrank for the life of the process, and when the
;; last one went the acceptor parked on the unbuffered work channel forever.
;; Measured before the fix with a pool of four: four injected faults, four dead
;; workers, and every request after them timed out.
(defn test-worker-survives-a-fault-in-connection-teardown []
  (let [real-close native/c-close
        pool 4
        armed (atom 0)]
    (with-redefs [native/c-close
                  (fn [fd]
                    (let [[was _] (swap-vals! armed (fn [n] (if (pos? n) (dec n) 0)))]
                      (if (pos? was)
                        (throw (ex-info "injected teardown fault" {:fd fd}))
                        (real-close fd))))]
      (let [server (adapter/run-server handler {:port 8573 :worker-threads pool})]
        (try
          (Thread/sleep 150)
          (check "teardown fault: healthy before" "HTTP/1.1 200 OK" (get-within 8573 3000))
          ;; twice the pool size, so under the old code every worker is gone
          (reset! armed (* 2 pool))
          (dotimes [_ (* 2 pool)] (get-within 8573 3000))
          (reset! armed 0)
          (Thread/sleep 150)
          (check "teardown fault: the pool still serves" "HTTP/1.1 200 OK" (get-within 8573 3000))
          (check "teardown fault: the pool still serves twice" "HTTP/1.1 200 OK" (get-within 8573 3000))
          (check "teardown fault: counted as a server fault"
                 true (pos? (:faults (adapter/server-stats server))))
          (check "teardown fault: the last one is kept"
                 :worker (:kind (:last-fault (adapter/server-stats server))))
          (finally (reset! armed 0) (adapter/stop-server server)))))))

;; The accept loop's only guard was a finally that freed the sockaddr, so a
;; throw from ONE connection's handoff unwound it and ended accepting for the
;; life of the process — and the future holding the throwable was never
;; deref'd, so the first report of it came out of stop-server as an
;; ExecutionException, if anyone ever stopped the server at all.
(defn test-accept-loop-survives-one-connections-fault []
  (let [real-peer socket/peer-ip
        armed (atom 0)]
    (with-redefs [socket/peer-ip
                  (fn [sa]
                    (let [[was _] (swap-vals! armed (fn [n] (if (pos? n) (dec n) 0)))]
                      (if (pos? was)
                        (throw (ex-info "injected accept fault" {}))
                        (real-peer sa))))]
      (let [server (adapter/run-server handler {:port 8574})]
        (try
          (Thread/sleep 150)
          (check "accept fault: healthy before" "HTTP/1.1 200 OK" (get-within 8574 3000))
          (reset! armed 1)                      ; exactly ONE connection faults
          ;; that connection is lost — its fd is released and the peer reads
          ;; EOF — and it is the only one that is
          (get-within 8574 3000)
          (Thread/sleep 150)
          (check "accept fault: still accepting" "HTTP/1.1 200 OK" (get-within 8574 3000))
          (check "accept fault: still accepting twice" "HTTP/1.1 200 OK" (get-within 8574 3000))
          (check "accept fault: counted as a server fault"
                 1 (:faults (adapter/server-stats server)))
          (finally (reset! armed 0) (adapter/stop-server server)))))))

;; A healthy server reports no faults — the counter above is only evidence if
;; it stays at zero when nothing is wrong.
(defn test-clean-run-reports-no-faults []
  (let [server (adapter/run-server handler {:port 8575})]
    (try
      (Thread/sleep 150)
      (dotimes [_ 5] (get-within 8575 3000))
      (let [st (adapter/server-stats server)]
        (check "clean run: no faults" 0 (:faults st))
        (check "clean run: no last fault" nil (:last-fault st)))
      (finally (adapter/stop-server server)))))

;; --- a connection the accept handoff lost ----------------------------------
;; The whole serving path hangs off the claim at the far end of the accept
;; handoff, so a connection that is handed on and never claimed is served by
;; nobody, closed by nobody — only an owner may free an fd number — and
;; reported by nobody, because nothing was thrown and no guard was crossed.
;; Measured on macOS under plain-mode connect churn as roughly one connection
;; in a few thousand, on BOTH strategies: the peer waited out its own timeout
;; against a server answering everyone else in a millisecond, and the fd sat
;; in CLOSE_WAIT for the life of the process. One is enough for ab to abort a
;; whole benchmark cell, which is how it was found. Injected below at each
;; strategy's own handoff, because there is no throw to inject.

(defn- take-arm!
  "True for exactly as many calls as the test armed, false after. Keeps an
  injection to the connections it is meant for."
  [armed]
  (pos? (first (swap-vals! armed (fn [n] (if (pos? n) (dec n) 0))))))

;; Threads: the rendezvous answers true — "a worker has it" — and no worker
;; does. Only puts on THIS server's work channel are intercepted: core.async
;; puts a thread's return value through >!! too, and that must not count.
(defn test-threads-lost-handoff-is-reaped-and-reported []
  (let [server (adapter/run-server handler {:port 8576 :worker-threads 2})
        work (:work server)
        real->!! a/>!!
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (check "lost handoff: healthy before" "HTTP/1.1 200 OK" (get-within 8576 3000))
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 1500
                    a/>!! (fn [ch v]
                            (if (and (identical? ch work) (take-arm! armed))
                              true
                              (real->!! ch v)))]
        (reset! armed 1)
        (check "lost handoff: that connection is not answered"
               :timeout (get-within 8576 400)))
      (Thread/sleep 2000)                   ; past the claim deadline and a sweep
      (let [st (adapter/server-stats server)]
        (check "lost handoff: reported as a server fault"
               :unclaimed (:kind (:last-fault st)))
        (check "lost handoff: counted once" 1 (:faults st))
        (check "lost handoff: the fd was released" 0 (:connections st)))
      (check "lost handoff: the server still serves" "HTTP/1.1 200 OK" (get-within 8576 3000))
      ;; the lost conn was also still counted as accept pressure, and that
      ;; count never comes back down on its own: `pending` stuck above zero is
      ;; under-pressure? stuck true, which is keep-alive declined on every
      ;; response from then on. Asked on the wire, because a client that sends
      ;; `Connection: close` is answered with one whatever the pressure is.
      (let [fd (client-connect 8576 3000)]
        (try
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
          (check "lost handoff: accept pressure did not leak"
                 false (str/includes? (str/lower-case (client-recv-until fd "\r\n\r\n"))
                                      "connection: close"))
          (finally (client-close fd))))
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; Fibers: the spawn returns without the go block ever existing, which is what
;; a go block that is never scheduled looks like from the accept loop.
(defn test-fibers-lost-spawn-is-reaped-and-reported []
  (let [server (adapter/run-server handler {:port 8577 :strategy :fibers})
        real-serve @#'ring-chez.adapter/fiber-serve
        armed (atom 0)]
    (try
      (Thread/sleep 250)
      (check "lost spawn: healthy before" "HTTP/1.1 200 OK" (get-within 8577 3000))
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 1500
                    ring-chez.adapter/fiber-serve
                    (fn [conn-info cfg entry conns]
                      (when-not (take-arm! armed)
                        (real-serve conn-info cfg entry conns)))]
        (reset! armed 1)
        (check "lost spawn: that connection is not answered"
               :timeout (get-within 8577 400)))
      (Thread/sleep 2000)
      (let [st (adapter/server-stats server)]
        (check "lost spawn: reported as a server fault"
               :unclaimed (:kind (:last-fault st)))
        (check "lost spawn: counted once" 1 (:faults st))
        (check "lost spawn: the fd was released" 0 (:connections st)))
      (check "lost spawn: the server still serves" "HTTP/1.1 200 OK" (get-within 8577 3000))
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; The other half of the rule, and the one that makes the reap safe to ship: a
;; connection QUEUED behind a busy pool is unclaimed too, and reaping it would
;; drop a live request. The claim clock starts only once somebody is provably
;; responsible for the conn — the rendezvous has answered, or the fiber is
;; spawned — so a conn still parked in the handoff is not on it, however long
;; the pool stays busy.
(defn test-queued-connection-is-not-reaped []
  (let [server (adapter/run-server (fn [_] (Thread/sleep 1200) {:status 200 :body "slow-ok"})
                                   {:port 8578 :worker-threads 1})]
    (try
      (Thread/sleep 150)
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 200]
        (let [busy   (a/thread (get-within 8578 9000))
              _      (Thread/sleep 150)
              queued (a/thread (get-within 8578 9000))]
          ;; the single worker is inside the handler for 1200ms, six times the
          ;; claim deadline, with the second conn parked in the handoff
          (check "queued conn: the busy worker's request is answered"
                 "HTTP/1.1 200 OK" (a/<!! busy))
          (check "queued conn: the queued request is answered too"
                 "HTTP/1.1 200 OK" (a/<!! queued))))
      (check "queued conn: nothing reported as lost"
             0 (:faults (adapter/server-stats server)))
      (finally (adapter/stop-server server)))))

;; --- a drop must not cost the connection fleet, and must be answered ---------
;; Two properties of the same loss. A conn whose handoff rendezvous completed
;; is not queued behind anybody: it stops counting as accept pressure the
;; moment the rendezvous answers, not five seconds later at the claim
;; deadline — or one dropped conn retires every idle keep-alive for its whole
;; backstop window, which is the ka collapse measured in benchmark/README.md.
;; And the dropped conn itself is ANSWERED — 503, Connection: close, the way
;; an overloaded Undertow answers — instead of shutting a silent fd on a peer
;; that then waits out its own timeout.

(defn test-lost-handoff-does-not-retire-keepalive []
  (let [server (adapter/run-server handler {:port 8580 :worker-threads 2})
        work (:work server)
        real->!! a/>!!
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (check "ka under drop: healthy before" "HTTP/1.1 200 OK" (get-within 8580 3000))
      (with-redefs [a/>!! (fn [ch v]
                            (if (and (identical? ch work) (take-arm! armed))
                              true
                              (real->!! ch v)))]
        (reset! armed 1)
        ;; the lost conn sits unclaimed until the sweeper answers it; under the
        ;; old code it counted as accept pressure the whole time, and the
        ;; first response on the conn beside it already declined keep-alive
        (let [lost (client-connect 8580 3000)]
          (client-send lost "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
          (let [fd (client-connect 8580 3000)]
            (try
              (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
              (let [r1 (client-recv-until fd "\r\n\r\n")]
                (check "ka under drop: first response keeps keep-alive"
                       false (str/includes? (str/lower-case (str r1))
                                            "connection: close"))
                ;; idle past the 2s pressure grace: under the old code the
                ;; lost conn still counts as pressure and this conn is
                ;; retired with a close instead of being answered
                (Thread/sleep 2300)
                (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
                (let [r2 (client-recv-until fd "\r\n\r\n")]
                  (check "ka under drop: idle conn still answered"
                         "HTTP/1.1 200 OK" (first (str/split (str r2) #"\r\n")))))
              (finally (client-close fd))))
          (client-close lost)))
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; --- accept pressure is conns nobody is free to take -----------------------
;; Retirement exists so a conn queued behind a busy pool is not starved by
;; idle keep-alives. Two ways it used to fire when nobody was starving, and
;; together they made a "keep-alive" ab run open a new connection for ~40%
;; of its requests (benchmark/README.md): a conn still in the microseconds of
;; its handoff counted as pressure even with idle workers parked on the
;; channel, and one queued conn retired EVERY busy keep-alive that answered
;; before it was claimed, when one freed worker is all it needs.

(defn- response-closes? [r]
  (str/includes? (str/lower-case (str r)) "connection: close"))

(defn test-handoff-with-idle-workers-is-not-pressure []
  (let [server (adapter/run-server handler {:port 8596 :worker-threads 4})
        work (:work server)
        real->!! a/>!!
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (let [fd (client-connect 8596 3000)]
        (try
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
          (client-recv-until fd "hello get")
          (with-redefs [a/>!! (fn [ch v]
                                (if (and (identical? ch work) (take-arm! armed))
                                  (do (Thread/sleep 500) (real->!! ch v))
                                  (real->!! ch v)))]
            (reset! armed 1)
            ;; the second conn sits in a slow handoff while three workers
            ;; are idle: nobody is waiting on the pool
            (let [other (client-connect 8596 3000)]
              (Thread/sleep 150)
              (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
              (let [r (client-recv-until fd "hello get")]
                (check "idle workers: keep-alive kept during a handoff"
                       false (response-closes? r)))
              (client-send other "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
              (check-has "idle workers: handed-off conn answered" "200"
                         (client-recv-until other "hello get"))
              (client-close other)))
          (finally (client-close fd))))
      (finally (reset! armed 0) (adapter/stop-server server)))))

(defn test-one-queued-conn-retires-one-keepalive []
  (let [server (adapter/run-server handler {:port 8597 :worker-threads 2})
        work (:work server)
        real-<!! a/<!!
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (let [[a b] (mapv (fn [_] (client-connect 8597 3000)) (range 2))]
        (try
          (doseq [fd [a b]]
            (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
            (client-recv-until fd "hello get"))
          ;; both workers own a conn; a third queues behind them
          (let [c (client-connect 8597 3000)]
            (try
              (Thread/sleep 150)
              ;; the worker that frees up is slow to reach the queued conn,
              ;; which leaves the window in which b answers
              (with-redefs [a/<!! (fn [ch]
                                    (when (and (identical? ch work) (take-arm! armed))
                                      (Thread/sleep 500))
                                    (real-<!! ch))]
                (reset! armed 1)
                (client-send a "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
                (check "one queued: the first answer retires its conn"
                       true (response-closes? (client-recv-until a "hello get")))
                (client-send b "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
                (check "one queued: the second keeps keep-alive"
                       false (response-closes? (client-recv-until b "hello get")))
                (client-send c "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
                (check-has "one queued: queued conn answered" "200"
                           (client-recv-until c "hello get")))
              (finally (client-close c))))
          (finally (client-close a) (client-close b))))
      (finally (reset! armed 0) (adapter/stop-server server)))))

(defn test-threads-lost-handoff-is-answered-503 []
  (let [server (adapter/run-server handler {:port 8581 :worker-threads 2})
        work (:work server)
        real->!! a/>!!
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 400
                    a/>!! (fn [ch v]
                            (if (and (identical? ch work) (take-arm! armed))
                              true
                              (real->!! ch v)))]
        (reset! armed 1)
        (let [fd (client-connect 8581 3000)]
          (try
            (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
            (let [r (client-recv-until fd "\r\n\r\n")]
              (check "lost handoff 503: answered, not silence"
                     "HTTP/1.1 503 Service Unavailable"
                     (first (str/split (str r) #"\r\n")))
              (check "lost handoff 503: declines keep-alive"
                     true (str/includes? (str/lower-case (str r))
                                         "connection: close")))
            (finally (client-close fd)))))
      (Thread/sleep 700)   ; past the shortened claim deadline and a sweep
      (check "lost handoff 503: still reported as a fault"
             :unclaimed (:kind (:last-fault (adapter/server-stats server))))
      (check "lost handoff 503: the server still serves"
             "HTTP/1.1 200 OK" (get-within 8581 3000))
      (finally (reset! armed 0) (adapter/stop-server server)))))

(defn test-fibers-lost-spawn-is-answered-503 []
  (let [server (adapter/run-server handler {:port 8582 :strategy :fibers})
        real-serve @#'ring-chez.adapter/fiber-serve
        armed (atom 0)]
    (try
      (Thread/sleep 250)
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 400
                    ring-chez.adapter/fiber-serve
                    (fn [conn-info cfg entry conns]
                      (when-not (take-arm! armed)
                        (real-serve conn-info cfg entry conns)))]
        (reset! armed 1)
        (let [fd (client-connect 8582 3000)]
          (try
            (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
            (let [r (client-recv-until fd "\r\n\r\n")]
              (check "lost spawn 503: answered, not silence"
                     "HTTP/1.1 503 Service Unavailable"
                     (first (str/split (str r) #"\r\n"))))
            (finally (client-close fd)))))
      (Thread/sleep 700)
      (check "lost spawn 503: still reported as a fault"
             :unclaimed (:kind (:last-fault (adapter/server-stats server))))
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; The other loss class, one hop further down: the worker's take DID answer
;; — the entry is owned — and then nothing ran. A conn claimed but never
;; served has no deadline under :threads at all: the unclaimed arm requires
;; not-owned, the keep-alive clock is per-request inside the loop that never
;; started, and the peer sits in silence. This is the "claim hop or below"
;; class from PR #39's narrowing, and it held an ab run for its full 30s poll
;; timeout on 2026-09-20 with every unclaimed conn answered around it.
;;
;; The answer is the sweeper's; the fd NUMBER is not. The conn is owned, and
;; only its owner may close it (conn-down!): a number closed under a late
;; owner is reused by the next accept, and the owner's loop then reads a live
;; connection belonging to somebody else. So the sweeper shuts the conn down
;; and leaves the number allocated — :connections stays up until the owner's
;; own finally runs — exactly as stop-server does for the conns it cannot
;; claim.
(defn test-threads-claimed-silent-conn-is-answered []
  (let [server (adapter/run-server handler {:port 8583 :worker-threads 2})
        real-loop @#'ring-chez.adapter/connection-loop
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (check "claimed silent: healthy before" "HTTP/1.1 200 OK" (get-within 8583 3000))
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 400
                    ring-chez.adapter/connection-loop
                    (fn [& args]
                      (if (take-arm! armed)
                        ;; claimed, then nothing: the loop is never entered,
                        ;; which is what a take that is never scheduled back
                        ;; looks like from the sweeper. The owner comes back
                        ;; 3s in — a second after the sweeper has given up on
                        ;; it — and its finally is what releases the fd.
                        (Thread/sleep 3000)
                        (apply real-loop args)))]
        (reset! armed 1)
        (let [fd (client-connect 8583 4000)]
          (try
            (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
            (let [r (client-recv-until fd "\r\n\r\n")]
              (check "claimed silent: answered 503, not silence"
                     "HTTP/1.1 503 Service Unavailable"
                     (first (str/split (str r) #"\r\n")))
              (check "claimed silent: declines keep-alive"
                     true (str/includes? (str/lower-case (str r))
                                         "connection: close"))
              ;; answered at ~2.1s; the owner is asleep until 3s, so the
              ;; number is still its to release
              (check "claimed silent: fd number left to its owner"
                     1 (:connections (adapter/server-stats server))))
            (finally (client-close fd)))))
      (Thread/sleep 1200)   ; the owner's finally has run by now
      (let [st (adapter/server-stats server)]
        (check "claimed silent: owner released it"
               0 (:connections st))
        (check "claimed silent: reported as its own fault kind"
               :claimed-silent (:kind (:last-fault st))))
      (check "claimed silent: the server still serves"
             "HTTP/1.1 200 OK" (get-within 8583 3000))
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; The claimed-silent arm must not reap a conn whose owner is merely late:
;; between the worker's claim and the loop's entry there is a scheduling
;; window, and a GC pause or a descheduled worker can stretch it past any
;; base deadline. The claimed arm therefore waits a multiple of the sweep
;; deadline beyond expiry before giving up — the owner is alive and adjacent
;; to serving, unlike the unclaimed loss where nobody has it at all.
(defn test-claimed-silent-arm-waits-for-a-late-owner []
  (let [server (adapter/run-server handler {:port 8584 :worker-threads 2})
        real-loop @#'ring-chez.adapter/connection-loop
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 400
                    ring-chez.adapter/connection-loop
                    (fn [& args]
                      (if (take-arm! armed)
                        ;; the owner was descheduled for 1s before entering
                        ;; its loop: 2.5x the base deadline, inside the
                        ;; claimed arm's extra headroom
                        (do (Thread/sleep 1000)
                            (apply real-loop args))
                        (apply real-loop args)))]
        (reset! armed 1)
        (let [fd (client-connect 8584 5000)]
          (try
            (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
            (let [r (client-recv-until fd "\r\n\r\n")]
              (check "late owner: served, not reaped"
                     "HTTP/1.1 200 OK" (first (str/split (str r) #"\r\n"))))
            (finally (client-close fd)))))
      (Thread/sleep 700)   ; a sweep tick past the whole window
      (check "late owner: no claimed-silent fault"
             nil (:kind (:last-fault (adapter/server-stats server))))
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; --- a spuriously failing compare-and-set! must not lose the claim ----------------
;; The root cause of every "lost handoff" above (PR #39, #40): jolt's
;; compare-and-set! sat on Chez's $record-cas!, one ldxr/stxr on AArch64,
;; which answers false with the value STILL THE EXPECTED ONE whenever the
;; core's exclusive monitor is cleared under it — a context switch, or the
;; acceptor storing into the atom beside :owned? on the same cache line. The
;; worker read that false as "stop-server owns this conn" and walked away; one
;; accept in ~1400 under ab on an M-series Mac, never on x86 (jolt-lang/jolt
;; #1072 makes the primitive strong). The adapter's every once-only step is
;; such a CAS, so it goes through cas!, which believes a false only once the
;; value has been seen to be something else. This injects exactly the hardware's
;; failure — false, atom untouched — into the first claim and expects the conn
;; served, nothing reaped, no pressure left behind.
(defn test-claim-survives-a-spurious-cas-failure []
  (let [server (adapter/run-server handler {:port 8585 :worker-threads 2})
        real-cas compare-and-set!
        armed (atom 0)]
    (try
      (Thread/sleep 150)
      (check "spurious cas: healthy before" "HTTP/1.1 200 OK" (get-within 8585 3000))
      (with-redefs [ring-chez.adapter/sweep-deadline-ms 400
                    compare-and-set!
                    (fn [a o n]
                      ;; false->true with the value false IS the worker's
                      ;; claim-close! (pending-done! goes the other way, and
                      ;; the sweeper's claim comes 400ms later); answer it
                      ;; the way the weak primitive did, exactly once
                      (if (and (false? o) (true? n) (false? @a) (take-arm! armed))
                        false
                        (real-cas a o n)))]
        (reset! armed 1)
        (let [fd (client-connect 8585 3000)]
          (try
            (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
            (let [r (client-recv-until fd "\r\n\r\n")]
              (check "spurious cas: the conn is served, not walked away from"
                     "HTTP/1.1 200 OK" (first (str/split (str r) #"\r\n")))
              (check "spurious cas: keep-alive kept — no pressure left behind"
                     false (str/includes? (str/lower-case (str r)) "connection: close")))
            (finally (client-close fd)))))
      (Thread/sleep 700)   ; past the shortened sweep deadline
      (check "spurious cas: nothing reaped" 0 (:faults (adapter/server-stats server)))
      (check "spurious cas: the arm was consumed" 0 @armed)
      (finally (reset! armed 0) (adapter/stop-server server)))))

;; --- Wave 2 round 2: handler deadline --------------------------------------------

;; Igropyr kills a worker stuck past stuck-ms and answers through on-failure.
;; We cannot kill a thread, but we can stop waiting for one: the client is
;; answered 503 and the worker goes back to serving (RFC-0009).
(def spin-unwound (atom nil))

(defn- deadline-handler [{:keys [uri]}]
  (cond
    (= uri "/hang")  (do @(promise) {:status 200 :body "never"})
    ;; a COMPUTE hang, distinct from /hang's wait: the deadline's interrupt
    ;; fires on reduction back-edges, so this loop unwinds (and the finally
    ;; observes it) while a parked wait only unwinds when it wakes
    (= uri "/spin")  (try (loop [] (recur))
                          (finally (reset! spin-unwound :unwound)))
    (= uri "/slow")  (do (Thread/sleep 700)
                         {:status 200 :headers {"Content-Type" "text/plain"} :body "slow done"})
    (= uri "/throw-late") (do (Thread/sleep 600) (throw (ex-info "late" {})))
    (= uri "/stream-long")
    (let [ch (a/chan)]
      (future (dotimes [i 4] (a/>!! ch (str "part" i)) (Thread/sleep 150))
              (a/close! ch))
      {:status 200 :headers {"Content-Type" "text/plain"} :body ch})
    :else {:status 200 :headers {"Content-Type" "text/plain"} :body "ok"}))

(defn- handler-deadline-reclaims [strategy port]
  ;; one worker, so "the next request is served" can only mean the worker
  ;; that was stuck on /hang is back
  (let [server (adapter/run-server deadline-handler
                                   {:port port :strategy strategy :worker-threads 1
                                    :handler-timeout-ms 400})
        label  (str (name strategy) " deadline: ")]
    (try
      (Thread/sleep 250)
      (let [t0 (System/currentTimeMillis)
            r  (http/get (str "http://127.0.0.1:" port "/hang")
                         {:throw-exceptions false})]
        (check (str label "hung handler answers 503") 503 (:status r))
        (check (str label "answered near the deadline") true
               (< (- (System/currentTimeMillis) t0) 4000)))
      (check (str label "worker reclaimed") 200
             (:status (http/get (str "http://127.0.0.1:" port "/"))))
      (finally (adapter/stop-server server)))))

;; --- TLS ---------------------------------------------------------------------
(def ^:private tls-opts {:cert-file "test/fixtures/tls/cert.pem"
                         :key-file "test/fixtures/tls/key.pem"})
(def ^:private tls-big (apply str (repeat (* 5 1024 1024) "b")))
(def ^:private tls-stream-closed (atom nil))

(defn- tls-handler [request]
  (case (:uri request)
    "/scheme" {:status 200 :body (name (:scheme request))}
    "/echo"   {:status 200 :body (let [o (java.io.ByteArrayOutputStream.)]
                                   (io/copy (:body request) o)
                                   (.toByteArray o))}
    "/big"    {:status 200 :body tls-big}
    "/stream" (let [ch (a/chan)]
                (a/go (a/>! ch "one") (a/>! ch "two") (a/close! ch))
                {:status 200 :body ch})
    "/quiet"  (let [ch (a/chan)]
                (reset! tls-stream-closed ch)
                (a/go (a/>! ch "first"))
                {:status 200 :body ch})
    {:status 200 :body "ok"}))

(defn- tls-open [port] (client-tls/tls-connect "127.0.0.1" port true 5000))
(defn- tls-write [st ^String s]
  ((jolt.host/ref-get st :write) st (.getBytes s "UTF-8")))
(defn- tls-write-frame [st frame]
  ((jolt.host/ref-get st :write) st (byte-array (map unchecked-byte frame))))
(defn- tls-close [st] ((jolt.host/ref-get st :close)))

(defn- tls-read-until
  "Bytes read from st as a latin-1 string, until pred holds or the stream ends."
  [st pred]
  (loop [acc ""]
    (if (pred acc)
      acc
      (let [bs (try ((jolt.host/ref-get st :read) st nil) (catch Throwable _ nil))]
        (if bs (recur (str acc (String. ^bytes bs "ISO-8859-1"))) acc)))))

(defn- count-of [^String hay ^String needle]
  (loop [i 0 n 0]
    (let [j (.indexOf hay needle i)]
      (if (neg? j) n (recur (+ j (count needle)) (inc n))))))

(defn- tls-url [port path] (str "https://127.0.0.1:" port path))

(defn- tls-serves [strategy]
  (let [label (str "tls " (name strategy) ": ")
        server (adapter/run-server tls-handler {:port 0 :strategy strategy :tls tls-opts})
        port (:port server)]
    (try
      (let [r (http/get (tls-url port "/scheme") {:insecure? true})]
        (check (str label "request sees :scheme :https") "https" (:body r)))
      ;; one connection, two requests one after the other, then two in one write
      (let [st (tls-open port)
            req "GET /scheme HTTP/1.1\r\nHost: x\r\n\r\n"]
        (try
          (tls-write st req)
          (check (str label "first keep-alive request") 1
                 (count-of (tls-read-until st #(str/includes? % "https")) "https"))
          (tls-write st req)
          (check (str label "second keep-alive request") 1
                 (count-of (tls-read-until st #(str/includes? % "https")) "https"))
          (tls-write st (str req req))
          (check (str label "pipelined requests both answered") 2
                 (count-of (tls-read-until st #(= 2 (count-of % "https"))) "https"))
          (finally (tls-close st))))
      ;; every octet value, across many records
      (let [bs (byte-array (map unchecked-byte (take (* 512 1024) (cycle (range 256)))))
            r (http/post (tls-url port "/echo") {:body bs :as :byte-array :insecure? true})]
        (check (str label "binary body round trip") (seq bs) (seq (:body r))))
      (check (str label "5 MB response") (count tls-big)
             (count (:body (http/get (tls-url port "/big") {:insecure? true}))))
      (let [st (tls-open port)]
        (try
          (tls-write st "GET /stream HTTP/1.1\r\nHost: x\r\n\r\n")
          (let [got (tls-read-until st #(str/includes? % "0\r\n\r\n"))]
            (check-has (str label "chunked: first chunk") "3\r\none\r\n" got)
            (check-has (str label "chunked: second chunk") "3\r\ntwo\r\n" got))
          (finally (tls-close st))))
      (finally (adapter/stop-server server)))))

(defn test-tls-threads [] (tls-serves :threads))
(defn test-tls-fibers [] (tls-serves :fibers))

(defn- tls-refuses-plaintext [strategy]
  (let [label (str "tls " (name strategy) ": ")
        server (adapter/run-server tls-handler {:port 0 :strategy strategy :tls tls-opts})
        port (:port server)]
    (try
      (let [fd (client-connect port)]
        (t-send-bytes fd (utf8-bytes "GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
        (let [got (client-recv-all fd)]
          (check (str label "plaintext gets no plaintext answer") false
                 (str/includes? got "HTTP/1.1"))))
      (check (str label "serves after plaintext") "https"
             (:body (http/get (tls-url port "/scheme") {:insecure? true})))
      (finally (adapter/stop-server server)))))

(defn test-tls-refuses-plaintext-threads [] (tls-refuses-plaintext :threads))
(defn test-tls-refuses-plaintext-fibers [] (tls-refuses-plaintext :fibers))

;; one worker, so a silent peer in the handshake holds the whole server until the
;; handshake's limit lets go of it
(defn- tls-handshake-limit [strategy]
  (let [label (str "tls " (name strategy) ": ")
        server (adapter/run-server tls-handler {:port 0 :strategy strategy :worker-threads 1
                                                :keep-alive-timeout-ms 400 :tls tls-opts})
        port (:port server)]
    (try
      (let [fd (client-connect port 5000)
            t0 (System/currentTimeMillis)
            got (client-recv-all fd)]
        (check (str label "silent handshake is dropped") "" got)
        (check (str label "dropped near the limit") true
               (< (- (System/currentTimeMillis) t0) 4000)))
      ;; a byte at a time: each read is prompt, so only a limit on the whole handshake
      ;; ends this. The start of a record that never completes.
      (let [fd (client-connect port)
            t0 (System/currentTimeMillis)
            closed? (loop [bs (concat [0x16 0x03 0x01 0x20 0x00] (repeat 50 0x01))]
                      (cond
                        (pos? (native/poll-one fd native/pollin 0)) true
                        (empty? bs) false
                        :else (do (t-send-bytes fd [(first bs)])
                                  (Thread/sleep 100)
                                  (recur (rest bs)))))]
        (client-close fd)
        (check (str label "trickled handshake is cut") true closed?)
        (check (str label "cut well before it finished") true
               (< (- (System/currentTimeMillis) t0) 3000)))
      ;; silent after the ClientHello, so the server has already written its half. The
      ;; write must leave the handshake's deadline where it found it.
      (let [fd (client-connect port 5000)
            hello (with-open [in (java.io.FileInputStream. "test/fixtures/tls/client-hello.bin")
                              o (java.io.ByteArrayOutputStream.)]
                    (io/copy in o)
                    (.toByteArray o))
            t0 (System/currentTimeMillis)]
        (t-send-bytes fd (mapv #(bit-and 0xff %) hello))
        (let [got (client-recv-all fd)]
          (check (str label "server answered the hello") true (pos? (count got)))
          (check (str label "stalled after the hello is dropped") true
                 (< (- (System/currentTimeMillis) t0) 3000))))
      (check (str label "serves after a stalled handshake") "https"
             (:body (http/get (tls-url port "/scheme") {:insecure? true})))
      (finally (adapter/stop-server server)))))

(defn test-tls-handshake-limit-threads [] (tls-handshake-limit :threads))
(defn test-tls-handshake-limit-fibers [] (tls-handshake-limit :fibers))

;; a client that says goodbye and keeps the socket open while the application is quiet.
;; close_notify is ciphertext, so a peek at the socket sees bytes, not the end.
(defn- tls-stream-notices-disconnect [strategy]
  (let [label (str "tls " (name strategy) ": ")
        server (adapter/run-server tls-handler {:port 0 :strategy strategy :tls tls-opts})
        port (:port server)]
    (try
      (let [st (tls-open port)]
        (tls-write st "GET /quiet HTTP/1.1\r\nHost: x\r\n\r\n")
        (check-has (str label "stream started") "first"
                   (tls-read-until st #(str/includes? % "first")))
        (#'client-tls/c-SSL-shutdown (jolt.host/ref-get st :ssl))
        (#'client-tls/flush-out st)
        (Thread/sleep 2500)
        (check (str label "adapter closed the body channel") false
               (a/put! @tls-stream-closed "nobody is listening"))
        (tls-close st))
      (check (str label "serves after the stream") "https"
             (:body (http/get (tls-url port "/scheme") {:insecure? true})))
      (finally (adapter/stop-server server)))))

(defn test-tls-stream-disconnect-threads [] (tls-stream-notices-disconnect :threads))
(defn test-tls-stream-disconnect-fibers [] (tls-stream-notices-disconnect :fibers))

(defn- tls-ws [strategy]
  (let [label (str "tls " (name strategy) " ws: ")
        server (adapter/run-server tls-handler
                 {:port 0 :strategy strategy :tls tls-opts
                  :ws-handler (fn [session]
                                (loop []
                                  (let [m (ws/recv! session)]
                                    (when (= :text (:type m))
                                      (ws/send! session (:data m))
                                      (recur)))))})
        port (:port server)
        st (tls-open port)]
    (try
      (tls-write st (str "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
                         "Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                         "Sec-WebSocket-Version: 13\r\n\r\n"))
      (check-has (str label "101") "101 Switching"
                 (tls-read-until st #(str/includes? % "\r\n\r\n")))
      (tls-write-frame st (ws-client-frame 0x1 (utf8-bytes "hello")))
      (check-has (str label "echo") "hello" (tls-read-until st #(str/includes? % "hello")))
      ;; bigger than a TLS record, so the frame spans several
      (let [big (apply str (repeat 100000 "w"))]
        (tls-write-frame st (ws-client-frame 0x1 (utf8-bytes big)))
        (check (str label "large echo") true
               (<= 100000 (count-of (tls-read-until st #(<= 100000 (count-of % "w"))) "w"))))
      (finally (tls-close st) (adapter/stop-server server)))))

(defn test-tls-ws-threads [] (tls-ws :threads))
(defn test-tls-ws-fibers [] (tls-ws :fibers))

;; A TLS 1.3 client may rotate keys and ask the server to do the same, any time. The
;; server answers from inside a read, on the same ciphertext path as everything else.
(ffi/defcfn c-SSL-key-update "SSL_key_update" [:pointer :int] :int)

(defn- tls-key-update [strategy]
  (let [label (str "tls " (name strategy) ": ")
        server (adapter/run-server tls-handler {:port 0 :strategy strategy :tls tls-opts})
        st (tls-open (:port server))]
    (try
      (dotimes [_ 3]
        (check (str label "KeyUpdate accepted") 1
               (c-SSL-key-update (jolt.host/ref-get st :ssl) 1))
        (tls-write st "GET /scheme HTTP/1.1\r\nHost: x\r\n\r\n")
        (check-has (str label "served across a KeyUpdate") "https"
                   (tls-read-until st #(str/includes? % "https"))))
      (finally (tls-close st) (adapter/stop-server server)))))

(defn test-tls-key-update-threads [] (tls-key-update :threads))
(defn test-tls-key-update-fibers [] (tls-key-update :fibers))

;; stop-server has to get through live connections and an idle handshake
(defn test-tls-stop-with-live-connections []
  (doseq [strategy [:threads :fibers]]
    (let [server (adapter/run-server tls-handler {:port 0 :strategy strategy :tls tls-opts})
          st (tls-open (:port server))
          fd (client-connect (:port server))
          t0 (System/currentTimeMillis)]
      (tls-write st "GET /scheme HTTP/1.1\r\nHost: x\r\n\r\n")
      (tls-read-until st #(str/includes? % "https"))
      (adapter/stop-server server)
      (check (str "tls " (name strategy) ": stop-server returns") true
             (< (- (System/currentTimeMillis) t0) 8000))
      (tls-close st))))

(defn- tls-refused? [tls]
  (try
    (adapter/stop-server (adapter/run-server tls-handler {:port 0 :tls tls}))
    false
    (catch clojure.lang.ExceptionInfo e (= :tls (:key (ex-data e))))))

(defn test-tls-options []
  (check "tls: must be a map" true (tls-refused? "yes"))
  (check "tls: needs a certificate" true (tls-refused? {:key-file (:key-file tls-opts)}))
  (check "tls: needs a key" true (tls-refused? {:cert-file (:cert-file tls-opts)}))
  (check "tls: a missing certificate file" true
         (tls-refused? (assoc tls-opts :cert-file "test/fixtures/tls/nope.pem")))
  (check "tls: a missing key file" true
         (tls-refused? (assoc tls-opts :key-file "test/fixtures/tls/nope.pem")))
  (check "tls: a key that does not match" true
         (tls-refused? (assoc tls-opts :key-file "test/fixtures/tls/other-key.pem")))
  (check "tls: a good pair is accepted" false (tls-refused? tls-opts)))

;; --- client certificates (mTLS) ------------------------------------------------
;; A small PKI made fresh with the openssl CLI: a CA the server trusts, a client
;; certificate it signed, and a second CA (with its own client) the server has never
;; heard of. The server keeps the fixture certificate; the clients don't verify it.

(def ^:private openssl-cli
  (let [brew "/opt/homebrew/opt/openssl@3/bin/openssl"]
    ;; /usr/bin/openssl on macOS is LibreSSL, whose PKCS#12 OpenSSL 3 may not read
    (if (.exists (java.io.File. brew)) brew "openssl")))

(defn- openssl! [dir & args]
  (let [{:keys [exit err]} (apply clojure.java.shell/sh openssl-cli (concat args [:dir dir]))]
    (when-not (zero? exit)
      (throw (ex-info (str "openssl " (first args) " failed: " err) {:args args})))))

(defn- file-bytes [path]
  (with-open [in (java.io.FileInputStream. ^String path)
              o (java.io.ByteArrayOutputStream.)]
    (io/copy in o)
    (.toByteArray o)))

(defn- make-ca! [dir nm]
  (openssl! dir "req" "-x509" "-newkey" "ec" "-pkeyopt" "ec_paramgen_curve:P-256" "-nodes"
            "-keyout" (str nm ".key") "-out" (str nm ".pem") "-days" "2"
            "-subj" (str "/CN=" nm)))

(defn- make-client! [dir ca nm]
  (openssl! dir "req" "-newkey" "ec" "-pkeyopt" "ec_paramgen_curve:P-256" "-nodes"
            "-keyout" (str nm ".key") "-out" (str nm ".csr") "-subj" (str "/O=ring-chez/CN=" nm))
  (openssl! dir "x509" "-req" "-in" (str nm ".csr") "-CA" (str ca ".pem") "-CAkey" (str ca ".key")
            "-CAcreateserial" "-out" (str nm ".pem") "-days" "2")
  (openssl! dir "pkcs12" "-export" "-in" (str nm ".pem") "-inkey" (str nm ".key")
            "-passout" "pass:secret" "-out" (str nm ".p12"))
  ;; one client ssl map per file, so http-client's context cache (keyed on the
  ;; bytes' identity) builds one context for it and reuses it
  {:key-store {:bytes (file-bytes (str dir "/" nm ".p12")) :pass "secret"}})

(def ^:private mtls-pki
  (delay
    (let [dir (str/trim (:out (clojure.java.shell/sh "mktemp" "-d")))]
      (make-ca! dir "trusted-ca")
      (make-ca! dir "rogue-ca")
      {:dir dir
       :ca-file (str dir "/trusted-ca.pem")
       :good (make-client! dir "trusted-ca" "good-client")
       :rogue (make-client! dir "rogue-ca" "rogue-client")})))

(defn- mtls-opts [client-auth]
  (assoc tls-opts :client-auth client-auth :client-ca-file (:ca-file @mtls-pki)))

(defn- mtls-handler [request]
  {:status 200
   :body (cond
           (not (contains? request :ssl-client-cert)) "no-cert"
           (nil? (:ssl-client-cert request)) "nil-cert"
           :else (str "cert:" (.getName (.getSubjectX500Principal (:ssl-client-cert request)))))})

(defn- mtls-get
  "One request over a fresh TLS connection presenting ssl's certificate (nil: none).
  The response text, or :failed if the handshake or the exchange did not complete. In
  TLS 1.3 the client's handshake ends before the server has seen its certificate, so
  a refusal may only show on the first read."
  [port ssl]
  (try
    (let [st (client-tls/tls-connect "127.0.0.1" port true 5000 nil ssl)]
      (try
        (tls-write st "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        (let [got (tls-read-until st (constantly false))]
          (if (str/includes? got "200 OK") got :failed))
        (finally (try (tls-close st) (catch Throwable _ nil)))))
    (catch Throwable _ :failed)))

(defn- mtls-body [got]
  (if (string? got) (subs got (+ 4 (str/index-of got "\r\n\r\n"))) got))

(def ^:private good-name "CN=good-client,O=ring-chez")

(defn- tls-client-auth [strategy]
  (let [label (str "mtls " (name strategy) ": ")
        {:keys [good rogue]} @mtls-pki]
    (let [server (adapter/run-server mtls-handler {:port 0 :strategy strategy
                                                   :tls (mtls-opts :need)})
          port (:port server)]
      (try
        (check (str label ":need, trusted cert is served with :ssl-client-cert")
               (str "cert:" good-name) (mtls-body (mtls-get port good)))
        (check (str label ":need, no cert fails") :failed (mtls-get port nil))
        (check (str label ":need, serves after a missing cert")
               (str "cert:" good-name) (mtls-body (mtls-get port good)))
        (check (str label ":need, untrusted CA fails") :failed (mtls-get port rogue))
        (check (str label ":need, serves after an untrusted cert")
               (str "cert:" good-name) (mtls-body (mtls-get port good)))
        (finally (adapter/stop-server server))))
    (let [server (adapter/run-server mtls-handler {:port 0 :strategy strategy
                                                   :tls (mtls-opts :want)})
          port (:port server)]
      (try
        (check (str label ":want, no cert is served without the key")
               "no-cert" (mtls-body (mtls-get port nil)))
        (check (str label ":want, trusted cert is served with :ssl-client-cert")
               (str "cert:" good-name) (mtls-body (mtls-get port good)))
        (check (str label ":want, an untrusted cert still fails") :failed (mtls-get port rogue))
        (finally (adapter/stop-server server))))
    (let [server (adapter/run-server mtls-handler {:port 0 :strategy strategy :tls tls-opts})
          port (:port server)]
      (try
        (check (str label "no :client-auth, an offered cert is not asked for")
               "no-cert" (mtls-body (mtls-get port good)))
        (finally (adapter/stop-server server))))))

(defn test-tls-client-auth-threads [] (tls-client-auth :threads))
(defn test-tls-client-auth-fibers [] (tls-client-auth :fibers))

;; the upgrade request a websocket session carries is the one the handler would see
(defn test-tls-client-auth-ws []
  (doseq [strategy [:threads :fibers]]
    (let [label (str "mtls " (name strategy) " ws: ")
          server (adapter/run-server mtls-handler
                   {:port 0 :strategy strategy :tls (mtls-opts :need)
                    :ws-handler (fn [session]
                                  (let [c (get-in session [:request :ssl-client-cert])]
                                    (ws/send! session (if c
                                                        (.getName (.getSubjectX500Principal c))
                                                        "no-cert"))))})
          st (client-tls/tls-connect "127.0.0.1" (:port server) true 5000 nil (:good @mtls-pki))]
      (try
        (tls-write st (str "GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
                           "Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                           "Sec-WebSocket-Version: 13\r\n\r\n"))
        (check-has (str label "the session's request has the client cert") good-name
                   (tls-read-until st #(str/includes? % good-name)))
        (finally (tls-close st) (adapter/stop-server server))))))

;; every request on a kept-alive connection carries the certificate, not just the first
(defn test-tls-client-auth-keep-alive []
  (doseq [strategy [:threads :fibers]]
    (let [label (str "mtls " (name strategy) " keep-alive: ")
          server (adapter/run-server mtls-handler {:port 0 :strategy strategy
                                                   :tls (mtls-opts :need)})
          st (client-tls/tls-connect "127.0.0.1" (:port server) true 5000 nil (:good @mtls-pki))]
      (try
        (tls-write st "GET /1 HTTP/1.1\r\nHost: x\r\n\r\n")
        (check-has (str label "first request has the cert") good-name
                   (tls-read-until st #(str/includes? % good-name)))
        (tls-write st "GET /2 HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        (check-has (str label "second request has the cert") good-name
                   (tls-read-until st (constantly false)))
        (finally (tls-close st) (adapter/stop-server server))))))

;; a certificate that verified but can't be handed on is a server fault, not silence
(defn test-tls-client-auth-peer-cert-fault []
  (doseq [strategy [:threads :fibers]]
    (let [label (str "mtls " (name strategy) " peer-cert fault: ")
          server (adapter/run-server mtls-handler {:port 0 :strategy strategy
                                                   :tls (mtls-opts :need)})
          port (:port server)]
      (try
        (with-redefs [ring-chez.tls/peer-certificate (fn [_] (throw (ex-info "no parse" {})))]
          (check (str label "the client gets no answer") :failed (mtls-get port (:good @mtls-pki))))
        (Thread/sleep 100)
        (let [st (adapter/server-stats server)]
          (check (str label "reported as a fault") :tls-peer-cert (:kind (:last-fault st)))
          (check (str label "counted once") 1 (:faults st)))
        (check (str label "serves afterwards") (str "cert:" good-name)
               (mtls-body (mtls-get port (:good @mtls-pki))))
        (finally (adapter/stop-server server))))))

(defn- tls-refusal
  "The message run-server refused tls with, or :accepted."
  [tls]
  (try
    (adapter/stop-server (adapter/run-server mtls-handler {:port 0 :tls tls}))
    :accepted
    (catch clojure.lang.ExceptionInfo e
      (if (= :tls (:key (ex-data e))) (ex-message e) (throw e)))))

(defn test-tls-client-auth-options []
  (let [ca (:ca-file @mtls-pki)
        refused (fn [label needle tls]
                  (let [r (tls-refusal tls)]
                    (if (string? r)
                      (check-has (str "mtls options: " label) needle r)
                      (check (str "mtls options: " label) :refused r))))]
    (refused ":client-auth must be :need or :want" ":client-auth must be :need or :want"
             (assoc tls-opts :client-auth :maybe :client-ca-file ca))
    (refused ":client-auth needs :client-ca-file" ":client-auth needs :client-ca-file"
             (assoc tls-opts :client-auth :need))
    (refused ":client-ca-file must be a string" ":client-ca-file must name a PEM file"
             (assoc tls-opts :client-auth :need :client-ca-file 42))
    (refused "a missing :client-ca-file" "file does not exist: test/fixtures/tls/nope.pem"
             (assoc tls-opts :client-auth :need :client-ca-file "test/fixtures/tls/nope.pem"))
    ;; a PEM that holds no certificate: the server key
    (refused "a :client-ca-file OpenSSL cannot read" "cannot use client CA file test/fixtures/tls/key.pem"
             (assoc tls-opts :client-auth :need :client-ca-file "test/fixtures/tls/key.pem"))
    (refused ":client-ca-file alone" ":client-ca-file does nothing without :client-auth"
             (assoc tls-opts :client-ca-file ca))
    (refused ":client-ca-file with :client-auth false" ":client-ca-file does nothing without :client-auth"
             (assoc tls-opts :client-auth false :client-ca-file ca))
    (check "mtls options: :client-auth false is off" :accepted
           (tls-refusal (assoc tls-opts :client-auth false)))
    (check "mtls options: :need accepted" :accepted (tls-refusal (mtls-opts :need)))
    (check "mtls options: :want accepted" :accepted (tls-refusal (mtls-opts :want)))))

;; the sweeper's 503 is plaintext, and on a TLS connection that is noise
(defn test-tls-unclaimed-gets-no-plaintext []
  (let [[a b] (native/loopback-pair)
        buf (ffi/alloc 64)]
    (try
      (native/set-blocking! b false)
      (#'adapter/answer-unclaimed! {:conn a :tls? true})
      (Thread/sleep 50)
      (let [[n _] (native/c-recv b buf 64 0)]
        (check "tls: no plaintext 503 on an unclaimed conn" true (neg? n)))
      (#'adapter/answer-unclaimed! {:conn a})
      (Thread/sleep 50)
      (let [[n _] (native/c-recv b buf 64 0)]
        (check "plain: an unclaimed conn still gets its 503" true (pos? n)))
      (finally (ffi/free buf) (native/c-close a) (native/c-close b)))))

;; :handler-on-fiber: no thread between the connection and the handler
(defn- on-fiber-handler [request]
  (case (:uri request)
    "/where"  {:status 200 :body (if (jolt.host/current-fiber) "fiber" "thread")}
    "/throw"  (throw (ex-info "boom" {}))
    "/park"   (do (a/<!! (a/timeout 300)) {:status 200 :body "parked"})
    "/echo"   {:status 200 :body (slurp (:body request))}
    {:status 404 :body "no"}))

(defn- on-fiber-get [port path]
  (http/get (str "http://127.0.0.1:" port path) {:throw-exceptions false}))

(defn test-handler-on-fiber []
  (let [off (adapter/run-server on-fiber-handler {:port 0 :strategy :fibers})]
    (try
      (check "default fibers server: handler on a thread" "thread"
             (:body (on-fiber-get (:port off) "/where")))
      (finally (adapter/stop-server off))))
  (let [server (adapter/run-server on-fiber-handler
                                   {:port 0 :strategy :fibers :handler-on-fiber true
                                    :on-failure (fn [_ _] {:status 500 :body "caught"})})
        port (:port server)]
    (try
      (check "on-fiber: handler on the fiber" "fiber" (:body (on-fiber-get port "/where")))
      (let [r (on-fiber-get port "/throw")]
        (check "on-fiber: a throw reaches :on-failure" 500 (:status r))
        (check "on-fiber: :on-failure's body" "caught" (:body r)))
      (check "on-fiber: serves after a throw" "fiber" (:body (on-fiber-get port "/where")))
      (let [body (apply str (repeat 20000 "x"))
            r (http/post (str "http://127.0.0.1:" port "/echo")
                         {:body body :throw-exceptions false})]
        (check "on-fiber: request body reaches the handler" body (:body r)))
      ;; 64 waits of 300 ms overlap only if a parked handler lets go of its carrier.
      ;; Pinning them would take at least 300 ms for every carrier's worth, which is
      ;; 1.9 s on ten cores and longer on fewer.
      (let [t0 (System/currentTimeMillis)
            rs (mapv deref (mapv (fn [_] (future (on-fiber-get port "/park"))) (range 64)))
            ms (- (System/currentTimeMillis) t0)]
        (check "on-fiber: every parked handler answers" (repeat 64 "parked") (map :body rs))
        (check "on-fiber: parked handlers overlap" true (< ms 1500)))
      (finally (adapter/stop-server server)))))

(defn- refused? [opts]
  (try
    (adapter/stop-server (adapter/run-server on-fiber-handler (assoc opts :port 0)))
    false
    (catch clojure.lang.ExceptionInfo e (contains? (ex-data e) :key))))

(defn test-handler-on-fiber-options []
  (check "on-fiber: must be a boolean" true
         (refused? {:strategy :fibers :handler-on-fiber "yes"}))
  (check "on-fiber: refused under :threads" true
         (refused? {:strategy :threads :handler-on-fiber true}))
  (check "on-fiber: refused under the default strategy" true
         (refused? {:handler-on-fiber true}))
  (check "on-fiber: refused with a handler deadline" true
         (refused? {:strategy :fibers :handler-on-fiber true :handler-timeout-ms 100}))
  (check "on-fiber: false is fine anywhere" false
         (refused? {:strategy :threads :handler-on-fiber false :handler-timeout-ms 100}))
  (check "on-fiber: a zero deadline means none" false
         (refused? {:strategy :fibers :handler-on-fiber true :handler-timeout-ms 0})))

(defn test-handler-deadline-threads [] (handler-deadline-reclaims :threads 8532))
(defn test-handler-deadline-fibers  [] (handler-deadline-reclaims :fibers 8533))

;; the deadline cuts what it should and nothing else: same handler, one
;; server with the deadline under its runtime and one with it disabled
(defn test-handler-deadline-bounds []
  (let [cut  (adapter/run-server deadline-handler {:port 8534 :handler-timeout-ms 250})]
    (try
      (Thread/sleep 250)
      (check "deadline: slow handler over the deadline is cut" 503
             (:status (http/get "http://127.0.0.1:8534/slow" {:throw-exceptions false})))
      (finally (adapter/stop-server cut))))
  (let [off (adapter/run-server deadline-handler {:port 8535 :handler-timeout-ms 0})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8535/slow")]
        (check "deadline 0: slow handler runs to completion" 200 (:status r))
        (check-has "deadline 0: its body is served" "slow done" (:body r)))
      (finally (adapter/stop-server off))))
  ;; and with room, the same handler is untouched
  (let [ok (adapter/run-server deadline-handler {:port 8536 :handler-timeout-ms 5000})]
    (try
      (Thread/sleep 250)
      (check "deadline: handler inside the deadline is untouched" 200
             (:status (http/get "http://127.0.0.1:8536/slow")))
      (finally (adapter/stop-server ok))))
  ;; the DEFAULT is off: enforcing a deadline costs a thread handoff per
  ;; request on this strategy (15-25%), which is not a toll to charge every
  ;; server by default. Locked down here so it cannot drift back.
  (let [dflt (adapter/run-server deadline-handler {:port 8558})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8558/slow")]
        (check "deadline: off by default" 200 (:status r))
        (check-has "deadline: default runs the handler to completion" "slow done" (:body r)))
      (finally (adapter/stop-server dflt)))))

;; the deadline does not just stop waiting: it interrupts the abandoned
;; handler, so a compute-stuck one unwinds — its finally runs and its thread
;; terminates — instead of spinning forever
(defn test-handler-deadline-interrupts []
  (reset! spin-unwound nil)
  (let [server (adapter/run-server deadline-handler {:port 8560 :handler-timeout-ms 300})]
    (try
      (Thread/sleep 250)
      (check "deadline: compute-stuck handler answers 503" 503
             (:status (http/get "http://127.0.0.1:8560/spin" {:throw-exceptions false})))
      (let [t0 (System/currentTimeMillis)]
        (loop []
          (when (and (nil? @spin-unwound)
                     (< (- (System/currentTimeMillis) t0) 5000))
            (Thread/sleep 50)
            (recur))))
      (check "deadline: the abandoned handler unwound (finally ran)" :unwound @spin-unwound)
      (finally (adapter/stop-server server)))))

;; a channel body is returned immediately and streams for longer than the
;; deadline — the deadline covers handler execution, not the stream
(defn test-handler-deadline-spares-streams []
  (let [server (adapter/run-server deadline-handler {:port 8537 :handler-timeout-ms 250})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8537 5000)]
        (client-send fd "GET /stream-long HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (client-recv-until fd "0\r\n\r\n")]
          (check-has "deadline: stream not cut (first part)" "part0" r)
          (check-has "deadline: stream not cut (last part)" "part3" r)
          (check-has "deadline: stream terminated" "0\r\n\r\n" r))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; what the hook is told, and that it is told once
(def deadline-failures (atom []))

(defn test-handler-deadline-hook []
  (reset! deadline-failures [])
  (let [server (adapter/run-server
                 deadline-handler
                 {:port 8538 :handler-timeout-ms 250
                  :on-failure (fn [request t]
                                (swap! deadline-failures conj
                                       [(:ring-chez/failure request) (.getMessage t)])
                                {:status 429 :headers {"Content-Type" "text/plain"}
                                 :body "come back later"})})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8538/hang" {:throw-exceptions false})]
        (check "deadline hook: its response is served" 429 (:status r))
        (check-has "deadline hook: its body is served" "come back later" (:body r)))
      (let [[info _] (first @deadline-failures)]
        (check "deadline hook: kind" :timeout (:kind info))
        (check "deadline hook: elapsed-ms reported" true
               (>= (or (:elapsed-ms info) 0) 250)))
      ;; a handler that throws AFTER the deadline fired must not report twice
      (reset! deadline-failures [])
      (http/get "http://127.0.0.1:8538/throw-late" {:throw-exceptions false})
      (Thread/sleep 700)
      (check "deadline hook: one report per request" 1 (count @deadline-failures))
      (finally (adapter/stop-server server)))))

;; Igropyr's make-fault-handler, Ring-shaped
(defn test-fault-handler-envelope []
  (let [server (adapter/run-server deadline-handler
                                   {:port 8539 :handler-timeout-ms 250
                                    :on-failure (fault/fault-handler)})]
    (try
      (Thread/sleep 250)
      (let [r (http/get "http://127.0.0.1:8539/hang" {:throw-exceptions false})]
        (check "fault: status" 503 (:status r))
        (check-has "fault: content-type" "application/json"
                   (get-in r [:headers "content-type"] ""))
        (check-has "fault: kind" "\"fault\":\"timeout\"" (:body r))
        (check-has "fault: retryable" "\"retryable\":true" (:body r)))
      ;; a crash reports as a crash, at the status the caller picked
      (let [r (http/get "http://127.0.0.1:8539/nope" {:throw-exceptions false})]
        (check "fault: non-failure request untouched" 200 (:status r)))
      (finally (adapter/stop-server server)))))

;; --- Wave 2 round 1: stop-server closes live connections ---------------------------

;; "Stopped" has to mean the same thing on both strategies. The fibers path
;; closes every live conn; the threads path used to close only the listen fd,
;; so a keep-alive connection opened before the stop was still served after
;; stop-server returned — and its worker stayed on it for up to the idle
;; timeout (RFC-0008).
(defn- stop-closes-live-conn [strategy port]
  (let [calls  (atom 0)
        ;; the body names THIS server, so a response served by anyone else is
        ;; identifiable rather than just an anonymous 200
        token  (str "tok-" (name strategy) "-" port)
        h      (fn [_] (swap! calls inc)
                 {:status 200 :headers {"Content-Type" "text/plain"} :body token})
        server (adapter/run-server h {:port port :strategy strategy})
        label  (str (name strategy) " stop: ")]
    (Thread/sleep 250)
    (let [fd (client-connect port 3000)]
      (try
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [first-resp (or (client-recv fd) "")]
          (check (str label "served before the stop") true
                 (str/starts-with? first-resp "HTTP/1.1 200"))
          ;; whoever answered must be this server: a 200 carrying another
          ;; server's token means the connection was served by a stale fiber
          (check (str label "answered by this server") true
                 (str/includes? first-resp token))
          (when-not (str/includes? first-resp token)
            (println "  [stop: fd" fd "wanted" token "got" (pr-str first-resp) "]")))
        (adapter/stop-server server)
        ;; the send itself may fail (RST on a closed peer) — that is the same
        ;; answer as reading EOF, so both count as "not served"
        (let [r (try (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
                     (or (client-recv fd) "")
                     (catch Throwable _ ""))]
          (check (str label "nothing served after") false (str/starts-with? r "HTTP")))
        (check (str label "handler not re-entered") 1 @calls)
        (finally (client-close fd))))))

(defn test-stop-closes-live-conn-threads []
  (stop-closes-live-conn :threads 8530))

(defn test-stop-closes-live-conn-fibers []
  (stop-closes-live-conn :fibers 8531))

;; A stopped server must never answer anybody else's connection.
;;
;; conn-close! used to close(fd) and THEN poller/forget!, and forget! resumes
;; every fiber parked on that fd — whose retry recv fiber-recv! assumed would
;; answer EBADF. close() had already freed the fd NUMBER by then, and the
;; resumed fiber does not run until the scheduler reaches it, so an acceptor
;; could be handed that number first: the stale fiber's recv then read a LIVE
;; connection belonging to somebody else and answered it out of its own,
;; already-stopped server's handler. ~2.5% of responses on :fibers; :threads
;; never showed it, because only the fibers path parks on the poller.
;;
;; Every server stamps its own token into every body, so a response carrying
;; the wrong one names the bug instead of looking like an unexplained flake.
(defn- serves-only-own-conns [strategy base]
  (let [rounds 16
        conns  8
        bad    (atom [])
        silent (atom [])]
    (dotimes [i rounds]
      (let [pa (+ base (* 2 (mod i 8)))
            pb (inc pa)
            ta (str "tok-A-" i)
            tb (str "tok-B-" i)
            a  (adapter/run-server (fn [_] {:status 200 :body ta})
                                   {:port pa :strategy strategy})]
        (Thread/sleep 60)
        ;; live conns on A, left open so their fibers are parked on the poller
        ;; when the stop below wakes them
        (let [fds (doall (for [_ (range conns)]
                           (let [fd (client-connect pa 2000)]
                             (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
                             (client-recv fd)
                             fd)))]
          (adapter/stop-server a)
          ;; release the client numbers too: the bigger the pool of
          ;; just-freed fds, the likelier the acceptor below is handed one
          (doseq [fd fds] (client-close fd)))
        (let [b (adapter/run-server (fn [_] {:status 200 :body tb})
                                    {:port pb :strategy strategy})]
          (try
            (Thread/sleep 60)
            (let [fds (doall (for [_ (range conns)] (client-connect pb 5000)))]
              (doseq [fd fds]
                (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
              (doseq [fd fds]
                (let [r (client-recv fd)]
                  (cond
                    ;; nil is a read that timed out, "" a peer that closed
                    ;; without answering. Neither is a cross-serve: that is a
                    ;; response which ARRIVED carrying another server's token.
                    ;; `(or ... "")` used to fold the three together, so a read
                    ;; that came back empty was reported as a stopped server
                    ;; answering somebody else's connection — CI failed exactly
                    ;; that way, with :got "".
                    (str/blank? r)
                    (swap! silent conj {:round i :recv (if (nil? r) :timeout :closed)})

                    (not (str/includes? r tb))
                    (swap! bad conj {:round i :wanted tb
                                     :got (subs r 0 (min 120 (count r)))}))))
              (doseq [fd fds] (client-close fd)))
            (finally (adapter/stop-server b))))))
    (doseq [b (take 3 @bad)]
      (println "  [cross-serve:" (pr-str b) "]"))
    (doseq [s (take 3 @silent)]
      (println "  [no response:" (pr-str s) "]"))
    (check (str (name strategy) " isolation: no conn served by a stopped server")
           0 (count @bad))
    ;; Still a failure, just its own one: a live server that never answers a
    ;; well-formed request is a bug too — an owner from the stopped server
    ;; closing an fd number the new acceptor was just handed would look like
    ;; this — but it is not the bug the check above names. 5s of client budget
    ;; keeps a slow runner from reading as one.
    (check (str (name strategy) " isolation: every conn answered")
           0 (count @silent))))

(defn test-serves-only-own-conns-fibers []
  (serves-only-own-conns :fibers 8600))

(defn test-serves-only-own-conns-threads []
  (serves-only-own-conns :threads 8620))

;; stop-server must leave nothing behind even while connections are arriving as
;; it runs. serve-loop read running? BEFORE register-conn! published the entry,
;; so a conn accepted in that window landed in `conns` AFTER the sweep had
;; passed over it — a stopped server went on serving it, and its fd leaked.
;; serve! now registers first and checks after, which interlocks with
;; stop-server clearing running? before it sweeps: the conn is either in `conns`
;; when the sweep reads it, or the check sees false and cleans up on the spot.
;;
;; Honest about what this is: the window is a couple of microseconds, so this
;; does not reproduce the bug on its own — a storm of 20 rounds never hit it.
;; Widening the window to 5ms made it 9 rounds in 12, and 0 with the fix in.
;; What this holds is the invariant, cheaply: the clients stay OPEN across the
;; stop, so a connection that missed the sweep keeps the count above zero
;; rather than being reaped for going away.
(defn- stop-leaves-nothing-behind [strategy port]
  (let [server (adapter/run-server (fn [_] {:status 200 :body "x"})
                                   {:port port :strategy strategy})
        held     (atom [])
        stop?    (atom false)
        ;; throttled and capped: this only needs a connect in flight when the
        ;; stop lands, and an unbounded storm would run the suite out of fds
        stormers (doall
                   (for [_ (range 3)]
                     (future
                       (while (and (not @stop?) (< (count @held) 60))
                         (try (let [fd (client-connect port 2000)]
                                (swap! held conj fd)
                                (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
                              (catch Throwable _ nil))
                         (Thread/sleep 2)))))]
    (try
      (Thread/sleep 150)
      (adapter/stop-server server)
      (reset! stop? true)
      (doseq [f stormers] (deref f 3000 :timeout))
      ;; an owner may still be unwinding when stop-server returns, so poll
      (let [give-up (+ (System/currentTimeMillis) 3000)
            final   (loop []
                      (let [c (:connections (adapter/server-stats server))]
                        (if (or (zero? c) (> (System/currentTimeMillis) give-up))
                          c
                          (do (Thread/sleep 50) (recur)))))]
        (check (str (name strategy) " stop: nothing left open afterwards") 0 final))
      (finally
        (reset! stop? true)
        (doseq [fd @held] (try (client-close fd) (catch Throwable _ nil)))))))

(defn test-stop-leaves-nothing-behind-fibers []
  (stop-leaves-nothing-behind :fibers 8640))

(defn test-stop-leaves-nothing-behind-threads []
  (stop-leaves-nothing-behind :threads 8641))

;; --- Round 6: operability ---------------------------------------------------------

(defn- echo-body-handler [req]
  (let [body (if-let [b (:body req)] (slurp b) "")]
    {:status 200
     :headers {"Content-Type" "text/plain; charset=utf-8"
               "X-Body-Octets" (str (alength (.getBytes ^String body "UTF-8")))
               "X-Body-Chars" (str (count body))}
     :body body}))



(defn- framing-ask
  "Send head (and body) on a fresh connection, read one response."
  [port req]
  (let [fd (client-connect port 3000)]
    (client-send fd req)
    (let [r (or (client-recv-until fd "\r\n\r\n") "")]
      (client-close fd)
      r)))



(defn test-graceful-drain []
  ;; stop-server used to slam every connection, including one mid-response.
  ;; It now stops accepting and waits for in-flight requests to finish
  ;; (Igropyr http-shutdown!, http.sc:1842).
  (let [server (adapter/run-server
                 (fn [_] (Thread/sleep 800) {:status 200 :body "finished"})
                 {:port 8520 :worker-threads 2})
        result (atom nil)]
    (Thread/sleep 250)
    (let [fd (client-connect 8520 6000)]
      (client-send fd "GET /slow HTTP/1.1\r\nHost: t\r\n\r\n")
      ;; give the handler time to start, then stop mid-request
      (Thread/sleep 200)
      (let [t0 (System/currentTimeMillis)]
        (future (reset! result (client-recv fd)))
        (adapter/stop-server server)
        (check "drain: stop waited for the in-flight request"
               true (>= (- (System/currentTimeMillis) t0) 400)))
      (Thread/sleep 300)
      (check-has "drain: in-flight request was answered" "finished" (or @result ""))
      (client-close fd))
    ;; and the port is free straight after
    (let [s2 (adapter/run-server handler {:port 8520 :worker-threads 1})]
      (Thread/sleep 150)
      (try (check "drain: port rebindable after stop" 200
                  (:status (http/get "http://127.0.0.1:8520/")))
           (finally (adapter/stop-server s2))))))

(defn test-drain-timeout []
  ;; a handler that never returns must not stop stop-server returning
  (let [server (adapter/run-server
                 (fn [_] (Thread/sleep 30000) {:status 200 :body "never"})
                 {:port 8521 :worker-threads 2})]
    (Thread/sleep 250)
    (let [fd (client-connect 8521 2000)]
      (client-send fd "GET /stuck HTTP/1.1\r\nHost: t\r\n\r\n")
      (Thread/sleep 200)
      (let [t0 (System/currentTimeMillis)]
        (adapter/stop-server server {:drain-timeout-ms 500})
        (let [dt (- (System/currentTimeMillis) t0)]
          (check (str "drain: bounded by :drain-timeout-ms (" dt "ms)")
                 true (< dt 2500))))
      (client-close fd))))

(defn test-server-stats []
  (let [server (adapter/run-server handler {:port 8522 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [s0 (adapter/server-stats server)]
        (check "stats: no requests yet" 0 (:requests s0))
        (check "stats: uptime is a number" true (number? (:uptime-ms s0))))
      (let [fd (client-connect 8522 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (client-recv fd)
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (client-recv fd)
        (let [s1 (adapter/server-stats server)]
          (check "stats: requests counted" 2 (:requests s1))
          (check "stats: connection counted open" 1 (:connections s1)))
        ;; :active is decremented in a finally AFTER the response is sent, so
        ;; the client can hold the response while the worker is still a line
        ;; short of the decrement — sampled the instant recv returned, this
        ;; read 1 on a slow CI runner. Give it a moment to settle.
        (check "stats: nothing in flight between requests"
               0 (loop [n 0]
                   (let [a (:active (adapter/server-stats server))]
                     (if (or (zero? a) (>= n 50))
                       a
                       (do (Thread/sleep 10) (recur (inc n)))))))
        (client-close fd))
      (Thread/sleep 300)
      (check "stats: connection no longer open" 0 (:connections (adapter/server-stats server)))
      (finally (adapter/stop-server server)))))

(defn test-handler-hot-swap []
  (let [server (adapter/run-server (fn [_] {:status 200 :body "first"})
                                   {:port 8523 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (check "swap: original handler" "first" (:body (http/get "http://127.0.0.1:8523/")))
      (adapter/swap-handler! server (fn [_] {:status 200 :body "second"}))
      (check "swap: replacement handler" "second" (:body (http/get "http://127.0.0.1:8523/")))
      ;; and on a connection that was already open
      (let [fd (client-connect 8523 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "swap: open keep-alive conn sees it too" "second" (client-recv fd))
        (adapter/swap-handler! server (fn [_] {:status 200 :body "third"}))
        (client-send fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (check-has "swap: mid-connection swap" "third" (client-recv fd))
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-uri-normalization []
  ;; the router matches on segments and drops empty ones, so "//admin/x"
  ;; routes exactly like "/admin/x" — while a guard written as a prefix test
  ;; on the raw string does not match. Normalizing once, here, closes that gap
  ;; for middleware, routers and static serving alike (Igropyr normalize-path).
  (let [server (adapter/run-server
                 (fn [req] {:status 200 :body (str (:uri req) " raw=" (:ring-chez/raw-uri req))})
                 {:port 8524 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (doseq [[target expect] [["/a/b" "/a/b"]
                               ["//admin/x" "/admin/x"]
                               ["/a/./b" "/a/b"]
                               ["/a/c/../b" "/a/b"]
                               ["/../../etc/passwd" "/etc/passwd"]
                               ["/a//b///c" "/a/b/c"]
                               ["/" "/"]
                               ["/a/" "/a"]]]
        (check-has (str "uri: " target " -> " expect)
                   (str "\r\n\r\n" expect " raw=")
                   (framing-ask 8524 (str "GET " target " HTTP/1.1\r\nHost: t\r\n\r\n"))))
      (check-has "uri: the raw target is still available"
                 "raw=//admin/x"
                 (framing-ask 8524 "GET //admin/x HTTP/1.1\r\nHost: t\r\n\r\n"))
      ;; the query string is split off before normalization and is not part of
      ;; the raw path either
      (check-has "uri: normalized with a query string attached"
                 "\r\n\r\n/a/b raw=/a//b"
                 (framing-ask 8524 "GET /a//b?q=1 HTTP/1.1\r\nHost: t\r\n\r\n"))
      (finally (adapter/stop-server server)))))

(defn test-head-and-status-framing []
  ;; RFC 9110 9.3.2: a HEAD response carries the same headers a GET would,
  ;; Content-Length included — we sent none at all, so a client sizing a
  ;; resource with HEAD learned nothing. 1xx and 204 forbid Content-Length;
  ;; 304 MAY carry the length the corresponding 200 would have had.
  (let [server (adapter/run-server
                 (fn [req] (case (:uri req)
                             "/ten" {:status 200 :headers {"Content-Type" "text/plain"}
                                     :body "0123456789"}
                             "/304" {:status 304 :headers {"ETag" "\"x\""} :body "0123456789"}
                             "/204" {:status 204 :body "ignored"}
                             {:status 200 :body "ok"}))
                 {:port 8525 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [r (framing-ask 8525 "HEAD /ten HTTP/1.1\r\nHost: t\r\n\r\n")]
        (check-has "HEAD: carries the length a GET would" "Content-Length: 10" r)
        (check "HEAD: and no body" true (str/ends-with? r "\r\n\r\n")))
      (let [r (framing-ask 8525 "GET /204 HTTP/1.1\r\nHost: t\r\n\r\n")]
        (check "204: no Content-Length" false
               (str/includes? (str/lower-case r) "content-length")))
      (let [r (framing-ask 8525 "GET /304 HTTP/1.1\r\nHost: t\r\n\r\n")]
        (check-has "304: may carry the length" "Content-Length: 10" r)
        (check "304: and no body" true (str/ends-with? r "\r\n\r\n")))
      ;; connection still framed after all of those
      (check-has "HEAD/304: connection still usable" "ok"
                 (framing-ask 8525 "GET / HTTP/1.1\r\nHost: t\r\n\r\n"))
      (finally (adapter/stop-server server)))))

(defn test-http10-keep-alive-and-close []
  ;; "Connection: keep-alive, close" is a token LIST and really does mean close
  (let [server (adapter/run-server handler {:port 8526 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [r (framing-ask 8526 (str "GET / HTTP/1.0\r\nHost: t\r\n"
                                     "Connection: keep-alive, close\r\n\r\n"))]
        (check-has "HTTP/1.0: keep-alive with close means close" "Connection: close" r))
      (let [r (framing-ask 8526 (str "GET / HTTP/1.0\r\nHost: t\r\n"
                                     "Connection: keep-alive\r\n\r\n"))]
        (check-has "HTTP/1.0: plain keep-alive is honored" "Connection: keep-alive" r))
      (finally (adapter/stop-server server)))))

;; --- Round 5: resource bounds -----------------------------------------------------

(defn test-request-deadline []
  ;; SO_RCVTIMEO re-arms on every recv and there was no cap on how long one
  ;; request may take to ARRIVE, so a client dribbling a byte just inside the
  ;; idle timeout held a worker indefinitely — with a fixed pool that is the
  ;; whole server (Igropyr request-deadline-ms, http.sc:72-78).
  (let [server (adapter/run-server handler {:port 8510 :worker-threads 2
                                            :keep-alive-timeout-ms 1000
                                            :request-timeout-ms 700})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8510 6000)
            t0 (System/currentTimeMillis)]
        ;; dribble: every write lands inside the idle timeout, so only a
        ;; whole-request deadline can end this
        (client-send fd "GET / HTTP/1.1\r\n")
        (Thread/sleep 400)
        (try (client-send fd "Host: t\r\n") (catch Throwable _ nil))
        (Thread/sleep 400)
        (try (client-send fd "X-A: 1\r\n") (catch Throwable _ nil))
        (let [r (or (client-recv fd) "")
              dt (- (System/currentTimeMillis) t0)]
          (check-has "deadline: dribbled request gets 408" "408" r)
          (check (str "deadline: cut near the deadline, not the idle timeout ("
                      dt "ms)")
                 true (< dt 2500)))
        (client-close fd))
      ;; the worker is free again
      (let [r (http/get "http://127.0.0.1:8510/")]
        (check "deadline: server still serving" 200 (:status r)))
      (finally (adapter/stop-server server)))))

(defn test-request-deadline-fibers []
  (let [server (adapter/run-server handler {:port 8511 :strategy :fibers
                                            :keep-alive-timeout-ms 1000
                                            :request-timeout-ms 700})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8511 6000)]
        (client-send fd "GET / HTTP/1.1\r\n")
        (Thread/sleep 400)
        (try (client-send fd "Host: t\r\n") (catch Throwable _ nil))
        (Thread/sleep 600)
        (check "deadline (fibers): dribbled request ended"
               true (contains? #{"" nil} (let [r (client-recv fd)]
                                           (if (and r (str/includes? r "408")) "" r))))
        (client-close fd))
      (let [r (http/get "http://127.0.0.1:8511/")]
        (check "deadline (fibers): server still serving" 200 (:status r)))
      (finally (adapter/stop-server server)))))

(defn test-header-limit []
  ;; the head used to share the 1 MiB request cap, so a head alone could be
  ;; 1 MiB. Igropyr caps headers separately at 8 KiB (http.sc:61) and checks
  ;; the size of a COMPLETE block too — the limit used to catch only a header
  ;; still arriving, so a peer that sent 9 KiB in one go was never checked.
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8512 :worker-threads 2
                                    :keep-alive-timeout-ms 1000})]
    (try
      (Thread/sleep 250)
      (let [pad (apply str (repeat 9000 "p"))]
        (check-has "header limit: 9KB head in one write is 431"
                   "431" (framing-ask 8512 (str "GET / HTTP/1.1\r\nHost: t\r\nX-Pad: "
                                                pad "\r\n\r\n"))))
      ;; a body well past the header limit is still fine: they are separate caps
      (let [body (apply str (repeat 20000 "b"))]
        (check-has "header limit: a large body is not a header"
                   "X-Body-Octets: 20000"
                   (framing-ask 8512 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                          "Content-Length: 20000\r\n\r\n" body))))
      (finally (adapter/stop-server server)))))

(defn test-header-limit-configurable []
  (let [server (adapter/run-server handler {:port 8513 :worker-threads 2
                                            :max-header-bytes 32768
                                            :keep-alive-timeout-ms 1000})]
    (try
      (Thread/sleep 250)
      (let [pad (apply str (repeat 9000 "p"))]
        (check-has "header limit: raised limit accepts a 9KB head"
                   "200" (framing-ask 8513 (str "GET / HTTP/1.1\r\nHost: t\r\nX-Pad: "
                                                pad "\r\n\r\n"))))
      (finally (adapter/stop-server server))))
  (try
    (let [s (adapter/run-server handler {:port 8514 :max-header-bytes 0})]
      (adapter/stop-server s)
      (check "header limit: 0 rejected at boot" :threw :did-not-throw))
    (catch Throwable t
      (check "header limit: 0 rejected at boot" :threw :threw)
      (check-has "header limit: names the key" ":max-header-bytes" (ex-message t)))))

(defn test-large-file-response []
  ;; a File body was copied into a ByteArrayOutputStream, then .toByteArray'd,
  ;; then written into an FFI buffer sized to the whole thing — three copies
  ;; resident at once. It is streamed from disk in bounded chunks now, framed
  ;; by the length on disk.
  (let [path (str (System/getProperty "java.io.tmpdir") "/ring-chez-big.bin")
        size (* 8 1024 1024)
        _ (spit path (apply str (repeat size "q")))
        f (io/file path)
        server (adapter/run-server (fn [_] {:status 200
                                            :headers {"Content-Type" "application/octet-stream"}
                                            :body f})
                                   {:port 8515 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8515 20000)]
        (client-send fd "GET /big HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [raw (client-recv-raw fd)
              body (response-body-bytes raw)]
          (check-has "big file: framed by the length on disk"
                     (str "Content-Length: " size) (latin1 raw))
          (check "big file: all octets delivered" size (alength body))
          (check "big file: octets are the file's" true
                 (every? #(= (byte \q) %) (take 1000 body)))
          (check "big file: last octet is the file's" (byte \q) (aget body (dec size))))
        (client-close fd))
      (finally (adapter/stop-server server) (.delete f)))))

(defn test-input-stream-response-framing []
  ;; an InputStream has no length until it ends. Small ones are buffered and
  ;; framed by Content-Length as before; one too big to buffer switches to
  ;; chunked rather than growing a buffer to whatever the stream turns out
  ;; to be.
  (let [small (byte-array (repeat 1000 (byte \s)))
        big (byte-array (repeat 500000 (byte \g)))
        server (adapter/run-server
                 (fn [req] {:status 200
                            :body (java.io.ByteArrayInputStream.
                                    (if (= "/big" (:uri req)) big small))})
                 {:port 8516 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8516 20000)]
        (client-send fd "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [raw (client-recv-raw fd)]
          (check-has "stream: a short stream keeps Content-Length"
                     "Content-Length: 1000" (latin1 raw))
          (check "stream: short body intact" 1000 (alength (response-body-bytes raw))))
        (client-close fd))
      (let [fd (client-connect 8516 20000)]
        (client-send fd "GET /big HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [raw (client-recv-until-bytes fd "0\r\n\r\n")
              view (latin1 raw)
              hdr-end (str/index-of view "\r\n\r\n")
              body (dechunk (java.util.Arrays/copyOfRange raw (+ hdr-end 4) (alength raw)))]
          (check-has "stream: a long stream switches to chunked"
                     "Transfer-Encoding: chunked" view)
          (check "stream: long body intact" 500000 (alength body))
          (check "stream: long body octets" true
                 (every? #(= (byte \g) %) (take 1000 body))))
        (client-close fd))
      (finally (adapter/stop-server server)))))

;; --- Round 4: request framing ----------------------------------------------------

(defn test-chunked-request-body []
  ;; Transfer-Encoding: chunked was answered 501, so every streaming upload
  ;; failed. Ported from Igropyr parse-chunked-body (http.sc:1428).
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8500 :worker-threads 2
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [r (framing-ask 8500 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: chunked\r\n\r\n"
                                     "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"))]
        (check-has "chunked: 200" "200" r)
        (check-has "chunked: chunks reassembled" "X-Body-Octets: 11" r))
      ;; chunk extensions on the size line are ignored, not fatal
      (let [r (framing-ask 8500 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: chunked\r\n\r\n"
                                     "5;foo=bar\r\nhello\r\n0\r\n\r\n"))]
        (check-has "chunked: chunk extensions ignored" "X-Body-Octets: 5" r))
      ;; trailers are validated and dropped
      (let [r (framing-ask 8500 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: chunked\r\n\r\n"
                                     "5\r\nhello\r\n0\r\nX-Note: done\r\n\r\n"))]
        (check-has "chunked: trailers accepted" "X-Body-Octets: 5" r))
      (let [r (framing-ask 8500 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: chunked\r\n\r\n"
                                     "5\r\nhello\r\n0\r\nContent-Length: 5\r\n\r\n"))]
        (check-has "chunked: forbidden trailer field is 400" "400" r))
      ;; a body arriving one chunk per write still frames
      (let [fd (client-connect 8500 3000)]
        (client-send fd (str "POST / HTTP/1.1\r\nHost: t\r\n"
                             "Transfer-Encoding: chunked\r\n\r\n"))
        (client-send fd "3\r\nabc\r\n")
        (client-send fd "3\r\ndef\r\n")
        (client-send fd "0\r\n\r\n")
        (check-has "chunked: body split across writes" "X-Body-Octets: 6"
                   (or (client-recv-until fd "\r\n\r\n") ""))
        (client-close fd))
      (let [r (framing-ask 8500 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: chunked\r\n\r\n"
                                     "zz\r\nhello\r\n0\r\n\r\n"))]
        (check-has "chunked: malformed chunk size is 400" "400" r))
      (let [r (framing-ask 8500 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: gzip, chunked\r\n\r\n"
                                     "0\r\n\r\n"))]
        (check-has "chunked: an unimplemented coding is still 501" "501" r))
      (finally (adapter/stop-server server)))))

(defn test-chunked-body-cap []
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8501 :worker-threads 2
                                    :max-request-bytes 4096
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [chunk (apply str (repeat 1000 "x"))
            body (apply str (concat (repeat 8 (str "3e8\r\n" chunk "\r\n")) ["0\r\n\r\n"]))
            r (framing-ask 8501 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                     "Transfer-Encoding: chunked\r\n\r\n" body))]
        (check-has "chunked: past the cap is 413" "413" r))
      (finally (adapter/stop-server server)))))

(defn test-smuggling-guards []
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8502 :worker-threads 2
                                    :keep-alive-timeout-ms 1000})]
    (try
      (Thread/sleep 250)
      (check-has "smuggling: Transfer-Encoding with Content-Length is 400"
                 "400" (framing-ask 8502 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                              "Content-Length: 5\r\n"
                                              "Transfer-Encoding: chunked\r\n\r\n"
                                              "0\r\n\r\n")))
      ;; chunked is HTTP/1.1 framing; an HTTP/1.0 request declaring it is one
      ;; whose message boundary a 1.0 intermediary reads differently
      (check-has "smuggling: chunked on HTTP/1.0 is 400"
                 "400" (framing-ask 8502 (str "POST / HTTP/1.0\r\n"
                                              "Transfer-Encoding: chunked\r\n\r\n"
                                              "0\r\n\r\n")))
      (check-has "smuggling: bare LF between headers is 400"
                 "400" (framing-ask 8502 "GET / HTTP/1.1\r\nHost: t\nX-A: 1\r\n\r\n"))
      (check-has "smuggling: stray CR in the head is 400"
                 "400" (framing-ask 8502 "GET / HTTP/1.1\r\nHost: t\rX-A: 1\r\n\r\n"))
      (check-has "smuggling: obs-fold continuation line is 400"
                 "400" (framing-ask 8502 "GET / HTTP/1.1\r\nHost: t\r\nX-A: 1\r\n 2\r\n\r\n"))
      (check-has "smuggling: non-token header name is 400"
                 "400" (framing-ask 8502 "GET / HTTP/1.1\r\nHost: t\r\nX Bad: 1\r\n\r\n"))
      (check-has "smuggling: signed Content-Length is 400"
                 "400" (framing-ask 8502 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                              "Content-Length: +5\r\n\r\nhello")))
      ;; a repeated value coalesces to one comma-joined field; identical
      ;; repeats are legal (RFC 7230 §3.3.2), differing ones are not
      (check-has "smuggling: identical repeated Content-Length is accepted"
                 "X-Body-Octets: 5"
                 (framing-ask 8502 (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                        "Content-Length: 5, 5\r\n\r\nhello")))
      (finally (adapter/stop-server server)))))

(defn test-duplicate-request-headers []
  ;; the header reduce used assoc, so repeats collapsed to the last value and
  ;; everything before it was dropped — X-Forwarded-For chains silently lost
  (let [server (adapter/run-server
                 (fn [req] {:status 200 :body (str (get-in req [:headers "x-multi"]))})
                 {:port 8503 :worker-threads 2})]
    (try
      (Thread/sleep 250)
      (check-has "dup headers: repeats joined with a comma"
                 "one,two,three"
                 (framing-ask 8503 (str "GET / HTTP/1.1\r\nHost: t\r\nX-Multi: one\r\n"
                                        "X-Multi: two\r\nX-Multi: three\r\n\r\n")))
      (finally (adapter/stop-server server)))))

(defn test-expect-100-continue []
  ;; without the interim response a client that asked for it waits out its own
  ;; timeout before sending the body (curl stalls ~1s)
  (let [server (adapter/run-server echo-body-handler
                                   {:port 8504 :worker-threads 2
                                    :keep-alive-timeout-ms 2000})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect 8504 3000)]
        (client-send fd (str "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n"
                             "Expect: 100-continue\r\n\r\n"))
        (let [interim (or (client-recv-until fd "\r\n\r\n") "")]
          (check-has "expect: 100 Continue sent before the body" "100 Continue" interim))
        (client-send fd "hello")
        (check-has "expect: body then answered" "X-Body-Octets: 5"
                   (or (client-recv-until fd "\r\n\r\n") ""))
        (client-close fd))
      ;; chunked bodies get it too
      (let [fd (client-connect 8504 3000)]
        (client-send fd (str "POST / HTTP/1.1\r\nHost: t\r\n"
                             "Transfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n"))
        (check-has "expect: 100 Continue for a chunked body" "100 Continue"
                   (or (client-recv-until fd "\r\n\r\n") ""))
        (client-send fd "2\r\nhi\r\n0\r\n\r\n")
        (check-has "expect: chunked body then answered" "X-Body-Octets: 2"
                   (or (client-recv-until fd "\r\n\r\n") ""))
        (client-close fd))
      ;; a bodyless request must not get one
      (let [r (framing-ask 8504 (str "GET / HTTP/1.1\r\nHost: t\r\n"
                                     "Expect: 100-continue\r\n\r\n"))]
        (check "expect: no interim for a bodyless request"
               false (str/includes? r "100 Continue")))
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
  (doseq [bad ["" "no.such.host.invalid" "999.1.1.1" 42]]
    (try
      (let [s (adapter/run-server handler {:port 8492 :host bad})]
        (adapter/stop-server s)
        (check (str "host: " (pr-str bad) " rejected at boot") :threw :did-not-throw))
      (catch Throwable t
        (check (str "host: " (pr-str bad) " rejected at boot") :threw :threw)
        (check-has (str "host: " (pr-str bad) " names :host") ":host" (ex-message t))))))

;; --- RFC-0016: hostname and IPv6 bind ---------------------------------------------

(defn test-bind-sockaddrs []
  ;; the resolver behind :host — getaddrinfo, so literals and names take one
  ;; path and the port is patched into the family-agnostic bytes 2-3
  (let [v4 (socket/resolve-bind-sockaddrs "127.0.0.1" 8505)
        v6 (socket/resolve-bind-sockaddrs "::1" 8506)
        lo (socket/resolve-bind-sockaddrs "localhost" 8507)
        port-of (fn [e] (+ (* 256 (ffi/read (:addr e) :uint8 2))
                           (ffi/read (:addr e) :uint8 3)))]
    (doseq [[label res n] [["v4 literal" v4 1] ["v6 literal" v6 1]]]
      (check (str "resolve: one candidate for a " label) n (count res)))
    (check "resolve: v4 family and length" [native/af-inet 16]
           (first (mapv (juxt :family :addrlen) v4)))
    (check "resolve: v6 family and length" [native/af-inet6 28]
           (first (mapv (juxt :family :addrlen) v6)))
    (check "resolve: v4 port patched in" 8505 (port-of (first v4)))
    (check "resolve: v6 port patched in" 8506 (port-of (first v6)))
    (check "resolve: localhost has a v4 answer"
           true (boolean (some #{native/af-inet} (map :family lo))))
    ;; a box with v6 has both answers for localhost — bind-all is the point
    (when (= 1 (count v6))
      (check "resolve: localhost has a v6 answer"
             true (boolean (some #{native/af-inet6} (map :family lo)))))
    (try
      (socket/resolve-bind-sockaddrs "no.such.host.invalid" 8505)
      (check "resolve: unresolvable name throws" :threw :did-not-throw)
      (catch Throwable t
        (check "resolve: unresolvable name throws" :threw :threw)
        (check "resolve: error names :host" true (str/includes? (ex-message t) ":host"))
        (check "resolve: error carries :gai-code" true (contains? (ex-data t) :gai-code))))
    (doseq [all [v4 v6 lo] e all] (ffi/free (:addr e)))))

(defn test-listen-sockets-uniform-port []
  ;; port 0 must hand back ONE port for every family bound, or the handle's
  ;; :port would name only the first socket
  (let [{fds :fds port :port} (socket/listen-sockets "localhost" 0 nil)]
    (try
      (check "listen-sockets: at least one fd" true (pos? (count fds)))
      (check "listen-sockets: every fd on the same port"
             true (apply = port (map socket/local-port fds)))
      (check "listen-sockets: every fd close-on-exec"
             true (every? native/close-on-exec? fds))
      (finally
        (doseq [fd fds]
        (native/c-shutdown fd 2)
        (native/c-close fd))))))

(defn test-peer-ip-v6 []
  ;; accept() now fills a sockaddr_storage; a v6 peer must format back
  ;; through inet_ntop, not the dotted-quad reader
  (let [sa (ffi/alloc 28)]
    (try
      (dotimes [i 28] (ffi/write sa :uint8 0 i))
      (if t-macos?
        (do (ffi/write sa :uint8 28 0) (ffi/write sa :uint8 native/af-inet6 1))
        (ffi/write sa :uint8 native/af-inet6 0))
      ;; ::1 — last byte of the 16 at offset 8
      (ffi/write sa :uint8 1 23)
      (check "peer-ip: ::1" "::1" (socket/peer-ip sa))
      ;; 2001:db8::1
      (dotimes [i 28] (ffi/write sa :uint8 0 i))
      (if t-macos?
        (do (ffi/write sa :uint8 28 0) (ffi/write sa :uint8 native/af-inet6 1))
        (ffi/write sa :uint8 native/af-inet6 0))
      (ffi/write sa :uint8 0x20 8) (ffi/write sa :uint8 0x01 9)
      (ffi/write sa :uint8 0x0d 10) (ffi/write sa :uint8 0xb8 11)
      (ffi/write sa :uint8 1 23)
      (check "peer-ip: 2001:db8::1" "2001:db8::1" (socket/peer-ip sa))
      (finally (ffi/free sa)))))

(defn test-ipv6-bind-and-serve []
  ;; :host "::1" — a v6-only server; remote-addr must read back as ::1
  (let [seen (atom nil)
        server (adapter/run-server (fn [req] (reset! seen (:remote-addr req))
                                     {:status 200 :body "ok"})
                                   {:port 8505 :host "::1"})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect-host "::1" 8505 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: [::1]:8505\r\n\r\n")
        (client-recv fd)
        (check "ipv6: serves on ::1" "::1" @seen)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-localhost-binds-both []
  ;; a name with both answers binds both families on the one port — the
  ;; bind-all rule of RFC-0016
  (let [seen (atom nil)
        server (adapter/run-server (fn [req] (reset! seen (:remote-addr req))
                                     {:status 200 :body "ok"})
                                   {:port 8506 :host "localhost"})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect-host "::1" 8506 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: [::1]:8506\r\n\r\n")
        (client-recv fd)
        (check "localhost: v6 answer served" "::1" @seen)
        (client-close fd))
      (let [fd (client-connect 8506 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        (client-recv fd)
        (check "localhost: v4 answer served" "127.0.0.1" @seen)
        (client-close fd))
      (finally (adapter/stop-server server)))))

(defn test-eaddrinuse-aborts-across-families []
  ;; a name whose v4 leg collides must not leave its v6 leg listening: the
  ;; abort-all policy, because "port in use" is how you notice a running server
  (let [first-srv (adapter/run-server handler {:port 8507 :host "127.0.0.1"})]
    (try
      (Thread/sleep 250)
      (try
        (adapter/stop-server (adapter/run-server handler {:port 8507 :host "localhost"}))
        (check "eaddrinuse: second server on a taken port throws" :threw :did-not-throw)
        (catch Throwable t
          (check "eaddrinuse: second server on a taken port throws" :threw :threw)
          (check "eaddrinuse: message names the port" true (str/includes? (ex-message t) "8507"))))
      ;; localhost answers ::1 first here, so the v6 leg was bound before the
      ;; v4 collision — the abort has to have closed it again
      (check "eaddrinuse: no v6 leg left listening"
             :refused
             (try (t-close (client-connect-host "::1" 8507 500)) :connected
                  (catch Throwable _ :refused)))
      (finally (adapter/stop-server first-srv)))))

(defn test-host-non-ascii []
  ;; the node buffer is sized in UTF-8 octets, not chars — a non-ASCII name
  ;; must fail resolution cleanly, not write past the buffer
  (try
    (adapter/stop-server (adapter/run-server handler {:port 8520 :host "ünïcödé.invalid"}))
    (check "host: non-ASCII name rejected" :threw :did-not-throw)
    (catch Throwable t
      (check "host: non-ASCII name rejected" :threw :threw)
      (check-has "host: non-ASCII name names :host" ":host" (ex-message t)))))

(defn test-ipv6-any []
  ;; "::" is the v6 wildcard, v6-only (IPV6_V6ONLY): it serves ::1 and leaves
  ;; the v4 side of the port alone
  (let [server (adapter/run-server handler {:port 8521 :host "::"})]
    (try
      (Thread/sleep 250)
      (let [fd (client-connect-host "::1" 8521 3000)]
        (client-send fd "GET / HTTP/1.1\r\nHost: [::1]\r\n\r\n")
        (check "ipv6 any: serves ::1" true (str/includes? (or (client-recv fd) "") "200"))
        (client-close fd))
      (check "ipv6 any: v4 side not bound"
             :refused
             (try (t-close (client-connect 8521 500)) :connected
                  (catch Throwable _ :refused)))
      (finally (adapter/stop-server server)))))

(defn test-ipv6-fibers []
  ;; the fibers arm runs its own accept loops — one per family as well
  (let [seen (atom [])
        server (adapter/run-server (fn [req] (swap! seen conj (:remote-addr req))
                                     {:status 200 :body "ok"})
                                   {:port 8522 :host "localhost" :strategy :fibers})]
    (try
      (Thread/sleep 250)
      (doseq [host ["::1" "127.0.0.1"]]
        (let [fd (client-connect-host host 8522 3000)]
          (client-send fd "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
          (client-recv fd)
          (client-close fd)))
      (check "ipv6 fibers: both families served" #{"::1" "127.0.0.1"} (set @seen))
      (finally (adapter/stop-server server)))))

(defn test-stop-closes-all-sockets []
  ;; stop-server tears down every listen fd, or the v6 leg would keep the
  ;; port after a stop
  (let [server (adapter/run-server handler {:port 8508 :host "localhost"})]
    (Thread/sleep 250)
    (adapter/stop-server server)
    (Thread/sleep 150)
    (doseq [[label host] [["v6" "::1"] ["v4" "127.0.0.1"]]]
      (check (str "stop: " label " listener closed")
             :refused
             (try (t-close (client-connect-host host 8508 500)) :connected
                  (catch Throwable _ :refused))))))

(defn test-port-zero-one-port-both-families []
  ;; port 0 + two families: the handle's :port names one port and both
  ;; families answer on it
  (let [server (adapter/run-server handler {:port 0 :host "localhost"})]
    (try
      (let [port (:port server)]
        (check "port 0 both: handle names the port" true (pos? port))
        (check "port 0 both: every fd on that port"
               true (every? #(= port (socket/local-port %)) (:sockets server)))
        (Thread/sleep 250)
        (check "port 0 both: v4 answers" 200
               (:status (http/get (str "http://127.0.0.1:" port "/"))))
        (let [fd (client-connect-host "::1" port 3000)]
          (client-send fd "GET / HTTP/1.1\r\nHost: [::1]\r\n\r\n")
          (let [resp (client-recv fd)]
            (check "port 0 both: v6 answers" true (str/includes? (or resp "") "200")))
          (client-close fd)))
      (finally (adapter/stop-server server)))))

(defn test-peer-ip-formatting []
  ;; the conversion itself, fed a sockaddr_in built by hand: accept() fills one
  ;; of these, and reading it is what replaced the hardcoded "127.0.0.1"
  (let [sa (ffi/alloc 16)]
    (try
      (dotimes [i 16] (ffi/write sa :uint8 0 i))
      ;; the family header, as accept() writes it: sin_len + sin_family on
      ;; macOS, a 16-bit sin_family elsewhere
      (if (= :macos native/os)
        (do (ffi/write sa :uint8 16 0) (ffi/write sa :uint8 native/af-inet 1))
        (ffi/write sa :uint16 native/af-inet 0))
      (doseq [[a b c d] [[127 0 0 1] [10 1 2 3] [192 168 250 17] [255 255 255 255]]]
        (ffi/write sa :uint8 a 4) (ffi/write sa :uint8 b 5)
        (ffi/write sa :uint8 c 6) (ffi/write sa :uint8 d 7)
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
      (check-has "framing: an unimplemented Transfer-Encoding is 501"
                 "501" (ask (str "POST / HTTP/1.1\r\nHost: t\r\n"
                                 "Transfer-Encoding: deflate\r\n\r\n5\r\nhello\r\n0\r\n\r\n")))
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

;; jolt's branch removing (write-array p arr off n) and (read-into! p arr off n)
;; raises on them, so a body that takes either path has to use the typed form.
(defn test-ffi-byte-copies []
  (let [buf (ffi/alloc 8)
        acc (byte-array 6)
        read-array ffi/read-array
        write-array ffi/write-array
        read-into ffi/read-into!
        body (vec (take 200000 (cycle (range 256))))
        server (adapter/run-server binary-response-handler
                                   {:port 8456 :worker-threads 1
                                    :keep-alive-timeout-ms 2000})]
    (try
      (ffi/write-array buf (byte-array [1 2 3 4]))
      (with-redefs [ffi/read-array (fn [& _] (throw (ex-info "fill! allocated a chunk" {})))]
        (#'ring-chez.http/fill! acc 2 buf 3))
      (check "fill!: chunk lands at the offset, no chunk array" [0 0 1 2 3 0] (vec acc))
      (Thread/sleep 250)
      (let [retired? (fn [args] (and (= 4 (count args)) (bytes? (second args))))
            used (atom [])
            guard (fn [nm f]
                    (fn [& args]
                      (when (retired? args) (swap! used conj nm))
                      (apply f args)))]
        (with-redefs [ffi/write-array (guard "write-array" write-array)
                      ffi/read-into! (guard "read-into!" read-into)]
          ;; bigger than the send window and than one recv, so the request goes
          ;; through fill! several times and the response through send-all's
          ;; packing of head then body
          (let [fd (client-connect 8456 3000)
                head (str "POST /echo HTTP/1.1\r\nHost: t\r\n"
                          "Content-Type: application/octet-stream\r\n"
                          "Content-Length: " (count body) "\r\n\r\n")]
            (t-send-bytes fd (concat (utf8-bytes head) body))
            (let [raw (client-recv-raw fd)]
              (check "ffi copies: large body round-trips"
                     (hex (unsigned->bytes body)) (hex (response-body-bytes raw))))
            (client-close fd)))
        (check "ffi copies: no byte-index forms" [] @used))
      (finally
        (ffi/free buf)
        (adapter/stop-server server)))))

;; each recv lands in the session buffer without a chunk array between them, for
;; all three shapes of append: room at the end, room after compacting, and growth
(defn test-ws-fill-without-chunk-array []
  (let [session (ws/make-session -1)
        ibuf (:buf session)
        fill! @#'ring-chez.websocket/fill!
        chunk (atom nil)
        contents (fn []
                   (let [{:keys [bs start end]} @ibuf]
                     (vec (take (- end start) (drop start bs)))))]
    (with-redefs [native/c-recv (fn [_ buf _ _]
                                  (ffi/write-array buf (byte-array @chunk))
                                  [(count @chunk) 0])
                  ffi/read-array (fn [& _] (throw (ex-info "fill! allocated a chunk" {})))]
      (reset! ibuf {:bs (byte-array 8) :start 0 :end 2})
      (aset-byte (:bs @ibuf) 0 1) (aset-byte (:bs @ibuf) 1 2)
      (reset! chunk [3 4])
      (check "ws fill!: reports a read" true (fill! session))
      (check "ws fill!: appends in place" [1 2 3 4] (contents))
      (check "ws fill!: end advances" 4 (:end @ibuf))
      (swap! ibuf assoc :start 3)
      (let [before (:bs @ibuf)]
        (reset! chunk [5 6 7 8 9 10])
        (fill! session)
        (check "ws fill!: compacts when that makes room" [4 5 6 7 8 9 10] (contents))
        (check "ws fill!: compacting keeps the array" true (identical? before (:bs @ibuf)))
        (check "ws fill!: compacting resets start" 0 (:start @ibuf)))
      (reset! chunk [11 12 13 14 15])
      (fill! session)
      (check "ws fill!: grows when it must" [4 5 6 7 8 9 10 11 12 13 14 15] (contents))
      (check "ws fill!: growth allocates a new array" 4096 (alength ^bytes (:bs @ibuf))))))

;; A 64 KB ffi/alloc zeroes the block through a fresh 64 KB bytevector, which was
;; the largest single cost of a small request, twice over.
(defn test-send-buffer-fits-the-payload []
  (let [window 65536
        send-all @#'ring-chez.http/send-all
        alloc ffi/alloc
        send-through (fn [parts]
                       (let [sizes (atom [])
                             wire (atom [])]
                         (with-redefs [ffi/alloc (fn [n] (swap! sizes conj n) (alloc n))
                                       native/c-send (fn [_ p n _]
                                                       (swap! wire into (vec (ffi/read-array p n)))
                                                       [n 0])]
                           {:ok (send-all -1 parts)
                            :sizes @sizes
                            :wire @wire})))
        bs (fn [n v] (byte-array n (byte v)))]
    (let [{:keys [ok sizes wire]} (send-through [(bs 100 1) (bs 13 2)])]
      (check "send buffer: small response succeeds" true ok)
      (check "send buffer: holds just the payload" [113] sizes)
      (check "send buffer: octets in order"
             (concat (repeat 100 1) (repeat 13 2)) (seq wire)))
    (let [{:keys [ok sizes wire]} (send-through [(bs 0 0)])]
      (check "send buffer: empty payload succeeds" true ok)
      (check "send buffer: empty payload sends nothing" [] wire)
      (check "send buffer: empty payload still allocates something" true (pos? (first sizes))))
    (doseq [total [(dec window) window (inc window) (+ 5 (* 2 window))]]
      (let [{:keys [ok sizes wire]} (send-through [(bs 7 1) (bs (- total 7) 2)])]
        (check (str "send buffer: " total " succeeds") true ok)
        (check (str "send buffer: " total " stays within the window")
               [(min total window)] sizes)
        (check (str "send buffer: " total " all octets arrive") total (count wire))
        (check (str "send buffer: " total " order")
               [1 2] [(nth wire 6) (nth wire 7)])))))

(defn test-recv-buffers-are-reused []
  (let [pool @#'ring-chez.socket/recv-buffers
        alloc ffi/alloc
        free ffi/free
        allocs (atom 0)
        frees (atom [])
        saved @pool
        req "GET /a HTTP/1.1\r\nHost: t\r\n\r\n"
        read-one (fn [recv!]
                   (ring-chez.http/read-request -1 (byte-array 0) 1048576 recv! recv!))
        stub-recv (fn [_ buf]
                    (ffi/write-array buf (.getBytes ^String req "UTF-8"))
                    (count req))]
    (reset! pool ())
    (try
      (with-redefs [ffi/alloc (fn [n] (when (= n socket/bufsize) (swap! allocs inc)) (alloc n))
                    ffi/free (fn [p] (swap! frees conj p) (free p))]
        (check "recv pool: first request parses" true (map? (read-one stub-recv)))
        (check "recv pool: first request allocates" 1 @allocs)
        (check "recv pool: second request parses" true (map? (read-one stub-recv)))
        (check "recv pool: second request reuses it" 1 @allocs)
        (check "recv pool: a throwing recv gives the buffer back"
               :threw (try (read-one (fn [_ _] (throw (ex-info "recv" {}))))
                           (catch Throwable _ :threw)))
        (check "recv pool: still one buffer after the throw" 1 (count @pool))
        (let [a (socket/take-recv-buffer!)
              b (socket/take-recv-buffer!)]
          (check "recv pool: buffers in use are distinct" true (not= a b))
          (socket/give-recv-buffer! a)
          (socket/give-recv-buffer! b))
        (check "recv pool: nothing freed while under the bound" [] @frees)
        (let [extra (mapv (fn [_] (ffi/alloc socket/bufsize))
                          (range (+ 3 @#'ring-chez.socket/recv-buffers-max)))]
          (doseq [p extra] (socket/give-recv-buffer! p))
          (check "recv pool: holds no more than its bound"
                 @#'ring-chez.socket/recv-buffers-max (count @pool))
          (check "recv pool: the surplus is freed" true (pos? (count @frees))))
        (let [session (ws/make-session -1)]
          (with-redefs [native/c-recv (fn [_ _ _ _] [0 0])]
            (let [before @allocs]
              (#'ring-chez.websocket/fill! session)
              (check "recv pool: websocket reads share it" before @allocs)))))
      (finally
        (doseq [p @pool] (free p))
        (reset! pool saved)))))

;; --- the framing decision, against the implementation it replaced ------------
;; This decision is where disagreeing with a proxy is a request-smuggling bug, so
;; the line-splitting original stays here as the oracle for the single-pass one.

(defn- oracle-lines [head]
  (let [n (count head)]
    (loop [i (if-let [b (str/index-of head "\r\n")] (+ b 2) n)
           out []]
      (if (>= i n)
        out
        (let [e (or (str/index-of head "\r\n" i) n)]
          (recur (+ e 2) (conj out (subs head i e))))))))

(defn- oracle-tchar? [c]
  (or (<= (int \a) (int c) (int \z))
      (<= (int \A) (int c) (int \Z))
      (<= (int \0) (int c) (int \9))
      (contains? #{\! \# \$ \% \& \' \* \+ \- \. \^ \_ \` \| \~} c)))

(defn- oracle-malformed? [head]
  (let [n (count head)]
    (or
      (loop [i 0]
        (cond
          (>= i n) false
          (= \newline (.charAt ^String head i))
          (if (and (pos? i) (= \return (.charAt ^String head (dec i)))) (recur (inc i)) true)
          (= \return (.charAt ^String head i))
          (if (and (< (inc i) n) (= \newline (.charAt ^String head (inc i)))) (recur (inc i)) true)
          :else (recur (inc i))))
      (boolean
        (some (fn [line]
                (let [i (str/index-of line ":")]
                  (or (zero? (count line))
                      (contains? #{\space \tab} (.charAt ^String line 0))
                      (not (and i (pos? i)))
                      (not (every? oracle-tchar? (subs line 0 i))))))
              (oracle-lines head))))))

(defn- oracle-named [head]
  (reduce (fn [m line]
            (let [i (str/index-of line ":")]
              (if-not (and i (pos? i))
                m
                (let [k (str/lower-case (str/trim (subs line 0 i)))]
                  (if (contains? #{"content-length" "transfer-encoding" "expect"} k)
                    (update m k (fnil conj []) (str/trim (subs line (inc i))))
                    m)))))
          {} (oracle-lines head)))

(defn- oracle-framing
  "What the old resolve-framing answered, minus the :head and offsets."
  [head]
  (let [comma-parts (fn [vals] (mapcat #(map str/trim (str/split % #"," -1)) vals))
        digits? (fn [s] (and (pos? (count s)) (every? #(<= (int \0) (int %) (int \9)) s)))
        version (let [e (or (str/index-of head "\r\n") (count head))
                      line (subs head 0 e)
                      i (str/last-index-of line " ")]
                  (when (and i (< (inc i) (count line))) (subs line (inc i))))
        f (if (oracle-malformed? head)
            :bad
            (let [named (oracle-named head)
                  tes (get named "transfer-encoding")
                  te (cond (empty? tes) :absent
                           (= ["chunked"] (map str/lower-case (comma-parts tes))) :chunked
                           :else :unsupported)
                  parts (comma-parts (get named "content-length"))
                  cl (cond (empty? parts) :absent
                           (not (every? digits? parts)) :bad
                           :else (let [ns (map parse-long parts)]
                                   (if (apply = ns) (first ns) :bad)))]
              (cond (= :bad cl) :bad
                    (= :unsupported te) :unsupported
                    (and (= :chunked te) (not= :absent cl)) :bad
                    (and (= :chunked te) (= "HTTP/1.0" version)) :bad
                    (= :chunked te) :chunked
                    (= :absent cl) 0
                    :else cl)))
        expect? (boolean (some #(= "100-continue" (str/lower-case %))
                               (get (oracle-named head) "expect")))]
    (cond
      (= :chunked f) {:chunked? true :expect? expect?}
      (number? f) {:length f :expect? (and expect? (pos? f))}
      :else f)))

(defn- framing-of
  "The live resolve-framing's answer for head, in oracle-framing's shape."
  [head]
  (let [bs (.getBytes ^String (str head "\r\n\r\n") "UTF-8")
        r (#'ring-chez.http/resolve-framing bs 0 (alength bs))]
    (if (map? r)
      (cond-> {:expect? (boolean (:expect? r))}
        (:chunked? r) (assoc :chunked? true)
        (:to r) (assoc :length (- (:to r) (:from r))))
      r)))

(def ^:private framing-corpus
  ["GET / HTTP/1.1\r\nHost: t"
   "" "GET / HTTP/1.1" "GET / HTTP/1.1\r\n" "\r\n" "\r\nHost: t"
   "GET / HTTP/1.1\r\n\r\nHost: t"
   "GET / HTTP/1.1\nHost: t" "GET / HTTP/1.1\rHost: t" "GET / HTTP/1.1\r\nHost: t\n"
   "GET / HTTP/1.1\r\nHost: t\r" "\nGET" "\rGET" "GET / HTTP/1.1\r\nA: b\r\r\nC: d"
   "GET / HTTP/1.1\n\r\nA: b" "GET / HTTP/1.1\r\n\nA: b"
   "GET / HTTP/1.1\r\n folded: x" "GET / HTTP/1.1\r\n\tfolded: x"
   "GET / HTTP/1.1\r\nA: b\r\n continued"
   "GET / HTTP/1.1\r\nHost : t" "GET / HTTP/1.1\r\n: novalue" "GET / HTTP/1.1\r\nnocolon"
   "GET / HTTP/1.1\r\nA:" "GET / HTTP/1.1\r\nA(b: c" "GET / HTTP/1.1\r\nnaéme: c"
   "GET / HTTP/1.1\r\nX-Note: content-length: 9"
   "GET / HTTP/1.1\r\nX: y\r\nnocolon\r\nZ: a:b"
   "POST / HTTP/1.1\r\nContent-Length: 5" "POST / HTTP/1.1\r\nCONTENT-LENGTH:5"
   "POST / HTTP/1.1\r\ncontent-length:   5  " "POST / HTTP/1.1\r\nContent-Length: 5, 5"
   "POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5"
   "POST / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 7"
   "POST / HTTP/1.1\r\nContent-Length: 5, 7" "POST / HTTP/1.1\r\nContent-Length: +5"
   "POST / HTTP/1.1\r\nContent-Length: -5" "POST / HTTP/1.1\r\nContent-Length:"
   "POST / HTTP/1.1\r\nContent-Length: 0x5" "POST / HTTP/1.1\r\nContent-Lengthx: 5"
   "POST / HTTP/1.1\r\nContent_Length: 5" "POST / HTTP/1.1\r\nContent-Lengt: 5"
   "POST / HTTP/1.1\r\nTransfer-Encoding: chunked"
   "POST / HTTP/1.1\r\ntransfer-encoding: CHUNKED"
   "POST / HTTP/1.0\r\nTransfer-Encoding: chunked"
   "POST / HTTP/1.1\r\nTransfer-Encoding: gzip, chunked"
   "POST / HTTP/1.1\r\nTransfer-Encoding: chunked, chunked"
   "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked"
   "POST / HTTP/1.1\r\nTransfer-Encoding:"
   "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 5"
   "POST / HTTP/1.1\r\nContent-Length: 5\r\nExpect: 100-continue"
   "POST / HTTP/1.1\r\nContent-Length: 0\r\nExpect: 100-continue"
   "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nexpect: 100-CONTINUE"
   "GET / HTTP/1.1\r\nExpect: 100-continue" "POST / HTTP/1.1\r\nExpect: nope\r\nContent-Length: 1"
   "POST / HTTP/1.1\r\nExpectx: 100-continue\r\nContent-Length: 1"])

(def ^:private framing-pieces
  ["\r\n" "\r\n" "\r\n" "\r\n" "\r" "\n" ":" ":" " " "\t" "," "a" "(" "é" "x-y" "Host"
   "Content-Length" "content-length" "CONTENT-LENGTH" "Content-Lengt" "Transfer-Encoding"
   "transfer-encoding" "Expect" "expect" "100-continue" "chunked" "CHUNKED" "gzip"
   "5" "7" "0" "GET / HTTP/1.1" "POST / HTTP/1.0" "Content-Length: 5" "Transfer-Encoding: chunked"
   "Expect: 100-continue" "A: b"])

(defn test-framing-matches-the-old-decision []
  (let [next-rand (fn [s] (mod (+ (* s 1103515245) 12345) 2147483648))
        rand-head (fn [seed]
                    (loop [s (next-rand seed)
                           k (+ 1 (mod (quot s 65536) 12))
                           out "GET / HTTP/1.1\r\n"]
                      (if (zero? k)
                        [s out]
                        (let [s' (next-rand s)
                              piece (nth framing-pieces (mod (quot s' 65536) (count framing-pieces)))]
                          (recur s' (dec k) (str out piece))))))
        seen (atom #{})
        mismatches (atom [])
        compare! (fn [raw]
                   ;; head-end stops at the first terminator, so that is the head
                   (let [head (subs raw 0 (or (str/index-of raw "\r\n\r\n") (count raw)))
                         want (oracle-framing head)
                         got (framing-of head)]
                     (swap! seen conj (cond (map? want) (if (:chunked? want) :chunked :length)
                                            :else want))
                     (when (not= (oracle-malformed? raw) (ring-chez.http/head-malformed? raw))
                       (swap! mismatches conj [:malformed raw]))
                     (when (not= want got)
                       (swap! mismatches conj [:framing head want got]))))]
    (doseq [h framing-corpus] (compare! h))
    (loop [i 0 s 20261105]
      (when (< i 20000)
        (let [[s' h] (rand-head s)]
          (compare! h)
          (recur (inc i) s'))))
    (check "framing oracle: both agree on every head" [] (vec (take 5 @mismatches)))
    (check "framing oracle: the cases reached every outcome"
           #{:bad :unsupported :chunked :length} @seen))
  ;; the point of the rewrite: no substring per line, on any path through here
  (with-redefs [ring-chez.http/head-lines (fn [_] (throw (ex-info "split the head" {})))]
    (check "framing: decided without splitting into lines" {:expect? false :length 0}
           (framing-of "GET / HTTP/1.1\r\nHost: t\r\nAccept: */*"))))

;; --- the response path and request->ring, against what they replaced ----------

(defn- oracle-conn-token? [tok v]
  (boolean (some #(= tok (str/lower-case %))
                 (map str/trim (str/split (or v "") #",")))))

(defn test-connection-tokens-match []
  (let [values [nil "" "close" "CLOSE" " close " "Close" "keep-alive" "Keep-Alive"
                "Keep-Alive, Close" "close, keep-alive" "keep-alive,close" ",close" "close,"
                "," ",," " , " "x,,close" "closed" "unclose" "clo se" "close x"
                "upgrade" "keep-alive, Upgrade" "\tclose\t" "a,b,c,d,close"]]
    (doseq [v values, tok ["close" "keep-alive" "upgrade"]]
      (check (str "conn token: " (pr-str tok) " in " (pr-str v))
             (oracle-conn-token? tok v)
             (ring-chez.http/connection-token? {:headers {"connection" v}} tok)))
    (doseq [[protocol want] [["HTTP/1.1" true] ["HTTP/1.0" false]]]
      (check (str "keep-alive?: " protocol " with no header") want
             (boolean (ring-chez.http/keep-alive? {:protocol protocol :headers {}}))))
    (doseq [v values]
      (check (str "keep-alive?: 1.1 " (pr-str v))
             (not (oracle-conn-token? "close" v))
             (boolean (ring-chez.http/keep-alive? {:protocol "HTTP/1.1" :headers {"connection" v}})))
      (check (str "keep-alive?: 1.0 " (pr-str v))
             (and (oracle-conn-token? "keep-alive" v) (not (oracle-conn-token? "close" v)))
             (boolean (ring-chez.http/keep-alive? {:protocol "HTTP/1.0" :headers {"connection" v}})))))
  (with-redefs [str/split (fn [& _] (throw (ex-info "regex split" {})))]
    (check "conn token: read without a regex" false
           (boolean (ring-chez.http/keep-alive? {:protocol "HTTP/1.1"
                                                 :headers {"connection" "Keep-Alive, Close"}})))))

(defn test-response-head-is-unchanged []
  (let [head @#'ring-chez.http/head->string]
    (doseq [[label resp keep? framing want]
            [["plain" {:status 200 :headers {"Content-Type" "text/plain"}} true 13
              "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\nConnection: keep-alive\r\n\r\n"]
             ["close" {:status 200 :headers {}} false 0
              "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"]
             ["no status, no headers" {} true :none
              "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\n\r\n"]
             ["unknown status" {:status 299} true :chunked
              "HTTP/1.1 299 Unknown\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"]
             ["handler's Connection wins" {:headers {"Connection" "upgrade"}} true 1
              "HTTP/1.1 200 OK\r\nConnection: upgrade\r\nContent-Length: 1\r\n\r\n"]
             ["any case" {:headers {"CONNECTION" "close"}} true 1
              "HTTP/1.1 200 OK\r\nCONNECTION: close\r\nContent-Length: 1\r\n\r\n"]
             ["keyword name" {:headers {:connection "close" :x-a "b"}} true 1
              "HTTP/1.1 200 OK\r\nconnection: close\r\nx-a: b\r\nContent-Length: 1\r\n\r\n"]
             ["a Connection dropped as unsafe still counts" {:headers {"Connection" "a\r\nb"}} true 1
              "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n"]
             ["unsafe name is not Connection" {:headers {"Connection\r\n" "x"}} true 1
              "HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: keep-alive\r\n\r\n"]
             ["vector values" {:headers {"Set-Cookie" ["a=1" "b\n2" "c=3"]}} true 1
              "HTTP/1.1 200 OK\r\nSet-Cookie: a=1\r\nSet-Cookie: c=3\r\nContent-Length: 1\r\nConnection: keep-alive\r\n\r\n"]
             ["vector Connection" {:headers {"Connection" ["x" "y"]}} true 1
              "HTTP/1.1 200 OK\r\nConnection: x\r\nConnection: y\r\nContent-Length: 1\r\n\r\n"]
             ["handler's framing dropped"
              {:headers {"Content-Length" "99" "transfer-encoding" "chunked" "A" 1}} true 2
              "HTTP/1.1 200 OK\r\nA: 1\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\n"]]]
      (check (str "response head: " label) want (head resp keep? framing)))))

(defn- oracle-request-fields
  "What request->ring derived from a head when it split with regexes: the status
  it refused with, or the request line's parts and the header map."
  [head]
  (let [lines (str/split head #"\r\n")
        parts (str/split (or (first lines) "") #" ")
        headers (reduce (fn [m line]
                          (let [i (str/index-of line ":")]
                            (if (and i (pos? i))
                              (let [k (str/lower-case (str/trim (subs line 0 i)))
                                    v (str/trim (subs line (inc i)))]
                                (assoc m k (if-let [prev (get m k)] (str prev "," v) v)))
                              m)))
                        {} (rest lines))]
    (cond
      (not= 3 (count parts)) 400
      (oracle-malformed? head) 400
      (not (contains? #{"HTTP/1.1" "HTTP/1.0"} (nth parts 2))) 505
      (and (= "HTTP/1.1" (nth parts 2)) (not (contains? headers "host"))) 400
      :else [(str/lower-case (first parts)) (second parts) (nth parts 2) headers])))

(defn- request-fields [head]
  (let [{:keys [request error]} (ring-chez.http/request->ring head (byte-array 0) {})]
    (if error
      (:status error)
      [(name (:request-method request))
       (str (:ring-chez/raw-uri request)
            (when-let [q (:query-string request)] (str "?" q)))
       (:protocol request)
       (:headers request)])))

(defn test-request-fields-match []
  (let [next-rand (fn [s] (mod (+ (* s 1103515245) 12345) 2147483648))
        pieces (into framing-pieces
                     ["Host: t" "Host: a" "HOST: b" "Accept: */*" "X: 1" "x: 2" " " "  "
                      "GET / HTTP/1.1\r\n" "GET /a?b=c HTTP/1.1\r\n" "GET / HTTP/1.0\r\n"
                      "GET / HTTP/2\r\n" "GET  /  HTTP/1.1\r\n" "GET / HTTP/1.1 \r\n"])
        heads (concat
                framing-corpus
                ["GET / HTTP/1.1\r\nHost: t\r\nX-F: a\r\nx-f: b"
                 "GET /a?b=c HTTP/1.1\r\nHost: t\r\n"
                 "GET / HTTP/1.1 \r\nHost: t" "GET  / HTTP/1.1\r\nHost: t" " GET / HTTP/1.1\r\nHost: t"
                 "GET / HTTP/1.0" "GET / HTTP/2\r\nHost: t" "GET /\r\nHost: t"
                 "GET / HTTP/1.1\r\nHost: t\r\n\r\n" "GET / HTTP/1.1\r\nHost:\r\nX: y"
                 "GET / HTTP/1.1\r\nHost: t\r\nEmpty:" "GET / HTTP/1.1\r\nHost:   spaced   "]
                (loop [i 0 s 99 out []]
                  (if (= i 20000)
                    out
                    (let [s1 (next-rand s)
                          k (+ 1 (mod (quot s1 65536) 9))
                          [s2 h] (loop [s s1 k k h (if (odd? (quot s1 4096)) "GET / HTTP/1.1\r\nHost: t" "")]
                                   (if (zero? k)
                                     [s h]
                                     (let [s' (next-rand s)]
                                       (recur s' (dec k)
                                              (str h (nth pieces (mod (quot s' 65536) (count pieces))))))))]
                      (recur (inc i) s2 (conj out h))))))
        outcomes (atom #{})
        mismatches (atom [])]
    (doseq [h heads]
      (let [want (oracle-request-fields h)
            got (request-fields h)]
        (swap! outcomes conj (if (vector? want) :parsed want))
        (when (not= want got) (swap! mismatches conj [h want got]))))
    (check "request fields: old and new agree on every head" [] (vec (take 5 @mismatches)))
    (check "request fields: the cases reached every outcome" #{400 505 :parsed} @outcomes)
    ;; what read-request hands the adapter has already passed head-malformed?
    (doseq [h heads]
      (when-not (ring-chez.http/head-malformed? h)
        (let [full (ring-chez.http/request->ring h (byte-array 0) {})
              framed (ring-chez.http/framed-request->ring h (byte-array 0) {})]
          (when (not= (:error full) (:error framed))
            (swap! mismatches conj [:error h]))
          (when (not= (dissoc (:request full) :body) (dissoc (:request framed) :body))
            (swap! mismatches conj [:request h])))))
    (check "request fields: the framed path agrees on every valid head" [] (vec (take 5 @mismatches))))
  (let [real ring-chez.http/head-malformed?
        calls (atom 0)
        server (atom nil)]
    (with-redefs [ring-chez.http/head-malformed? (fn [h] (swap! calls inc) (real h))]
      (reset! server (adapter/run-server handler {:port 8457 :worker-threads 1}))
      (try
        (Thread/sleep 150)
        (let [fd (client-connect 8457 3000)]
          (client-send fd "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
          (check-has "request fields: served" "200 OK" (client-recv fd))
          (client-close fd))
        (check "request fields: a request is validated once" 1 @calls)
        (let [fd (client-connect 8457 3000)]
          (client-send fd "GET / HTTP/1.1\r\nHost : t\r\nConnection: close\r\n\r\n")
          (check-has "request fields: a malformed head is still refused" "400" (client-recv fd))
          (client-close fd))
        (finally (adapter/stop-server @server))))))

(defn run-test
  "Run one test, attributing a throw to it instead of ending the run. A test
  that dies on a socket error — an EPIPE from a peer that closed a beat early,
  say — used to abort -main where it stood, and the sixty tests after it never
  ran, so a transient fault in one read as silence from all of them."
  [nm f]
  (try (f)
       (catch Throwable t
         (swap! failures inc)
         (println "  FAIL" nm "— threw" (str t)))))

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
  (run-test "test-port-zero-binds-a-free-port" test-port-zero-binds-a-free-port)
  (run-test "test-listen-socket-is-close-on-exec" test-listen-socket-is-close-on-exec)
  (run-test "test-status-reasons" test-status-reasons)
  (run-test "test-connection-header-list" test-connection-header-list)
  (run-test "test-handler-connection-close" test-handler-connection-close)
  (run-test "test-vector-header-values" test-vector-header-values)
  (run-test "test-bad-request-lines" test-bad-request-lines)
  (run-test "test-header-cap-is-431" test-header-cap-is-431)

  ;; --- Phase 1 ---
  (run-test "test-concurrent-slow-requests" test-concurrent-slow-requests)
  (run-test "test-single-worker-queues" test-single-worker-queues)
  (run-test "test-stop-is-prompt" test-stop-is-prompt)

  ;; --- Phase 2 ---
  (run-test "test-keep-alive-two-requests" test-keep-alive-two-requests)
  (run-test "test-connection-close-honored" test-connection-close-honored)
  (run-test "test-keep-alive-idle-timeout" test-keep-alive-idle-timeout)
  (run-test "test-pipelined-requests" test-pipelined-requests)

  ;; --- Phase 3 ---
  (run-test "test-stream-chunked" test-stream-chunked)
  (run-test "test-stream-client-disconnect-aborts" test-stream-client-disconnect-aborts)
  (run-test "test-stream-idle-client-disconnect-reclaims" test-stream-idle-client-disconnect-reclaims)
  (run-test "test-stream-idle-client-disconnect-reclaims-fibers" test-stream-idle-client-disconnect-reclaims-fibers)
  (run-test "test-stream-pipelined-request-is-not-a-disconnect" test-stream-pipelined-request-is-not-a-disconnect)
  (run-test "test-stream-http10-close-delimited" test-stream-http10-close-delimited)
  (run-test "test-stream-204-no-framing" test-stream-204-no-framing)

  ;; --- Phase 4 ---
  (run-test "test-sse" test-sse)
  (run-test "test-stream-bad-chunk-reaches-on-failure" test-stream-bad-chunk-reaches-on-failure)

  ;; --- Phase 5 ---
  (run-test "test-ws-sha1" test-ws-sha1)
  (run-test "test-plain-server-does-not-load-crypto" test-plain-server-does-not-load-crypto)
  (run-test "test-websocket" test-websocket)

  ;; --- Phase 6 ---
  (run-test "test-max-request-size" test-max-request-size)
  (run-test "test-worker-survives-bad-chunk" test-worker-survives-bad-chunk)
  (run-test "test-keep-alive-fairness" test-keep-alive-fairness)
  (run-test "test-pipelined-under-pressure" test-pipelined-under-pressure)

  ;; --- concurrency strategies ---
  (run-test "test-rebind-same-port-after-stop" test-rebind-same-port-after-stop)
  (run-test "test-fiber-basic" test-fiber-basic)
  (run-test "test-fiber-idle-connections-do-not-pin" test-fiber-idle-connections-do-not-pin)
  (run-test "test-fiber-keep-alive-and-pipelining" test-fiber-keep-alive-and-pipelining)
  (run-test "test-fiber-streaming" test-fiber-streaming)
  (run-test "test-fiber-idle-timeout" test-fiber-idle-timeout)
  (run-test "test-fiber-stop-wakes-parked-conns" test-fiber-stop-wakes-parked-conns)
  (run-test "test-fiber-restart-leaves-poller-clean" test-fiber-restart-leaves-poller-clean)
  (run-test "test-bad-strategy-throws" test-bad-strategy-throws)
  (run-test "test-fibers-without-a-poller-throws" test-fibers-without-a-poller-throws)
  (run-test "test-bind-failure-carries-errno" test-bind-failure-carries-errno)
  (run-test "test-string-content-length-keep-alive" test-string-content-length-keep-alive)
  (run-test "test-bind-eaddrinuse-friendly" test-bind-eaddrinuse-friendly)
  (run-test "test-boot-validation" test-boot-validation)
  (run-test "test-on-failure-hook" test-on-failure-hook)
  (run-test "test-on-failure-hook-throw-falls-back" test-on-failure-hook-throw-falls-back)
  (run-test "test-nil-response-is-500" test-nil-response-is-500)
  (run-test "test-ws-failure-notifies-hook" test-ws-failure-notifies-hook)
  (run-test "test-ws-guard-accepts" test-ws-guard-accepts)
  (run-test "test-ws-session-carries-request" test-ws-session-carries-request)
  (run-test "test-ws-close-with-status" test-ws-close-with-status)
  (run-test "test-ws-guard-rejects-with-response" test-ws-guard-rejects-with-response)
  (run-test "test-ws-guard-nil-is-403" test-ws-guard-nil-is-403)
  (run-test "test-ws-guard-throw-is-request-failure" test-ws-guard-throw-is-request-failure)
  (run-test "test-write-timeout-cuts-stalled-peer" test-write-timeout-cuts-stalled-peer)
  (run-test "test-write-timeout-zero-disables" test-write-timeout-zero-disables)

  ;; --- Wave 2 round 6: reuse-port and trusted-proxy addressing ---
  (run-test "test-reuse-port" test-reuse-port)
  (run-test "test-trusted-proxy-addr" test-trusted-proxy-addr)

  ;; --- Wave 2 round 5: static files ---
  (run-test "test-static-serves" test-static-serves)
  (run-test "test-static-conditional" test-static-conditional)
  (run-test "test-static-gzip" test-static-gzip)
  (run-test "test-static-large-file" test-static-large-file)
  (run-test "test-static-path-safety" test-static-path-safety)
  (run-test "test-static-cache-window" test-static-cache-window)

  ;; --- Wave 2 round 4: gzip ---
  (run-test "test-gzip-compresses" test-gzip-compresses)
  (run-test "test-gzip-negotiation" test-gzip-negotiation)
  (run-test "test-gzip-skips" test-gzip-skips)
  (run-test "test-gzip-etag" test-gzip-etag)
  (run-test "test-gzip-stream" test-gzip-stream)
  (run-test "test-zstd-library" test-zstd-library)
  (run-test "test-zstd-negotiation" test-zstd-negotiation)
  (run-test "test-zstd-stream" test-zstd-stream)

  ;; --- Wave 2 round 3: multipart uploads ---
  (run-test "test-multipart-upload" test-multipart-upload)
  (run-test "test-multipart-large-chunked" test-multipart-large-chunked)
  (run-test "test-multipart-truncated" test-multipart-truncated)
  (run-test "test-multipart-passthrough" test-multipart-passthrough)
  (run-test "test-multipart-core-reexport" test-multipart-core-reexport)
  (run-test "test-stale-acceptor-does-not-take-the-next-servers-connection"
            test-stale-acceptor-does-not-take-the-next-servers-connection)
  (run-test "test-worker-survives-a-fault-in-connection-teardown"
            test-worker-survives-a-fault-in-connection-teardown)
  (run-test "test-accept-loop-survives-one-connections-fault"
            test-accept-loop-survives-one-connections-fault)
  (run-test "test-clean-run-reports-no-faults" test-clean-run-reports-no-faults)
  (run-test "test-threads-lost-handoff-is-reaped-and-reported"
            test-threads-lost-handoff-is-reaped-and-reported)
  (run-test "test-fibers-lost-spawn-is-reaped-and-reported"
            test-fibers-lost-spawn-is-reaped-and-reported)
  (run-test "test-queued-connection-is-not-reaped" test-queued-connection-is-not-reaped)
  (run-test "test-lost-handoff-does-not-retire-keepalive"
            test-lost-handoff-does-not-retire-keepalive)
  (run-test "test-handoff-with-idle-workers-is-not-pressure"
            test-handoff-with-idle-workers-is-not-pressure)
  (run-test "test-one-queued-conn-retires-one-keepalive"
            test-one-queued-conn-retires-one-keepalive)
  (run-test "test-threads-lost-handoff-is-answered-503"
            test-threads-lost-handoff-is-answered-503)
  (run-test "test-fibers-lost-spawn-is-answered-503"
            test-fibers-lost-spawn-is-answered-503)
  (run-test "test-threads-claimed-silent-conn-is-answered"
            test-threads-claimed-silent-conn-is-answered)
  (run-test "test-claimed-silent-arm-waits-for-a-late-owner"
            test-claimed-silent-arm-waits-for-a-late-owner)
  (run-test "test-claim-survives-a-spurious-cas-failure"
            test-claim-survives-a-spurious-cas-failure)

  ;; --- Wave 2 round 2: handler deadline ---
  (run-test "test-handler-deadline-threads" test-handler-deadline-threads)
  (run-test "test-handler-deadline-fibers" test-handler-deadline-fibers)
  (run-test "test-handler-deadline-bounds" test-handler-deadline-bounds)
  (run-test "test-handler-deadline-interrupts" test-handler-deadline-interrupts)
  (run-test "test-handler-deadline-spares-streams" test-handler-deadline-spares-streams)
  (run-test "test-handler-deadline-hook" test-handler-deadline-hook)
  (run-test "test-fault-handler-envelope" test-fault-handler-envelope)

  ;; --- Wave 2 round 1: stop-server closes live connections ---
  (run-test "test-stop-closes-live-conn-threads" test-stop-closes-live-conn-threads)
  (run-test "test-stop-closes-live-conn-fibers" test-stop-closes-live-conn-fibers)
  (run-test "test-serves-only-own-conns-fibers" test-serves-only-own-conns-fibers)
  (run-test "test-serves-only-own-conns-threads" test-serves-only-own-conns-threads)
  (run-test "test-stop-leaves-nothing-behind-fibers" test-stop-leaves-nothing-behind-fibers)
  (run-test "test-stop-leaves-nothing-behind-threads" test-stop-leaves-nothing-behind-threads)

  ;; --- Round 6: operability ---
  (run-test "test-graceful-drain" test-graceful-drain)
  (run-test "test-drain-timeout" test-drain-timeout)
  (run-test "test-server-stats" test-server-stats)
  (run-test "test-handler-hot-swap" test-handler-hot-swap)
  (run-test "test-uri-normalization" test-uri-normalization)
  (run-test "test-head-and-status-framing" test-head-and-status-framing)
  (run-test "test-http10-keep-alive-and-close" test-http10-keep-alive-and-close)

  ;; --- Round 5: resource bounds ---
  (run-test "test-request-deadline" test-request-deadline)
  (run-test "test-request-deadline-fibers" test-request-deadline-fibers)
  (run-test "test-header-limit" test-header-limit)
  (run-test "test-header-limit-configurable" test-header-limit-configurable)
  (run-test "test-large-file-response" test-large-file-response)
  (run-test "test-input-stream-response-framing" test-input-stream-response-framing)

  ;; --- Round 4: request framing ---
  (run-test "test-chunked-request-body" test-chunked-request-body)
  (run-test "test-chunked-body-cap" test-chunked-body-cap)
  (run-test "test-smuggling-guards" test-smuggling-guards)
  (run-test "test-duplicate-request-headers" test-duplicate-request-headers)
  (run-test "test-expect-100-continue" test-expect-100-continue)

  ;; --- Round 3: bind address and peer ---
  (run-test "test-bind-host-option" test-bind-host-option)
  (run-test "test-bind-sockaddrs" test-bind-sockaddrs)
  (run-test "test-listen-sockets-uniform-port" test-listen-sockets-uniform-port)
  (run-test "test-peer-ip-v6" test-peer-ip-v6)
  (run-test "test-ipv6-bind-and-serve" test-ipv6-bind-and-serve)
  (run-test "test-localhost-binds-both" test-localhost-binds-both)
  (run-test "test-eaddrinuse-aborts-across-families" test-eaddrinuse-aborts-across-families)
  (run-test "test-stop-closes-all-sockets" test-stop-closes-all-sockets)
  (run-test "test-port-zero-one-port-both-families" test-port-zero-one-port-both-families)
  (run-test "test-host-non-ascii" test-host-non-ascii)
  (run-test "test-ipv6-any" test-ipv6-any)
  (run-test "test-ipv6-fibers" test-ipv6-fibers)
  (run-test "test-peer-ip-formatting" test-peer-ip-formatting)
  (run-test "test-request-addressing" test-request-addressing)
  (run-test "test-fiber-request-addressing" test-fiber-request-addressing)

  ;; --- Round 2: RFC 6455 codec ---
  (run-test "test-harness-handshake-surplus" test-harness-handshake-surplus)
  (run-test "test-ws-large-frame" test-ws-large-frame)
  (run-test "test-ws-leftover-frame" test-ws-leftover-frame)
  (run-test "test-ws-fragmented-message" test-ws-fragmented-message)
  (run-test "test-ws-unmasked-client-frame-rejected" test-ws-unmasked-client-frame-rejected)
  (run-test "test-ws-rsv-bits-rejected" test-ws-rsv-bits-rejected)
  (run-test "test-ws-invalid-utf8-is-1007" test-ws-invalid-utf8-is-1007)
  (run-test "test-ws-close-code-echoed" test-ws-close-code-echoed)
  (run-test "test-ws-bad-close-code-is-1002" test-ws-bad-close-code-is-1002)
  (run-test "test-ws-oversized-frame-rejected" test-ws-oversized-frame-rejected)
  (run-test "test-ws-binary-round-trip" test-ws-binary-round-trip)
  (run-test "test-ws-handshake-validation" test-ws-handshake-validation)

  ;; --- Round 1: correctness fixes ---
  (run-test "test-timeout-granularity" test-timeout-granularity)
  (run-test "test-fiber-slow-handler-not-reaped" test-fiber-slow-handler-not-reaped)
  (run-test "test-fiber-long-stream-not-reaped" test-fiber-long-stream-not-reaped)
  (run-test "test-header-crlf-is-dropped" test-header-crlf-is-dropped)
  (run-test "test-content-length-follows-body" test-content-length-follows-body)

  ;; --- UTF-8 request framing ---
  (run-test "test-utf8-request-body" test-utf8-request-body)
  (run-test "test-utf8-split-across-reads" test-utf8-split-across-reads)
  (run-test "test-utf8-pipelined" test-utf8-pipelined)
  (run-test "test-utf8-large-body" test-utf8-large-body)
  (run-test "test-utf8-fiber-request-body" test-utf8-fiber-request-body)
  (run-test "test-unframeable-requests" test-unframeable-requests)
  (run-test "test-binary-request-body" test-binary-request-body)
  (run-test "test-binary-response-body" test-binary-response-body)
  (run-test "test-binary-response-chunks" test-binary-response-chunks)
  (run-test "test-binary-round-trip" test-binary-round-trip)
  (run-test "test-ffi-byte-copies" test-ffi-byte-copies)
  (run-test "test-ws-fill-without-chunk-array" test-ws-fill-without-chunk-array)
  (run-test "test-send-buffer-fits-the-payload" test-send-buffer-fits-the-payload)
  (run-test "test-recv-buffers-are-reused" test-recv-buffers-are-reused)
  (run-test "test-framing-matches-the-old-decision" test-framing-matches-the-old-decision)
  (run-test "test-connection-tokens-match" test-connection-tokens-match)
  (run-test "test-response-head-is-unchanged" test-response-head-is-unchanged)
  (run-test "test-request-fields-match" test-request-fields-match)
  (run-test "test-tls-threads" test-tls-threads)
  (run-test "test-tls-fibers" test-tls-fibers)
  (run-test "test-tls-refuses-plaintext-threads" test-tls-refuses-plaintext-threads)
  (run-test "test-tls-refuses-plaintext-fibers" test-tls-refuses-plaintext-fibers)
  (run-test "test-tls-handshake-limit-threads" test-tls-handshake-limit-threads)
  (run-test "test-tls-handshake-limit-fibers" test-tls-handshake-limit-fibers)
  (run-test "test-tls-stream-disconnect-threads" test-tls-stream-disconnect-threads)
  (run-test "test-tls-stream-disconnect-fibers" test-tls-stream-disconnect-fibers)
  (run-test "test-tls-key-update-threads" test-tls-key-update-threads)
  (run-test "test-tls-key-update-fibers" test-tls-key-update-fibers)
  (run-test "test-tls-ws-threads" test-tls-ws-threads)
  (run-test "test-tls-ws-fibers" test-tls-ws-fibers)
  (run-test "test-tls-stop-with-live-connections" test-tls-stop-with-live-connections)
  (run-test "test-tls-options" test-tls-options)
  (run-test "test-tls-client-auth-threads" test-tls-client-auth-threads)
  (run-test "test-tls-client-auth-fibers" test-tls-client-auth-fibers)
  (run-test "test-tls-client-auth-ws" test-tls-client-auth-ws)
  (run-test "test-tls-client-auth-keep-alive" test-tls-client-auth-keep-alive)
  (run-test "test-tls-client-auth-peer-cert-fault" test-tls-client-auth-peer-cert-fault)
  (run-test "test-tls-client-auth-options" test-tls-client-auth-options)
  (run-test "test-tls-unclaimed-gets-no-plaintext" test-tls-unclaimed-gets-no-plaintext)
  (run-test "test-handler-on-fiber" test-handler-on-fiber)
  (run-test "test-handler-on-fiber-options" test-handler-on-fiber-options)

  (if (zero? @failures)
    (println "all passed")
    (println @failures "FAILED"))
  ;; exit like clojure test runners: the servers' worker pools would otherwise
  ;; hold the process, as jolt (like the JVM) waits for non-daemon threads
  (System/exit (if (pos? @failures) 1 0)))
