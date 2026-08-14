(ns ring-chez.adapter
  "A Ring adapter for jolt: a minimal HTTP/1.1 server over BSD sockets, bound
   directly through jolt.ffi (no jolt built-in, no JVM). Synchronous Ring 1.x
   handlers. Serves loopback (127.0.0.1).

       (require '[ring-chez.adapter :as adapter])
       (def server (adapter/run-server my-handler {:port 3000}))
       ;; ... later ...
       (adapter/stop-server server)"
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [ring-chez.websocket :as ws]
            [jolt.ffi :as ffi]))

;; The libc/socket symbols are declared in deps.edn (:jolt/native :process) and
;; loaded by jolt before this namespace is required, so the bindings resolve.

;; accept/recv/send may block — :blocking emits them collect-safe so a parked
;; accept thread never pins the garbage collector.
(ffi/defcfn c-socket     "socket"     [:int :int :int] :int)
(ffi/defcfn c-bind       "bind"       [:int :pointer :int] :int)
(ffi/defcfn c-listen     "listen"     [:int :int] :int)
(ffi/defcfn c-setsockopt "setsockopt" [:int :int :int :pointer :int] :int)
(ffi/defcfn c-close      "close"      [:int] :int)
(ffi/defcfn c-accept     "accept"     [:int :pointer :pointer] :int :blocking)
(ffi/defcfn c-recv       "recv"       [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-send       "send"       [:int :pointer :size_t :int] :ssize_t :blocking)
;; poller (fiber strategy): poll(2) over registered fds + a self-pipe so new
;; registrations can wake a blocked poll. struct pollfd { int fd; short
;; events; short revents; } — fd at 0, events at byte 4, revents at bytes 6-7.
(ffi/defcfn c-poll       "poll"       [:pointer :int :int] :int :blocking)
(ffi/defcfn c-pipe       "pipe"       [:pointer] :int)
(ffi/defcfn c-read       "read"       [:int :pointer :size_t] :ssize_t :blocking)
(ffi/defcfn c-write      "write"      [:int :pointer :size_t] :ssize_t :blocking)

(def ^:private POLLIN  0x001)
(def ^:private POLLERR 0x008)
(def ^:private POLLHUP 0x010)

(def ^:private AF-INET 2)
(def ^:private SOCK-STREAM 1)
(def ^:private macos?
  (str/includes? (str/lower-case (or (System/getProperty "os.name") "")) "mac"))
;; SOL_SOCKET / SO_REUSEADDR differ by platform: macOS 0xffff / 4, Linux 1 / 2.
(def ^:private sol-socket  (if macos? 0xffff 1))
(def ^:private so-reuse    (if macos? 4 2))
(def ^:private so-rcvtimeo (if macos? 0x1006 20))

;; struct timeval { time_t tv_sec; suseconds_t tv_usec; } — 8 + (4 macOS | 8 Linux)
(defn- set-rcvtimeo! [fd ms]
  (let [tv (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write tv :uint8 i 0))
    (ffi/write tv :uint64 0 (quot ms 1000))
    (if macos?
      (ffi/write tv :uint 8 (rem ms 1000))
      (ffi/write tv :uint64 8 (rem ms 1000)))
    (c-setsockopt fd sol-socket so-rcvtimeo tv 16)
    (ffi/free tv)))

;; sockaddr_in for 127.0.0.1:port. macOS: byte0 = sin_len (16), byte1 = family;
;; Linux: bytes0-1 = family (little-endian, so byte0 = AF_INET).
(defn- make-sockaddr [port]
  (let [sa (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write sa :uint8 i 0))
    (if macos?
      (do (ffi/write sa :uint8 0 16) (ffi/write sa :uint8 1 AF-INET))
      (ffi/write sa :uint8 0 AF-INET))
    (ffi/write sa :uint8 2 (bit-and (bit-shift-right port 8) 0xff))   ; port hi (network order)
    (ffi/write sa :uint8 3 (bit-and port 0xff))                       ; port lo
    (ffi/write sa :uint8 4 127) (ffi/write sa :uint8 5 0)             ; 127.0.0.1
    (ffi/write sa :uint8 6 0)   (ffi/write sa :uint8 7 1)
    sa))

(defn- listen-socket [port]
  (let [fd (c-socket AF-INET SOCK-STREAM 0)]
    (when (neg? fd) (throw (ex-info "socket() failed" {})))
    (let [opt (ffi/alloc 4)]
      (ffi/write opt :int 0 1)
      (c-setsockopt fd sol-socket so-reuse opt 4)
      (ffi/free opt))
    (let [sa (make-sockaddr port)]
      (when (neg? (c-bind fd sa 16))
        (c-close fd) (ffi/free sa) (throw (ex-info (str "bind() failed on port " port) {})))
      (ffi/free sa))
    (when (neg? (c-listen fd 64)) (c-close fd) (throw (ex-info "listen() failed" {})))
    fd))

;; --- io poller (fiber strategy) ----------------------------------------------
;; One thread blocked in poll(2) over every registered connection fd. Fibers
;; (core.async go blocks) park on a per-registration channel until the poller
;; reports their fd readable, so an idle keep-alive connection pins no thread.

(defn- start-poller!
  "Start the poller thread. Returns {:regs :running :wake :pr :pw}. Registrations
  are one-shot: wait-readable parks the caller until fd is readable (or the
  poller stops), then the registration is consumed."
  []
  (let [pfds (ffi/alloc 8)]
    (when (neg? (c-pipe pfds))
      (ffi/free pfds)
      (throw (ex-info "pipe() failed" {})))
    (let [pr  (ffi/read pfds :int 0)
          pw  (ffi/read pfds :int 4)
          regs     (atom {})           ; {fd {:ch delivery-chan :events POLLIN}}
          running? (atom true)
          wake-buf (ffi/alloc 4096)
          wake     (fn [] (c-write pw wake-buf 1))]
      (ffi/free pfds)
      (future
        (loop []
          (if-not @running?
            ;; shutdown: release every parked waiter (chan closes -> nil) and
            ;; close the pipe so a late wake write just fails harmlessly
            (do (doseq [[_ {:keys [ch]}] @regs] (async/close! ch))
                (c-close pr) (c-close pw))
            (let [m    @regs
                  fds  (keys m)
                  n    (count fds)
                  pf   (ffi/alloc (* 8 (inc n)))]
              ;; slot 0: the pipe read end (wakeup), then one slot per fd
              (ffi/write pf :int 0 pr)
              (ffi/write pf :uint8 4 POLLIN)
              (doseq [[i fd] (map-indexed vector fds)]
                (ffi/write pf :int (* 8 (inc i)) fd)
                (ffi/write pf :uint8 (+ 4 (* 8 (inc i))) POLLIN))
              (let [ret (c-poll pf (inc n) -1)
                    ;; drain the wake pipe only if it fired (at least one byte
                    ;; is buffered; a single read never blocks and stale bytes
                    ;; re-fire slot 0 on the next poll)
                    _    (when (pos? (ffi/read pf :uint8 6))
                           (c-read pr wake-buf 4096))]
                (when (pos? ret)
                  (doseq [[i [fd {:keys [ch]}]] (map-indexed vector (seq m))]
                    (let [slot (* 8 (inc i))
                          lo   (ffi/read pf :uint8 (+ 6 slot))
                          hi   (ffi/read pf :uint8 (+ 7 slot))]
                      (when (pos? (bit-or lo hi))
                        ;; one-shot: consume, then deliver revents
                        (swap! regs dissoc fd)
                        (async/put! ch (+ lo (* 256 hi))))))))
              (ffi/free pf)
              (recur)))))
      {:regs regs :running running? :wake wake :pr pr :pw pw})))

(defn- wait-readable
  "Register fd with the poller; returns a channel that yields the revents when
  fd becomes readable (nil when the poller stops). Must be consumed inside a
  go block (parking take)."
  [poller fd]
  (let [ch (async/chan 1)]
    (swap! (:regs poller) assoc fd {:ch ch :events POLLIN})
    ((:wake poller))
    ;; the poller may have stopped between the swap and the wake
    (when-not @(:running poller)
      (when-not (contains? (deref (:regs poller)) fd)
        (async/close! ch)))
    ch))

(defn- stop-poller!
  "Flag the poller down and wake it; it releases parked waiters and closes its
  pipe fds on the way out."
  [poller]
  (reset! (:running poller) false)
  ((:wake poller))
  nil)

;; --- request reading --------------------------------------------------------
(def ^:private bufsize 65536)

(defn- content-length [text hdr-end]
  (let [hdrs (str/lower-case (subs text 0 hdr-end))
        i (str/index-of hdrs "content-length:")]
    (if-not i
      0
      (let [s (+ i (count "content-length:"))
            e (loop [j s] (if (or (>= j (count hdrs))
                                  (= \return (nth hdrs j)) (= \newline (nth hdrs j))) j (recur (inc j))))]
        (or (parse-long (str/trim (subs hdrs s e))) 0)))))

;; read one complete request from conn; acc carries unconsumed bytes from a
;; previous read (pipelined requests). recv! abstracts the blocking read:
;; the threads strategy passes plain c-recv, the fiber strategy passes a
;; poller-parking variant. Returns {:text t :leftover s} when a full request
;; (headers + Content-Length body) is available, :closed when the peer went
;; away (or recv timed out) before sending anything, :bad on EOF/timeout
;; mid-request.
(defn- read-request
  "Reads one request (head + content-length body). Accumulation is capped at
  max-bytes — a client that never terminates (or ships an oversized request)
  gets :too-big instead of exhausting memory."
  [conn acc max-bytes recv!]
  (let [buf (ffi/alloc bufsize)]
    (try
      (loop [acc acc]
        (if-let [hdr-end (str/index-of acc "\r\n\r\n")]
          (let [cl (content-length acc hdr-end)]
            (if (>= (- (count acc) (+ hdr-end 4)) cl)
              {:text (subs acc 0 (+ hdr-end 4 cl))
               :leftover (subs acc (+ hdr-end 4 cl))}
              (if (> (+ (count acc) cl) max-bytes)
                :too-big                    ; body exceeds the cap -> 413
                (let [n (recv! conn buf)]
                  (if (pos? n)
                    (recur (str acc (ffi/read-bytes buf n)))
                    :bad)))))
          (if (> (count acc) max-bytes)
            :headers-too-big              ; headers never ended -> 431
            (let [n (recv! conn buf)]
              (cond
                (pos? n) (recur (str acc (ffi/read-bytes buf n)))
                (str/blank? acc) :closed
                :else :bad)))))
      (finally (ffi/free buf)))))

;; --- request -> Ring map ----------------------------------------------------
(defn- request->ring
  "Parse raw request text into {:request ring-map}, or {:error response} when
  the request is malformed (400) or speaks an unsupported HTTP version (505).
  HTTP/1.1 requests must carry a Host header (RFC 7230 §5.4)."
  [text port]
  (let [blank (str/index-of text "\r\n\r\n")
        head (if blank (subs text 0 blank) text)
        body (if blank (subs text (+ blank 4)) "")
        lines (str/split head #"\r\n")
        parts (str/split (or (first lines) "") #" ")
        headers (reduce (fn [m line]
                          (let [i (str/index-of line ":")]
                            (if (and i (pos? i))
                              (assoc m (str/lower-case (str/trim (subs line 0 i))) (str/trim (subs line (inc i))))
                              m)))
                        {} (rest lines))
        bad (fn [status msg] {:error {:status status
                                       :headers {"Content-Type" "text/plain"}
                                       :body msg}})]
    (cond
      (not= 3 (count parts))
      (bad 400 "Bad Request")

      (not (every? (fn [line] (let [i (str/index-of line ":")] (and i (pos? i))))
                   (rest lines)))
      (bad 400 "Bad Request")

      (not (contains? #{"HTTP/1.1" "HTTP/1.0"} (nth parts 2)))
      (bad 505 "HTTP Version Not Supported")

      (and (= "HTTP/1.1" (nth parts 2)) (not (contains? headers "host")))
      (bad 400 "Bad Request")

      :else
      (let [target (second parts)
            qi (str/index-of target "?")
            [uri qs] (if qi [(subs target 0 qi) (subs target (inc qi))] [target nil])]
        {:request {:server-port    port
                   :server-name    "127.0.0.1"
                   :remote-addr    "127.0.0.1"
                   :uri            uri
                   :query-string   qs
                   :scheme         :http
                   :request-method (keyword (str/lower-case (first parts)))
                   :protocol       (nth parts 2)
                   :headers        headers
                   :body           (when (pos? (count body)) (java.io.StringReader. body))}}))))

;; --- Ring response -> the response string -----------------------------------
(def ^:private status-text
  {100 "Continue" 101 "Switching Protocols" 102 "Processing" 103 "Early Hints"
   200 "OK" 201 "Created" 202 "Accepted" 203 "Non-Authoritative Information"
   204 "No Content" 205 "Reset Content" 206 "Partial Content" 207 "Multi-Status"
   208 "Already Reported" 226 "IM Used"
   300 "Multiple Choices" 301 "Moved Permanently" 302 "Found" 303 "See Other"
   304 "Not Modified" 305 "Use Proxy" 307 "Temporary Redirect" 308 "Permanent Redirect"
   400 "Bad Request" 401 "Unauthorized" 402 "Payment Required" 403 "Forbidden"
   404 "Not Found" 405 "Method Not Allowed" 406 "Not Acceptable"
   407 "Proxy Authentication Required" 408 "Request Timeout" 409 "Conflict"
   410 "Gone" 411 "Length Required" 412 "Precondition Failed"
   413 "Content Too Large" 414 "URI Too Long" 415 "Unsupported Media Type"
   416 "Range Not Satisfiable" 417 "Expectation Failed" 418 "I'm a teapot"
   421 "Misdirected Request" 422 "Unprocessable Content" 423 "Locked"
   424 "Failed Dependency" 425 "Too Early" 426 "Upgrade Required"
   428 "Precondition Required" 429 "Too Many Requests"
   431 "Request Header Fields Too Large" 451 "Unavailable For Legal Reasons"
   500 "Internal Server Error" 501 "Not Implemented" 502 "Bad Gateway"
   503 "Service Unavailable" 504 "Gateway Timeout"
   505 "HTTP Version Not Supported" 506 "Variant Also Negotiates"
   507 "Insufficient Storage" 508 "Loop Detected" 510 "Not Extended"
   511 "Network Authentication Required"})

(defn- body->string [b]
  (cond (nil? b) ""
        (string? b) b
        (or (seq? b) (vector? b)) (apply str b)
        ;; a File / InputStream / Reader body (ring's resource + file responses):
        ;; read its contents rather than printing the object.
        :else (try (slurp b) (catch Throwable _ (str b)))))

(defn- head->string
  "Response head only. framing: a number (Content-Length), :chunked, or :none
  (no body framing — bodyless status, HEAD, or close-delimited)."
  [resp keep-alive? framing]
  (let [status (or (:status resp) 200)
        sb (StringBuilder.)]
    (.append sb (str "HTTP/1.1 " status " " (get status-text status "Unknown") "\r\n"))
    (doseq [[k v] (:headers resp)]
      (let [kn (str/lower-case (if (keyword? k) (name k) (str k)))
            emit (fn [v] (.append sb (str (if (keyword? k) (name k) (str k)) ": " v "\r\n")))]
        (when (and (not= kn "content-length") (not= kn "transfer-encoding"))
          ;; vector values emit one header line per element
          (if (vector? v) (doseq [vv v] (emit vv)) (emit v)))))
    (cond (number? framing) (.append sb (str "Content-Length: " framing "\r\n"))
          (= :chunked framing) (.append sb "Transfer-Encoding: chunked\r\n"))
    ;; the handler's own Connection header (if any) was emitted above; only add
    ;; ours when it did not set one
    (when-not (some #(= "connection" (str/lower-case (if (keyword? (key %)) (name (key %)) (str (key %)))))
                    (:headers resp))
      (.append sb (str "Connection: " (if keep-alive? "keep-alive" "close") "\r\n")))
    (.append sb "\r\n")
    (.toString sb)))

(defn- response->string
  ([resp] (response->string resp false))
  ([resp keep-alive?]
   (let [body (body->string (:body resp))
         ;; Content-Length is the body's octet count. ring-defaults'
         ;; wrap-content-length already sets it (as UTF-8 bytes); honor that
         ;; and only compute when absent, so we never stamp a second, conflicting
         ;; Content-Length.
         len (or (->> (:headers resp)
                      (some (fn [[k v]]
                              (let [kn (str/lower-case (if (keyword? k) (name k) (str k)))]
                                (when (= kn "content-length") v)))))
                 (alength (.getBytes body "UTF-8")))]
     (str (head->string resp keep-alive? len) body))))

;; Connection headers are comma-separated token lists, case-insensitive
;; (RFC 7230 §6.1): "Keep-Alive, Close" means close.
(defn- header-tokens [v]
  (map str/trim (str/split (or v "") #",")))

(defn- conn-token? [tok v]
  (some #(= tok (str/lower-case %)) (header-tokens v)))

(defn- keep-alive? [req]
  (let [c (get-in req [:headers "connection"])]
    (if (= "HTTP/1.0" (:protocol req))
      (conn-token? "keep-alive" c)
      (not (conn-token? "close" c)))))

(defn- response-conn-close?
  "True when the handler's own response headers ask to close."
  [resp]
  (->> (:headers resp)
       (some (fn [[k v]]
               (let [kn (str/lower-case (if (keyword? k) (name k) (str k)))]
                 (when (= kn "connection")
                   (if (vector? v)
                     (some #(conn-token? "close" %) v)
                     (conn-token? "close" v))))))))

(defn- send-all
  "Write s to conn; false when the peer is gone (caller closes)."
  [conn s]
  (let [buf (ffi/alloc (max 1 (* 4 (count s))))     ; UTF-8 worst case 4 bytes/char
        n (ffi/write-bytes buf s)
        ok (loop [off 0]
             (if (< off n)
               (let [sent (c-send conn (+ buf off) (- n off) 0)]
                 (and (pos? sent) (recur (+ off sent))))
               true))]
    (ffi/free buf)
    ok))


(defn- stream-body
  "Pump a channel body onto conn: chunked framing for HTTP/1.1, raw bytes for
  HTTP/1.0 (close-delimited; caller closes). take! abstracts the channel take
  (blocking on worker threads, parking inside fibers). True when the stream
  finished cleanly (terminator sent); false when the client went away — the
  channel is then closed so a parked producer's put returns false instead of
  hanging."
  [conn ch http10? take!]
  (loop []
    (let [v (take! ch)]
      (cond
        ;; closed: end of stream. (empty string chunks carry no data and would
        ;; frame as a bogus terminator, so skip them)
        (or (nil? v) (= "" v))
        (if http10? true (send-all conn "0\r\n\r\n"))

        (send-all conn (if http10?
                         v
                         (str (format "%x" (alength (.getBytes ^String v "UTF-8")))
                              "\r\n" v "\r\n")))
        (recur)

        :else (do (async/close! ch) false)))))

(defn- send-response
  "Send resp for req on conn. Returns true when the connection may be reused."
  [conn req resp take!]
  (let [keep?     (and (keep-alive? req) (not (response-conn-close? resp)))
        status    (or (:status resp) 200)
        bodyless? (or (contains? #{100 101 204 304} status)
                      (= :head (:request-method req)))
        b         (:body resp)
        ch        (when (async/chan? b) b)
        http10?   (= "HTTP/1.0" (:protocol req))]
    (cond
      ;; channel body that must not be written at all
      (and ch bodyless?)
      (do (async/close! ch)
          (and (send-all conn (head->string resp keep? :none)) keep?))

      ;; channel body: stream it
      ch
      (if http10?
        ;; unknown length on 1.0 -> close-delimited, connection ends after
        (do (send-all conn (head->string resp false :none))
            (stream-body conn ch true take!)
            false)
        (and (send-all conn (head->string resp keep? :chunked))
             (stream-body conn ch false take!)
             keep?))

      bodyless?
      (and (send-all conn (head->string resp keep? :none)) keep?)

      :else
      (and (send-all conn (response->string resp keep?)) keep?))))

;; --- the accept loop --------------------------------------------------------
;; Clean shutdown: stop-server closes the listen fd (which unblocks accept) and
;; clears `running?`; the loop then exits instead of spinning on the dead fd.
(defn- serve-loop [listen-fd running? serve!]
  (loop []
    (let [conn (c-accept listen-fd ffi/null ffi/null)]
      (cond
        (not @running?) nil
        (neg? conn) (when @running? (recur))
        :else
        (do (serve! conn)
            (recur))))))

;; Each worker parks on the work channel until a connection fd arrives, then
;; owns that connection: with keep-alive it serves requests until the client
;; goes away, sends Connection: close, or the idle recv timeout fires.
;; Blocking handler calls park the worker thread, so the pool size is the
;; concurrency bound.
(defn- upgrade-request?
  "True for an HTTP request asking to switch to websocket."
  [req]
  (and (= "websocket" (str/lower-case (get-in req [:headers "upgrade"] "")))
       (get-in req [:headers "sec-websocket-key"])))

;; io: the per-strategy primitives. recv! reads from the socket (blocking on
;; worker threads; parking on the poller inside fibers), take! takes from a
;; channel body, run! executes a blocking fn (the sync Ring handler or the ws
;; session) directly on the calling thread or on a parking-spawned thread.
(def ^:private threads-io
  {:recv! (fn [conn buf] (c-recv conn buf bufsize 0))
   :take! (fn [ch] (async/<!! ch))
   :run!  (fn [f] (f))})

(defn- fiber-io
  "Fiber-strategy io for one connection. Must only be used inside the owning
  go block: recv! races poller readiness against the idle timeout (so an idle
  keep-alive connection parks without pinning a thread), and run! moves
  blocking work — the synchronous handler, the websocket session — onto a
  thread while the fiber parks."
  [poller conn ka-ms]
  {:recv! (fn [_ buf]
            (let [[v _] (async/alts! [(wait-readable poller conn) (async/timeout ka-ms)])]
              (if (nil? v) -1 (c-recv conn buf bufsize 0))))
   :take! (fn [ch] (async/<! ch))
   :run!  (fn [f] (async/<! (async/thread (f))))})

(defn- connection-loop [conn handler port ka-ms ws-handler max-bytes io]
  (set-rcvtimeo! conn ka-ms)
  (loop [acc ""]
    (let [r (read-request conn acc max-bytes (:recv! io))]
      (cond
        (= :bad r) (send-all conn (response->string
                                    {:status 400 :headers {"Content-Type" "text/plain"}
                                     :body "Bad Request"} false))
        (= :too-big r) (send-all conn (response->string
                                       {:status 413 :headers {"Content-Type" "text/plain"
                                                              "Connection" "close"}
                                        :body "Payload Too Large"} false))
        (= :headers-too-big r) (send-all conn (response->string
                                               {:status 431 :headers {"Content-Type" "text/plain"
                                                                      "Connection" "close"}
                                                :body "Request Header Fields Too Large"} false))
        (map? r)
        (let [{:keys [request error]} (request->ring (:text r) port)]
          (cond
            error (send-all conn (response->string error false))
            (and ws-handler (upgrade-request? request))
            ;; websocket takeover: 101, then the session owns the fd until it
            ;; returns; the connection is not reused afterwards
            (when (send-all conn (str "HTTP/1.1 101 Switching Protocols\r\n"
                                      "Upgrade: websocket\r\n"
                                      "Connection: Upgrade\r\n"
                                      "Sec-WebSocket-Accept: "
                                      (ws/accept-token (get-in request [:headers "sec-websocket-key"]))
                                      "\r\n\r\n"))
              ((:run! io) #(try (ws-handler (ws/make-session conn))
                                (catch Throwable _ nil))))
            :else
            (let [resp ((:run! io) #(try (handler request)
                                         (catch Throwable _
                                           {:status 500 :headers {"Content-Type" "text/plain"}
                                            :body "Internal Server Error"})))]
              (when (send-response conn request resp (:take! io))
                (recur (:leftover r))))))
        :else nil))))

(defn- worker [handler port ka-ms ws-handler max-bytes work]
  (loop []
    (when-let [conn (async/<!! work)]
      (connection-loop conn handler port ka-ms ws-handler max-bytes threads-io)
      (c-close conn)
      (recur))))

(defn- fiber-serve
  "Serve one connection on a go block parked on the io poller. The accept loop
  spawns one per accepted connection — there is no fixed bound, but idle
  connections hold no threads."
  [poller handler port ka-ms ws-handler max-bytes conn]
  (async/go
    (try
      (connection-loop conn handler port ka-ms ws-handler max-bytes
                       (fiber-io poller conn ka-ms))
      (catch Throwable _ nil)
      (finally (c-close conn)))))

(defn run-server
  "Start the server; return a handle {:socket :port :running}. opts:
    :port                  listen port (default 3000)
    :strategy              :threads (default) — fixed worker pool, one thread
                           per busy connection, :worker-threads of them
                           (default = available processors); slow or idle
                           keep-alive connections occupy a worker each.
                           :fibers — one core.async go block per connection
                           parked on a shared poll(2) io poller; idle
                           keep-alive connections pin no thread. Blocking
                           handlers and websocket sessions still run on
                           threads, but only while actually working.
    :worker-threads        worker pool size (threads strategy)
    :keep-alive-timeout-ms idle keep-alive timeout (default 30000)
    :max-request-bytes     request cap (default 1048576; 413/431 beyond)
    :ws-handler            fn of a ring-chez.websocket Session, run when a
                           websocket upgrade request arrives."
  [handler opts]
  (let [strategy (get opts :strategy :threads)]
    (when-not (contains? #{:threads :fibers} strategy)
      (throw (ex-info "run-server: :strategy must be :threads or :fibers"
                      {:strategy strategy :given opts})))
    (let [port   (get opts :port 3000)
          n      (get opts :worker-threads (.availableProcessors (Runtime/getRuntime)))
          ka-ms  (get opts :keep-alive-timeout-ms 30000)
          ws-handler (get opts :ws-handler)
          max-bytes (get opts :max-request-bytes 1048576)
          fd     (listen-socket port)
          running? (atom true)]
      (if (= :fibers strategy)
        (let [poller (start-poller!)]
          (future (serve-loop fd running? (partial fiber-serve poller handler port ka-ms ws-handler max-bytes)))
          {:socket fd :port port :running running? :poller poller})
        (let [work (async/chan)]   ; unbuffered: acceptor parks when all workers busy;
          (dotimes [_ n] (async/thread (worker handler port ka-ms ws-handler max-bytes work)))
          (future (serve-loop fd running? #(async/>!! work %)))
          {:socket fd :port port :running running? :work work})))))

(defn stop-server
  "Stop the server: unblock + exit the accept loop and close the listen socket.
  Fiber strategy: the poller is stopped too, releasing every parked fiber."
  [server]
  (reset! (:running server) false)
  (if-let [poller (:poller server)]
    (stop-poller! poller)
    (async/close! (:work server)))
  (c-close (:socket server))
  nil)
