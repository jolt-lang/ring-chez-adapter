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
            [jolt.io-poller :as poller]
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
(ffi/defcfn c-shutdown   "shutdown"   [:int :int] :int)
(ffi/defcfn c-accept     "accept"     [:int :pointer :pointer] :int :blocking)
(ffi/defcfn c-recv       "recv"       [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-send       "send"       [:int :pointer :size_t :int] :ssize_t :blocking)
;; threads-strategy idle reads wait in poll(2) slices (see run-server's
;; :idle-recv!). struct pollfd { int fd; short events; short revents; } — fd
;; at 0, events at byte 4, revents at bytes 6-7.
(ffi/defcfn c-poll       "poll"       [:pointer :int :int] :int :blocking)
(ffi/defcfn c-strerror  "strerror"   [:int] :pointer)

;; errno + strerror read immediately after a syscall failure return — errno
;; is thread-local and stale across syscalls, so it is only meaningful in
;; the window between the failure and any other FFI call.
(defn- errno-info []
  (let [n (poller/errno)]
    {:errno n
     :strerror (or (try (ffi/ptr->string (c-strerror n)) (catch Throwable _ nil)) "")}))

(def ^:private POLLIN  0x001)

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

(def ^:private f-getfl 3)
(def ^:private f-setfl 4)
(def ^:private o-nonblock (if macos? 0x4 0x800))

(defn- blocking!
  "Clear O_NONBLOCK (websocket takeover: the session runs on a plain thread
  via :run! and relies on blocking recv bounded by SO_RCVTIMEO). No-op for a
  socket that never went nonblocking."
  [fd]
  (let [flags (poller/c-fcntl fd f-getfl 0)]
    (poller/c-fcntl fd f-setfl (bit-and-not flags o-nonblock))))

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
    (when (neg? fd)
      (let [e (errno-info)]
        (throw (ex-info (str "socket() failed: " (:strerror e))
                        (assoc e :syscall "socket")))))
    (let [opt (ffi/alloc 4)]
      (ffi/write opt :int 0 1)
      (when (neg? (c-setsockopt fd sol-socket so-reuse opt 4))
        (let [e (errno-info)]
          (c-close fd) (ffi/free opt)
          (throw (ex-info (str "setsockopt() failed: " (:strerror e))
                          (assoc e :syscall "setsockopt")))))
      (ffi/free opt))
    (let [sa (make-sockaddr port)]
      (when (neg? (c-bind fd sa 16))
        (let [e (errno-info)]
          (c-close fd) (ffi/free sa)
          (throw (ex-info (str "bind() failed on port " port ": " (:strerror e)
                               " (errno " (:errno e) ")")
                          (assoc e :syscall "bind" :port port)))))
      (ffi/free sa))
    (when (neg? (c-listen fd 64))
      (let [e (errno-info)]
        (c-close fd)
        (throw (ex-info (str "listen() failed: " (:strerror e))
                        (assoc e :syscall "listen")))))
    fd))

;; --- fiber strategy io: jolt.io-poller ---------------------------------------
;; Accepted fds are set O_NONBLOCK (poller/nonblock!); reads and writes are
;; raw nonblocking syscalls that park the connection's fiber on
;; jolt.io-poller — kqueue/epoll with persistent, kernel-held registrations
;; (the old hand-rolled loop rebuilt its pollfd set from scratch on every
;; wake). The poller is process-global and never stops, so every close path
;; must forget! the fd: that keeps the poller table bounded, drops stale
;; readiness tombstones before the kernel reuses the fd number, and wakes a
;; fiber still parked on the fd (close first, then forget! — a forget-first
;; waker retries recv on the still-open fd, sees EAGAIN, re-parks on a dead
;; registration and hangs).

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
;; poller-parking variant. idle-recv! handles the first read of the next
;; request on an idle keep-alive connection — under the threads strategy it
;; waits in poll(2) slices so a queued connection can retire the idle one
;; promptly instead of waiting out the full keep-alive timeout. Returns
;; {:text t :leftover s} when a full request (headers + Content-Length body)
;; is available, :closed when the peer went away (or recv timed out) before
;; sending anything, :bad on EOF/timeout mid-request.
(defn- read-request
  "Reads one request (head + content-length body). Accumulation is capped at
  max-bytes — a client that never terminates (or ships an oversized request)
  gets :too-big instead of exhausting memory."
  [conn acc max-bytes recv! idle-recv!]
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
            (let [n ((if (str/blank? acc) idle-recv! recv!) conn buf)]
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
  "Write s to conn; false when the peer is gone (caller closes). wait-write!
  parks the caller until the socket can take more bytes (fiber strategy,
  O_NONBLOCK sockets); nil leaves -1 meaning peer-gone (blocking sockets
  block instead of returning EAGAIN)."
  ([conn s] (send-all conn s nil))
  ([conn s wait-write!]
   (let [buf (ffi/alloc (max 1 (* 4 (count s))))     ; UTF-8 worst case 4 bytes/char
         n (ffi/write-bytes buf s)
         ok (loop [off 0]
              (if (< off n)
                (let [sent (c-send conn (+ buf off) (- n off) 0)]
                  (cond
                    (pos? sent) (recur (+ off sent))
                    (and (neg? sent) (poller/eintr?)) (recur off)
                    (and (neg? sent) wait-write! (poller/eagain?))
                    (do (wait-write!) (recur off))
                    :else false))
                true))]
     (ffi/free buf)
     ok)))

(defn- stream-body
  "Pump a channel body onto conn: chunked framing for HTTP/1.1, raw bytes for
  HTTP/1.0 (close-delimited; caller closes). take! abstracts the channel take
  (blocking on worker threads, parking inside fibers); send! likewise (plain
  send-all on blocking sockets, parking on writability inside fibers). True
  when the stream finished cleanly (terminator sent); false when the client
  went away — the channel is then closed so a parked producer's put returns
  false instead of hanging."
  [conn ch http10? take! send!]
  (loop []
    (let [v (take! ch)]
      (cond
        ;; closed: end of stream. (empty string chunks carry no data and would
        ;; frame as a bogus terminator, so skip them)
        (or (nil? v) (= "" v))
        (if http10? true (send! conn "0\r\n\r\n"))

        (send! conn (if http10?
                      v
                      (str (format "%x" (alength (.getBytes ^String v "UTF-8")))
                           "\r\n" v "\r\n")))
        (recur)

        :else (do (async/close! ch) false)))))

(defn- send-response
  "Send resp for req on conn. Returns true when the connection may be reused."
  [conn req resp take! send!]
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
          (and (send! conn (head->string resp keep? :none)) keep?))

      ;; channel body: stream it
      ch
      (if http10?
        ;; unknown length on 1.0 -> close-delimited, connection ends after
        (do (send! conn (head->string resp false :none))
            (stream-body conn ch true take! send!)
            false)
        (and (send! conn (head->string resp keep? :chunked))
             (stream-body conn ch false take! send!)
             keep?))

      bodyless?
      (and (send! conn (head->string resp keep? :none)) keep?)

      :else
      (and (send! conn (response->string resp keep?)) keep?))))

;; --- the accept loop --------------------------------------------------------
;; Clean shutdown: stop-server shutdown()s the listen fd (SHUT_RDWR), which
;; wakes the acceptor parked in accept() on both Linux and macOS (close()
;; alone does NOT wake it on Linux), then closes it; the loop sees `running?`
;; false and exits instead of spinning on the dead fd.
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
;; channel body, send! writes to the socket (plain send-all on blocking
;; sockets, parking on writability inside fibers), run! executes a blocking
;; fn (the sync Ring handler or the ws session) directly on the calling
;; thread or on a parking-spawned thread.
(def ^:private threads-io
  {:recv! (fn [conn buf] (c-recv conn buf bufsize 0))
   :take! (fn [ch] (async/<!! ch))
   :run!  (fn [f] (f))})

(defn- fiber-recv!
  "One recv for a fiber-held conn — the stdlib socket.io-call contract: EINTR
  retries, EAGAIN parks the CURRENT fiber on jolt.io-poller until readable,
  anything else is the syscall's answer. No waker go block and no per-read
  timeout race: deadlines are enforced by the connection sweeper, whose
  close+forget! wakes this parked fiber and its retry recv answers negative
  (EBADF) → :closed. (An earlier waker+alts! design leaked: a waker that
  registered after close parked forever, and its stale poller entry
  misdirected a reused fd number's wakeups — 10% failure rate under
  stop/restart stress.)"
  [conn buf]
  (loop []
    (let [n (c-recv conn buf bufsize 0)]
      (cond
        (and (neg? n) (poller/eintr?)) (recur)
        (and (neg? n) (poller/eagain?)) (do (poller/wait-ready conn :read) (recur))
        :else n))))

(defn- fiber-io
  "Fiber-strategy io for one connection. Must only be used inside the owning
  fiber-backed go block (see fiber-serve): recv! parks the fiber on read
  readiness, send! parks on writability, and run! moves blocking work — the
  synchronous handler, the websocket session — onto a thread while the fiber
  parks. Idle deadlines live in the sweeper, not here."
  [conn]
  {:recv! (fn [_ buf] (fiber-recv! conn buf))
   :send! (fn [c s] (send-all c s #(poller/wait-ready c :write)))
   :take! (fn [ch] (async/<! ch))
   :run!  (fn [f] (async/<! (async/thread (f))))})

(defn- handle-failure
  "Every abnormal handler completion lands here (Igropyr's on-failure
   semantics). The hook gets one attempt: a throw or a non-map/nil return
   falls back to the plain 500, and the worker always survives. The returned
   map goes through send-response like any handler response, so keep-alive
   is preserved across a failure."
  [on-failure request t]
  (or (when on-failure
        (let [r (try (on-failure request t) (catch Throwable _ nil))]
          (when (map? r) r)))
      {:status 500 :headers {"Content-Type" "text/plain"}
       :body "Internal Server Error"}))

(defn- connection-loop [conn handler port ka-ms ws-handler on-failure max-bytes io deadline]
  (set-rcvtimeo! conn ka-ms)
  (let [recv!      (:recv! io)
        idle-recv! (or (:idle-recv! io) recv!)
        send!      (or (:send! io) send-all)]
    (loop [acc ""]
      ;; deadline: this request (idle wait or mid-request trickle) must finish
      ;; within ka-ms of starting — the fibers-strategy sweeper closes the conn
      ;; past it (a parked read wakes as :closed). nil on the threads strategy,
      ;; where SO_RCVTIMEO already bounds every recv.
      (when deadline (reset! deadline (+ (System/currentTimeMillis) ka-ms)))
      (let [r (read-request conn acc max-bytes recv! idle-recv!)]
        (cond
          (= :bad r) (send! conn (response->string
                                   {:status 400 :headers {"Content-Type" "text/plain"}
                                    :body "Bad Request"} false))
          (= :too-big r) (send! conn (response->string
                                       {:status 413 :headers {"Content-Type" "text/plain"
                                                              "Connection" "close"}
                                        :body "Payload Too Large"} false))
          (= :headers-too-big r) (send! conn (response->string
                                               {:status 431 :headers {"Content-Type" "text/plain"
                                                                      "Connection" "close"}
                                                :body "Request Header Fields Too Large"} false))
          (map? r)
          (let [{:keys [request error]} (request->ring (:text r) port)]
            (cond
              error (send! conn (response->string error false))
              (and ws-handler (upgrade-request? request))
              ;; websocket takeover: 101, then the session owns the fd until it
              ;; returns; the connection is not reused afterwards. The session
              ;; runs on a plain thread (see :run!) whose recv blocks bounded
              ;; by SO_RCVTIMEO — restore the blocking mode the fiber strategy
              ;; set aside at accept.
              (when (send! conn (str "HTTP/1.1 101 Switching Protocols\r\n"
                                     "Upgrade: websocket\r\n"
                                     "Connection: Upgrade\r\n"
                                     "Sec-WebSocket-Accept: "
                                     (ws/accept-token (get-in request [:headers "sec-websocket-key"]))
                                     "\r\n\r\n"))
                (blocking! conn)
                ;; the session owns the fd now; its own SO_RCVTIMEO bounds an
                ;; idle peer, so the sweeper must not reap it mid-session
                (when deadline (reset! deadline Long/MAX_VALUE))
                ;; post-101 there is no response to serve — close IS the
                ;; truncation signal (Igropyr). on-failure still observes it.
                ((:run! io) #(try (ws-handler (ws/make-session conn))
                                  (catch Throwable t
                                    (try (when on-failure (on-failure request t))
                                         (catch Throwable _ nil))))))
              :else
               (let [resp (let [r ((:run! io) #(try (handler request)
                                                    (catch Throwable t
                                                      (handle-failure on-failure request t))))]
                            ;; nil is a failure per Ring (Jetty answers 500),
                            ;; tagged so a hook can tell it from a throw
                            (if (map? r) r
                                (handle-failure on-failure request
                                                (ex-info "handler returned nil"
                                                         {:type :ring-chez/nil-response}))))
                    ;; retire under accept pressure: another connection is
                    ;; waiting for this worker, so decline keep-alive and free
                    ;; it instead of parking on an idle conn while others
                    ;; starve. Only when no pipelined request is already
                    ;; buffered — serving that costs no park.
                    resp (if (and (io :under-pressure?) ((io :under-pressure?))
                                  (keep-alive? request)
                                  (str/blank? (:leftover r)))
                           (update resp :headers #(assoc (or % {}) "Connection" "close"))
                           resp)]
                (when (send-response conn request resp (:take! io) send!)
                  (recur (:leftover r))))))
          :else nil)))))

(defn- worker [handler port ka-ms ws-handler on-failure max-bytes io work]
  (loop []
    (when-let [conn (async/<!! work)]
      ;; claim at take-time: pending then counts only conns accepted but not
      ;; yet claimed — decrementing in the acceptor after >!! leaves a window
      ;; where a claimed conn still reads as pressure and over-retires
      ((io :claim!))
      (try
        (connection-loop conn handler port ka-ms ws-handler on-failure max-bytes io nil)
        ;; an escaping throwable must not kill the worker: a dead worker
        ;; shrinks the pool permanently and starves later connections
        (catch Throwable _)
        ;; shutdown before close: on Linux close() alone does not deliver
        ;; FIN to the peer holding a blocked recv (same family as 791d5c4)
        (finally (c-shutdown conn 2)
                 (c-close conn)))
      (recur))))

(defn- conn-close!
  "Idempotent teardown for one fiber-held conn, shared by the serving fiber's
  finally and stop-server's sweep: shutdown -> close -> forget!, exactly once
  (a second close() on an fd number the kernel already handed to another
  socket would close the wrong one), then deregister from the live set.

  shutdown -> close: on Linux close() alone does not deliver FIN to the peer's
  blocked recv. forget! after close: it wakes any fiber still parked on the
  fd, and the woken read must see EBADF — forget-first lets it see EAGAIN,
  re-park on a dead registration and hang."
  [conns entry]
  (when (compare-and-set! (:closed? entry) false true)
    (c-shutdown (:conn entry) 2)
    (c-close (:conn entry))
    (poller/forget! (:conn entry)))
  (swap! conns disj entry))

(defn- start-sweeper!
  "Fibers-strategy deadline enforcement (the counterpart of SO_RCVTIMEO on
  the threads strategy): every 100ms, close conns whose deadline passed.
  conn-close! wakes the fiber parked on the fd (forget! resumes registered
  waiters), so a parked read ends as :closed, not a hang."
  [conns]
  (let [stop? (atom false)]
    (future
      (loop []
        (Thread/sleep 100)
        (when-not @stop?
          (let [now (System/currentTimeMillis)]
            (doseq [e @conns]
              (when (and (not @(e :closed?)) (< @(e :deadline) now))
                (conn-close! conns e))))
          (recur))))
    stop?))

(defn- fiber-serve
  "Serve one connection on a fiber-backed go block parked on jolt.io-poller.
  The accept loop spawns one per accepted connection — there is no fixed
  bound, and idle connections hold no thread. The spawn runs under
  *go-backend* :fiber: the default :thread backend runs go bodies on OS
  threads, where wait-ready would block the carrier instead of parking."
  [handler port ka-ms ws-handler on-failure max-bytes conn conns]
  (let [deadline (atom (+ (System/currentTimeMillis) ka-ms))
        entry {:conn conn :closed? (atom false) :deadline deadline}]
    (swap! conns conj entry)
    (binding [async/*go-backend* :fiber]
      (async/go
        (try
          (connection-loop conn handler port ka-ms ws-handler on-failure max-bytes
                           (fiber-io conn) deadline)
          (catch Throwable _ nil)
          (finally (conn-close! conns entry)))))))

;; Igropyr http.sc: bad config must crash HERE, at boot — deferred to request
;; time it raises inside the reader and the connection just drops. One error
;; per throw, first failing key; unknown keys pass (forward compatibility).
(defn- validate-opts! [opts]
  (letfn [(bad! [k given expected]
            (throw (ex-info (str "run-server: " k " must be " expected)
                            {:key k :given given :expected expected})))
          (check-num [k lo hi]
            (when (contains? opts k)
              (let [v (get opts k)]
                (when-not (and (int? v) (>= v lo) (<= v hi))
                  (bad! k v (str "an integer in " lo ".." hi)))))
            (get opts k))
          (check-ifn [k]
            (when (contains? opts k)
              (let [v (get opts k)]
                (when-not (fn? v) (bad! k v "a function"))))
            (get opts k))]
    {:port               (check-num :port 1 65535)
     :worker-threads     (check-num :worker-threads 1 ##Inf)
     :ka-ms              (check-num :keep-alive-timeout-ms 1 ##Inf)
     :max-bytes          (check-num :max-request-bytes 1 ##Inf)
     :on-failure         (check-ifn :on-failure)
     :ws-guard           (check-ifn :ws-guard)
     :write-timeout-ms   (check-num :write-timeout-ms 0 ##Inf)}))

(defn run-server
  "Start the server; return a handle {:socket :port :running}. opts:
    :port                  listen port (default 3000)
    :strategy              :threads (default) — fixed worker pool, one thread
                           per busy connection, :worker-threads of them
                           (default = available processors); slow or idle
                           keep-alive connections occupy a worker each.
                           :fibers — one fiber-backed go block per connection
                           parked on jolt.io-poller (kqueue/epoll, persistent
                           registrations); idle keep-alive connections pin no
                           thread. Blocking handlers and websocket sessions
                           still run on threads, but only while actually
                           working.
    :worker-threads        worker pool size (threads strategy)
                            (default = available processors); slow or idle
                            keep-alive connections occupy a worker each,
                            but under accept pressure keep-alive is declined
                            (Connection: close) so queued connections never
                            starve
    :keep-alive-timeout-ms idle keep-alive timeout (default 30000)
    :max-request-bytes     request cap (default 1048576; 413/431 beyond)
    :ws-handler            fn of a ring-chez.websocket Session, run when a
                           websocket upgrade request arrives."
  [handler opts]
  (let [strategy (get opts :strategy :threads)]
    (when-not (contains? #{:threads :fibers} strategy)
      (throw (ex-info "run-server: :strategy must be :threads or :fibers"
                      {:strategy strategy :given opts})))
    (let [v      (validate-opts! opts)
          port   (or (:port v) 3000)
          n      (or (:worker-threads v) (.availableProcessors (Runtime/getRuntime)))
          ka-ms  (or (:ka-ms v) 30000)
          ws-handler (get opts :ws-handler)
          on-failure (get opts :on-failure)
          max-bytes (or (:max-bytes v) 1048576)
          fd     (listen-socket port)
          running? (atom true)]
      (if (= :fibers strategy)
        (let [conns (atom #{})   ; live conn entries; sweeper + stop sweep them
              sweep (start-sweeper! conns)]
          (future (serve-loop fd running?
                              (fn [conn]
                                (poller/nonblock! conn)
                                (fiber-serve handler port ka-ms ws-handler on-failure max-bytes conn conns))))
          {:socket fd :port port :running running? :conns conns :sweep sweep})
        (let [work (async/chan)   ; unbuffered: acceptor parks when all workers busy
              pending (atom 0)    ; conns accepted but not yet claimed by a worker
               io (assoc threads-io
                         :under-pressure? #(pos? @pending)
                         :claim! #(swap! pending dec)
                         ;; idle keep-alive first read: wait in poll(2) slices
                         ;; instead of parking a full ka-timeout recv. Enforces
                         ;; the ka deadline itself (poll has no timeout), and
                         ;; under accept pressure retires a connection only
                         ;; after a grace period of quiet — never instantly,
                         ;; or a client mid-reuse would race a reset. Returns
                         ;; the recv contract (n>0 data, 0 closed/retire).
                         :idle-recv! (fn [conn buf]
                                       (let [pfds (ffi/alloc 8)]
                                         (ffi/write pfds :int 0 conn)
                                         (ffi/write pfds :uint8 4 POLLIN)
                                         (try
                                           (let [deadline (+ (System/currentTimeMillis) ka-ms)
                                                 grace    (+ (System/currentTimeMillis) 2000)]
                                             (loop []
                                               (let [rc  (c-poll pfds 1 250)
                                                     now (System/currentTimeMillis)]
                                                 (cond
                                                   (pos? rc) (c-recv conn buf bufsize 0)
                                                   (neg? rc) 0
                                                   (>= now deadline) 0
                                                   (and (>= now grace) (pos? @pending)) 0
                                                   :else (recur)))))
                                           (finally (ffi/free pfds))))))]
           (dotimes [_ n] (async/thread (worker handler port ka-ms ws-handler on-failure max-bytes io work)))
          (future (serve-loop fd running?
                              (fn [conn]
                                (swap! pending inc)
                                (async/>!! work conn))))
          {:socket fd :port port :running running? :work work})))))

(defn stop-server
  "Stop the server: unblock + exit the accept loop and close the listen socket.
  Fiber strategy: every live connection is closed and forgotten too —
  jolt.io-poller is process-global and never stops, so a parked fiber is
  released only by its fd going away (the woken read sees EBADF/EOF).
  shutdown(SHUT_RDWR) before close() is required on Linux: close() alone
  leaves the acceptor parked in accept() holding the port binding, so
  rebinding the same port fails with EADDRINUSE."
  [server]
  (reset! (:running server) false)
  (when-let [sweep (:sweep server)] (reset! sweep true))
  (if-let [conns (:conns server)]
    (doseq [entry @conns] (conn-close! conns entry))
    (async/close! (:work server)))
  (c-shutdown (:socket server) 2)        ; SHUT_RDWR: wake parked accept()
  (c-close (:socket server))
  nil)
