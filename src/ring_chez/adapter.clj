(ns ring-chez.adapter
  "A Ring adapter for jolt: a minimal HTTP/1.1 server over BSD sockets, bound
   directly through jolt.ffi (no jolt built-in, no JVM). Synchronous Ring 1.x
   handlers. Binds loopback by default; :host picks the interface.

       (require '[ring-chez.adapter :as adapter])
       (def server (adapter/run-server my-handler {:port 3000}))
       ;; ... later ...
       (adapter/stop-server server)"
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [ring-chez.socket :as socket]
            [ring-chez.http :as http]
            [ring-chez.websocket :as ws]
            [jolt.io-poller :as poller]
            [jolt.ffi :as ffi]))

;; --- fiber strategy io: jolt.io-poller ---------------------------------------
;; Accepted fds are set O_NONBLOCK (poller/nonblock!); reads and writes are
;; raw nonblocking syscalls that park the connection's fiber on
;; jolt.io-poller — kqueue/epoll with persistent, kernel-held registrations
;; (the old hand-rolled loop rebuilt its pollfd set from scratch on every
;; wake). The poller is process-global and never stops, so every teardown path
;; must forget! the fd: that keeps the poller table bounded, drops stale
;; readiness tombstones before the kernel reuses the fd number, and wakes a
;; fiber still parked on the fd. Order matters, and it is shutdown -> forget!
;; -> close: the woken recv sees EOF and stops, and the close that frees the
;; fd NUMBER happens last, in the fiber that owns it, once nobody else can
;; syscall on it. See conn-down!.

;; --- server-level faults ----------------------------------------------------
(def ^:private fault-report-limit
  "How many server-level faults reach stderr before it goes quiet. The count in
  server-stats keeps rising either way; what has to be seen is the FIRST one,
  and a fault that repeats per connection must not become the load itself."
  20)

(defn- make-fault!
  "Record and report a fault that is the SERVER's rather than a request's:
  something thrown where no response can carry it and no caller is left to
  catch it — the accept loop, a worker's take/claim/release, a fiber's
  teardown — or, for :unclaimed and :claimed-silent, a connection the accept
  handoff lost or a worker took and never served, where nothing was thrown at
  all and the silence was the whole failure.
  :on-failure is not the hook for these, because it answers a REQUEST and
  there is no request here.

  Every one of them used to be silent, and silence is what made them
  expensive: a throw in the acceptor ended accepting, a throw in a worker's
  release ended that worker, and the future or thread holding the throwable
  was never looked at, so a server that had stopped serving was
  indistinguishable from one that was merely busy. stderr gets the first few
  and server-stats gets the count and the last one."
  [stats]
  (fn [kind t]
    (let [n (swap! (:faults stats) inc)]
      (reset! (:last-fault stats)
              {:kind kind :error (str t) :at (System/currentTimeMillis)})
      (when (<= n fault-report-limit)
        (binding [*out* *err*]
          (println (str "ring-chez: " (name kind) " fault: " t
                        (when (= n fault-report-limit)
                          " — further faults are counted in server-stats, not printed"))))))))

;; --- the accept loop --------------------------------------------------------
;; Clean shutdown: stop-server ends the acceptor and WAITS for it before it
;; frees the listen fd's number — see stop-server for the two platform halves
;; of the wake-up. The loop sees `running?` false, or a failing accept() on a
;; socket that no longer listens, and exits instead of spinning on the dead
;; fd. It never closes the listen fd itself: the number is freed by
;; stop-server once this thread has provably stopped using it, under the same
;; rule conn-down! applies to a connection.
(defn- serve-loop
  "Accept forever, handing each connection and its peer address to serve!.
  accept() fills the sockaddr, which is where :remote-addr comes from — it
  used to be passed ffi/null and the Ring field was a hardcoded literal.

  ONE CONNECTION'S FAULT IS NOT THE SERVER'S, and it used to be. A throw out
  of serve! unwound this loop, the finally freed the sockaddr, and accepting
  ended for the life of the process — silently, because the loop runs in a
  future nothing derefs until stop-server. What that looks like from outside
  is exactly a server under load: the port is still held, server-stats still
  answers, in-flight requests still finish, and every connection after it sits
  in the listen backlog, where it costs the process no fd and shows up in no
  count. Measured with one injected throw: the next request, and every request
  after it, timed out, and the only report of it came out of stop-server as an
  ExecutionException. So serve! runs under a guard, and a loop that ends while
  the server is running says so."
  [listen-fd running? serve! fault!]
  (let [[sa salen] (socket/alloc-peer-sockaddr)]
    (try
      (loop [backoff 0]
        (ffi/write salen :int socket/sockaddr-size 0)
        (let [conn (socket/c-accept listen-fd sa salen)]
          (cond
            ;; already stopped: nobody downstream will ever see this fd, so
            ;; close it here or it leaks. This is only an early-out — the
            ;; check that actually closes the race is inside serve!, which
            ;; registers the conn BEFORE reading running? (see run-server).
            (not @running?) (when-not (neg? conn) (socket/c-close conn))
            ;; a failing accept that is retried immediately burns a core:
            ;; EMFILE persists until an fd is released, and nothing here
            ;; releases one. Back off, capped, and reset on the next success.
            (neg? conn) (when @running?
                          (when (pos? backoff) (Thread/sleep backoff))
                          (recur (min 100 (if (zero? backoff) 1 (* 2 backoff)))))
            :else
            (let [;; read the peer BEFORE serve! is entered, so the two faults
                  ;; can be told apart. serve! owns the fd from its first
                  ;; instruction — both strategies guard their own body and
                  ;; release the conn on their fault path — so a throw out of
                  ;; it must NOT be answered with a close here: that frees a
                  ;; NUMBER the kernel may already have handed to another
                  ;; socket, the one failure worse than the leak it would be
                  ;; fixing (see conn-down!). A throw from peer-ip is the
                  ;; other case, and there the fd is still nobody's.
                  peer (try (socket/peer-ip sa)
                            (catch Throwable t (fault! :peer-addr t) nil))]
              (if (nil? peer)
                (do (socket/c-shutdown conn 2) (socket/c-close conn))
                (try (serve! conn peer)
                     (catch Throwable t (fault! :accept t))))
              (recur 0)))))
      (catch Throwable t (fault! :acceptor t))
      (finally
        (when @running?
          (fault! :acceptor-exit
                  (ex-info "accept loop ended while the server was running"
                           {:type :ring-chez/acceptor-exit})))
        (ffi/free sa) (ffi/free salen)))))

;; Each worker parks on the work channel until a connection fd arrives, then
;; owns that connection: with keep-alive it serves requests until the client
;; goes away, sends Connection: close, or the idle recv timeout fires.
;; Blocking handler calls park the worker thread, so the pool size is the
;; concurrency bound.
(defn- upgrade-attempt?
  "True for a request that is trying to reach the websocket endpoint at all,
  well-formed or not. Anything naming websocket in Upgrade, or carrying either
  of the handshake headers, counts — so a broken handshake is answered as one
  (400) instead of quietly falling through to the Ring handler."
  [req]
  (or (= "websocket" (str/lower-case (get-in req [:headers "upgrade"] "")))
      (contains? (:headers req) "sec-websocket-key")
      (contains? (:headers req) "sec-websocket-version")))

(defn- upgrade-request?
  "True only for a complete RFC 6455 opening handshake (Igropyr
   websocket-key, http.sc:577): GET over HTTP/1.1, Upgrade: websocket, an
   `upgrade` token in Connection, version 13, and a Sec-WebSocket-Key that
   really is a base64 16-byte nonce. Checking only Upgrade and the presence
   of a key let malformed and downlevel handshakes through to a 101."
  [req]
  (and (= :get (:request-method req))
       (= "HTTP/1.1" (:protocol req))
       (= "websocket" (str/lower-case (get-in req [:headers "upgrade"] "")))
       (http/connection-token? req "upgrade")
       (= "13" (get-in req [:headers "sec-websocket-version"]))
       (ws/valid-client-key? (get-in req [:headers "sec-websocket-key"]))))

;; io: the per-strategy primitives. recv! reads from the socket (blocking on
;; worker threads; parking on the poller inside fibers), take! takes from a
;; channel body, send! writes to the socket (plain send-all on blocking
;; sockets, parking on writability inside fibers), run! executes a blocking
;; fn (the ws session) directly on the calling thread or on a parking-spawned
;; thread, and run-timed! runs the Ring handler under a deadline.

(def ^:private timed-out ::timeout)
(def ^:private interrupted ::interrupted)

;; The handler cannot be abandoned from its own stack, so a deadline means
;; running it elsewhere and giving up on the channel — which is why ms of 0
;; keeps the direct call rather than paying a handoff for a deadline nobody
;; asked for. The PORT decides, not the value: a handler that legitimately
;; returns nil delivers [nil ch], which is the nil-response path, not a
;; timeout (Igropyr stuck-ms; RFC-0009).
;;
;; Giving up on the channel is not the end of the stuck handler: the deadline
;; also fires jolt.host/interrupt! on the token this wrapper armed, and jolt's
;; timer-based cooperative interrupt unwinds the handler at its next reduction
;; — the escape is a continuation jump, so a catch-all inside the handler
;; cannot swallow it, while its finally blocks still run — and the thread
;; terminates instead of spinning forever. Cooperative has one edge: a handler
;; blocked inside a foreign call or a wait (an upstream socket read, a channel
;; take) only observes the interrupt when that call returns to Scheme, so a
;; thread can outlive its deadline by however long that blocking call takes.
(defn- run-interruptible*
  "Run f under jolt's cooperative interrupt; the deadline's interrupt! makes
  it answer ::interrupted (nobody reads that — the response already went out
  through the timeout path) instead of killing the thread's channel with a
  throw. f catches Throwable itself, so only the interrupt escapes it."
  [tok f]
  (try (jolt.host/run-interruptible tok f)
       (catch Exception e
         (if (:jolt/interrupted (ex-data e)) interrupted (throw e)))))
(defn- blocking-recv!
  "One recv on a blocking socket, retrying a signal.

  EINTR is not the peer going away, and read-request cannot tell the
  difference: at the start of a request a non-positive return is :closed —
  the connection is dropped with no response — and mid-request it is :bad, a
  400 on a request that was fine. fiber-recv! and send-window! have always
  retried it; these paths did not, and the paths should agree.

  Defensive, not load-bearing. Nothing in an ordinary run delivers a signal
  that runs a handler — every disposition but SIGINT/SIGQUIT is SIG_DFL and
  SIGPIPE is SIG_IGN — so this arm should never fire outside a program that
  installs its own handlers. It is not what was behind the intermittent
  failures; RFC-0014 is.

  EAGAIN is deliberately NOT retried: with SO_RCVTIMEO that is the idle
  timeout firing, which is exactly what read-request should read as a peer
  that stopped talking."
  [conn buf]
  (loop []
    (let [n (socket/c-recv conn buf socket/bufsize 0)]
      (if (and (neg? n) (poller/eintr?)) (recur) n))))

(def ^:private stream-idle-check-ms
  "How often a streaming response with nothing to send asks whether its client
  is still there. One poll(2) per idle stream per second — the cost of not
  holding a worker, an fd and an application's producer for a client that has
  been gone since the last chunk."
  1000)

(def ^:private threads-io
  {:recv! blocking-recv!
   :take! (fn [ch conn]
            (loop []
              (let [[v p] (async/alts!! [ch (async/timeout stream-idle-check-ms)])]
                (cond
                  (= p ch)                 v
                  (socket/peer-gone? conn) http/peer-gone
                  :else                    (recur)))))
   :run!  (fn [f] (f))
   :run-timed! (fn [f ms]
                 (if (zero? ms)
                   (f)
                   (let [tok (jolt.host/make-interrupt)
                         ch (async/thread (run-interruptible* tok f))
                         [v p] (async/alts!! [ch (async/timeout ms)])]
                     (if (= p ch)
                       v
                       (do (jolt.host/interrupt! tok) timed-out)))))})

(defn- fiber-recv!
  "One recv for a fiber-held conn — the stdlib socket.io-call contract: EINTR
  retries, EAGAIN parks the CURRENT fiber on jolt.io-poller until readable,
  anything else is the syscall's answer. No waker go block and no per-read
  timeout race: deadlines are enforced by the connection sweeper, whose
  shutdown+forget! wakes this parked fiber and its retry recv answers 0 (EOF)
  → :closed.

  EOF, not EBADF, and the difference was a bug: teardown used to close() the
  fd here, so the retry could land on an fd NUMBER the kernel had already
  handed to another socket and read somebody else's connection. The fd stays
  allocated until this fiber is done with it — see conn-down!. (An earlier
  waker+alts! design leaked the same way from the other end: a waker that
  registered after close parked forever, and its stale poller entry
  misdirected a reused fd number's wakeups.)"
  [conn buf]
  (loop []
    (let [n (socket/c-recv conn buf socket/bufsize 0)]
      (cond
        (and (neg? n) (poller/eintr?)) (recur)
        (and (neg? n) (poller/eagain?)) (do (poller/wait-ready conn :read) (recur))
        :else n))))

(def ^:private no-deadline Long/MAX_VALUE)

(defn- fiber-io
  "Fiber-strategy io for one connection. Must only be used inside the owning
  fiber-backed go block (see fiber-serve): recv! parks the fiber on read
  readiness, send! parks on writability, and run! moves blocking work — the
  synchronous handler, the websocket session — onto a thread while the fiber
  parks.

  Deadlines are enforced by the sweeper, which only knows how to close an fd,
  so what the deadline MEANS has to be set here, around each park. It is armed
  for a read (the caller does that before read-request) and for a write park,
  and suspended otherwise — a fiber that is not waiting on the peer is not
  waiting on anything the peer can stall. Arming it once for the whole
  exchange, as this used to, reaped connections out from under a handler
  slower than :keep-alive-timeout-ms and cut streams that outlived it.

  jolt.io-poller has no timeout on wait-ready, so parking on writability is
  bounded the only way available: arm :write-timeout-ms before each park, and
  let the sweeper's close wake the fiber (the woken send answers negative,
  which send-all already reads as peer-gone). This is the fibers counterpart
  of SO_SNDTIMEO on the threads strategy — per park rather than per syscall,
  and 0 disables it there as here."
  [conn deadline write-timeout-ms]
  (let [suspend! #(reset! deadline no-deadline)
        arm-write! (if (pos? write-timeout-ms)
                     #(reset! deadline (+ (System/currentTimeMillis) write-timeout-ms))
                     suspend!)]
    {:recv! (fn [_ buf] (fiber-recv! conn buf))
     :send! (fn [c s]
              (try (http/send-all c s (fn [] (arm-write!) (poller/wait-ready c :write)))
                   (finally (suspend!))))
     :take! (fn [ch c]
              (loop []
                (let [[v p] (async/alts! [ch (async/timeout stream-idle-check-ms)])]
                  (cond
                    (= p ch)              v
                    (socket/peer-gone? c) http/peer-gone
                    :else                 (recur)))))
     :run!  (fn [f] (async/<! (async/thread (f))))
     ;; the handler is already on a thread here, so the deadline is only an
     ;; alts! away — the fiber stops waiting, and the fd, the fiber and the
     ;; connection come back; the interrupt reclaims the handler's thread too
     :run-timed! (fn [f ms]
                   (if (zero? ms)
                     (async/<! (async/thread (f)))
                     (let [tok (jolt.host/make-interrupt)
                           ch (async/thread (run-interruptible* tok f))
                           [v p] (async/alts! [ch (async/timeout ms)])]
                       (if (= p ch)
                         v
                         (do (jolt.host/interrupt! tok) timed-out)))))}))

(defn- idle-poll-recv!
  "First read of the next request on an idle keep-alive connection (threads
  strategy). Waits in poll(2) slices instead of parking a full keep-alive
  recv, so it can enforce the keep-alive deadline itself and, under accept
  pressure, retire the connection after a grace period of quiet — never
  instantly, or a client mid-reuse would race a reset. Returns the recv
  contract: n>0 data, 0 closed/retire.

  Only recv when poll says the socket is READABLE. Acting on poll's return
  count alone meant any wake led to a blocking recv, which on a socket with
  nothing to read blocks for the whole SO_RCVTIMEO — pinning the worker, and
  with it every connection queued behind it. A signal is likewise not the peer
  going away: EINTR polls again rather than reporting the connection closed."
  [conn buf ka-ms pending]
  (let [pfds (ffi/alloc socket/pollfd-size)]
    (try
      (socket/init-pollfd! pfds conn socket/POLLIN)
      (let [start    (System/currentTimeMillis)
            deadline (+ start ka-ms)
            grace    (+ start 2000)]
        (loop []
          (let [rc  (socket/c-poll pfds 1 250)
                now (System/currentTimeMillis)]
            (cond
              (and (pos? rc)
                   (pos? (bit-and (socket/pollfd-revents pfds) socket/poll-readable)))
              ;; poll said readable, but the recv that follows can still be
              ;; interrupted — and a signal here would retire a live
              ;; connection, since the caller reads non-positive as gone
              (blocking-recv! conn buf)

              (and (neg? rc) (poller/eintr?)) (recur)
              (neg? rc) 0
              (>= now deadline) 0
              (and (>= now grace) (pos? @pending)) 0
              :else (recur)))))
      (finally (ffi/free pfds)))))

(defn- failure-kind
  "What went wrong, when the call site did not say: an ex-data tag we set
  ourselves, or a plain crash — which is any throwable the handler raised."
  [t]
  (case (:type (ex-data t))
    :ring-chez/nil-response    :nil-response
    :ring-chez/handler-timeout :timeout
    :crash))

(defn- handle-failure
  "Every abnormal handler completion lands here (Igropyr's on-failure
   semantics). The hook gets one attempt: a throw or a non-map/nil return
   falls back to the plain 500, and the worker always survives. The returned
   map goes through http/send-response like any handler response, so keep-alive
   is preserved across a failure.

   info is Igropyr's failure alist — {:kind :elapsed-ms} — handed to the hook
   as :ring-chez/failure on the request rather than as a third argument, so
   hooks written against the two-argument shape keep working. The fallback
   status follows the kind: a timeout may well succeed on retry (503), a
   crash is the server's own fault (500)."
  ([on-failure request t] (handle-failure on-failure request t nil))
  ([on-failure request t info]
   (let [kind    (or (:kind info) (failure-kind t))
         request (assoc request :ring-chez/failure (assoc info :kind kind))]
     (or (when on-failure
           (let [r (try (on-failure request t) (catch Throwable _ nil))]
             (when (and (map? r) (contains? r :status)) r)))
         (if (= :timeout kind)
           {:status 503 :headers {"Content-Type" "text/plain"}
            :body "Service Unavailable"}
           {:status 500 :headers {"Content-Type" "text/plain"}
            :body "Internal Server Error"})))))

(defn- ws-guard-decision
  "Igropyr's ws-reject, Ring-shaped: a guard may refuse an upgrade BEFORE
   the 101 is sent — an unauthenticated peer never gets the socket. Returns
   :upgrade, or a response map to serve instead: a map carrying :status is
   the guard's answer, nil/false is a bare 403, and a throw routes through
   handle-failure so :on-failure observes it."
  [guard request on-failure]
  (if-not guard
    :upgrade
    (let [v (try (guard request)
                 (catch Throwable t
                   (handle-failure on-failure request t {:kind :ws-guard})))]
      (cond
        (and (map? v) (contains? v :status)) v
        v :upgrade
        :else {:status 403 :headers {"Content-Type" "text/plain"}
               :body "Forbidden"}))))

;; cfg: everything about the server that does not vary per connection —
;; the swappable handler boxes, the hooks, and every bound and timeout. It
;; is built once in run-server and threaded through unchanged. It used to be
;; fourteen positional arguments on connection-loop, worker and fiber-serve,
;; which is why the last few options were miserable to add and why the call
;; sites were unreadable.
(defn- connection-loop [conn conn-info cfg io deadline entry]
  (let [{:keys [handler-box ws-handler-box ws-guard on-failure ka-ms max-bytes
                max-header-bytes request-timeout-ms write-timeout-ms
                handler-timeout-ms stats]} cfg]
  (socket/set-rcvtimeo! conn ka-ms)
  (socket/set-sndtimeo! conn write-timeout-ms)
  ;; the sweeper's claimed-silent arm stops here: past this line the conn has
  ;; an owner that is provably running its serving loop, and only the loop's
  ;; own deadlines (ka-ms per read, write-timeout-ms per write) govern it
  (reset! (:serving? entry) true)
  (let [recv!      (:recv! io)
        idle-recv! (or (:idle-recv! io) recv!)
        send!      (or (:send! io) http/send-all)]
    (loop [acc http/no-bytes]
      ;; deadline: the read of this request (idle wait or mid-request trickle)
      ;; must finish within ka-ms of starting — the fibers-strategy sweeper
      ;; closes the conn past it (a parked read wakes as :closed). nil on the
      ;; threads strategy, where SO_RCVTIMEO already bounds every recv.
      (when deadline (reset! deadline (+ (System/currentTimeMillis) ka-ms)))
      (let [r (http/read-request
                conn acc
                {:max-bytes max-bytes
                 :max-header-bytes max-header-bytes
                 :recv! recv!
                 :idle-recv! idle-recv!
                 :request-timeout-ms request-timeout-ms
                 ;; Expect: 100-continue — answered before the body is
                 ;; collected, or the client waits out its own timeout first
                 :continue! #(send! conn "HTTP/1.1 100 Continue\r\n\r\n")
                 ;; fibers: a parked read ends only when the sweeper closes the
                 ;; fd, so the deadline it enforces has to be the tighter of
                 ;; the idle timeout and this request's own deadline
                 :arm-read! (when deadline
                              (fn [request-deadline]
                                (reset! deadline
                                        (min (+ (System/currentTimeMillis) ka-ms)
                                             (or request-deadline Long/MAX_VALUE)))))})
            ;; the read is over: whatever happens next — a handler, a stream,
            ;; a websocket session, an error response — is not the peer
            ;; failing to send, so nothing below this point may be reaped for
            ;; taking longer than ka-ms. Writes re-arm it for themselves (see
            ;; fiber-io); the next iteration re-arms it for the next read.
            _ (when deadline (reset! deadline no-deadline))
            ;; resolved AFTER the read, not before: the read parks for as long
            ;; as the peer is idle, and a swap during that wait must reach this
            ;; request rather than the one after it
            handler    @handler-box
            ws-handler @ws-handler-box]
        (cond
          (= :bad r) (send! conn (http/response->parts
                                   {:status 400 :headers {"Content-Type" "text/plain"}
                                    :body "Bad Request"} false))
          (= :too-big r) (send! conn (http/response->parts
                                       {:status 413 :headers {"Content-Type" "text/plain"
                                                              "Connection" "close"}
                                        :body "Payload Too Large"} false))
          (= :headers-too-big r) (send! conn (http/response->parts
                                               {:status 431 :headers {"Content-Type" "text/plain"
                                                                      "Connection" "close"}
                                                :body "Request Header Fields Too Large"} false))
          (= :timeout r) (send! conn (http/response->parts
                                       {:status 408 :headers {"Content-Type" "text/plain"
                                                              "Connection" "close"}
                                        :body "Request Timeout"} false))
          (= :unsupported r) (send! conn (http/response->parts
                                           {:status 501 :headers {"Content-Type" "text/plain"
                                                                  "Connection" "close"}
                                            :body "Not Implemented"} false))
          (map? r)
          (let [{:keys [request error]} (http/request->ring (:head r) (:body r) conn-info)]
            (cond
              error (send! conn (http/response->parts error false))
               ;; A 101 ends HTTP framing: every byte after the head is read
               ;; as websocket frames. An upgrade that ALSO declares a body has
               ;; two readings — body then frames, or frames straight away —
               ;; and the declared octets would reach the frame parser as if
               ;; they were frames. Refuse the ambiguity before the handshake
               ;; (Igropyr http.sc:1598).
               (and ws-handler (upgrade-request? request) (some? (:body request)))
               (send! conn (http/response->parts
                             {:status 400 :headers {"Content-Type" "text/plain"
                                                    "Connection" "close"}
                              :body "Bad Request"} false))

               ;; an upgrade attempt this server cannot complete is answered as
               ;; a failed handshake, not handed to the Ring handler
               (and ws-handler (upgrade-attempt? request) (not (upgrade-request? request)))
               (send! conn (http/response->parts
                             {:status 400 :headers {"Content-Type" "text/plain"
                                                    "Connection" "close"}
                              :body "Bad WebSocket Handshake"} false))

               (and ws-handler (upgrade-request? request))
               ;; websocket takeover: 101, then the session owns the fd until it
               ;; returns; the connection is not reused afterwards. The session
               ;; runs on a plain thread (see :run!) whose recv blocks bounded
               ;; by SO_RCVTIMEO — restore the blocking mode the fiber strategy
               ;; set aside at accept.
               (let [decision (ws-guard-decision ws-guard request on-failure)]
                 (if-not (= :upgrade decision)
                   ;; refused before any handshake bytes: the guard's response
                   ;; goes through the normal send path, so the connection
                   ;; stays keep-alive-usable and no session ever runs
                   (when (http/send-response conn request decision (:take! io) send!)
                     (recur (:leftover r)))
                   (when (send! conn (str "HTTP/1.1 101 Switching Protocols\r\n"
                                          "Upgrade: websocket\r\n"
                                          "Connection: Upgrade\r\n"
                                          "Sec-WebSocket-Accept: "
                                          (ws/accept-token (get-in request [:headers "sec-websocket-key"]))
                                          "\r\n\r\n"))
                     (socket/blocking! conn)
                     ;; the session owns the fd now and bounds an idle peer with
                     ;; its own SO_RCVTIMEO; the deadline is already suspended
                     ;; for everything past the read, so the sweeper leaves it be
                     ;; post-101 there is no response to serve — close IS the
                     ;; truncation signal (Igropyr). on-failure still observes it.
                     ;; the session inherits whatever the head read overshot
                     ;; into: a client may pipeline its first frames in the
                     ;; same segment as the upgrade request, and dropping them
                     ;; parked the session on bytes already delivered
                     ((:run! io) #(try (ws-handler (ws/make-session conn (:leftover r)))
                                       (catch Throwable t
                                         (try (when on-failure
                                                (on-failure
                                                  (assoc request :ring-chez/failure
                                                         {:kind :ws-session})
                                                  t))
                                              (catch Throwable _ nil))))))))
              :else
               (let [_ (do (swap! (:requests stats) inc) (swap! (:active stats) inc))
                     ;; one report per request: a handler that throws just as
                     ;; the deadline fires would otherwise call the hook twice,
                     ;; once from each path. Whoever claims first reports; the
                     ;; loser's work is abandoned along with the rest of it.
                     claim!   (let [claimed (atom false)]
                                #(compare-and-set! claimed false true))
                     started  (System/currentTimeMillis)
                     ;; NOT `r`: that is the read, and its :leftover carries
                     ;; the pipelined bytes this loop recurs on
                     ran ((:run-timed! io)
                          #(try (handler request)
                                (catch Throwable t
                                  (if (claim!)
                                    (handle-failure on-failure request t)
                                    ;; the deadline already answered this request
                                    {:status 500})))
                          handler-timeout-ms)
                     resp (cond
                            ;; past :handler-timeout-ms: nothing waits for the
                            ;; handler, the connection is usable again, and the
                            ;; interrupt run-timed! fired unwinds the handler
                            ;; at its next reduction so its thread is reclaimed
                            ;; (Igropyr stuck-ms kills the worker outright;
                            ;; jolt's interrupt is the cooperative equivalent)
                            (= timed-out ran)
                            (let [info {:kind :timeout
                                        :elapsed-ms (- (System/currentTimeMillis) started)}
                                  t (ex-info "handler timed out"
                                             {:type :ring-chez/handler-timeout
                                              :timeout-ms handler-timeout-ms})]
                              (if (claim!)
                                (handle-failure on-failure request t info)
                                (handle-failure nil request t info)))

                            (map? ran) ran

                            ;; nil is a failure per Ring (Jetty answers 500),
                            ;; tagged so a hook can tell it from a throw
                            :else
                            (handle-failure on-failure request
                                            (ex-info "handler returned nil"
                                                     {:type :ring-chez/nil-response})))
                    ;; retire under accept pressure: another connection is
                    ;; waiting for this worker, so decline keep-alive and free
                    ;; it instead of parking on an idle conn while others
                    ;; starve. Only when no pipelined request is already
                    ;; buffered — serving that costs no park.
                    resp (if (and (io :under-pressure?) ((io :under-pressure?))
                                  (http/keep-alive? request)
                                  (zero? (alength ^bytes (:leftover r))))
                           (update resp :headers #(assoc (or % {}) "Connection" "close"))
                           resp)]
                (let [reusable (try (http/send-response conn request resp (:take! io) send!)
                                    (finally (swap! (:active stats) dec)))]
                  (when reusable (recur (:leftover r)))))))
          :else nil))))))

(def ^:private sweep-deadline-ms
  "How long a connection may sit registered but UNSERVED — unclaimed by any
  owner, or claimed by one that never entered its serving loop — before the
  sweeper gives up on it (see start-sweeper!). The clock starts only once
  somebody is provably responsible for the conn — a worker has taken it off
  the work channel, or the fiber has been spawned — so it never runs on a
  conn that is merely queued behind a busy pool.

  Either handoff costs microseconds, so 500ms is two-plus orders of headroom;
  and a false reap is now cheaper than what it guards against: the conn is
  ANSWERED (503, Connection: close — see answer-unclaimed!), so a client
  crossed by one retries immediately instead of waiting, and a conn nothing
  has claimed this long is one the client has already been waiting on with
  no response at all."
  500)

(defn- register-conn!
  "Enter one accepted connection in the server's live set. Registered at
  ACCEPT, not when a worker claims it: a conn still sitting in the work
  channel when the server stops has no worker to close it, and used to leak
  its fd.

  Teardown has three separate once-only steps, so three atoms: :down? the
  shutdown that wakes everyone off the fd, :owned? the claim on releasing
  it, and :released? the close itself. See conn-down!.

  :claim-by is the deadline for somebody taking responsibility at all, armed
  by whoever hands the conn on (see sweep-deadline-ms) and no-deadline until
  then; :pending? is this conn's own membership in the threads strategy's
  accept-pressure count, so leaving that count is once-only per conn rather
  than a bare decrement at each exit (see pending-done!)."
  [conns conn peer poller?]
  (let [entry (cond-> {:conn conn :peer peer
                       :down?     (atom false)
                       :owned?    (atom false)
                       :released? (atom false)
                       :claim-by  (atom no-deadline)
                       :serving?  (atom false)
                       :pending?  (atom false)}
                poller? (assoc :poller? true
                               ;; the sweeper's deadline; armed by fiber-io
                               :deadline (atom Long/MAX_VALUE)))]
    (swap! conns conj entry)
    entry))

(defn- conn-down!
  "Take one connection out of service WITHOUT releasing its fd number. This is
  what the sweeper and stop-server call; only the conn's owner releases it.

  shutdown(SHUT_RDWR) makes every pending and subsequent read on the fd answer
  EOF and every write EPIPE, so whoever holds it unwinds on its own — and,
  unlike close(), it leaves the fd NUMBER allocated. That distinction is the
  whole point. This used to be close() followed by poller/forget!, and forget!
  RESUMES the fiber parked on the fd; its retry recv was assumed to answer
  EBADF. But close() had already freed the number, and a resumed fiber does
  not run until the scheduler reaches it, so an acceptor could be handed that
  number first — and the woken fiber's recv then read a LIVE connection
  belonging to somebody else, answering it out of its own stopped server's
  handler. Measured at ~2.5% of responses under stop/restart churn.

  forget! after shutdown is safe in a way forget-before-close never was: the
  retry recv sees EOF, not EAGAIN, so it cannot re-park on a dead
  registration. Only the fibers path has a registration to forget."
  [entry]
  (when (compare-and-set! (:down? entry) false true)
    (socket/c-shutdown (:conn entry) 2)
    (when (:poller? entry) (poller/forget! (:conn entry)))))

(defn- claim-close!
  "Take responsibility for releasing this conn's fd. Exactly one caller wins:
  the worker or fiber that serves it, or stop-server for a conn that no worker
  ever claimed. A caller that LOSES must not touch the fd again — the winner
  may already have released the number to another socket."
  [entry]
  (compare-and-set! (:owned? entry) false true))

(defn- pending-done!
  "Take one conn out of the threads strategy's accept-pressure count, exactly
  once however it leaves: claimed by a worker, released by the acceptor
  because it was never handed over, or reaped unclaimed by the sweeper.

  Once-only per conn, and not a bare decrement at each of those places,
  because a conn lost between the take and the claim used to leave the count
  raised for the life of the server — and `pending` stuck above zero is
  under-pressure? stuck true, which is keep-alive declined on every response
  from then on."
  [pending entry]
  (when (compare-and-set! (:pending? entry) true false)
    (swap! pending dec)))

(defn- conn-release!
  "Release one conn's fd, exactly once, by whoever claimed it. Everything that
  is not the owner calls conn-down! instead.

  forget! before close here, the opposite of the old order and safe for the
  opposite reason: the owner is unwinding, so no reader is left to see EAGAIN
  and re-park. Forgetting AFTER the close would race the freed fd number into
  another socket's registration.

  The :open count is decremented here rather than in each caller's finally, so
  it is exactly-once for the same reason the close is: a conn stop-server
  releases before any worker claimed it never reaches a finally at all."
  [conns entry stats]
  (conn-down! entry)
  (when (compare-and-set! (:released? entry) false true)
    (when (:poller? entry) (poller/forget! (:conn entry)))
    (socket/c-close (:conn entry))
    (swap! (:open stats) dec))
  (swap! conns disj entry))

(defn- worker
  "Serve connections off the work channel until it closes.

  NOTHING IN THE BODY MAY END THE LOOP, and the guard below the claim was not
  enough for that. It covered the connection — a handler that throws must not
  cost the pool a thread — but the claim, the take-side bookkeeping and above
  all conn-release! sat outside it, and conn-release! runs in a FINALLY, where
  a throw beats the catch beside it and leaves the loop with the worker.

  What that costs is not one connection. A worker that leaves is never
  replaced, so the pool shrinks for the life of the process; when the last one
  goes, the acceptor parks on the unbuffered work channel forever and the
  server answers nothing at all while still holding the port and reporting
  :active 0. Measured with a throw injected into the release path, pool of
  four: four faults, four dead workers, and every request after them timed
  out. start-worker! is the second line of the same defence."
  [base-info cfg io work conns]
  (let [fault! (:fault! cfg)]
    (loop []
      (when-let [entry (async/<!! work)]
        (try
          ;; claim at take-time: pending then counts only conns accepted but not
          ;; yet claimed — decrementing in the acceptor after >!! leaves a window
          ;; where a claimed conn still reads as pressure and over-retires
          ((io :claim!) entry)
          ;; stop-server releases conns nobody claimed. If it got to this one
          ;; first, the fd number may already belong to another socket, so losing
          ;; the claim means leaving the fd alone entirely.
          (when (claim-close! entry)
            (try
              (connection-loop (:conn entry) (assoc base-info :remote-addr (:peer entry)) cfg io nil entry)
              ;; an escaping throwable must not kill the worker: a dead worker
              ;; shrinks the pool permanently and starves later connections
              (catch Throwable _)
              (finally (conn-release! conns entry (:stats cfg)))))
          (catch Throwable t (fault! :worker t)))
        (recur)))))

(defn- start-worker!
  "One pool thread, replaced if it ever leaves for a reason other than the work
  channel closing. worker guards its own body, so what is left for this to
  cover is the take and the runtime beneath it — and the point of covering it
  is that \"a dead worker shrinks the pool permanently\" stops being a property
  of the design and becomes something that cannot happen."
  [base-info cfg io work conns running?]
  (async/thread
    (loop []
      (let [failed? (try (worker base-info cfg io work conns) false
                         (catch Throwable t ((:fault! cfg) :worker-exit t) true))]
        (when (and failed? @running?)
          ;; a fault that repeats instantly would otherwise spin a core
          (Thread/sleep 10)
          (recur))))))

(def ^:private claimed-silent-headroom
  "How many sweep deadlines a CLAIMED conn gets past its expiry before the
  claimed-silent arm gives up on it (see start-sweeper!). Claimed means a
  worker took it, so the owner is alive and adjacent to serving — a GC pause
  or a descheduled worker can stretch the claim-to-loop-entry window past
  any base deadline, and reaping then would close a live connection. Four
  multiples is 2s at the 500ms default: orders above any scheduling pause,
  and still bounded when the loop truly never starts."
  4)

(defn- answer-unclaimed!
  "Best-effort 503 to a conn the sweeper is about to release for being
  unclaimed. Undertow answers an overloaded exchange with 503 and
  Connection: close rather than silence, and a conn the handoff lost
  deserves the same: its peer is waiting on a request that will never be
  served, and an answered close lets it fail fast and retry instead of
  sitting out its own timeout against a server answering everyone else.

  Bounded by a zero-timeout poll because the sweeper is one thread serving
  every deadline on the server: it may neither park a fiber nor block on a
  busy peer — not even for 100ms, or a burst of answered drops would delay
  every other reap behind it. A conn that cannot take these ~130 bytes right
  now is answered by the close that follows immediately after. Failure is
  fine: the answer is a courtesy, the release is the guarantee."
  [entry]
  (try
    (let [conn (:conn entry)
          pfds (ffi/alloc socket/pollfd-size)]
      (try
        (socket/init-pollfd! pfds conn socket/POLLOUT)
        (let [rc (socket/c-poll pfds 1 0)]
          (when (and (pos? rc)
                     (pos? (bit-and (socket/pollfd-revents pfds)
                                    socket/POLLOUT)))
            (http/send-all
              conn (.getBytes (str "HTTP/1.1 503 Service Unavailable\r\n"
                                   "Content-Type: text/plain\r\n"
                                   "Connection: close\r\n"
                                   "Content-Length: 19\r\n\r\n"
                                   "Service Unavailable")
                              "UTF-8"))))
        (finally (ffi/free pfds))))
    (catch Throwable _ nil)))

(defn- start-sweeper!
  "Every 100ms, take out of service the conns that have run out of time. Both
  strategies run one; there are three deadlines and they are not the same
  thing.

  A CONNECTION NOBODY EVER CLAIMED is the first, and it runs on both
  strategies. The accept path registers a conn and then hands it on — onto the
  work channel for a worker, or into a fiber-backed go block — and the whole
  serving path hangs off the claim at the far end of that hop. A conn that is
  handed on and never claimed is therefore served by nobody, closed by nobody
  and reported by nobody: no guard is crossed, so no fault is recorded, and
  because only an OWNER may free an fd number (see conn-down!) the fd is held
  for the life of the process while the peer waits on a request that will
  never be answered. Measured on macOS under plain-mode connect churn as one
  connection in a few thousand, on both strategies, which is enough for ab to
  abort a whole benchmark cell over a server that is otherwise healthy.

  The sweeper is what closes that hole: past :claim-by it takes the claim
  ITSELF — winning the same CAS a late worker or fiber would have to win, so
  the two can never both serve the conn — releases the fd, and reports the
  loss as a server-level fault. A late owner finds the claim gone and leaves
  the conn alone, which is the rule everywhere else too.

  A CONNECTION CLAIMED BUT NEVER SERVED is the same loss one hop down: the
  owner won the claim and its serving loop never started. The sweeper cannot
  take that claim — somebody holds it — so it does what stop-server does with
  a conn it cannot claim: answers the peer, shuts the conn down so the owner
  reads EOF whenever it runs, and leaves the fd number for the owner to
  release. Under a longer deadline than the unclaimed arm's, because an owner
  can be late without being gone (see claimed-silent-headroom).

  A FIBER-HELD CONN PAST ITS IDLE DEADLINE is the third, and it is the
  counterpart of SO_RCVTIMEO on the threads strategy. conn-down! wakes the
  fiber parked on the fd (forget! resumes registered waiters), so a parked
  read ends as :closed, not a hang — and the fiber, not the sweeper, is what
  then frees the fd number.

  A tick that throws must not end the sweep, for the reason a fault must not
  end a worker: this future is the only thing enforcing either deadline,
  nothing restarts it, and nothing would have reported it going away. Idle
  and dropped connections would then accumulate for the life of the process,
  each holding an fd, with the server looking entirely healthy."
  [conns stats fault! unpend!]
  (let [stop? (atom false)]
    (future
      (loop []
        (Thread/sleep 100)
        (when-not @stop?
          (try
            (let [now (System/currentTimeMillis)]
              (doseq [e @conns]
                (cond
                  ;; owned, never serving: the claim answered and then the
                  ;; serving loop never started — a worker take that was never
                  ;; scheduled back, one hop below the unclaimed loss. The peer
                  ;; gets the same answered close, and the owner (if it ever
                  ;; does run) wakes on the shutdown as EOF and unwinds.
                  ;; HEADROOM, not the bare deadline: between the claim and the
                  ;; loop's first line sits a scheduling window a GC pause or a
                  ;; descheduled worker can stretch past any base deadline, and
                  ;; reaping a conn whose owner is about to serve it would
                  ;; close a live connection under ordinary load. The unclaimed
                  ;; arm needs no such margin — nobody has the conn at all.
                  ;;
                  ;; SHUTDOWN, NOT RELEASE: this conn has an owner, and only
                  ;; the owner closes (conn-down!). Closing the number here
                  ;; hands it to the next accept, and the owner — late, not
                  ;; gone — then runs its loop on a live connection that
                  ;; belongs to somebody else. The number stays allocated
                  ;; until the owner's own finally releases it, the way
                  ;; stop-server leaves every conn it cannot claim; an owner
                  ;; that never runs at all costs one fd, and is a lost pool
                  ;; thread besides. The CAS is conn-down!'s, inlined so the
                  ;; answer can go out before the shutdown makes writes EPIPE.
                  ;;
                  ;; Subtraction, not addition: :claim-by is no-deadline
                  ;; (Long/MAX_VALUE) until the acceptor arms it, and under
                  ;; :threads the worker can own the conn before that — a sum
                  ;; would overflow there, a difference is merely negative.
                  (and @(e :owned?) (not @(e :serving?))
                       (> (- now @(e :claim-by))
                          (* sweep-deadline-ms claimed-silent-headroom)))
                  (when (compare-and-set! (:down? e) false true)
                    (answer-unclaimed! e)
                    (socket/c-shutdown (:conn e) 2)
                    (when (:poller? e) (poller/forget! (:conn e)))
                    (fault! :claimed-silent
                            (ex-info "connection claimed but never served"
                                     {:type :ring-chez/claimed-silent
                                      :peer (:peer e)})))

                  ;; handed on, and still nobody's: the handoff lost it
                  (and (not @(e :owned?)) (< @(e :claim-by) now))
                  (when (claim-close! e)
                    (unpend! e)
                    ;; answer the peer BEFORE anything else: the release
                    ;; below shuts the fd, and after it there is no conn to
                    ;; answer with. Reclaim the fd BEFORE reporting the
                    ;; loss, not after: this sweeper is the only thing that
                    ;; will ever free this conn — the claim is taken, so no
                    ;; late owner may touch it again — and fault! writes to
                    ;; stderr, which can throw. A report that failed would
                    ;; then cost the very fd it had just won back, for the
                    ;; life of the process.
                    (answer-unclaimed! e)
                    (conn-release! conns e stats)
                    (fault! :unclaimed
                            (ex-info "connection accepted and handed on, but never claimed"
                                     {:type :ring-chez/unclaimed-conn
                                      :peer (:peer e)})))

                  ;; down, not released: the fiber that owns this conn is the
                  ;; only one allowed to free its fd number, and the shutdown
                  ;; here is what wakes it to do that
                  (and (:poller? e) (not @(e :down?)) (< @(e :deadline) now))
                  (conn-down! e))))
            (catch Throwable t (fault! :sweeper t)))
          (recur))))
    stop?))

(defn- fiber-serve
  "Serve one connection on a fiber-backed go block parked on jolt.io-poller.
  The accept loop spawns one per accepted connection — there is no fixed
  bound, and idle connections hold no thread. The spawn runs under
  *go-backend* :fiber: the default :thread backend runs go bodies on OS
  threads, where wait-ready would block the carrier instead of parking."
  [conn-info cfg entry conns]
  (let [conn     (:conn entry)
        deadline (:deadline entry)]
    (reset! deadline (+ (System/currentTimeMillis) (:ka-ms cfg)))
    (binding [async/*go-backend* :fiber]
      (async/go
        ;; stop-server may have released this conn between the accept that
        ;; registered it and this go block being scheduled; if it did, the fd
        ;; number may already be another socket's and nothing here may use it
        (when (claim-close! entry)
          (try
            (connection-loop conn conn-info cfg
                             (fiber-io conn deadline (:write-timeout-ms cfg)) deadline entry)
            (catch Throwable _ nil)
            ;; the release is guarded for the reason the worker's is: it runs
            ;; in a FINALLY, where a throw beats the catch above it. Here it
            ;; costs one connection rather than a pool thread — there is no
            ;; fixed pool to shrink — but it is the same silence, and the fd
            ;; it leaks is just as gone.
            (finally
              (try (conn-release! conns entry (:stats cfg))
                   (catch Throwable t ((:fault! cfg) :conn-release t))))))))))

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
            (get opts k))
          (check-bool [k]
            (when (contains? opts k)
              (let [v (get opts k)]
                (when-not (boolean? v) (bad! k v "true or false"))))
            (get opts k))
          (check-host []
            (when (contains? opts :host)
              (let [v (get opts :host)]
                ;; inet_pton decides, so the message names what it rejected
                (when-not (socket/ipv4->octets v)
                  (bad! :host v "an IPv4 address (e.g. \"127.0.0.1\" or \"0.0.0.0\")"))))
            (get opts :host))]
    {:port               (check-num :port 1 65535)
     :host               (check-host)
     :worker-threads     (check-num :worker-threads 1 ##Inf)
     :ka-ms              (check-num :keep-alive-timeout-ms 1 ##Inf)
     :max-bytes          (check-num :max-request-bytes 1 ##Inf)
     :max-header-bytes   (check-num :max-header-bytes 1 ##Inf)
     :request-timeout-ms (check-num :request-timeout-ms 0 ##Inf)
     :on-failure         (check-ifn :on-failure)
     :ws-guard           (check-ifn :ws-guard)
     :write-timeout-ms   (check-num :write-timeout-ms 0 ##Inf)
     :handler-timeout-ms (check-num :handler-timeout-ms 0 ##Inf)
     :reuse-port         (check-bool :reuse-port)}))

(defn run-server
  "Start the server; return a handle {:socket :port :host :running}. opts:
    :port                  listen port (default 3000)
    :host                  interface to bind, as an IPv4 address (default
                           \"127.0.0.1\"); \"0.0.0.0\" for every interface
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
    :handler-timeout-ms    how long one handler call may take (default 0 =
                           no deadline). Past it the request is answered
                           through :on-failure, then a plain 503, and the
                           worker goes back to serving; the abandoned
                           handler's thread is not reclaimed. Costs a thread
                           handoff per request under :threads (~15-25%
                           throughput), nothing under :fibers
    :reuse-port            bind with SO_REUSEPORT (default false), so several
                           processes can share the port and the kernel spreads
                           connections over them (Linux)
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
          ws-guard (get opts :ws-guard)
          on-failure (get opts :on-failure)
          max-bytes (or (:max-bytes v) 1048576)
          ;; the head is capped separately from the body (Igropyr header-limit,
          ;; 8 KiB): a request may legitimately carry a megabyte of body, a
          ;; head may not, and parsing one is not linear in its size
          max-header-bytes (or (:max-header-bytes v) 8192)
          ;; how long ONE request may take to arrive, however steadily it
          ;; dribbles. The idle timeout only bounds the GAP between segments
          ;; and re-arms on every one, so without this a client sending a byte
          ;; just inside it holds a worker indefinitely — slowloris, at
          ;; trivial cost to the attacker. 0 disables.
          request-timeout-ms (or (:request-timeout-ms v) 60000)
          write-timeout-ms (or (:write-timeout-ms v) 30000)
          ;; how long the HANDLER may take. Igropyr kills a worker stuck past
          ;; stuck-ms and replaces it; we cannot kill a thread, but we can
          ;; stop waiting for one — which reclaims the worker, the fd and the
          ;; connection, and answers the client 503 instead of never.
          ;;
          ;; OFF by default, because enforcing it means running the handler
          ;; off the worker thread (it cannot be abandoned from its own
          ;; stack) and that handoff measures 15-25% of throughput on the
          ;; threads strategy — a cost every request pays for a failure most
          ;; servers never have. Under :fibers the handler is already on a
          ;; thread and the deadline measures free, so a fibers server can
          ;; turn it on for nothing. See the README.
          handler-timeout-ms (or (:handler-timeout-ms v) 0)
          ;; loopback by default: Igropyr binds 0.0.0.0, but this is a library
          ;; and a version bump must not put a server that was private on the
          ;; network. Opt in with :host "0.0.0.0".
          host   (or (:host v) "127.0.0.1")
          base-info {:server-port port :server-name host}
          ;; boxed so a running server can be re-pointed without a restart
          ;; (Igropyr http-swap! / http-set-ws!) — REPL work is the whole point
          handler-box (atom handler)
          ws-handler-box (atom ws-handler)
          stats {:open (atom 0) :requests (atom 0) :active (atom 0)
                 ;; server-level faults (see make-fault!): a wedge that used to
                 ;; be invisible is a number a caller can read
                 :faults (atom 0) :last-fault (atom nil)
                 :started (System/currentTimeMillis)}
          fault! (make-fault! stats)
          ;; everything that does not vary per connection, built once and
          ;; threaded through the serving path unchanged
          cfg {:handler-box handler-box :ws-handler-box ws-handler-box
               :ws-guard ws-guard :on-failure on-failure :fault! fault!
               :ka-ms ka-ms :max-bytes max-bytes :max-header-bytes max-header-bytes
               :request-timeout-ms request-timeout-ms
               :write-timeout-ms write-timeout-ms
               :handler-timeout-ms handler-timeout-ms
               :stats stats}
          fd     (socket/listen-socket host port {:reuse-port? (boolean (:reuse-port v))})
          running? (atom true)]
      (if (= :fibers strategy)
        (let [conns (atom #{})   ; live conn entries; sweeper + stop sweep them
              ;; nothing to un-count on this strategy: accept pressure and its
              ;; keep-alive retirement are the fixed pool's problem
              sweep (start-sweeper! conns stats fault! (fn [_]))]
          {:socket fd :port port :host host :running running? :conns conns :sweep sweep
           :handler handler-box :ws-handler ws-handler-box :stats stats
           :acceptor
           (future (serve-loop fd running?
                              (fn [conn peer]
                                (swap! (:open stats) inc)
                                ;; publish, THEN check — the order is the whole
                                ;; interlock. stop-server clears running? before
                                ;; it sweeps, so a conn is either already in
                                ;; `conns` when the sweep reads it, or this read
                                ;; sees false and cleans up here. Reading
                                ;; running? first (serve-loop's cond, which is
                                ;; where this used to be decided) let a conn land
                                ;; in `conns` AFTER the sweep had passed over it,
                                ;; with nobody left to take it down: a stopped
                                ;; server went on serving it, and its fd leaked.
                                (let [entry (register-conn! conns conn peer true)]
                                  ;; the spawn below is the handoff, and from
                                  ;; here the conn is somebody's: a go block
                                  ;; that never runs leaves it registered,
                                  ;; unclaimed and unserved, which is the drop
                                  ;; the sweeper reaps and reports
                                  (reset! (:claim-by entry)
                                          (+ (System/currentTimeMillis) sweep-deadline-ms))
                                  (if @running?
                                    ;; the spawn, not the session: a throw here
                                    ;; means the go block never ran, so this
                                    ;; conn is still unclaimed and releasing it
                                    ;; is ours to do. A fault inside the
                                    ;; session is the fiber's (see fiber-serve).
                                    ;;
                                    ;; nonblock! is inside the guard, and it
                                    ;; used to run before the conn was
                                    ;; registered at all: a throw from it was
                                    ;; then a connection nothing had a record
                                    ;; of, so the fault said a conn was lost
                                    ;; without leaving anything able to close
                                    ;; it. Registered first, it is releasable.
                                    (try (poller/nonblock! conn)
                                         (fiber-serve (assoc base-info :remote-addr peer)
                                                      cfg entry conns)
                                         (catch Throwable t
                                           (fault! :spawn t)
                                           (when (claim-close! entry)
                                             (conn-release! conns entry stats))))
                                    (when (claim-close! entry)
                                      (conn-release! conns entry stats)))))
                              fault!))})
        (let [work  (async/chan)  ; unbuffered: acceptor parks when all workers busy
              conns (atom #{})    ; live conn entries; the stop sweep closes them
              pending (atom 0)    ; conns accepted but not yet claimed by a worker
              ;; the claim backstop runs here too: >!! answers only once a
              ;; worker has the conn, so a conn still unclaimed after that is
              ;; one the take/claim hop lost, not one queued behind a busy pool
              sweep (start-sweeper! conns stats fault! #(pending-done! pending %))
               io (assoc threads-io
                         :under-pressure? #(pos? @pending)
                         :claim! #(pending-done! pending %)
                         ;; idle keep-alive first read: wait in poll(2) slices
                         ;; instead of parking a full ka-timeout recv. Enforces
                         ;; the ka deadline itself (poll has no timeout), and
                         ;; under accept pressure retires a connection only
                         ;; after a grace period of quiet — never instantly,
                         ;; or a client mid-reuse would race a reset. Returns
                         ;; the recv contract (n>0 data, 0 closed/retire).
                         :idle-recv! #(idle-poll-recv! %1 %2 ka-ms pending))]
           (dotimes [_ n] (start-worker! base-info cfg io work conns running?))
          {:socket fd :port port :host host :running running? :work work :conns conns
           :sweep sweep :handler handler-box :ws-handler ws-handler-box :stats stats
           :acceptor
           (future (serve-loop fd running?
                              (fn [conn peer]
                                (swap! (:open stats) inc)
                                ;; publish, then check — see the fibers arm. The
                                ;; put is part of the same condition: >!! answers
                                ;; false on a channel stop-server has closed, and
                                ;; a conn nobody can take off it needs the same
                                ;; cleanup as one accepted after the sweep.
                                ;;
                                ;; The handoff is its own expression so the
                                ;; cleanup below runs exactly once whether the
                                ;; put answered false or threw: pending is
                                ;; decremented on the not-handed path only, and
                                ;; a throw out of the release cannot take the
                                ;; accept loop with it (see serve-loop).
                                (let [entry (register-conn! conns conn peer false)
                                      _ (do (reset! (:pending? entry) true)
                                            (swap! pending inc))
                                      handed? (try (and @running? (async/>!! work entry))
                                                   (catch Throwable t
                                                     (fault! :handoff t)
                                                     false))]
                                  (if handed?
                                    ;; a worker has it — the rendezvous does not
                                    ;; answer until one does — so from here an
                                    ;; unclaimed conn is a lost one, not a
                                    ;; queued one. Arm the backstop.
                                    (do
                                      ;; Pressure counts conns no worker has
                                      ;; taken yet, and the rendezvous
                                      ;; answering IS the take: past this point
                                      ;; the conn is nobody's backlog, so
                                      ;; leaving it counted retires idle
                                      ;; keep-alives for the whole claim window
                                      ;; over a loss that is already past the
                                      ;; queue — the ka collapse measured
                                      ;; 2026-09-20. Once-only either way: the
                                      ;; worker's take-time claim and this both
                                      ;; go through pending-done!'s CAS.
                                      (pending-done! pending entry)
                                      (reset! (:claim-by entry)
                                              (+ (System/currentTimeMillis) sweep-deadline-ms)))
                                    (do
                                      (pending-done! pending entry)
                                      (try (when (claim-close! entry)
                                             (conn-release! conns entry stats))
                                           (catch Throwable t (fault! :conn-release t)))))))
                              fault!))})))))

(defn swap-handler!
  "Point a running server at a new Ring handler (Igropyr http-swap!). Takes
  effect on the next request, including on connections already open."
  [server handler]
  (reset! (:handler server) handler)
  server)

(defn swap-ws-handler!
  "Point a running server at a new websocket handler, or nil to stop offering
  the upgrade (Igropyr http-set-ws!). Sessions already running are unaffected."
  [server ws-handler]
  (reset! (:ws-handler server) ws-handler)
  server)

(defn server-stats
  "A snapshot of what the server is doing: :connections open right now,
  :active requests in flight, :requests answered since boot, :uptime-ms
  (Igropyr http-stats), and the server-level fault count with the last one
  (:faults / :last-fault — see make-fault!; these are the server's own
  failures, not a handler's, which :on-failure answers)."
  [server]
  (let [{:keys [open requests active started faults last-fault]} (:stats server)]
    {:connections @open
     :active @active
     :requests @requests
     ;; server-level faults, not request failures (:on-failure has those):
     ;; anything thrown in the accept loop, a worker's take/claim/release or a
     ;; fiber's teardown, plus :unclaimed — a connection the accept handoff
     ;; lost. Nonzero means something the server recovered from happened where
     ;; nothing could report it to a caller — see make-fault!.
     :faults (if faults @faults 0)
     :last-fault (when last-fault @last-fault)
     :uptime-ms (- (System/currentTimeMillis) started)}))

(defn stop-server
  "Stop the server: unblock + exit the accept loop, wait for in-flight requests
  to finish, then close the listen socket.

  The drain is Igropyr's http-shutdown!: stop accepting, then let whatever is
  already running answer, rather than cutting a response off mid-write. It is
  bounded by :drain-timeout-ms (default 5000) so a handler that never returns
  cannot stop this returning either.

  Then every live connection is shut down, on both strategies: stopped has to
  mean stopped. A keep-alive connection opened before the stop would
  otherwise keep being served on the threads strategy — closing only the
  listen fd leaves a worker on it until the peer goes away or
  :keep-alive-timeout-ms fires (RFC-0008). The drain runs first, so what the
  sweep shuts down is always a connection between requests, never a response
  mid-write.

  The sweep shuts down rather than closes: shutdown ends the connection at
  once but keeps the fd NUMBER reserved, and only the worker or fiber holding
  it frees that (conn-down!). stop-server releases just the conns nobody ever
  claimed. An owner may therefore still be unwinding when this returns; it is
  reading EOF by then, and holding an fd a moment longer is much cheaper than
  handing its number to another socket while somebody still has it.

  Fiber strategy: the same sweep also forgets each fd — jolt.io-poller is
  process-global and never stops, so a parked fiber is released only by
  forget! resuming it (the woken read sees EOF).

  The listen fd's NUMBER is freed last, after the acceptor thread has exited.
  It used to be closed before the acceptor was known to be done with it, and
  an acceptor that had handed a connection off and not yet re-entered accept()
  — a preemption or a collection is enough — then called accept() on a number
  the next run-server's socket() had already taken as its own listener. It won
  that listener's first connection, read its own running? as false and closed
  the fd unread: the client saw EOF with no header. So the socket is ended in
  place and the acceptor waited for; the number is closed once nothing can
  syscall on it, and the port is free when this returns.

  Ending it in place takes two calls because the platforms disagree.
  shutdown(SHUT_RDWR) on a listening socket wakes a parked accept() on Linux
  (EINVAL, and every accept() after it fails the same way) but is ENOTCONN
  and does nothing on macOS, where close() is what wakes accept() — and
  close() is the very thing that frees the number. dup2(/dev/null) over the
  fd is the portable move: it drops the listener's last reference — which on
  macOS drains the parked accept() (ECONNABORTED) — while the number keeps
  naming an open file, so an accept() the acceptor makes later is ENOTSOCK
  rather than a call on somebody else's socket. Linux does not drain a
  syscall on dup2, hence the shutdown first."
  ([server] (stop-server server nil))
  ([server {:keys [drain-timeout-ms] :or {drain-timeout-ms 5000}}]
  (reset! (:running server) false)
  ;; stop accepting first, so the drain below is over a set that only shrinks
  (when-let [active (get-in server [:stats :active])]
    (let [give-up (+ (System/currentTimeMillis) drain-timeout-ms)]
      (loop []
        (when (and (pos? @active) (< (System/currentTimeMillis) give-up))
          (Thread/sleep 20)
          (recur)))))
  (when-let [sweep (:sweep server)] (reset! sweep true))
  ;; close the work channel first: idle workers leave their take loop, and the
  ;; acceptor's parked put gives up instead of handing over a conn the sweep
  ;; below has already closed
  (when-let [work (:work server)] (async/close! work))
  (let [conns (:conns server)]
    ;; take every live conn out of service first: shutdown makes each pending
    ;; and subsequent read answer EOF, so the worker or fiber holding it
    ;; unwinds and releases its own fd. Nothing here frees an fd NUMBER that
    ;; somebody else may still syscall on — that was the bug.
    (doseq [entry @conns] (conn-down! entry))
    ;; then release the ones nobody ever claimed: a conn still sitting in the
    ;; work channel, or one whose fiber was never scheduled, has no finally to
    ;; run for it. Whoever wins the claim owns the close, so this cannot race
    ;; a worker that took the same conn.
    (doseq [entry @conns]
      (when (claim-close! entry) (conn-release! conns entry (:stats server)))))
  (let [fd (:socket server)]
    (socket/c-shutdown fd 2)                    ; Linux: wakes and fails accept(); macOS: ENOTCONN
    (let [reserved? (not (neg? (socket/c-dup2 @socket/devnull-fd fd)))] ; both: the number is ours until the close below
      ;; Every accept() from here fails at once, so the wait is bounded by the
      ;; acceptor's own backoff (<= 100ms) plus scheduling; the timeout guards
      ;; against a runtime fault. An acceptor still alive past it may yet
      ;; syscall on the number, so the number is left reserved rather than
      ;; freed — the safe side of the race, at the cost of one /dev/null fd.
      (let [acceptor (:acceptor server)
            exited?  (or (nil? acceptor)
                         (not= ::timeout (deref acceptor drain-timeout-ms ::timeout)))]
        (when (or exited? (not reserved?))
          (socket/c-close fd)))))
  nil))
