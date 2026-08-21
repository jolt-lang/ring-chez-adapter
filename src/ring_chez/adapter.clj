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
;; wake). The poller is process-global and never stops, so every close path
;; must forget! the fd: that keeps the poller table bounded, drops stale
;; readiness tombstones before the kernel reuses the fd number, and wakes a
;; fiber still parked on the fd (close first, then forget! — a forget-first
;; waker retries recv on the still-open fd, sees EAGAIN, re-parks on a dead
;; registration and hangs).

;; --- the accept loop --------------------------------------------------------
;; Clean shutdown: stop-server shutdown()s the listen fd (SHUT_RDWR), which
;; wakes the acceptor parked in accept() on both Linux and macOS (close()
;; alone does NOT wake it on Linux), then closes it; the loop sees `running?`
;; false and exits instead of spinning on the dead fd.
(defn- serve-loop [listen-fd running? serve!]
  (loop []
    (let [conn (socket/c-accept listen-fd ffi/null ffi/null)]
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
  {:recv! (fn [conn buf] (socket/c-recv conn buf socket/bufsize 0))
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
     :take! (fn [ch] (async/<! ch))
     :run!  (fn [f] (async/<! (async/thread (f))))}))

(defn- handle-failure
  "Every abnormal handler completion lands here (Igropyr's on-failure
   semantics). The hook gets one attempt: a throw or a non-map/nil return
   falls back to the plain 500, and the worker always survives. The returned
   map goes through http/send-response like any handler response, so keep-alive
   is preserved across a failure."
  [on-failure request t]
  (or (when on-failure
        (let [r (try (on-failure request t) (catch Throwable _ nil))]
          (when (and (map? r) (contains? r :status)) r)))
      {:status 500 :headers {"Content-Type" "text/plain"}
       :body "Internal Server Error"}))

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
                 (catch Throwable t (handle-failure on-failure request t)))]
      (cond
        (and (map? v) (contains? v :status)) v
        v :upgrade
        :else {:status 403 :headers {"Content-Type" "text/plain"}
               :body "Forbidden"}))))

(defn- connection-loop [conn handler port ka-ms ws-handler ws-guard on-failure max-bytes write-timeout-ms io deadline]
  (socket/set-rcvtimeo! conn ka-ms)
  (socket/set-sndtimeo! conn write-timeout-ms)
  (let [recv!      (:recv! io)
        idle-recv! (or (:idle-recv! io) recv!)
        send!      (or (:send! io) http/send-all)]
    (loop [acc http/no-bytes]
      ;; deadline: the read of this request (idle wait or mid-request trickle)
      ;; must finish within ka-ms of starting — the fibers-strategy sweeper
      ;; closes the conn past it (a parked read wakes as :closed). nil on the
      ;; threads strategy, where SO_RCVTIMEO already bounds every recv.
      (when deadline (reset! deadline (+ (System/currentTimeMillis) ka-ms)))
      (let [r (http/read-request conn acc max-bytes recv! idle-recv!)
            ;; the read is over: whatever happens next — a handler, a stream,
            ;; a websocket session, an error response — is not the peer
            ;; failing to send, so nothing below this point may be reaped for
            ;; taking longer than ka-ms. Writes re-arm it for themselves (see
            ;; fiber-io); the next iteration re-arms it for the next read.
            _ (when deadline (reset! deadline no-deadline))]
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
          (= :unsupported r) (send! conn (http/response->parts
                                           {:status 501 :headers {"Content-Type" "text/plain"
                                                                  "Connection" "close"}
                                            :body "Not Implemented"} false))
          (map? r)
          (let [{:keys [request error]} (http/request->ring (:head r) (:body r) port)]
            (cond
              error (send! conn (http/response->parts error false))
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
                     ((:run! io) #(try (ws-handler (ws/make-session conn))
                                       (catch Throwable t
                                         (try (when on-failure (on-failure request t))
                                              (catch Throwable _ nil))))))))
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
                                  (http/keep-alive? request)
                                  (zero? (alength ^bytes (:leftover r))))
                           (update resp :headers #(assoc (or % {}) "Connection" "close"))
                           resp)]
                (when (http/send-response conn request resp (:take! io) send!)
                  (recur (:leftover r))))))
          :else nil)))))

(defn- worker [handler port ka-ms ws-handler ws-guard on-failure max-bytes write-timeout-ms io work]
  (loop []
    (when-let [conn (async/<!! work)]
      ;; claim at take-time: pending then counts only conns accepted but not
      ;; yet claimed — decrementing in the acceptor after >!! leaves a window
      ;; where a claimed conn still reads as pressure and over-retires
      ((io :claim!))
      (try
        (connection-loop conn handler port ka-ms ws-handler ws-guard on-failure max-bytes write-timeout-ms io nil)
        ;; an escaping throwable must not kill the worker: a dead worker
        ;; shrinks the pool permanently and starves later connections
        (catch Throwable _)
        ;; shutdown before close: on Linux close() alone does not deliver
        ;; FIN to the peer holding a blocked recv (same family as 791d5c4)
        (finally (socket/c-shutdown conn 2)
                 (socket/c-close conn)))
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
    (socket/c-shutdown (:conn entry) 2)
    (socket/c-close (:conn entry))
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
  [handler port ka-ms ws-handler ws-guard on-failure max-bytes write-timeout-ms conn conns]
  (let [deadline (atom (+ (System/currentTimeMillis) ka-ms))
        entry {:conn conn :closed? (atom false) :deadline deadline}]
    (swap! conns conj entry)
    (binding [async/*go-backend* :fiber]
      (async/go
        (try
          (connection-loop conn handler port ka-ms ws-handler ws-guard on-failure
                           max-bytes write-timeout-ms
                           (fiber-io conn deadline write-timeout-ms) deadline)
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
          ws-guard (get opts :ws-guard)
          on-failure (get opts :on-failure)
          max-bytes (or (:max-bytes v) 1048576)
          write-timeout-ms (or (:write-timeout-ms v) 30000)
          fd     (socket/listen-socket port)
          running? (atom true)]
      (if (= :fibers strategy)
        (let [conns (atom #{})   ; live conn entries; sweeper + stop sweep them
              sweep (start-sweeper! conns)]
          (future (serve-loop fd running?
                              (fn [conn]
                                (poller/nonblock! conn)
                                (fiber-serve handler port ka-ms ws-handler ws-guard on-failure max-bytes write-timeout-ms conn conns))))
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
                                         (ffi/write pfds :uint8 4 socket/POLLIN)
                                         (try
                                           (let [deadline (+ (System/currentTimeMillis) ka-ms)
                                                 grace    (+ (System/currentTimeMillis) 2000)]
                                             (loop []
                                               (let [rc  (socket/c-poll pfds 1 250)
                                                     now (System/currentTimeMillis)]
                                                 (cond
                                                   (pos? rc) (socket/c-recv conn buf socket/bufsize 0)
                                                   (neg? rc) 0
                                                   (>= now deadline) 0
                                                   (and (>= now grace) (pos? @pending)) 0
                                                   :else (recur)))))
                                           (finally (ffi/free pfds))))))]
           (dotimes [_ n] (async/thread (worker handler port ka-ms ws-handler ws-guard on-failure max-bytes write-timeout-ms io work)))
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
  (socket/c-shutdown (:socket server) 2)        ; SHUT_RDWR: wake parked accept()
  (socket/c-close (:socket server))
  nil)
