(ns ring-chez.socket
  "The server's socket logic over jolt.socket.native: listen-socket setup
   (every family a host resolves to, port-0 unification, failure policy), the
   peer-gone? probe, and the few settings the strategies share. The C calls,
   constants and struct layouts are jolt's, for macOS, Linux and Windows.
   Every fd-holding helper here assumes the caller owns the close path —
   shutdown(SHUT_RDWR) before close() on every socket, every close path."
  (:require [jolt.ffi :as ffi]
            [jolt.socket.native :as native]))

(def bufsize 65536)

(defn- fail-ex
  "ex-info for a failed syscall, naming it and the error behind it."
  [what syscall e]
  (ex-info (str what " failed: " (native/error-message e))
           {:errno e :strerror (native/error-message e) :syscall syscall}))

(defn set-rcvtimeo!
  "SO_RCVTIMEO in milliseconds; 0 waits forever."
  [fd ms]
  (native/set-timeout! fd native/so-rcvtimeo ms))

;; Igropyr default-write-timeout-ms: a peer that stops draining must not pin
;; the worker forever. SO_SNDTIMEO bounds each blocking send; a timed-out
;; send answers negative, which send-all already treats as peer-gone (abandon
;; the response and close). 0 disables. Fiber-strategy sockets are
;; non-blocking, where sends park in wait-write! under the ka sweeper instead.
(defn set-sndtimeo! [fd ms]
  (when (pos? ms)
    (native/set-timeout! fd native/so-sndtimeo ms)))

(defn peer-ip
  "The presentation-form peer address out of the sockaddr accept() filled in.
   A scope zone is not included, so a link-local peer prints as fe80::1."
  [sa]
  (or (native/sockaddr-ip sa)
      (throw (ex-info "accept() returned an address of an unknown family"
                      {:family (native/sockaddr-family sa)}))))

(defn- setsockopt-flag!
  "Set one boolean socket option, or throw with the error behind it. Closing
   fd on failure is the caller's job — listen-sockets closes it on every
   failure path, and closing it here too was a double close, which can take
   down whatever socket reused the number between."
  [fd level opt-name opt-const]
  (let [[r e] (native/set-int-option! fd level opt-const 1)]
    (when (neg? r)
      (throw (ex-info (str "setsockopt(" opt-name ") failed: " (native/error-message e))
                      {:errno e :strerror (native/error-message e)
                       :syscall "setsockopt" :option opt-name})))))

(defn resolve-bind-sockaddrs
  "The bind candidates for host:port, as {:family :addr :addrlen} maps whose
   :addr buffers the caller frees."
  [host port]
  (let [{:keys [addrs error message]} (native/resolve-addrs host port {:passive? true})]
    (cond
      error
      (throw (ex-info (str "run-server: :host could not be resolved: "
                           (pr-str host) " — " message)
                      {:key :host :given host :gai-code error}))
      (empty? addrs)
      (throw (ex-info (str "run-server: :host resolved to no addresses: " (pr-str host))
                      {:key :host :given host :gai-code 0}))
      :else addrs)))

(defn local-port
  "The port fd is bound to — what the kernel picked when bind() was asked for
  port 0."
  [fd]
  (let [p (native/local-port fd)]
    (when (neg? p)
      (throw (ex-info "getsockname() failed" {:syscall "getsockname"})))
    p))

(defn- bind-failure-ex
  "The bind() error as it should be reported: friendly for EADDRINUSE (name
   the port, say what is on it), errno + strerror otherwise."
  [host port errno strerror]
  (ex-info
   (if (= errno native/eaddrinuse)
     (str "port " port " is already in use — another process is "
          "listening on " (pr-str host) ":" port " (" strerror
          ", errno " errno "); stop it or pass a different :port")
     (str "bind() failed on " (pr-str host) ":" port ": " strerror
          " (errno " errno ")"))
   (cond-> {:errno errno :strerror strerror :syscall "bind" :port port}
     (= errno native/eaddrinuse) (assoc :errno-name "EADDRINUSE"))))

(declare listen-sockets*)

(defn listen-sockets
  "Bind and listen on EVERY candidate resolve-bind-sockaddrs returns — a
   name with both a v4 and a v6 answer serves both families on the one
   port. Returns {:fds [...] :port n}: :port is the unified port (port 0
   asks the kernel for one on the first bind, and the rest bind it
   explicitly), or throws. Every AF_INET6 listener is IPV6_V6ONLY, so a
   bound address serves exactly its family and dual-stack comes from a
   name that answers in both.

   Failure policy: EADDRNOTAVAIL on one of several candidates is skipped —
   that is an address family this box does not have, and the others cover
   it — but as the ONLY candidate it throws. Anything else aborts the
   whole set: every fd opened so far is closed (shutdown first, per the
   house rule) and the bind error rethrown, because a half-bound server
   (v6 up, v4 not) hides exactly the \"already running\" collision
   EADDRINUSE exists to report.

   Port 0 is the exception: the kernel's pick for the first family says
   nothing about the others, so the second bind can find it taken. That
   EADDRINUSE is not a running server, and the whole set is retried for a
   fresh pick."
  ([host port] (listen-sockets host port nil))
  ([host port opts]
   (loop [attempt 1]
     (let [r (try (listen-sockets* host port opts)
                  (catch Throwable t
                    (if (and (zero? port) (< attempt 8)
                             (= "EADDRINUSE" (get (ex-data t) :errno-name)))
                      ::retry
                      (throw t))))]
       (if (identical? r ::retry) (recur (inc attempt)) r)))))

(defn- listen-sockets*
  ([host port {:keys [reuse-port?]}]
   (let [entries (resolve-bind-sockaddrs host port)]
     (loop [remaining entries opened [] chosen nil]
       (if (empty? remaining)
         (if (seq opened)
           {:fds opened :port (or chosen port)}
           (throw (ex-info (str "run-server: none of " (pr-str host)
                                " could be bound on this machine")
                           {:key :host :given host})))
         (let [{:keys [family addr addrlen]} (first remaining)
               [fd se] (native/c-socket family native/sock-stream 0)]
           (if (neg? fd)
             (do
               (doseq [f opened] (native/c-shutdown f native/shut-rdwr) (native/c-close f))
               (doseq [{a :addr} (rest remaining)] (ffi/free a))
               (ffi/free addr)
               (throw (fail-ex "socket()" "socket" se)))
             (let [abandon! (fn []
                              (doseq [f opened]
                                (native/c-shutdown f native/shut-rdwr)
                                (native/c-close f))
                              (doseq [{a :addr} (rest remaining)] (ffi/free a)))
                   target (or chosen port)]
               (when chosen
                 ;; the first bind with port 0 picked for the whole set;
                 ;; every later candidate binds that explicit port
                 (native/set-sockaddr-port! addr chosen))
               (let [outcome
                     (try
                       ;; SO_REUSEPORT before bind, or it does not apply to it:
                       ;; several processes then bind the same port and the
                       ;; kernel spreads new connections over them (Linux).
                       ;; Opt-in, because without it a second bind SHOULD fail —
                       ;; that error is how you find out a server is already
                       ;; running.
                       (try (native/close-on-exec! fd) (catch Throwable _ nil))
                       (let [[r e] (native/set-listener-reuse! fd)]
                         (when (neg? r)
                           (throw (fail-ex "setsockopt(SO_REUSEADDR)" "setsockopt" e))))
                       (when reuse-port?
                         (when-not native/so-reuseport
                           (throw (ex-info "run-server: :reuse-port is not supported on this platform"
                                           {:key :reuse-port :os native/os})))
                         (setsockopt-flag! fd native/sol-socket "SO_REUSEPORT" native/so-reuseport))
                       (when (= family native/af-inet6)
                         (setsockopt-flag! fd native/ipproto-ipv6 "IPV6_V6ONLY" native/ipv6-v6only))
                       (let [[r e] (native/c-bind fd addr addrlen)]
                         (when (neg? r)
                           (throw (ex-info "bind" {:errno e :strerror (native/error-message e)
                                                   :syscall "bind"}))))
                       ;; 511, as Igropyr uses (http.sc:1899): the backlog is
                       ;; what absorbs an accept burst while the acceptor is
                       ;; busy, and 64 is small enough that a modest connection
                       ;; storm gets refused rather than queued.
                       (let [[r e] (native/c-listen fd 511)]
                         (when (neg? r)
                           (throw (ex-info "listen" {:errno e :strerror (native/error-message e)
                                                     :syscall "listen"}))))
                       ;; non-blocking: the acceptor waits in poll slices
                       ;; and never blocks in accept(), which is what lets
                       ;; stop-server end it without closing the fd under it
                       (let [[r e] (native/set-blocking! fd false)]
                         (when (neg? r)
                           (throw (fail-ex "set non-blocking" "fcntl" e))))
                       (ffi/free addr)
                       :ok
                       (catch Throwable t t))]
                 (if (identical? outcome :ok)
                   (recur (rest remaining)
                          (conj opened fd)
                          (or chosen (when (zero? port) (local-port fd))))
                   (let [errno (get (ex-data outcome) :errno -1)]
                     (cond
                       ;; family absent on this box, and another candidate
                       ;; covers it — skip, but close our fd first
                       (and (= errno native/eaddrnotavail) (seq (rest remaining)))
                       (do (native/c-close fd)
                           (ffi/free addr)
                           (recur (rest remaining) opened chosen))

                       :else
                       (do (abandon!)
                           (native/c-close fd)
                           (ffi/free addr)
                           ;; only bind's failure is reworded; setsockopt's
                           ;; already names itself, and listen gets its own
                           (throw (case (get (ex-data outcome) :syscall)
                                    "bind" (bind-failure-ex host target errno
                                                            (get (ex-data outcome) :strerror ""))
                                    "listen" (ex-info (str "listen() failed on " (pr-str host) ":"
                                                           target ": " (get (ex-data outcome) :strerror ""))
                                                      (ex-data outcome))
                                    outcome)))))))))))))))



(defn peer-gone?
  "True when the far end of fd has closed. Consumes nothing if it has not.

  This is what lets a response that is streaming notice a client that walked
  away while the application had nothing to send: without it, a stream only
  finds out at its next write, and a stream whose application is quiet never
  finds out at all.

  poll with a zero timeout answers immediately. A hangup or error is
  conclusive; readability is not, because a peer may equally have pipelined
  its next request, so readability is settled by a PEEKING recv — end of
  stream reports 0, and real bytes stay queued for the reader they belong to.
  EINTR is not an answer either way, so it asks again."
  [fd]
  (let [buf (ffi/alloc 1)]
    (try
      (loop []
        (let [rev (native/poll-one fd native/pollin 0)]
          (cond
            (zero? rev) false
            (neg? rev)  true
            (pos? (bit-and rev (bit-or native/pollerr native/pollhup native/pollnval))) true
            (pos? (bit-and rev native/pollin))
            (let [[n e] (native/c-recv fd buf 1 native/msg-peek)]
              (cond
                (zero? n) true                              ; orderly shutdown
                (pos? n)  false                             ; the peer is talking
                :else     (if (native/eintr? e) (recur) false)))
            :else false)))
      (finally (ffi/free buf)))))
