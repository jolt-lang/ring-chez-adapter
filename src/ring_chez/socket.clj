(ns ring-chez.socket
  "BSD socket bindings for jolt.ffi: listen-socket setup, sockopts, errno.
   Every fd-holding helper here assumes the caller owns the close path —
   shutdown(SHUT_RDWR) before close() on every socket, every close path."
  (:require [clojure.string :as str]
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
(ffi/defcfn c-getsockname "getsockname" [:int :pointer :pointer] :int)
(ffi/defcfn c-dup2       "dup2"       [:int :int] :int)
(ffi/defcfn c-open       "open"       [:string :int] :int)
;; One /dev/null for the process, opened on first use: stop-server dup2()s it
;; over a listen fd to end the listener without freeing the fd's NUMBER (see
;; stop-server). O_RDWR is 2 on every platform this runs on.
(def devnull-fd (delay (let [fd (c-open "/dev/null" 2)]
                         (when (neg? fd)
                           (throw (ex-info "open(/dev/null) failed" {:errno (ffi/errno)})))
                         fd)))
(ffi/defcfn c-recv       "recv"       [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-send       "send"       [:int :pointer :size_t :int] :ssize_t :blocking)
;; threads-strategy idle reads wait in poll(2) slices (see run-server's
;; :idle-recv!). struct pollfd { int fd; short events; short revents; } — fd
;; at 0, events at byte 4, revents at bytes 6-7.
(ffi/defcfn c-poll       "poll"       [:pointer :int :int] :int :blocking)
(ffi/defcfn c-strerror  "strerror"   [:int] :pointer)
;; inet_pton parses a presentation-form address into network byte order —
;; the same routine every other client of the sockets API uses, so ":host"
;; accepts exactly what the platform accepts and rejects the rest.
(ffi/defcfn c-inet-pton  "inet_pton"  [:int :pointer :pointer] :int)
;; inet_ntop is the way back: network bytes to presentation form, with the
;; RFC 5952 v6 compression the hand-rolled formatters never get right.
(ffi/defcfn c-inet-ntop  "inet_ntop"  [:int :pointer :pointer :uint] :pointer)
;; getaddrinfo resolves :host — numeric literals and names, v4 and v6, one
;; call. gai_strerror because the codes are NOT errno (EAI_NONAME is 8 on
;; macOS and -2 on Linux), so strerror would name the wrong thing.
(ffi/defcfn c-getaddrinfo  "getaddrinfo"  [:pointer :pointer :pointer :pointer] :int :blocking)
(ffi/defcfn c-freeaddrinfo "freeaddrinfo" [:pointer] :void)
(ffi/defcfn c-gai-strerror "gai_strerror" [:int] :pointer)

(def POLLIN  0x001)
(def POLLOUT 0x004)
(def ^:private POLLERR  0x008)
(def ^:private POLLHUP  0x010)
(def ^:private POLLNVAL 0x020)

;; The conditions under which a recv answers immediately: data waiting, the
;; peer hung up (0), or the fd is bad (-1). Anything else poll reports —
;; writability above all — means a recv would BLOCK, for the whole
;; SO_RCVTIMEO, on a socket with nothing to read.
(def poll-readable (bit-or POLLIN POLLERR POLLHUP POLLNVAL))

;; struct pollfd { int fd; short events; short revents; } — fd at 0, events at
;; 4-5, revents at 6-7. There is no 16-bit FFI scalar, so the short fields go a
;; byte at a time, little-endian (as make-sockaddr already assumes).
(def pollfd-size 8)

(defn init-pollfd!
  "Zero a pollfd and arm it for events on fd. Zeroing matters: writing only the
  LOW byte of events left the high byte holding whatever the allocator handed
  over, and the POLLOUT-family bits all live up there (POLLWRNORM 0x100,
  POLLWRBAND 0x200, POLLMSG 0x400). One of those set turns every poll on a
  writable socket into an instant wake."
  [pfds fd events]
  (dotimes [i pollfd-size] (ffi/write pfds :uint8 0 i))
  (ffi/write pfds :int fd 0)
  (ffi/write pfds :uint8 (bit-and 0xff events) 4)
  (ffi/write pfds :uint8 (bit-and 0xff (bit-shift-right events 8)) 5))

(defn pollfd-revents
  "What poll actually reported. Acting on the return count alone treats a wake
  for any reason as \"readable\"."
  [pfds]
  (bit-or (ffi/read pfds :uint8 6)
          (bit-shift-left (ffi/read pfds :uint8 7) 8)))

(def af-inet 2)
(def ^:private SOCK-STREAM 1)
(def ^:private macos?
  (str/includes? (str/lower-case (or (System/getProperty "os.name") "")) "mac"))
;; AF_INET6: 30 on Darwin, 10 on Linux — same family, different number.
(def af-inet6 (if macos? 30 10))
(def ^:private IPPROTO-IPV6 41)
;; IPV6_V6ONLY: 27 on the BSDs, 26 on Linux.
(def ^:private ipv6-v6only (if macos? 27 26))
;; EADDRINUSE / EADDRNOTAVAIL: 48 / 49 on macOS, 98 / 99 on Linux.
(def ^:private eaddrinuse    (if macos? 48 98))
(def ^:private eaddrnotavail (if macos? 49 99))
;; SOL_SOCKET / SO_REUSEADDR differ by platform: macOS 0xffff / 4, Linux 1 / 2.
(def ^:private sol-socket  (if macos? 0xffff 1))
(def ^:private so-reuse    (if macos? 4 2))
;; SO_REUSEPORT: macOS 0x0200, Linux 15. Same name, different meanings — on
;; Linux it load-balances new connections across every socket bound to the
;; port, which is the point; the BSDs allow the bind but do not balance.
(def ^:private so-reuseport (if macos? 0x0200 15))
(def ^:private so-rcvtimeo (if macos? 0x1006 20))
;; darwin keeps the sockopt block contiguous: SO_SNDBUF 0x1001 ... SO_SNDTIMEO
;; 0x1005, SO_RCVTIMEO 0x1006 (0x2005 is nothing — setsockopt ENOPROTOOPTs)
(def ^:private so-sndtimeo (if macos? 0x1005 21))

;; errno + strerror read immediately after a syscall failure return — errno
;; is thread-local and stale across syscalls, so it is only meaningful in
;; the window between the failure and any other FFI call.
(defn errno-info []
  (let [n (poller/errno)]
    {:errno n
     :strerror (or (try (ffi/ptr->string (c-strerror n)) (catch Throwable _ nil)) "")}))

;; struct timeval { time_t tv_sec; suseconds_t tv_usec; } — 8 + (4 macOS | 8 Linux)
;; tv_usec is MICROseconds: the sub-second part of a millisecond timeout is
;; (* 1000 (rem ms 1000)). Writing the milliseconds straight in made every
;; timeout lose that part — :keep-alive-timeout-ms 900 became 900µs and cut
;; the connection in ~1ms, :write-timeout-ms 500 became a 500µs send timeout.
;; Only multiples of 1000 came out right, which is why the defaults hid it.
(defn- write-timeval! [tv ms]
  (dotimes [i 16] (ffi/write tv :uint8 0 i))
  (ffi/write tv :uint64 (quot ms 1000) 0)
  (let [usec (* 1000 (rem ms 1000))]
    (if macos?
      (ffi/write tv :uint usec 8)
      (ffi/write tv :uint64 usec 8))))

(defn set-rcvtimeo! [fd ms]
  (let [tv (ffi/alloc 16)]
    (write-timeval! tv ms)
    (c-setsockopt fd sol-socket so-rcvtimeo tv 16)
    (ffi/free tv)))

;; Igropyr default-write-timeout-ms: a peer that stops draining must not pin
;; the worker forever. SO_SNDTIMEO bounds each blocking send; a timed-out
;; send returns EAGAIN, which send-all already treats as peer-gone (abandon
;; the response and close). 0 disables. Fiber-strategy sockets are
;; O_NONBLOCK, where sends park in wait-write! under the ka sweeper instead.
(defn set-sndtimeo! [fd ms]
  (when (pos? ms)
    (let [tv (ffi/alloc 16)]
      (write-timeval! tv ms)
      (c-setsockopt fd sol-socket so-sndtimeo tv 16)
      (ffi/free tv))))

;; F_GETFD / F_SETFD / FD_CLOEXEC are 1 / 2 / 1 on macOS and Linux alike.
(def ^:private f-getfd 1)
(def ^:private f-setfd 2)
(def ^:private fd-cloexec 1)

(defn cloexec?
  "Whether fd is marked close-on-exec."
  [fd]
  (pos? (bit-and (poller/c-fcntl fd f-getfd 0) fd-cloexec)))

(defn close-on-exec!
  "Mark fd close-on-exec, so a process the application forks does not inherit
  it. Without it every child an application spawns — a build, a REPL, a test
  runner — holds a duplicate of the listening socket, and the port stays
  bound for as long as ANY of them lives: stop the server with one still up
  and the next start fails with EADDRINUSE against a server that is gone.
  The same holds for an accepted connection: a child holding one keeps the
  client's connection open past the server's close. True when the flag is
  set, read back rather than assumed."
  [fd]
  (poller/c-fcntl fd f-setfd fd-cloexec)
  (cloexec? fd))

(def ^:private f-getfl 3)
(def ^:private f-setfl 4)
(def ^:private o-nonblock (if macos? 0x4 0x800))

(defn blocking!
  "Clear O_NONBLOCK (websocket takeover: the session runs on a plain thread
   via :run! and relies on blocking recv bounded by SO_RCVTIMEO). No-op for a
   socket that never went nonblocking."
  [fd]
  (let [flags (poller/c-fcntl fd f-getfl 0)]
    (poller/c-fcntl fd f-setfl (bit-and-not flags o-nonblock))))

;; struct addrinfo { int ai_flags; int ai_family; int ai_socktype;
;;   int ai_protocol; socklen_t ai_addrlen; struct sockaddr *ai_addr;
;;   char *ai_canonname; struct addrinfo *ai_next; } — identical layout on
;; macOS and Linux: the four ints at 0/4/8/12, socklen_t ai_addrlen at
;; 16 (8 bytes, 64-bit), then the pointers — ai_canonname at 24, ai_addr
;; at 32, ai_next at 40 — 48 bytes total. AI_PASSIVE=1, AF_UNSPEC=0.
(def ^:private addrinfo-size 48)
(def ^:private AI-PASSIVE 1)

(defn- gai-message [code]
  (or (try (ffi/ptr->string (c-gai-strerror code)) (catch Throwable _ nil)) ""))

(defn resolve-bind-sockaddrs
  "The bind candidates for host:port, as {:family :addr :addrlen} maps. The
   :addr buffers are OURS — the getaddrinfo chain is freed before returning,
   so nothing downstream holds a pointer into it — with the port already
   written in at bytes 2-3, which is sin_port/sin6_port in both families.
   Literals and names take the one getaddrinfo path; the caller frees each
   :addr buffer."
  [host port]
  (let [node (ffi/alloc (inc (count host)))
        hints (ffi/alloc addrinfo-size)
        resp  (ffi/alloc 8)]
    (try
      (dotimes [i (inc (count host))] (ffi/write node :uint8 0 i))
      (ffi/write-bytes node host)             ; zero-filled, so NUL-terminated
      (dotimes [i addrinfo-size] (ffi/write hints :uint8 0 i))
      (ffi/write hints :int AI-PASSIVE 0)
      (ffi/write hints :int 0 4)               ; AF_UNSPEC: every family
      (ffi/write hints :int SOCK-STREAM 8)
      (let [rc (c-getaddrinfo node ffi/null hints resp)]
        (if-not (zero? rc)
          (throw (ex-info (str "run-server: :host could not be resolved: "
                               (pr-str host) " — " (gai-message rc))
                          {:key :host :given host :gai-code rc}))
          (let [head (ffi/read resp :pointer)]
            (when (or (nil? head) (ffi/null? head))
              (throw (ex-info (str "run-server: :host resolved to no addresses: "
                                   (pr-str host))
                              {:key :host :given host :gai-code 0})))
            (loop [ai head out (transient [])]
              (if (or (nil? ai) (ffi/null? ai))
                (do (c-freeaddrinfo head)
                    (persistent! out))
                (let [family   (ffi/read ai :int 4)
                      addrlen  (ffi/read ai :int 16)
                      addr-ptr (ffi/read ai :pointer 32)
                      entry (when (and (or (= family af-inet) (= family af-inet6))
                                       (pos? addrlen)
                                       (not (ffi/null? addr-ptr)))
                              (let [addr (ffi/alloc addrlen)]
                                (dotimes [i addrlen]
                                  (ffi/write addr :uint8 (ffi/read addr-ptr :uint8 i) i))
                                (ffi/write addr :uint8 (bit-and (bit-shift-right port 8) 0xff) 2)
                                (ffi/write addr :uint8 (bit-and port 0xff) 3)
                                {:family family :addr addr :addrlen addrlen}))]
                  (recur (ffi/read ai :pointer 40)
                         (if entry (conj! out entry) out))))))))
      (finally (ffi/free node) (ffi/free hints) (ffi/free resp)))))

(defn peer-ip
  "The presentation-form peer address out of a sockaddr accept() filled in —
   dotted quad for v4, inet_ntop for v6 (RFC 5952 compression included; a
   scope zone is not, so a link-local peer prints as fe80::1). The Ring
   :remote-addr used to be the literal \"127.0.0.1\" whatever the peer was,
   which is not something rate limiting or an access log can work from."
  [sa]
  (let [family (if macos?
                 (ffi/read sa :uint8 1)
                 (bit-or (ffi/read sa :uint8 0)
                         (bit-shift-left (ffi/read sa :uint8 1) 8)))]
    (if (= family af-inet6)
      (let [out (ffi/alloc 46)]
        (try
          (let [p (c-inet-ntop af-inet6 (+ sa 8) out 46)]
            (if (or (nil? p) (ffi/null? p))
              "::"
              (ffi/ptr->string out)))
          (finally (ffi/free out))))
      (str/join "." (map #(ffi/read sa :uint8 (+ 4 %)) (range 4))))))

;; sockaddr_storage (128 bytes) rather than sockaddr_in: one accept() buffer
;; holds either family; accept() wants the capacity in/out through a
;; socklen_t pointer, and writes back what it used.
(def sockaddr-size 128)

(defn alloc-peer-sockaddr
  "A zeroed sockaddr_storage plus its socklen_t, ready for accept()."
  []
  (let [sa (ffi/alloc sockaddr-size)
        len (ffi/alloc 4)]
    (dotimes [i sockaddr-size] (ffi/write sa :uint8 0 i))
    (ffi/write len :int sockaddr-size 0)
    [sa len]))

(defn- setsockopt-flag!
  "Set one boolean socket option, or throw with the errno behind it. The
   2-arity names the protocol level: every option but IPV6_V6ONLY lives at
   SOL_SOCKET."
  ([fd opt-name opt-const]
   (setsockopt-flag! fd sol-socket opt-name opt-const))
  ([fd level opt-name opt-const]
   (let [opt (ffi/alloc 4)]
     (ffi/write opt :int 1 0)
     (try
       (when (neg? (c-setsockopt fd level opt-const opt 4))
         (let [e (errno-info)]
           (c-close fd)
           (throw (ex-info (str "setsockopt(" opt-name ") failed: " (:strerror e))
                           (assoc e :syscall "setsockopt" :option opt-name)))))
       (finally (ffi/free opt))))))

(defn local-port
  "The port fd is bound to — what the kernel picked when bind() was asked for
  port 0."
  [fd]
  (let [sa (ffi/alloc sockaddr-size)
        len (ffi/alloc 4)]
    (try
      (dotimes [i sockaddr-size] (ffi/write sa :uint8 0 i))
      (ffi/write len :int sockaddr-size 0)
      (when (neg? (c-getsockname fd sa len))
        (let [e (errno-info)]
          (throw (ex-info (str "getsockname() failed: " (:strerror e))
                          (assoc e :syscall "getsockname")))))
      (+ (* 256 (ffi/read sa :uint8 2)) (ffi/read sa :uint8 3))
      (finally (ffi/free sa) (ffi/free len)))))


(defn- bind-failure-ex
  "The bind() error as it should be reported: friendly for EADDRINUSE (name
   the port, say what is on it), errno + strerror otherwise."
  [host port errno strerror]
  (ex-info
   (if (= errno eaddrinuse)
     (str "port " port " is already in use — another process is "
          "listening on " (pr-str host) ":" port " (" strerror
          ", errno " errno "); stop it or pass a different :port")
     (str "bind() failed on " (pr-str host) ":" port ": " strerror
          " (errno " errno ")"))
   (cond-> {:errno errno :strerror strerror :syscall "bind" :port port}
     (= errno eaddrinuse) (assoc :errno-name "EADDRINUSE"))))

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
   EADDRINUSE exists to report."
  ([host port] (listen-sockets host port nil))
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
               fd (c-socket family SOCK-STREAM 0)]
           (if (neg? fd)
             (let [e (errno-info)]
               (doseq [f opened] (c-shutdown f 2) (c-close f))
               (doseq [{a :addr} (rest remaining)] (ffi/free a))
               (ffi/free addr)
               (throw (ex-info (str "socket() failed: " (:strerror e))
                               (assoc e :syscall "socket"))))
             (let [abandon! (fn []
                              (doseq [f opened] (c-shutdown f 2) (c-close f))
                              (doseq [{a :addr} (rest remaining)] (ffi/free a)))
                   target (or chosen port)]
               (when chosen
                 ;; the first bind with port 0 picked for the whole set;
                 ;; every later candidate binds that explicit port
                 (ffi/write addr :uint8 (bit-and (bit-shift-right chosen 8) 0xff) 2)
                 (ffi/write addr :uint8 (bit-and chosen 0xff) 3))
               (let [outcome
                     (try
                       ;; SO_REUSEPORT before bind, or it does not apply to it:
                       ;; several processes then bind the same port and the
                       ;; kernel spreads new connections over them (Linux).
                       ;; Opt-in, because without it a second bind SHOULD fail —
                       ;; that error is how you find out a server is already
                       ;; running.
                       (try (close-on-exec! fd) (catch Throwable _ nil))
                       (setsockopt-flag! fd "SO_REUSEADDR" so-reuse)
                       (when reuse-port?
                         (setsockopt-flag! fd "SO_REUSEPORT" so-reuseport))
                       (when (= family af-inet6)
                         (setsockopt-flag! fd IPPROTO-IPV6 "IPV6_V6ONLY" ipv6-v6only))
                       (when (neg? (c-bind fd addr addrlen))
                         (let [e (errno-info)]
                           (throw (ex-info "bind" e))))
                       ;; 511, as Igropyr uses (http.sc:1899): the backlog is
                       ;; what absorbs an accept burst while the acceptor is
                       ;; busy, and 64 is small enough that a modest connection
                       ;; storm gets refused rather than queued.
                       (when (neg? (c-listen fd 511))
                         (let [e (errno-info)]
                           (throw (ex-info "listen" (assoc e :syscall "listen")))))
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
                       (and (= errno eaddrnotavail) (seq (rest remaining)))
                       (do (c-close fd)
                           (ffi/free addr)
                           (recur (rest remaining) opened chosen))

                       :else
                       (do (abandon!)
                           (c-close fd)
                           (ffi/free addr)
                           (throw (if (map? (ex-data outcome))
                                    (bind-failure-ex host target
                                                     (get (ex-data outcome) :errno -1)
                                                     (get (ex-data outcome) :strerror ""))
                                    outcome)))))))))))))))

(def bufsize 65536)

(def ^:private MSG-PEEK
  "recv(2)'s MSG_PEEK — 0x2 on Linux, macOS and the BSDs alike."
  0x2)

(defn peer-gone?
  "True when the far end of fd has closed. Consumes nothing if it has not.

  This is what lets a response that is streaming notice a client that walked
  away while the application had nothing to send: without it, a stream only
  finds out at its next write, and a stream whose application is quiet never
  finds out at all.

  poll(2) with a zero timeout answers immediately. A hangup or error is
  conclusive; readability is not, because a peer may equally have pipelined
  its next request, so readability is settled by a PEEKING recv — end of
  stream reports 0, and real bytes stay queued for the reader they belong to.
  EINTR is not an answer either way, so it asks again."
  [fd]
  (let [pfds (ffi/alloc pollfd-size)
        buf  (ffi/alloc 1)]
    (try
      (init-pollfd! pfds fd POLLIN)
      (loop []
        (let [rc (c-poll pfds 1 0)]
          (cond
            (zero? rc) false
            (neg? rc)  (if (poller/eintr?) (recur) true)

            (pos? (bit-and (pollfd-revents pfds)
                           (bit-or POLLERR POLLHUP POLLNVAL)))
            true

            (pos? (bit-and (pollfd-revents pfds) POLLIN))
            (let [n (c-recv fd buf 1 MSG-PEEK)]
              (cond
                (zero? n) true                              ; orderly shutdown
                (pos? n)  false                             ; the peer is talking
                :else     (if (poller/eintr?) (recur) false)))

            :else false)))
      (finally
        (ffi/free pfds)
        (ffi/free buf)))))
