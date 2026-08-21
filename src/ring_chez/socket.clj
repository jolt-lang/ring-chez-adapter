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

(def POLLIN  0x001)
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
  (dotimes [i pollfd-size] (ffi/write pfds :uint8 i 0))
  (ffi/write pfds :int 0 fd)
  (ffi/write pfds :uint8 4 (bit-and 0xff events))
  (ffi/write pfds :uint8 5 (bit-and 0xff (bit-shift-right events 8))))

(defn pollfd-revents
  "What poll actually reported. Acting on the return count alone treats a wake
  for any reason as \"readable\"."
  [pfds]
  (bit-or (ffi/read pfds :uint8 6)
          (bit-shift-left (ffi/read pfds :uint8 7) 8)))

(def ^:private AF-INET 2)
(def ^:private SOCK-STREAM 1)
(def ^:private macos?
  (str/includes? (str/lower-case (or (System/getProperty "os.name") "")) "mac"))
;; SOL_SOCKET / SO_REUSEADDR differ by platform: macOS 0xffff / 4, Linux 1 / 2.
(def ^:private sol-socket  (if macos? 0xffff 1))
(def ^:private so-reuse    (if macos? 4 2))
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
  (dotimes [i 16] (ffi/write tv :uint8 i 0))
  (ffi/write tv :uint64 0 (quot ms 1000))
  (let [usec (* 1000 (rem ms 1000))]
    (if macos?
      (ffi/write tv :uint 8 usec)
      (ffi/write tv :uint64 8 usec))))

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

;; sockaddr_in is 16 bytes: family, port (network order) at 2-3, address at
;; 4-7. macOS: byte0 = sin_len (16), byte1 = family; Linux: bytes0-1 = family
;; (little-endian, so byte0 = AF_INET).
(defn- write-family! [sa]
  (if macos?
    (do (ffi/write sa :uint8 0 16) (ffi/write sa :uint8 1 AF-INET))
    (ffi/write sa :uint8 0 AF-INET)))

(defn ipv4->octets
  "The four octets of a dotted-quad host, or nil if the platform's inet_pton
  does not accept it. Parsing is not hand-rolled: inet_pton is what the rest of
  the sockets API uses, so :host takes exactly the forms the OS takes."
  [host]
  (when (and (string? host) (pos? (count host)))
    (let [src (ffi/alloc (inc (count host)))
          dst (ffi/alloc 4)]
      (try
        (dotimes [i (inc (count host))] (ffi/write src :uint8 i 0))
        (ffi/write-bytes src host)          ; the zero fill leaves it NUL-terminated
        (dotimes [i 4] (ffi/write dst :uint8 i 0))
        (when (= 1 (c-inet-pton AF-INET src dst))
          (mapv #(ffi/read dst :uint8 %) (range 4)))
        (finally (ffi/free src) (ffi/free dst))))))

(defn- make-sockaddr [host port]
  (let [octets (or (ipv4->octets host)
                   (throw (ex-info (str "run-server: :host must be an IPv4 address"
                                        " (e.g. \"127.0.0.1\" or \"0.0.0.0\"), got "
                                        (pr-str host))
                                   {:key :host :given host
                                    :expected "an IPv4 address"})))
        sa (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write sa :uint8 i 0))
    (write-family! sa)
    (ffi/write sa :uint8 2 (bit-and (bit-shift-right port 8) 0xff))   ; port hi (network order)
    (ffi/write sa :uint8 3 (bit-and port 0xff))                       ; port lo
    (dotimes [i 4] (ffi/write sa :uint8 (+ 4 i) (nth octets i)))
    sa))

(defn peer-ip
  "The dotted-quad address out of a sockaddr_in accept() filled in. The Ring
  :remote-addr used to be the literal \"127.0.0.1\" whatever the peer was,
  which is not something rate limiting or an access log can work from."
  [sa]
  (str/join "." (map #(ffi/read sa :uint8 (+ 4 %)) (range 4))))

;; sockaddr_in is 16 bytes; accept() wants the capacity in/out through a
;; socklen_t pointer, and writes back what it used.
(def sockaddr-size 16)

(defn alloc-peer-sockaddr
  "A zeroed sockaddr_in plus its socklen_t, ready for accept()."
  []
  (let [sa (ffi/alloc sockaddr-size)
        len (ffi/alloc 4)]
    (dotimes [i sockaddr-size] (ffi/write sa :uint8 i 0))
    (ffi/write len :int 0 sockaddr-size)
    [sa len]))

(defn listen-socket [host port]
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
    (let [sa (make-sockaddr host port)]
      (when (neg? (c-bind fd sa 16))
        (let [e (errno-info)
              in-use? (= (if macos? 48 98) (:errno e))]
          (c-close fd) (ffi/free sa)
          (throw (ex-info (if in-use?
                            (str "port " port " is already in use — another process is "
                                 "listening on 127.0.0.1:" port " (" (:strerror e)
                                 ", errno " (:errno e) "); stop it or pass a different :port")
                            (str "bind() failed on port " port ": " (:strerror e)
                                 " (errno " (:errno e) ")"))
                          (cond-> (assoc e :syscall "bind" :port port)
                            in-use? (assoc :errno-name "EADDRINUSE"))))))
      (ffi/free sa))
    ;; 511, as Igropyr uses (http.sc:1899): the backlog is what absorbs an
    ;; accept burst while the acceptor is busy, and 64 is small enough that a
    ;; modest connection storm gets refused rather than queued.
    (when (neg? (c-listen fd 511))
      (let [e (errno-info)]
        (c-close fd)
        (throw (ex-info (str "listen() failed: " (:strerror e))
                        (assoc e :syscall "listen")))))
    fd))

(def bufsize 65536)
