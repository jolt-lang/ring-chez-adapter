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

(def POLLIN  0x001)

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

(defn listen-socket [port]
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
    (when (neg? (c-listen fd 64))
      (let [e (errno-info)]
        (c-close fd)
        (throw (ex-info (str "listen() failed: " (:strerror e))
                        (assoc e :syscall "listen")))))
    fd))

(def bufsize 65536)
