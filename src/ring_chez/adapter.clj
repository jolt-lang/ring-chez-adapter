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
;; previous read (pipelined requests). Returns {:text t :leftover s} when a
;; full request (headers + Content-Length body) is available, :closed when the
;; peer went away (or recv timed out) before sending anything, :bad on
;; EOF/timeout mid-request.
(defn- read-request
  "Reads one request (head + content-length body). Accumulation is capped at
  max-bytes — a client that never terminates (or ships an oversized request)
  gets :too-big instead of exhausting memory."
  [conn acc max-bytes]
  (let [buf (ffi/alloc bufsize)]
    (try
      (loop [acc acc]
        (if-let [hdr-end (str/index-of acc "\r\n\r\n")]
          (let [cl (content-length acc hdr-end)]
            (if (>= (- (count acc) (+ hdr-end 4)) cl)
              {:text (subs acc 0 (+ hdr-end 4 cl))
               :leftover (subs acc (+ hdr-end 4 cl))}
              (if (> (+ (count acc) cl) max-bytes)
                :too-big
                (let [n (c-recv conn buf bufsize 0)]
                  (if (pos? n)
                    (recur (str acc (ffi/read-bytes buf n)))
                    :bad)))))
          (if (> (count acc) max-bytes)
            :too-big
            (let [n (c-recv conn buf bufsize 0)]
              (cond
                (pos? n) (recur (str acc (ffi/read-bytes buf n)))
                (str/blank? acc) :closed
                :else :bad)))))
      (finally (ffi/free buf)))))

;; --- request -> Ring map ----------------------------------------------------
(defn- request->ring [text port]
  (let [blank (str/index-of text "\r\n\r\n")
        head (if blank (subs text 0 blank) text)
        body (if blank (subs text (+ blank 4)) "")
        lines (str/split head #"\r\n")
        parts (str/split (or (first lines) "GET / HTTP/1.1") #" ")
        method (or (first parts) "GET")
        target (or (second parts) "/")
        proto  (or (nth parts 2 "HTTP/1.1") "HTTP/1.1")
        qi (str/index-of target "?")
        [uri qs] (if qi [(subs target 0 qi) (subs target (inc qi))] [target nil])
        headers (reduce (fn [m line]
                          (let [i (str/index-of line ":")]
                            (if (and i (pos? i))
                              (assoc m (str/lower-case (str/trim (subs line 0 i))) (str/trim (subs line (inc i))))
                              m)))
                        {} (rest lines))]
    {:server-port    port
     :server-name    "127.0.0.1"
     :remote-addr    "127.0.0.1"
     :uri            uri
     :query-string   qs
     :scheme         :http
      :request-method (keyword (str/lower-case method))
      :protocol       proto
     :headers        headers
     :body           (when (pos? (count body)) (java.io.StringReader. body))}))

;; --- Ring response -> the response string -----------------------------------
(def ^:private status-text
  {200 "OK" 201 "Created" 204 "No Content" 301 "Moved Permanently" 302 "Found"
   303 "See Other" 304 "Not Modified" 400 "Bad Request" 401 "Unauthorized"
   403 "Forbidden" 404 "Not Found" 405 "Method Not Allowed" 500 "Internal Server Error"})

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
    (.append sb (str "HTTP/1.1 " status " " (get status-text status "OK") "\r\n"))
    (doseq [[k v] (:headers resp)]
      (let [kn (str/lower-case (if (keyword? k) (name k) (str k)))]
        (when (and (not= kn "content-length") (not= kn "transfer-encoding"))
          (.append sb (str (if (keyword? k) (name k) (str k)) ": " v "\r\n")))))
    (cond (number? framing) (.append sb (str "Content-Length: " framing "\r\n"))
          (= :chunked framing) (.append sb "Transfer-Encoding: chunked\r\n"))
    (.append sb (str "Connection: " (if keep-alive? "keep-alive" "close") "\r\n\r\n"))
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

(defn- keep-alive? [req]
  (let [c (get-in req [:headers "connection"])]
    (if (= "HTTP/1.0" (:protocol req))
      (= "keep-alive" c)
      (not= "close" c))))

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
  HTTP/1.0 (close-delimited; caller closes). True when the stream finished
  cleanly (terminator sent); false when the client went away — the channel is
  then closed so a parked producer's put returns false instead of hanging."
  [conn ch http10?]
  (loop []
    (let [v (async/<!! ch)]
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
  [conn req resp]
  (let [keep?     (keep-alive? req)
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
            (stream-body conn ch true)
            false)
        (and (send-all conn (head->string resp keep? :chunked))
             (stream-body conn ch false)
             keep?))

      bodyless?
      (and (send-all conn (head->string resp keep? :none)) keep?)

      :else
      (and (send-all conn (response->string resp keep?)) keep?))))

;; --- the accept loop --------------------------------------------------------
;; Clean shutdown: stop-server closes the listen fd (which unblocks accept) and
;; clears `running?`; the loop then exits instead of spinning on the dead fd.
(defn- serve-loop [listen-fd running? work]
  (loop []
    (let [conn (c-accept listen-fd ffi/null ffi/null)]
      (cond
        (not @running?) nil
        (neg? conn) (when @running? (recur))
        :else
        (do (async/>!! work conn)
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

(defn- connection-loop [conn handler port ka-ms ws-handler max-bytes]
  (set-rcvtimeo! conn ka-ms)
  (loop [acc ""]
    (let [r (read-request conn acc max-bytes)]
      (cond
        (= :bad r) (send-all conn (response->string
                                    {:status 400 :headers {"Content-Type" "text/plain"}
                                     :body "Bad Request"} false))
        (= :too-big r) (send-all conn (response->string
                                       {:status 413 :headers {"Content-Type" "text/plain"
                                                              "Connection" "close"}
                                        :body "Payload Too Large"} false))
        (map? r)
        (let [req (request->ring (:text r) port)]
          (if (and ws-handler (upgrade-request? req))
            ;; websocket takeover: 101, then the session owns the fd until it
            ;; returns; the connection is not reused afterwards
            (when (send-all conn (str "HTTP/1.1 101 Switching Protocols\r\n"
                                      "Upgrade: websocket\r\n"
                                      "Connection: Upgrade\r\n"
                                      "Sec-WebSocket-Accept: "
                                      (ws/accept-token (get-in req [:headers "sec-websocket-key"]))
                                      "\r\n\r\n"))
              (try (ws-handler (ws/make-session conn))
                   (catch Throwable _ nil)))
            (let [resp (try (handler req)
                            (catch Throwable _
                              {:status 500 :headers {"Content-Type" "text/plain"}
                               :body "Internal Server Error"}))]
              (when (send-response conn req resp)
                (recur (:leftover r))))))
        :else nil))))

(defn- worker [handler port ka-ms ws-handler max-bytes work]
  (loop []
    (when-let [conn (async/<!! work)]
      (connection-loop conn handler port ka-ms ws-handler max-bytes)
      (c-close conn)
      (recur))))

(defn run-server
  "Start the server; return a handle {:socket :port :running}. The accept loop
  runs on a background thread; connections are served by a fixed worker pool
  (:worker-threads, default = available processors) so slow handlers do not
  serialize. opts: :port (default 3000), :worker-threads, :keep-alive-timeout-ms
  (default 30000), :ws-handler — fn of a ring-chez.websocket Session, run on
  the worker thread when a websocket upgrade request arrives."
  [handler opts]
  (let [port   (get opts :port 3000)
        n      (get opts :worker-threads (.availableProcessors (Runtime/getRuntime)))
        ka-ms  (get opts :keep-alive-timeout-ms 30000)
        ws-handler (get opts :ws-handler)
        max-bytes (get opts :max-request-bytes 1048576)
        fd     (listen-socket port)
        running? (atom true)
        work (async/chan)]   ; unbuffered: acceptor parks when all workers busy;
    (dotimes [_ n] (async/thread (worker handler port ka-ms ws-handler max-bytes work)))
    (future (serve-loop fd running? work))
    {:socket fd :port port :running running? :work work}))

(defn stop-server
  "Stop the server: unblock + exit the accept loop and close the listen socket."
  [server]
  (reset! (:running server) false)
  (async/close! (:work server))
  (c-close (:socket server))
  nil)
