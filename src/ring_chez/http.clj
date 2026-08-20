(ns ring-chez.http
  "HTTP/1.1 codec over a socket fd: read-request (bounded accumulation),
   request->ring, response formatting, keep-alive parsing, and the response
   send path (send-all / send-response). Strategy-agnostic — the blocking
   behavior of reads and writes is injected by ring-chez.adapter."
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [ring-chez.socket :as socket]
            [jolt.io-poller :as poller]
            [jolt.ffi :as ffi]))

(defn- head-lines
  "The head's header lines, request line dropped. Hand-rolled rather than
   str/split — this runs on every request, and the regex split cost ~8x the
   whole rest of the framing decision."
  [head]
  (let [n (count head)]
    (loop [i (if-let [b (str/index-of head "\r\n")] (+ b 2) n)
           out []]
      (if (>= i n)
        out
        (let [e (or (str/index-of head "\r\n" i) n)]
          (recur (+ e 2) (conj out (subs head i e))))))))

(defn- framing-headers
  "The head's Content-Length / Transfer-Encoding values, keyed by field name.
   Reading the field name off each line — rather than scanning the whole head
   for \"content-length:\" — keeps a value like \"X-Note: content-length: 9\"
   from being mistaken for framing."
  [head]
  (reduce (fn [m line]
            (let [i (str/index-of line ":")]
              (if-not (and i (pos? i))
                m
                (let [k (str/lower-case (str/trim (subs line 0 i)))]
                  (if (or (= k "content-length") (= k "transfer-encoding"))
                    (update m k (fnil conj []) (str/trim (subs line (inc i))))
                    m)))))
          {} (head-lines head)))

(defn- body-framing
  "Octets of body the head declares: 0 when it carries no Content-Length,
   :bad when the framing is unrecoverable (a Content-Length that is not a
   single non-negative integer — RFC 7230 §3.3.3), :unsupported when it
   declares a Transfer-Encoding. Chunked decoding is not implemented, and
   framing such a request by its Content-Length (or as bodyless) would let
   the undecoded body through as a second, forged request."
  [head]
  (let [named (framing-headers head)
        cls (get named "content-length")
        lens (map #(let [n (parse-long %)] (when (and n (not (neg? n))) n)) cls)]
    (cond
      (contains? named "transfer-encoding") :unsupported
      (empty? cls) 0
      (some nil? lens) :bad
      (apply = lens) (first lens)
      :else :bad)))

(def no-bytes (byte-array 0))

(defn- append-bytes
  "acc plus the first n bytes sitting in the FFI buffer buf."
  [^bytes acc buf n]
  (let [have (alength acc)]
    (if (zero? have)
      (ffi/read-array buf n)
      (let [out (byte-array (+ have n))]
        (System/arraycopy acc 0 out 0 have)
        (System/arraycopy (ffi/read-array buf n) 0 out have n)
        out))))

(defn- head-end
  "Index of the \r\n\r\n that ends the request head, or nil. from skips the
  prefix an earlier pass already scanned (callers back it off by 3 so a
  terminator straddling the seam is still seen)."
  [^bytes bs from]
  (let [last-start (- (alength bs) 4)]
    (loop [i (max 0 from)]
      (when (<= i last-start)
        (if (and (= 13 (aget bs i))       (= 10 (aget bs (+ i 1)))
                 (= 13 (aget bs (+ i 2))) (= 10 (aget bs (+ i 3))))
          i
          (recur (inc i)))))))

;; read one complete request from conn; acc carries unconsumed bytes from a
;; previous read (pipelined requests). Accumulation is raw octets, not a
;; decoded string: Content-Length counts octets, and a multibyte codepoint
;; may straddle two recv calls — decoding each chunk on its own would both
;; mis-frame the body and corrupt it. recv! abstracts the blocking read:
;; the threads strategy passes plain c-recv, the fiber strategy passes a
;; poller-parking variant. idle-recv! handles the first read of the next
;; request on an idle keep-alive connection — under the threads strategy it
;; waits in poll(2) slices so a queued connection can retire the idle one
;; promptly instead of waiting out the full keep-alive timeout. Returns
;; {:text t :leftover bs} when a full request (headers + Content-Length body)
;; is available, :closed when the peer went away (or recv timed out) before
;; sending anything, :bad on EOF/timeout mid-request or an unframeable head,
;; :unsupported for a Transfer-Encoding we do not decode.
(defn read-request
  "Reads one request (head + content-length body). Accumulation is capped at
  max-bytes — a client that never terminates (or ships an oversized request)
  gets :too-big instead of exhausting memory. acc and :leftover are byte
  arrays; :text is the framed request decoded as UTF-8."
  [conn acc max-bytes recv! idle-recv!]
  (let [buf (ffi/alloc socket/bufsize)]
    (try
      ;; scanned: how far into acc the head-terminator search already ran.
      ;; frame: the whole request's length in octets, resolved once the head
      ;; is complete (not re-parsed on every trickle of the body).
      (loop [^bytes acc acc, scanned 0, frame nil]
        (let [have  (alength acc)
              frame (or frame
                        (when-let [he (head-end acc scanned)]
                          (let [f (body-framing (String. acc 0 he "UTF-8"))]
                            (if (number? f) (+ he 4 f) f))))]
          (cond
            (keyword? frame) frame          ; :bad -> 400, :unsupported -> 501

            (nil? frame)                    ; head still incomplete
            (if (> have max-bytes)
              :headers-too-big              ; headers never ended -> 431
              (let [n ((if (zero? have) idle-recv! recv!) conn buf)]
                (cond
                  (pos? n) (recur (append-bytes acc buf n) (max 0 (- have 3)) nil)
                  (zero? have) :closed
                  :else :bad)))

            (> frame max-bytes) :too-big    ; declared request exceeds the cap -> 413

            (>= have frame)
            {:text (String. acc 0 frame "UTF-8")
             :leftover (if (= have frame)
                         no-bytes
                         (java.util.Arrays/copyOfRange acc frame have))}

            :else
            (let [n (recv! conn buf)]
              (if (pos? n)
                (recur (append-bytes acc buf n) scanned frame)
                :bad)))))
      (finally (ffi/free buf)))))

;; --- request -> Ring map ----------------------------------------------------
(defn request->ring
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

(defn response->string
  ([resp] (response->string resp false))
  ([resp keep-alive?]
   (let [body (body->string (:body resp))
          ;; Content-Length is the body's octet count. ring-defaults'
          ;; wrap-content-length already sets it (as UTF-8 bytes); honor that
          ;; and only compute when absent, so we never stamp a second, conflicting
          ;; Content-Length. Middleware commonly sets it as a *string* —
          ;; normalize to a number, or head->string emits no framing at all
          ;; and keep-alive clients hang waiting for a body terminator.
          len (or (->> (:headers resp)
                       (some (fn [[k v]]
                               (let [kn (str/lower-case (if (keyword? k) (name k) (str k)))]
                                 (when (= kn "content-length")
                                   (if (string? v) (parse-long v) v))))))
                  (alength (.getBytes body "UTF-8")))]
     (str (head->string resp keep-alive? len) body))))

;; Connection headers are comma-separated token lists, case-insensitive
;; (RFC 7230 §6.1): "Keep-Alive, Close" means close.
(defn- header-tokens [v]
  (map str/trim (str/split (or v "") #",")))

(defn- conn-token? [tok v]
  (some #(= tok (str/lower-case %)) (header-tokens v)))

(defn keep-alive? [req]
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

(defn send-all
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
                (let [sent (socket/c-send conn (+ buf off) (- n off) 0)]
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

(defn send-response
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
