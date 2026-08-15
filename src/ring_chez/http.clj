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
(defn read-request
  "Reads one request (head + content-length body). Accumulation is capped at
  max-bytes — a client that never terminates (or ships an oversized request)
  gets :too-big instead of exhausting memory."
  [conn acc max-bytes recv! idle-recv!]
  (let [buf (ffi/alloc socket/bufsize)]
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
