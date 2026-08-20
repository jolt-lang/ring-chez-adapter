(ns ring-chez.http
  "HTTP/1.1 codec over a socket fd: read-request (bounded accumulation),
   request->ring, response formatting, keep-alive parsing, and the response
   send path (send-all / send-response). Strategy-agnostic — the blocking
   behavior of reads and writes is injected by ring-chez.adapter."
  (:require [clojure.string :as str]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
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

(defn- ensure-capacity
  "acc, with room for n more bytes past have. Grows by DOUBLING: a body that
  arrives over many reads is then copied an amortised constant number of times
  per byte, not once per read — reallocating to fit each chunk made receiving a
  10 MB upload move ~800 MB. limit caps the growth at the largest capacity that
  could ever be useful, so a declared Content-Length never becomes a memory
  reservation the client has not paid for yet."
  [^bytes acc have n limit]
  (let [cap (alength acc)
        need (+ have n)]
    (if (<= need cap)
      acc
      (let [out (byte-array (max need (min (* 2 cap) limit)))]
        (System/arraycopy acc 0 out 0 have)
        out))))

(defn- fill!
  "Copy the first n bytes of the FFI buffer buf into acc at off."
  [^bytes acc off buf n]
  ;; jolt.ffi/read-array allocates the chunk and we copy it in; jolt.ffi's
  ;; read-into! (unreleased) reads straight into acc and drops this copy.
  (System/arraycopy (ffi/read-array buf n) 0 acc off n))

(defn- concat-bytes
  "One byte array from several."
  [arrays]
  (let [total (loop [as (seq arrays), n 0]
                (if as (recur (next as) (+ n (alength ^bytes (first as)))) n))
        out (byte-array total)]
    (loop [as (seq arrays), off 0]
      (if-not as
        out
        (let [^bytes a (first as)]
          (System/arraycopy a 0 out off (alength a))
          (recur (next as) (+ off (alength a))))))))

(defn- head-end
  "Index of the \r\n\r\n that ends the request head within bs[0,have), or nil.
  from skips the prefix an earlier pass already scanned (callers back it off by
  3 so a terminator straddling the seam is still seen)."
  [^bytes bs from have]
  (let [last-start (- have 4)]
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
;; {:head s :body bs :leftover bs} when a full request (headers +
;; Content-Length body) is available, :closed when the peer went away (or
;; recv timed out) before sending anything, :bad on EOF/timeout mid-request
;; or an unframeable head, :unsupported for a Transfer-Encoding we do not
;; decode.
(defn read-request
  "Reads one request (head + content-length body). Accumulation is capped at
  max-bytes — a client that never terminates (or ships an oversized request)
  gets :too-big instead of exhausting memory. acc, :body and :leftover are
  byte arrays; only :head — which RFC 7230 restricts to ASCII — is decoded.
  The body stays opaque octets: it may be an image, a gzip stream, or text in
  some other charset, none of which survive a UTF-8 decode."
  [conn acc max-bytes recv! idle-recv!]
  (let [buf (ffi/alloc socket/bufsize)]
    (try
      ;; acc is a capacity buffer, valid over [0,have) — it is not sliced to
      ;; size until the request is framed. scanned: how far into it the
      ;; head-terminator search already ran. framing: {:head s :from i :to i} —
      ;; where the body starts and ends — resolved once the head is complete,
      ;; so the head is neither re-scanned nor re-parsed on every trickle of
      ;; the body. A read can overshoot :to (the next pipelined request rode
      ;; along), which is why the buffer keeps room past it rather than being
      ;; sized to the frame exactly.
      (loop [^bytes acc acc, have (alength acc), scanned 0, framing nil]
        (let [framing (or framing
                          (when-let [he (head-end acc scanned have)]
                            (let [head (String. acc 0 he "UTF-8")
                                  f (body-framing head)]
                              (if (number? f)
                                {:head head :from (+ he 4) :to (+ he 4 f)}
                                f))))
              ;; map? not framing? — an unframeable head is a KEYWORD here, and
              ;; (:to :bad) is nil
              limit   (+ (if (map? framing) (:to framing) max-bytes) socket/bufsize)]
          (cond
            (keyword? framing) framing      ; :bad -> 400, :unsupported -> 501

            (nil? framing)                  ; head still incomplete
            (if (> have max-bytes)
              :headers-too-big              ; headers never ended -> 431
              (let [n ((if (zero? have) idle-recv! recv!) conn buf)]
                (cond
                  (pos? n) (let [acc (ensure-capacity acc have n limit)]
                             (fill! acc have buf n)
                             (recur acc (+ have n) (max 0 (- have 3)) nil))
                  (zero? have) :closed
                  :else :bad)))

            ;; declared request exceeds the cap -> 413
            (> (:to framing) max-bytes) :too-big

            (>= have (:to framing))
            (let [{:keys [head from to]} framing]
              {:head head
               :body (if (= from to) no-bytes (java.util.Arrays/copyOfRange acc from to))
               :leftover (if (= have to) no-bytes (java.util.Arrays/copyOfRange acc to have))})

            :else
            (let [n (recv! conn buf)]
              (if (pos? n)
                (let [acc (ensure-capacity acc have n limit)]
                  (fill! acc have buf n)
                  (recur acc (+ have n) scanned framing))
                :bad)))))
      (finally (ffi/free buf)))))

;; --- request -> Ring map ----------------------------------------------------
(defn request->ring
  "Parse a request head (as read-request framed it) and its body octets into
  {:request ring-map}, or {:error response} when the request is malformed
  (400) or speaks an unsupported HTTP version (505). HTTP/1.1 requests must
  carry a Host header (RFC 7230 §5.4)."
  [head ^bytes body port]
  (let [lines (str/split head #"\r\n")
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
                   ;; an InputStream per the Ring spec, over the body's own
                   ;; octets — a handler that wants text slurps it (UTF-8 by
                   ;; default), one that wants bytes reads them unmangled
                   :body           (when (pos? (alength body))
                                     (java.io.ByteArrayInputStream. body))}}))))

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

(defn- body->bytes
  "The octets a response body puts on the wire. Strings encode as UTF-8;
  byte arrays, InputStreams and Files pass through as their own bytes, so an
  image or a gzip stream is served as sent rather than mangled by a charset
  round-trip. A seq/vector body contributes each element's octets in turn —
  for the seq-of-strings Ring defines, that is the same as encoding the
  concatenation, since UTF-8 concatenates."
  [b]
  (cond
    (nil? b) no-bytes
    (string? b) (.getBytes ^String b "UTF-8")
    (bytes? b) b
    (or (seq? b) (vector? b)) (concat-bytes (mapv body->bytes b))
    ;; a File / InputStream / Reader body (ring's resource + file responses):
    ;; copy its contents rather than printing the object.
    :else (try (let [out (java.io.ByteArrayOutputStream.)]
                 (io/copy b out)
                 (.toByteArray out))
               (catch Throwable _ (.getBytes ^String (str b) "UTF-8")))))

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

(defn- declared-length
  "The Content-Length the handler's own headers carry, if any. ring-defaults'
  wrap-content-length already sets it (as UTF-8 bytes); honor that and only
  compute when absent, so we never stamp a second, conflicting
  Content-Length. Middleware commonly sets it as a *string* — normalize to a
  number, or head->string emits no framing at all and keep-alive clients hang
  waiting for a body terminator."
  [resp]
  (->> (:headers resp)
       (some (fn [[k v]]
               (let [kn (str/lower-case (if (keyword? k) (name k) (str k)))]
                 (when (= kn "content-length")
                   (if (string? v) (parse-long v) v)))))))

(defn response->parts
  "The whole response as send-all parts: the head, then the body's octets.
  Left as parts rather than spliced — send-all writes them back to back into
  one buffer, so a large body is not copied a second time just to be sent."
  ([resp] (response->parts resp false))
  ([resp keep-alive?]
   (let [body (body->bytes (:body resp))
         len (or (declared-length resp) (alength body))]
     [(head->string resp keep-alive? len) body])))

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

(defn- part-capacity [p]
  (if (string? p) (* 4 (count p)) (alength ^bytes p)))   ; UTF-8 worst case 4 bytes/char

(defn- write-part!
  "Encode p into buf at off; returns the octets written."
  [buf off p]
  (if (string? p)
    (ffi/write-bytes (+ buf off) p)
    (ffi/write-array (+ buf off) p)))

(defn send-all
  "Write data to conn: a string (encoded as UTF-8), a byte array, or a vector
  of those written back to back. false when the peer is gone (caller closes).
  wait-write! parks the caller until the socket can take more bytes (fiber
  strategy, O_NONBLOCK sockets); nil leaves -1 meaning peer-gone (blocking
  sockets block instead of returning EAGAIN)."
  ([conn data] (send-all conn data nil))
  ([conn data wait-write!]
   (let [parts (if (vector? data) data [data])
         buf (ffi/alloc (max 1 (reduce (fn [n p] (+ n (part-capacity p))) 0 parts)))
         n (reduce (fn [off p] (+ off (write-part! buf off p))) 0 parts)
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

(defn- chunk->bytes
  "One stream chunk's octets. Chunks are strings or byte arrays — anything
  else is a handler bug, and throwing hands it to the worker's catch (which
  abandons the connection) instead of serializing garbage into the stream."
  [v]
  (cond
    (string? v) (.getBytes ^String v "UTF-8")
    (bytes? v) v
    :else (throw (ex-info "stream chunk must be a string or byte array"
                          {:type :ring-chez/bad-chunk :chunk-type (type v)}))))

(defn- chunk-parts
  "bs wrapped in chunked framing — the size line counts octets, so a
  multibyte or binary chunk is framed by what actually goes on the wire."
  [^bytes bs]
  [(str (format "%x" (alength bs)) "\r\n") bs "\r\n"])

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
    (let [v (take! ch)
          bs (when (some? v) (chunk->bytes v))]
      (cond
        ;; closed, or an empty chunk: end of stream. (an empty chunk carries no
        ;; data and would frame as a bogus terminator)
        (or (nil? bs) (zero? (alength bs)))
        (if http10? true (send! conn "0\r\n\r\n"))

        (send! conn (if http10? bs (chunk-parts bs)))
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
      (and (send! conn (response->parts resp keep?)) keep?))))
