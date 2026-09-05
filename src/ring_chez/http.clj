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

(defn- tchar?
  "RFC 7230 §3.2.6 token character. A header name is a token and nothing else:
   a space before the colon (\"Host : t\") is not a fussy detail but a
   smuggling wedge, since parsers disagree about whether to trim it."
  [c]
  (or (<= (int \a) (int c) (int \z))
      (<= (int \A) (int c) (int \Z))
      (<= (int \0) (int c) (int \9))
      (contains? #{\! \# \$ \% \& \' \* \+ \- \. \^ \_ \` \| \~} c)))

(defn head-malformed?
  "True when the head breaks the rules a framing decision has to be able to
   rely on (Igropyr parse-headers, http.sc:401). Every line ends CRLF — a bare
   LF is the classic smuggling wedge, since this parser would split on it while
   a CRLF-strict proxy in front reads the same bytes as one folded value, and
   the two then disagree about which Content-Length the request has. A stray CR
   goes the same way, and would hand a control character to handlers and to
   token matchers besides. obs-fold continuation lines are gone from HTTP/1.1
   and are rejected rather than guessed at, and a header name must be a token.

   The head as read-request hands it over excludes the terminating CRLFCRLF, so
   the last line legitimately has no CRLF of its own."
  [head]
  (let [n (count head)]
    (or
      ;; no bare LF, no stray CR
      (loop [i 0]
        (cond
          (>= i n) false
          (= \newline (.charAt ^String head i))
          (if (and (pos? i) (= \return (.charAt ^String head (dec i)))) (recur (inc i)) true)
          (= \return (.charAt ^String head i))
          (if (and (< (inc i) n) (= \newline (.charAt ^String head (inc i)))) (recur (inc i)) true)
          :else (recur (inc i))))
      (boolean
        (some (fn [line]
                (let [i (str/index-of line ":")]
                  (or (zero? (count line))
                      (contains? #{\space \tab} (.charAt ^String line 0))   ; obs-fold
                      (not (and i (pos? i)))
                      (not (every? tchar? (subs line 0 i))))))
              (head-lines head))))))

(defn- head-version
  "The HTTP version token off the request line, or nil."
  [head]
  (let [e (or (str/index-of head "\r\n") (count head))
        line (subs head 0 e)
        i (str/last-index-of line " ")]
    (when (and i (< (inc i) (count line))) (subs line (inc i)))))

(defn- framing-headers
  "The head's Content-Length / Transfer-Encoding / Expect values, keyed by
   field name. Reading the field name off each line — rather than scanning the
   whole head for \"content-length:\" — keeps a value like \"X-Note:
   content-length: 9\" from being mistaken for framing."
  [head]
  (reduce (fn [m line]
            (let [i (str/index-of line ":")]
              (if-not (and i (pos? i))
                m
                (let [k (str/lower-case (str/trim (subs line 0 i)))]
                  (if (contains? #{"content-length" "transfer-encoding" "expect"} k)
                    (update m k (fnil conj []) (str/trim (subs line (inc i))))
                    m)))))
          {} (head-lines head)))

(defn- comma-parts [vals]
  (mapcat #(map str/trim (str/split % #"," -1)) vals))

(defn- all-digits? [s]
  (and (pos? (count s)) (every? #(<= (int \0) (int %) (int \9)) s)))

(defn- content-length-value
  "The declared body length: :absent, :bad, or a non-negative integer. Repeats
   coalesce into one comma-joined field, so a valid value is one or more
   IDENTICAL digit strings (RFC 7230 §3.3.2) — \"5, 5\" is legal, \"5, 7\" is
   not, and neither is \"+5\", which parse-long would otherwise accept."
  [vals]
  (let [parts (comma-parts vals)]
    (cond
      (empty? parts) :absent
      (not (every? all-digits? parts)) :bad
      :else (let [ns (map parse-long parts)]
              (if (apply = ns) (first ns) :bad)))))

(defn- transfer-encoding-value
  "The only transfer coding this server decodes is a single final \"chunked\".
   Anything else is refused rather than falling back to Content-Length and
   disagreeing with an upstream proxy about where the message ends."
  [vals]
  (if (empty? vals)
    :absent
    (if (= ["chunked"] (map str/lower-case (comma-parts vals))) :chunked :unsupported)))

(defn- expect-continue? [named]
  (boolean (some #(= "100-continue" (str/lower-case %)) (get named "expect"))))

(defn- body-framing
  "How the head says its body is delimited: a non-negative octet count, or
   :chunked, or :bad / :unsupported.

   :bad covers a head that is malformed at all (above), a Content-Length that
   is not one or more identical non-negative integers, a request that declares
   both Content-Length and Transfer-Encoding — two framings, and the peer at
   each end may believe a different one, which is the whole mechanism of
   request smuggling — and chunked on HTTP/1.0, which is HTTP/1.1 framing an
   HTTP/1.0 intermediary would read differently. :unsupported is a transfer
   coding this server does not implement (501, per RFC 9112 §7)."
  [head]
  (if (head-malformed? head)
    :bad
    (let [named (framing-headers head)
          te (transfer-encoding-value (get named "transfer-encoding"))
          cl (content-length-value (get named "content-length"))]
      (cond
        (= :bad cl) :bad
        (= :unsupported te) :unsupported
        (and (= :chunked te) (not= :absent cl)) :bad
        (and (= :chunked te) (= "HTTP/1.0" (head-version head))) :bad
        (= :chunked te) :chunked
        (= :absent cl) 0
        :else cl))))

(def no-bytes (byte-array 0))

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


;; --- chunked request bodies (Igropyr parse-chunked-body, http.sc:1428) --------
;; Bounds are what make this safe to run against a hostile peer: a chunk-size
;; line four kilobytes long is malformed however large the body may be, the
;; per-chunk overhead is capped so a body of one-byte chunks cannot cost more
;; framing than payload, and the chunk count is capped so the same body cannot
;; cost an unbounded number of allocations.
(def ^:private chunk-line-limit 4096)
(def ^:private chunk-overhead-limit 65536)
(def ^:private trailer-limit 8192)

(defn- chunk-count-limit [max-bytes] (max 16384 (quot max-bytes 64)))

;; A trailer may not carry a field that changes how the message was framed or
;; routed — it arrives after the decision has been made.
(def ^:private forbidden-trailer-fields
  #{"transfer-encoding" "content-length" "host" "connection" "trailer" "upgrade"})

(defn- crlf-index
  "Index of the CRLF at or after from, within bs[0,have), or nil."
  [^bytes bs from have]
  (loop [i from]
    (cond
      (>= (inc i) have) nil
      (and (= 13 (aget bs i)) (= 10 (aget bs (inc i)))) i
      :else (recur (inc i)))))

(defn- parse-chunk-size
  "The hex chunk size on bs[start,end), stopping at a ';' chunk extension;
   nil if malformed. Promoting arithmetic on purpose: an absurd hex size must
   grow into a bignum and be rejected by the body cap, not wrap a long."
  [^bytes bs start end]
  (loop [i start, v 0, any false]
    (if (= i end)
      (when any v)
      (let [b (bit-and 0xff (aget bs i))]
        (cond
          (= b 59) (when any v)                                  ; ';'
          (<= 48 b 57)  (recur (inc i) (+' (*' v 16) (- b 48)) true)
          (<= 97 b 102) (recur (inc i) (+' (*' v 16) (- b 87)) true)
          (<= 65 b 70)  (recur (inc i) (+' (*' v 16) (- b 55)) true)
          :else nil)))))

(defn- valid-trailer-line? [^bytes bs start end]
  (let [line (String. bs start (- end start) "UTF-8")
        i (str/index-of line ":")]
    (and i (pos? i)
         (every? tchar? (subs line 0 i))
         (not (contains? forbidden-trailer-fields (str/lower-case (subs line 0 i)))))))

(defn- parse-chunked-body
  "Try to decode a complete chunked body out of bs[body-start,have). state is
   nil for a fresh parse or the resume state from a previous :more — chunks
   already extracted are never re-parsed and never re-copied, so a body
   drip-fed in tiny segments costs each byte once instead of O(segments)
   rescans.

   -> [:done body end] | [:more state] | [:too-large] | [:bad]
    | [:trailers-too-large]"
  [^bytes bs body-start have state max-bytes]
  (loop [pos    (or (:pos state) body-start)
         chunks (or (:chunks state) [])
         len    (or (:len state) 0)
         cnt    (or (:count state) 0)]
    (let [eol (crlf-index bs pos have)
          resume {:pos pos :chunks chunks :len len :count cnt}]
      (cond
        (> (- pos body-start) (+ max-bytes chunk-overhead-limit)) [:too-large]
        (nil? eol) (if (> (- have pos) chunk-line-limit) [:too-large] [:more resume])
        (> (- eol pos) chunk-line-limit) [:too-large]
        :else
        (let [size (parse-chunk-size bs pos eol)]
          (cond
            (nil? size) [:bad]
            (> (+ len size) max-bytes) [:too-large]

            (zero? size)
            ;; the last chunk; optional trailers, then a blank line
            (let [trailer-start (+ eol 2)]
              (loop [p trailer-start]
                (let [e2 (crlf-index bs p have)]
                  (cond
                    (nil? e2) (if (> (- have trailer-start) trailer-limit)
                                [:trailers-too-large]
                                [:more resume])
                    (> (- (+ e2 2) trailer-start) trailer-limit) [:trailers-too-large]
                    (= e2 p) [:done (concat-bytes chunks) (+ p 2)]
                    (valid-trailer-line? bs p e2) (recur (+ e2 2))
                    :else [:bad]))))

            (>= cnt (chunk-count-limit max-bytes)) [:too-large]

            :else
            (let [dstart (+ eol 2)]
              (if (< have (+ dstart size 2))
                [:more resume]
                (if-not (and (= 13 (aget bs (+ dstart size)))
                             (= 10 (aget bs (+ dstart size 1))))
                  [:bad]
                  (recur (+ dstart size 2)
                         (conj chunks (java.util.Arrays/copyOfRange bs dstart (+ dstart size)))
                         (+ len size)
                         (inc cnt)))))))))))

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
(defn- resolve-framing
  "The framing decision for a complete head: {:head :from :to} for a
  Content-Length body, {:head :from :chunked? true} for a chunked one, or the
  keyword body-framing refused it with. :expect? marks a body the client is
  waiting for a 100 Continue before sending."
  [^bytes acc scanned have]
  (when-let [he (head-end acc scanned have)]
    (let [head (String. acc 0 he "UTF-8")
          f (body-framing head)
          from (+ he 4)
          expect? (expect-continue? (framing-headers head))]
      (cond
        (= :chunked f) {:head head :from from :chunked? true :expect? expect?}
        (number? f) {:head head :from from :to (+ from f)
                     :expect? (and expect? (pos? f))}
        :else f))))

(defn read-request
  "Reads one request (head + body). Accumulation is capped at max-bytes — a
  client that never terminates (or ships an oversized request) gets :too-big
  instead of exhausting memory. acc, :body and :leftover are byte arrays; only
  :head — which RFC 7230 restricts to ASCII — is decoded. The body stays opaque
  octets: it may be an image, a gzip stream, or text in some other charset,
  none of which survive a UTF-8 decode.

  continue! is called once, before the body is collected, when the head asked
  for `Expect: 100-continue` and declares a body — a client that asked and is
  not answered waits out its own timeout first (curl stalls about a second)."
  ([conn acc max-bytes recv! idle-recv!]
   (read-request conn acc {:max-bytes max-bytes :recv! recv! :idle-recv! idle-recv!}))
  ([conn acc {:keys [max-bytes max-header-bytes recv! idle-recv! continue!
                     request-timeout-ms arm-read!]
              :or {max-header-bytes 8192 request-timeout-ms 0}}]
  (let [buf (ffi/alloc socket/bufsize)
        ;; a head can never be allowed past the whole-request cap either: with
        ;; :max-request-bytes below the header limit the tighter one governs,
        ;; or a run-on head would sit unchecked between the two
        max-header-bytes (min max-header-bytes max-bytes)
        ;; when the first octet of THIS request arrived. nil while the
        ;; connection is idle between requests, which may last the whole
        ;; keep-alive timeout and is not a request in progress.
        started (volatile! (when (pos? (alength ^bytes acc)) (System/currentTimeMillis)))
        deadline (fn [] (when (and (pos? request-timeout-ms) @started)
                          (+ @started request-timeout-ms)))
        expired? (fn [] (when-let [d (deadline)] (> (System/currentTimeMillis) d)))]
    (try
      ;; acc is a capacity buffer, valid over [0,have) — it is not sliced to
      ;; size until the request is framed. scanned: how far into it the
      ;; head-terminator search already ran. framing: {:head s :from i :to i} —
      ;; where the body starts and ends — resolved once the head is complete,
      ;; so the head is neither re-scanned nor re-parsed on every trickle of
      ;; the body. A read can overshoot :to (the next pipelined request rode
      ;; along), which is why the buffer keeps room past it rather than being
      ;; sized to the frame exactly.
      (loop [^bytes acc acc, have (alength acc), scanned 0, framing nil, state nil]
        (let [fresh   (when (nil? framing) (resolve-framing acc scanned have))
              ;; fire the interim response exactly once, as the head resolves
              _       (when (and (map? fresh) (:expect? fresh) continue!) (continue!))
              framing (or framing fresh)
              ;; map? not framing? — an unframeable head is a KEYWORD here, and
              ;; (:to :bad) is nil
              limit   (+ (cond
                           (not (map? framing)) max-bytes
                           (:chunked? framing) (+ max-bytes chunk-overhead-limit)
                           :else (:to framing))
                         socket/bufsize)
              more!   (fn [acc have scanned framing state]
                        (if (expired?)
                          :timeout
                          (do
                            ;; the fibers sweeper is the only thing that can
                            ;; end a parked read, so it has to know which
                            ;; deadline applies to this one
                            (when arm-read! (arm-read! (deadline)))
                            (let [n ((if (zero? have) idle-recv! recv!) conn buf)]
                              (cond
                                (pos? n) (let [acc (ensure-capacity acc have n limit)]
                                           (fill! acc have buf n)
                                           (when-not @started
                                             (vreset! started (System/currentTimeMillis)))
                                           [acc (+ have n) scanned framing state])
                                (zero? have) :closed
                                (expired?) :timeout
                                :else :bad)))))]
          (cond
            (keyword? framing) framing      ; :bad -> 400, :unsupported -> 501

            ;; the head is capped separately from the body: a request may
            ;; legitimately carry a megabyte, a HEAD may not. Checked here for
            ;; a block still arriving and below for one that arrived whole —
            ;; the completeness test used to win the cond, so a peer that sent
            ;; an oversized head in a single write was never checked at all
            ;; (Igropyr http.sc:1520).
            (and (map? framing) (> (:from framing) max-header-bytes)) :headers-too-big

            (nil? framing)                  ; head still incomplete
            (if (> have max-header-bytes)
              :headers-too-big              ; headers never ended -> 431
              (let [r (more! acc have (max 0 (- have 3)) nil nil)]
                (if (keyword? r) r (let [[a h sc fr st] r] (recur a h sc fr st)))))

            (:chunked? framing)
            (let [[status a b] (parse-chunked-body acc (:from framing) have state max-bytes)]
              (case status
                :done {:head (:head framing)
                       :body a
                       :leftover (if (= have b) no-bytes (java.util.Arrays/copyOfRange acc b have))}
                :more (let [r (more! acc have scanned framing a)]
                        (if (keyword? r) r (let [[a h sc fr st] r] (recur a h sc fr st))))
                :too-large :too-big
                :trailers-too-large :headers-too-big
                :bad))

            ;; declared request exceeds the cap -> 413
            (> (:to framing) max-bytes) :too-big

            (>= have (:to framing))
            (let [{:keys [head from to]} framing]
              {:head head
               :body (if (= from to) no-bytes (java.util.Arrays/copyOfRange acc from to))
               :leftover (if (= have to) no-bytes (java.util.Arrays/copyOfRange acc to have))})

            :else
            (let [r (more! acc have scanned framing state)]
              (if (keyword? r) r (let [[a h sc fr st] r] (recur a h sc fr st)))))))
      (finally (ffi/free buf))))))

;; --- request -> Ring map ----------------------------------------------------
(defn normalize-path
  "Collapse \"//\", drop \".\" and resolve \"..\" (RFC 3986 remove_dot_segments),
  so every layer sees the same path. A router matches on segments and silently
  drops empty ones, so \"//admin/x\" routes exactly like \"/admin/x\" — while a
  guard written the obvious way, (str/starts-with? (:uri req) \"/admin\"),
  compares the raw string and does not match. That gap lets a request skip an
  auth or rate-limit guard and still reach the guarded handler. Normalizing
  once, here, closes it for middleware, routing and static serving alike
  (Igropyr normalize-path).

  \"..\" can never escape the root: it pops a segment only when there is one."
  [path]
  (let [segs (reduce (fn [acc seg]
                       (cond
                         (or (= "" seg) (= "." seg)) acc
                         (= ".." seg) (if (seq acc) (pop acc) acc)
                         :else (conj acc seg)))
                     [] (str/split (str path) #"/" -1))]
    (if (empty? segs) "/" (str "/" (str/join "/" segs)))))

(defn- host-name
  "The name part of a Host header — the port, if it named one, is not part of
  the server name. Only a trailing \":digits\" is stripped, so an IPv6 literal
  in brackets survives intact."
  [h]
  (let [h (str/trim (or h ""))
        i (str/last-index-of h ":")]
    (if (and i (pos? i) (< (inc i) (count h))
             (every? #(<= (int \0) (int %) (int \9)) (subs h (inc i))))
      (subs h 0 i)
      h)))

(defn request->ring
  "Parse a request head (as read-request framed it) and its body octets into
  {:request ring-map}, or {:error response} when the request is malformed
  (400) or speaks an unsupported HTTP version (505). HTTP/1.1 requests must
  carry a Host header (RFC 7230 §5.4).

  conn-info carries what the request itself cannot say: {:server-port
  :server-name :remote-addr}, where :server-name is the bind address and
  :remote-addr the peer accept() reported."
  [head ^bytes body conn-info]
  (let [lines (str/split head #"\r\n")
        parts (str/split (or (first lines) "") #" ")
        ;; repeats coalesce into one comma-joined field (RFC 7230 §3.2.2, and
        ;; what Ring's other adapters do). Overwriting instead dropped every
        ;; value but the last — an X-Forwarded-For chain silently lost all but
        ;; its final hop.
        headers (reduce (fn [m line]
                          (let [i (str/index-of line ":")]
                            (if (and i (pos? i))
                              (let [k (str/lower-case (str/trim (subs line 0 i)))
                                    v (str/trim (subs line (inc i)))]
                                (assoc m k (if-let [prev (get m k)] (str prev "," v) v)))
                              m)))
                        {} (rest lines))
        bad (fn [status msg] {:error {:status status
                                       :headers {"Content-Type" "text/plain"}
                                       :body msg}})]
    (cond
      (not= 3 (count parts))
      (bad 400 "Bad Request")

      ;; bare LF, stray CR, obs-fold, a header name that is not a token: the
      ;; framing decision already refused these, and this keeps the guarantee
      ;; for anyone calling request->ring directly
      (head-malformed? head)
      (bad 400 "Bad Request")

      (not (contains? #{"HTTP/1.1" "HTTP/1.0"} (nth parts 2)))
      (bad 505 "HTTP Version Not Supported")

      (and (= "HTTP/1.1" (nth parts 2)) (not (contains? headers "host")))
      (bad 400 "Bad Request")

      :else
      (let [target (second parts)
            qi (str/index-of target "?")
            [raw-uri qs] (if qi [(subs target 0 qi) (subs target (inc qi))] [target nil])
            uri (normalize-path raw-uri)]
        {:request {:server-port    (:server-port conn-info)
                   ;; Ring: the resolved server name. The Host header is what
                   ;; the client asked for and what virtual-host middleware
                   ;; needs; the bind address is the fallback when there is
                   ;; none (HTTP/1.0 may omit it).
                   :server-name    (if-let [h (get headers "host")]
                                     (host-name h)
                                     (:server-name conn-info))
                   :remote-addr    (:remote-addr conn-info)
                   :uri            uri
                   ;; the target exactly as it arrived, for anything that needs
                   ;; to see what the client actually asked for
                   :ring-chez/raw-uri raw-uri
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

(defn- header-safe?
  "False for a header name or value carrying CR or LF (Igropyr header-safe?,
  http.sc:677). Such a value written verbatim ends the head early and the
  bytes after it are read as further headers — a handler that echoes user
  data into a response header would otherwise inject whole headers (response
  splitting). Unsafe headers are dropped, not escaped: there is no encoding
  a client would read back as the intended single value."
  [s]
  (let [t (str s)]
    (not (or (str/index-of t "\r") (str/index-of t "\n")))))

(defn- head->string
  "Response head only. framing: a number (Content-Length), :chunked, or :none
  (no body framing — bodyless status, HEAD, or close-delimited)."
  [resp keep-alive? framing]
  (let [status (or (:status resp) 200)
        sb (StringBuilder.)]
    (.append sb (str "HTTP/1.1 " status " " (get status-text status "Unknown") "\r\n"))
    (doseq [[k v] (:headers resp)]
      (let [kname (if (keyword? k) (name k) (str k))
            kn (str/lower-case kname)
            emit (fn [v] (when (header-safe? v)
                           (.append sb (str kname ": " v "\r\n"))))]
        (when (and (not= kn "content-length") (not= kn "transfer-encoding")
                   (header-safe? kname))
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

(defn response->parts
  "The whole response as send-all parts: the head, then the body's octets.
  Left as parts rather than spliced — send-all writes them back to back into
  one buffer, so a large body is not copied a second time just to be sent.

  Content-Length counts the octets that actually go on the wire, never the
  handler's own declaration (Igropyr framing-header?, http.sc:671: the
  framework owns framing and drops any the caller set). A response declaring
  a length that disagrees with its body desynchronises the connection for
  good — the peer reads the surplus as the head of the next response — which
  is a response-splitting and cache-poisoning primitive, and no middleware
  can be trusted to keep the two in step through a body rewrite. Middleware
  that sets the header correctly (ring-defaults' wrap-content-length) agrees
  with the count and loses nothing."
  ([resp] (response->parts resp false))
  ([resp keep-alive?]
   (let [body (body->bytes (:body resp))]
     [(head->string resp keep-alive? (alength body)) body])))

;; Connection headers are comma-separated token lists, case-insensitive
;; (RFC 7230 §6.1): "Keep-Alive, Close" means close.
(defn- header-tokens [v]
  (map str/trim (str/split (or v "") #",")))

(defn- conn-token? [tok v]
  (some #(= tok (str/lower-case %)) (header-tokens v)))

(defn connection-token?
  "True when the request's Connection header carries tok. Connection is a
  comma-separated token list, so \"keep-alive, Upgrade\" really does offer an
  upgrade and \"Keep-Alive, Close\" really does mean close."
  [req tok]
  (boolean (conn-token? tok (get-in req [:headers "connection"]))))

(defn keep-alive? [req]
  (let [c (get-in req [:headers "connection"])]
    (if (= "HTTP/1.0" (:protocol req))
      ;; a token list, so "keep-alive, close" really does mean close — reading
      ;; it as keep-alive would reuse a socket the peer is closing
      (and (conn-token? "keep-alive" c) (not (conn-token? "close" c)))
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

(def ^:private send-window
  "Octets of native buffer a single send-all holds. Everything used to go into
  one buffer sized to the whole payload — 4x the character count for a string,
  worst case for UTF-8 — so serving a 100 MB body reserved 400 MB of native
  memory the GC cannot even see. Parts are packed into a fixed window and
  flushed as it fills, which also keeps a small head and its body in ONE send
  rather than splitting them across segments."
  65536)

(defn- ^bytes part-bytes [p]
  (if (string? p) (.getBytes ^String p "UTF-8") p))

(defn- send-window!
  "Write n octets from buf; false when the peer is gone."
  [conn buf n wait-write!]
  (loop [off 0]
    (if (< off n)
      (let [sent (socket/c-send conn (+ buf off) (- n off) 0)]
        (cond
          (pos? sent) (recur (+ off sent))
          (and (neg? sent) (poller/eintr?)) (recur off)
          (and (neg? sent) wait-write! (poller/eagain?))
          (do (wait-write!) (recur off))
          :else false))
      true)))

(defn send-all
  "Write data to conn: a string (encoded as UTF-8), a byte array, or a vector
  of those written back to back. false when the peer is gone (caller closes).
  wait-write! parks the caller until the socket can take more bytes (fiber
  strategy, O_NONBLOCK sockets); nil leaves -1 meaning peer-gone (blocking
  sockets block instead of returning EAGAIN)."
  ([conn data] (send-all conn data nil))
  ([conn data wait-write!]
   (let [parts (mapv part-bytes (if (vector? data) data [data]))
         buf (ffi/alloc send-window)]
     ;; try/finally, not a free after the loop: a throw mid-write leaked the
     ;; buffer and nothing ever gave it back
     (try
       ;; ps: parts still to write, off: how far into the head of ps, held:
       ;; octets packed into buf and not yet sent
       (loop [ps (seq parts), off 0, held 0]
         (cond
           (nil? ps) (or (zero? held) (send-window! conn buf held wait-write!))

           (= off (alength ^bytes (first ps))) (recur (next ps) 0 held)

           (= held send-window)
           (if (send-window! conn buf held wait-write!) (recur ps off 0) false)

           :else
           (let [^bytes p (first ps)
                 n (min (- (alength p) off) (- send-window held))]
             (ffi/write-array (+ buf held) p off n)
             (recur ps (+ off n) (+ held n)))))
       (finally (ffi/free buf))))))

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

(def peer-gone
  "What a strategy's take! answers when the client disappeared while it waited
  for the application's next chunk."
  ::peer-gone)

(defn- stream-body
  "Pump a channel body onto conn: chunked framing for HTTP/1.1, raw bytes for
  HTTP/1.0 (close-delimited; caller closes). take! abstracts the channel take
  (blocking on worker threads, parking inside fibers, and answering
  `peer-gone` when the client went away first); send! likewise (plain send-all
  on blocking sockets, parking on writability inside fibers). True when the
  stream finished cleanly (terminator sent); false when the client went away —
  the channel is then closed so a parked producer's put returns false instead
  of hanging, and so an application that is simply quiet stops being told to
  produce for nobody."
  [conn ch http10? take! send!]
  (loop []
    (let [v (take! ch conn)
          bs (when (and (some? v) (not= peer-gone v)) (chunk->bytes v))]
      (cond
        ;; the client left while we waited: nothing to send, nothing to frame
        (= peer-gone v)
        (do (async/close! ch) false)

        ;; closed, or an empty chunk: end of stream. (an empty chunk carries no
        ;; data and would frame as a bogus terminator)
        (or (nil? bs) (zero? (alength bs)))
        (if http10? true (send! conn "0\r\n\r\n"))

        (send! conn (if http10? bs (chunk-parts bs)))
        (recur)

        :else (do (async/close! ch) false)))))

(def ^:private stream-window
  "Octets read from a File or InputStream body per write. A body that does not
  fit in memory must not have to."
  65536)

(defn- pump-file!
  "Stream f's octets onto conn, framed by the length already declared. false
  once the peer is gone, or if the file turns out shorter than it said it was
  — a short body is a truncated response, and the caller closes on false, which
  is the only honest signal for one."
  [conn ^java.io.File f len send!]
  (with-open [in (java.io.FileInputStream. f)]
    (let [buf (byte-array stream-window)]
      (loop [remaining len]
        (if (zero? remaining)
          true
          (let [n (.read in buf 0 (int (min stream-window remaining)))]
            (if-not (pos? n)
              false
              (and (send! conn (if (= n stream-window)
                                 buf
                                 (java.util.Arrays/copyOfRange buf 0 n)))
                   (recur (- remaining n))))))))))

(defn- read-up-to
  "Up to n octets from in. Returns [bytes eof?] — eof? true when the stream
  ended within n, which is what makes the length known."
  [^java.io.InputStream in n]
  (let [buf (byte-array n)]
    (loop [off 0]
      (if (= off n)
        [buf false]
        (let [r (.read in buf off (- n off))]
          (if (neg? r)
            [(java.util.Arrays/copyOfRange buf 0 off) true]
            (recur (+ off r))))))))

(defn- pump-stream!
  "Stream the rest of in onto conn after prefix, chunked (or bare, on
  HTTP/1.0). The length was never known, so this is the framing that does not
  require knowing it."
  [conn ^java.io.InputStream in prefix http10? send!]
  (let [buf (byte-array stream-window)
        write (fn [^bytes bs]
                (send! conn (if http10? bs (chunk-parts bs))))]
    (and (or (zero? (alength ^bytes prefix)) (write prefix))
         (loop []
           (let [n (.read in buf 0 stream-window)]
             (cond
               (not (pos? n)) (if http10? true (send! conn "0\r\n\r\n"))
               (write (if (= n stream-window) buf (java.util.Arrays/copyOfRange buf 0 n)))
               (recur)
               :else false))))))

(defn- status-forbids-body?
  "1xx, 204 and 304 are defined as bodyless: a client stops at the blank line
  whatever follows it, so writing a body desynchronises a persistent connection
  exactly the way a body on a HEAD does (RFC 9110 6.4.1 / 15.4.5)."
  [status]
  (or (<= 100 status 199) (= 204 status) (= 304 status)))

(defn- status-forbids-length?
  "Narrower than the above on purpose: Content-Length is forbidden on 1xx and
  204, but a 304 MAY carry the length the corresponding 200 would have had — it
  is metadata there, not framing (RFC 9110 8.6 / 15.4.5)."
  [status]
  (or (<= 100 status 199) (= 204 status)))

(defn- body-length
  "The octets a body would put on the wire, without writing them, or nil when
  that cannot be known without reading it. Used for a HEAD, whose response must
  carry the Content-Length its GET would have (RFC 9112 3.3.2) — and where 0 is
  not a stand-in for \"unknown\", it means the resource is empty."
  [b]
  (cond
    (nil? b) 0
    (string? b) (alength (.getBytes ^String b "UTF-8"))
    (bytes? b) (alength ^bytes b)
    (instance? java.io.File b) (.length ^java.io.File b)
    (or (seq? b) (vector? b)) (reduce (fn [n x] (some-> n (+ (or (body-length x) 0)))) 0 b)
    :else nil))

(defn send-response
  "Send resp for req on conn. Returns true when the connection may be reused."
  [conn req resp take! send!]
  (let [keep?     (and (keep-alive? req) (not (response-conn-close? resp)))
        status    (or (:status resp) 200)
        head?     (= :head (:request-method req))
        bodyless? (or (status-forbids-body? status) head?)
        b         (:body resp)
        ch        (when (async/chan? b) b)
        http10?   (= "HTTP/1.0" (:protocol req))
        ;; what the head declares when no body follows it: nothing for a status
        ;; that forbids a length, otherwise whatever a GET would have sent —
        ;; and :none when that is unknowable, since omitting the field is how
        ;; RFC 9112 says to say "unknown"
        head-framing (fn [] (if (status-forbids-length? status)
                              :none
                              (or (body-length b) :none)))]
    (cond
      ;; channel body that must not be written at all
      (and ch bodyless?)
      (do (async/close! ch)
          (and (send! conn (head->string resp keep? (head-framing))) keep?))

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
      (and (send! conn (head->string resp keep? (head-framing))) keep?)

      ;; a File knows its length without being read, so it is framed by
      ;; Content-Length and streamed from disk. It used to be copied into a
      ;; ByteArrayOutputStream, toByteArray'd, and then written into a native
      ;; buffer sized to the whole thing — three copies of the file resident to
      ;; serve it once.
      (instance? java.io.File b)
      (let [^java.io.File f b
            len (.length f)]
        (and (send! conn (head->string resp keep? len))
             (pump-file! conn f len send!)
             keep?))

      ;; an InputStream has no length until it ends. A short one is buffered
      ;; and framed by Content-Length exactly as before; one that does not end
      ;; within the first window switches to chunked rather than growing a
      ;; buffer to whatever the stream turns out to be.
      (instance? java.io.InputStream b)
      (let [^java.io.InputStream in b
            [^bytes prefix eof?] (read-up-to in stream-window)]
        (if eof?
          (and (send! conn [(head->string resp keep? (alength prefix)) prefix]) keep?)
          (if http10?
            (do (send! conn (head->string resp false :none))
                (pump-stream! conn in prefix true send!)
                false)
            (and (send! conn (head->string resp keep? :chunked))
                 (pump-stream! conn in prefix false send!)
                 keep?))))

      :else
      (and (send! conn (response->parts resp keep?)) keep?))))
