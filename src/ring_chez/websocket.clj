(ns ring-chez.websocket
  "RFC 6455 codec and session primitives, ported from Igropyr's
   `websocket.sc`: the SHA-1 + base64 handshake key, frame encode/decode
   (masking, fragmentation, ping/pong, close), and a session object owned by
   one connection.

       (ws/recv! session)        ; blocks -> {:type :text|:binary|:close ...}
       (ws/send! session \"hi\")
       (ws/send-binary! session bs)
       (ws/close! session)       ; idempotent

   recv! answers pings, reassembles fragments, and fails the connection with
   the RFC's status codes (1002 protocol error, 1007 invalid UTF-8, 1009
   message too big) rather than handing a malformed message to the caller."
  (:require [jolt.ffi :as ffi]
            [jolt.io-poller :as poller]
            [jolt.crypto :as crypto]))

(ffi/defcfn c-send  "send"  [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-recv  "recv"  [:int :pointer :size_t :int] :ssize_t :blocking)

;; Igropyr's caps (websocket.sc:26-28). A declared length is never a memory
;; reservation: a frame past max-frame is refused from its header alone, and a
;; message is bounded across fragments too, so neither a single huge frame nor
;; an endless run of small ones can grow the session buffer without limit.
(def ^:const max-frame 1048576)     ; single frame payload cap
(def ^:const max-message 8388608)   ; reassembled multi-frame message cap
(def ^:const max-fragments 16384)   ; bound per-frame allocation overhead

(def ^:private recv-bufsize 65536)

;; --- base64 ------------------------------------------------------------------

(def ^:private b64-alphabet
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")

(defn- base64-encode
  "byte seq (unsigned ints) -> base64 string with padding."
  [bs]
  (let [n (count bs)
        full (quot n 3)
        sb (StringBuilder.)]
    (dotimes [i full]
      (let [j (* i 3)
            b0 (nth bs j) b1 (nth bs (inc j)) b2 (nth bs (+ j 2))
            triple (+ (bit-shift-left b0 16) (bit-shift-left b1 8) b2)]
        (doto sb
          (.append (nth b64-alphabet (bit-shift-right triple 18)))
          (.append (nth b64-alphabet (bit-and 0x3f (bit-shift-right triple 12))))
          (.append (nth b64-alphabet (bit-and 0x3f (bit-shift-right triple 6))))
          (.append (nth b64-alphabet (bit-and 0x3f triple))))))
    (case (rem n 3)
      1 (let [b0 (last bs)
              pair (bit-shift-left b0 8)]
          (doto sb
            (.append (nth b64-alphabet (bit-shift-right pair 10)))
            (.append (nth b64-alphabet (bit-and 0x3f (bit-shift-right pair 4))))
            (.append "==")))
      2 (let [b0 (nth bs (- n 2)) b1 (nth bs (dec n))
              triple (+ (bit-shift-left b0 16) (bit-shift-left b1 8))]
          (doto sb
            (.append (nth b64-alphabet (bit-shift-right triple 18)))
            (.append (nth b64-alphabet (bit-and 0x3f (bit-shift-right triple 12))))
            (.append (nth b64-alphabet (bit-and 0x3f (bit-shift-right triple 6))))
            (.append \=)))
      sb)
    (.toString sb)))

(defn- b64-value [c]
  (let [i (.indexOf ^String b64-alphabet (int c))]
    (when-not (neg? i) i)))

(defn- base64-decoded-length
  "How many octets a base64 string decodes to, or nil if it is not valid
  base64. Only the length is wanted (Igropyr ws-valid-client-key?), so the
  octets themselves are never built."
  [s]
  (let [s (str s)
        n (count s)]
    (when (and (pos? n) (zero? (rem n 4)))
      (let [pad (cond (.endsWith s "==") 2 (.endsWith s "=") 1 :else 0)
            body (subs s 0 (- n pad))]
        (when (every? b64-value body)
          (- (* 3 (quot n 4)) pad))))))

;; --- handshake ----------------------------------------------------------------

(def ^:private guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

(defn accept-token
  "RFC 6455 §4.2.2: base64(SHA1(key + GUID))."
  [ws-key]
  (base64-encode
    (map #(bit-and 0xff (long %))
         (crypto/digest crypto/c-sha1 20 (str ws-key guid)))))

(defn valid-client-key?
  "RFC 6455 §4.1: Sec-WebSocket-Key is a base64-encoded 16-byte nonce.
  Anything else is not an opening handshake (Igropyr ws-valid-client-key?)."
  [k]
  (= 16 (base64-decoded-length k)))

;; --- frame codec ---------------------------------------------------------------

(defn encode-frame
  "Server->client frame, FIN set and unmasked (only clients mask). All three
  length encodings: 7-bit, 16-bit, and the 64-bit form — without which a
  payload of 65536 or more built a 16-bit header out of a byte that does not
  fit in one, and the write failed outright."
  ^bytes [op ^bytes payload]
  (let [n (alength payload)
        hlen (cond (< n 126) 2 (< n 65536) 4 :else 10)
        out (byte-array (+ hlen n))]
    (aset-byte out 0 (unchecked-byte (bit-or 0x80 op)))
    (cond
      (< n 126) (aset-byte out 1 (unchecked-byte n))
      (< n 65536) (do (aset-byte out 1 (unchecked-byte 126))
                      (aset-byte out 2 (unchecked-byte (bit-shift-right n 8)))
                      (aset-byte out 3 (unchecked-byte (bit-and 0xff n))))
      :else (do (aset-byte out 1 (unchecked-byte 127))
                (dotimes [i 8]
                  (aset-byte out (+ 2 i)
                             (unchecked-byte (bit-and 0xff (bit-shift-right n (* 8 (- 7 i)))))))))
    (System/arraycopy payload 0 out hlen n)
    out))

(defn- u8 [^bytes bs i] (bit-and 0xff (aget bs i)))

(defn- frame-length
  "The declared payload length and how many bytes encoded it, or nil when the
  header is not all here yet. The 64-bit form uses promoting arithmetic on
  purpose: an absurd declared size must become a bignum and be rejected by the
  max-frame check, not silently wrap a long."
  [^bytes bs start have len7]
  (cond
    (= len7 126) (when (>= have 4)
                   [(+ (bit-shift-left (u8 bs (+ start 2)) 8) (u8 bs (+ start 3))) 2])
    (= len7 127) (when (>= have 10)
                   [(loop [i 0, v 0] (if (= i 8) v (recur (inc i) (+' (*' v 256) (u8 bs (+ start 2 i))))))
                    8])
    :else [len7 0]))

(defn decode-frame
  "One frame out of bs[start,limit), payload unmasked. expect-masked? is the
  peer's role requirement — a server requires client frames masked (RFC 6455
  §5.1). Returns {:fin? :op :payload :end} (end absolute, like start), or
  :more / :bad / :too-large.

  Every check here is fatal to the connection rather than skippable: masking
  must match the role, RSV bits must be zero (no extension was negotiated),
  the opcode must be one of the six defined, a control frame must be final and
  at most 125 bytes, the extended lengths must use their minimal encoding and
  the 64-bit form must not set its sign bit, and a close payload is never
  exactly one byte (reading a code out of it would index past the end)."
  [^bytes bs start limit expect-masked?]
  (let [have (- limit start)]
    (if (< have 2)
      :more
      (let [b0 (u8 bs start)
            b1 (u8 bs (+ start 1))
            fin? (pos? (bit-and 0x80 b0))
            rsv (bit-and 0x70 b0)
            op (bit-and 0x0f b0)
            masked? (pos? (bit-and 0x80 b1))
            len7 (bit-and 0x7f b1)
            control? (>= op 8)]
        (cond
          (not= masked? (boolean expect-masked?)) :bad
          (not (zero? rsv)) :bad
          (not (contains? #{0 1 2 8 9 10} op)) :bad
          (and control? (or (not fin?) (> len7 125))) :bad
          :else
          (if-let [[plen lenbytes] (frame-length bs start have len7)]
            (cond
              (and (= len7 126) (< plen 126)) :bad
              (and (= len7 127) (or (< plen 65536) (> (u8 bs (+ start 2)) 127))) :bad
              (and (= op 8) (= plen 1)) :bad
              (> plen max-frame) :too-large
              :else
              (let [mask-off (+ start 2 lenbytes)
                    data-off (+ mask-off (if masked? 4 0))
                    end (+ data-off plen)]
                (if (< limit end)
                  :more
                  (let [payload (java.util.Arrays/copyOfRange bs data-off end)]
                    (when masked?
                      ;; per byte of every client frame, so it stays a plain
                      ;; xor against (bit-and i 3) rather than a mod
                      (dotimes [i plen]
                        (aset-byte payload i
                                   (unchecked-byte (bit-xor (u8 payload i)
                                                            (u8 bs (+ mask-off (bit-and i 3))))))))
                    {:fin? fin? :op op :payload payload :end end}))))
            :more))))))

;; --- strict UTF-8 (RFC 3629) ----------------------------------------------------

(defn- cont? [^bytes bs i] (= 0x80 (bit-and 0xc0 (u8 bs i))))

(defn valid-utf8?
  "Rejects overlong encodings, surrogates and code points above U+10FFFF.
  String. substitutes U+FFFD instead of failing, so a text message has to be
  validated here to honour RFC 6455's 1007 requirement (Igropyr valid-utf8?)."
  [^bytes bs]
  (let [n (alength bs)]
    (loop [i 0]
      (if (>= i n)
        true
        (let [b (u8 bs i)]
          (cond
            (< b 0x80) (recur (inc i))                    ; ASCII
            (< b 0xc2) false                              ; stray cont / overlong
            (< b 0xe0) (and (< (+ i 1) n) (cont? bs (+ i 1))
                            (recur (+ i 2)))
            (< b 0xf0) (and (< (+ i 2) n) (cont? bs (+ i 1)) (cont? bs (+ i 2))
                            (let [b1 (u8 bs (+ i 1))]
                              (cond (= b 0xe0) (>= b1 0xa0)     ; no overlong
                                    (= b 0xed) (<= b1 0x9f)     ; no surrogate
                                    :else true))
                            (recur (+ i 3)))
            (< b 0xf5) (and (< (+ i 3) n) (cont? bs (+ i 1))
                            (cont? bs (+ i 2)) (cont? bs (+ i 3))
                            (let [b1 (u8 bs (+ i 1))]
                              (cond (= b 0xf0) (>= b1 0x90)     ; no overlong
                                    (= b 0xf4) (<= b1 0x8f)     ; <= U+10FFFF
                                    :else true))
                            (recur (+ i 4)))
            :else false))))))

;; --- session -------------------------------------------------------------------
;; TCP delivers whatever arrived, not what was asked for, so reads go through a
;; per-session buffer. Consuming a frame is an offset bump, not a copy of
;; everything behind it — the old vector-of-boxed-Longs buffer paid O(n) per
;; frame and ~16-48 bytes per wire byte.

(def ^:private empty-bytes (byte-array 0))

(defn- ib-append!
  [ibuf ^bytes chunk]
  (swap! ibuf
         (fn [{:keys [^bytes bs start end]}]
           (let [n (alength chunk)]
             (if (<= (+ end n) (alength bs))
               (do (System/arraycopy chunk 0 bs end n)
                   {:bs bs :start start :end (+ end n)})
               (let [used (- end start)
                     need (+ used n)
                     ;; System/arraycopy is memmove-safe, so compacting in
                     ;; place is fine when the array is already big enough
                     ^bytes out (if (<= need (alength bs)) bs (byte-array (max 4096 (* 2 need))))]
                 (System/arraycopy bs start out 0 used)
                 (System/arraycopy chunk 0 out used n)
                 {:bs out :start 0 :end need}))))))

(defn- ib-consume! [ibuf k]
  (swap! ibuf (fn [{:keys [bs start end]}]
                (let [start (+ start k)]
                  (if (= start end) {:bs bs :start 0 :end 0}
                      {:bs bs :start start :end end})))))

(defrecord Session [fd buf closed?])

(defn make-session
  "A session over conn. leftover is whatever the handshake read overshot into:
  a client may pipeline its first frames in the same segment as the upgrade
  request, and dropping those bytes left the session parked forever on a
  message it had already been handed."
  ([fd] (make-session fd empty-bytes))
  ([fd ^bytes leftover]
   (->Session fd
              (atom (if (pos? (alength leftover))
                      {:bs leftover :start 0 :end (alength leftover)}
                      {:bs (byte-array 4096) :start 0 :end 0}))
              (atom false))))

(defn- send-bytes
  "Raw octets onto fd; true on success, false once the peer is gone."
  [fd ^bytes bs]
  (let [n (alength bs)
        buf (ffi/alloc (max 1 n))]
    (try
      (ffi/write-array buf bs)
      (loop [off 0]
        (if (< off n)
          (let [sent (c-send fd (+ buf off) (- n off) 0)]
            (cond
              (pos? sent) (recur (+ off sent))
              (and (neg? sent) (poller/eintr?)) (recur off)
              :else false))
          true))
      (finally (ffi/free buf)))))

(defn- send-frame! [session op ^bytes payload]
  (and (not @(:closed? session))
       (send-bytes (:fd session) (encode-frame op payload))))

(defn send!
  "Send a text frame. Returns false when the peer is gone."
  [session s]
  (send-frame! session 0x1 (.getBytes ^String (str s) "UTF-8")))

(defn- ->bytes ^bytes [bs]
  (cond
    (bytes? bs) bs
    (string? bs) (.getBytes ^String bs "UTF-8")
    :else (byte-array (map unchecked-byte bs))))

(defn send-binary!
  "Send a binary frame. bs is a byte array (a seq of octets is accepted too)."
  [session bs]
  (send-frame! session 0x2 (->bytes bs)))

(defn close!
  "Send a close frame with no status. Idempotent."
  [session]
  (when (compare-and-set! (:closed? session) false true)
    (send-bytes (:fd session) (encode-frame 0x8 empty-bytes)))
  true)

(defn- valid-close-code?
  "Codes a peer may legitimately send (RFC 6455 §7.4.1): 1000-1003, 1007-1011,
  1012-1014, and the private range 3000-4999. 1004/1005/1006 are reserved and
  never appear on the wire; below 1000 and 1015..2999 are unassigned."
  [c]
  (or (<= 1000 c 1003) (<= 1007 c 1011) (<= 1012 c 1014) (<= 3000 c 4999)))

(defn- fail!
  "Close with a status code (1002 protocol error, 1007 invalid UTF-8, 1009
  message too big, or a peer's own code echoed back)."
  [session code]
  (when (compare-and-set! (:closed? session) false true)
    (send-bytes (:fd session)
                (encode-frame 0x8 (byte-array [(unchecked-byte (bit-shift-right code 8))
                                               (unchecked-byte (bit-and 0xff code))]))))
  true)

(defn- fill!
  "One recv into the session buffer; false at EOF or on a read timeout.

  EINTR retries, so that this agrees with the send path below and with
  fiber-recv!: treating a signal as EOF would end a live session, and the
  adapter would close a connection whose peer is still there. Defensive
  rather than load-bearing — no signal in an ordinary run has a handler to
  interrupt this. A read timeout (EAGAIN under SO_RCVTIMEO) still ends the
  session: that is the idle peer being reaped, which is what the timeout is
  for."
  [session]
  (let [fbuf (ffi/alloc recv-bufsize)]
    (try
      (loop []
        (let [got (c-recv (:fd session) fbuf recv-bufsize 0)]
          (cond
            (pos? got) (do (ib-append! (:buf session) (ffi/read-array fbuf got))
                           true)
            (and (neg? got) (poller/eintr?)) (recur)
            :else nil)))
      (finally (ffi/free fbuf)))))

(defn- next-frame!
  "Block until a whole frame is available. A frame map, or :protocol-error /
  :message-too-large / :close."
  [session]
  (loop []
    (let [{:keys [bs start end]} @(:buf session)
          r (decode-frame bs start end true)]     ; server role: client frames masked
      (cond
        (map? r) (do (ib-consume! (:buf session) (- (:end r) start)) r)
        (= :bad r) :protocol-error
        (= :too-large r) :message-too-large
        (fill! session) (recur)
        :else :close))))

(defn recv!
  "Blocking read of one message. Pings are answered and pongs ignored (control
  frames never surface), fragments are reassembled, and a message that breaks
  the protocol closes the connection with the RFC's status code instead of
  reaching the caller. Returns {:type :text :data s} | {:type :binary :data
  bytes} | {:type :close} — :close also covers a peer close (echoed per RFC
  6455 §5.5.1), a timeout, and a peer that vanished."
  [session]
  (letfn [(deliver-msg [op parts total]
            (let [body (byte-array total)]
              (loop [ps (reverse parts), off 0]
                (when (seq ps)
                  (let [^bytes p (first ps)]
                    (System/arraycopy p 0 body off (alength p))
                    (recur (next ps) (+ off (alength p))))))
              (cond
                (= 2 op) {:type :binary :data body}
                (valid-utf8? body) {:type :text :data (String. body "UTF-8")}
                :else (do (fail! session 1007) {:type :close}))))]
    (if @(:closed? session)
      {:type :close}
      ;; op: opcode of the message in progress (nil = none); size: octets so far
      (loop [op nil, parts (), size 0, fragments 0]
        (let [f (next-frame! session)]
          (if-not (map? f)
            (do (case f
                  :protocol-error (fail! session 1002)
                  :message-too-large (fail! session 1009)
                  (close! session))
                {:type :close})
            (let [fop (:op f)
                  ^bytes payload (:payload f)
                  new-size (+ size (alength payload))]
              (cond
                ;; ping -> pong. The pong can fail (a peer that stopped
                ;; reading makes it time out); carrying on into another read
                ;; on a dead connection is how a session parks forever.
                (= 9 fop) (if (send-frame! session 0xA payload)
                            (recur op parts size fragments)
                            {:type :close})
                (= 10 fop) (recur op parts size fragments)     ; pong

                (= 8 fop)
                (let [n (alength payload)]
                  (cond
                    (zero? n) (close! session)
                    :else (let [code (+ (bit-shift-left (u8 payload 0) 8) (u8 payload 1))]
                            ;; a conforming peer expects its own code back;
                            ;; answering with an empty close made every clean
                            ;; shutdown look like "no status received"
                            (fail! session (if (valid-close-code? code) code 1002))))
                  {:type :close})

                (zero? fop)                                     ; continuation
                (cond
                  (nil? op) (do (fail! session 1002) {:type :close})
                  (or (> new-size max-message) (>= fragments max-fragments))
                  (do (fail! session 1009) {:type :close})
                  :else (let [parts (conj parts payload)]
                          (if (:fin? f)
                            (deliver-msg op parts new-size)
                            (recur op parts new-size (inc fragments)))))

                :else                                           ; text / binary
                (cond
                  ;; a new data frame while a message is still in progress
                  ;; breaks the fragmentation sequence
                  op (do (fail! session 1002) {:type :close})
                  (> new-size max-message) (do (fail! session 1009) {:type :close})
                  (:fin? f) (deliver-msg fop (list payload) new-size)
                  :else (recur fop (list payload) new-size 1))))))))))
