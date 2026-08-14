(ns ring-chez.websocket
  (:require [jolt.ffi :as ffi]
            [jolt.crypto :as crypto]))

(ffi/defcfn c-send  "send"  [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn c-recv  "recv"  [:int :pointer :size_t :int] :ssize_t :blocking)

(def ^:private guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

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

(defn accept-token
  "RFC 6455 §4.2.2: base64(SHA1(key + GUID))."
  [ws-key]
  (base64-encode
    (map #(bit-and 0xff (long %))
         (crypto/digest crypto/c-sha1 20 (str ws-key guid)))))

(defn- send-bytes
  "Raw byte seq onto fd; true on success."
  [fd bs]
  (let [n (count bs) buf (ffi/alloc (max 1 n))]
    (doseq [[i b] (map-indexed vector bs)] (ffi/write buf :uint8 i b))
    (let [ok (loop [off 0]
               (if (< off n)
                 (let [sent (c-send fd (+ buf off) (- n off) 0)]
                   (and (pos? sent) (recur (+ off sent))))
                 true))]
      (ffi/free buf)
      ok)))

;; TCP delivers whatever arrived, not what was asked for, so reads go through
;; a per-session pending buffer: fill! pulls a packet in, take-n! hands back
;; exactly n bytes and keeps the rest for the next frame.
(defn- fill!
  [fd pending]
  (let [buf (ffi/alloc 65536)]
    (try
      (let [got (c-recv fd buf 65536 0)]
        (when (pos? got)
          (swap! pending into (map #(ffi/read buf :uint8 %) (range got)))
          true))
      (finally (ffi/free buf)))))

(defn- take-n!
  "Exactly n buffered bytes (fewer only when the peer closed / timed out)."
  [fd pending n]
  (while (and (< (count @pending) n) (fill! fd pending)))
  (let [out (vec (take n @pending))]
    (swap! pending (fn [p] (vec (drop n p))))
    out))

(defn encode-frame
  "Server->client frame (unmasked, so no mask bit on the length byte).
  Handles 7-bit / 16-bit length encodings."
  [opcode bs]
  (let [n (count bs)]
    (concat
      [(bit-or 0x80 opcode)]
      (if (< n 126)
        [n]
        [126 (bit-shift-right n 8) (bit-and 0xff n)])
      bs)))

(defn- recv-header [fd pending]
  (let [h (take-n! fd pending 2)]
    (when (= 2 (count h))
      (let [b0 (first h) b1 (second h)
            len7 (bit-and 0x7f b1)]
        {:fin (pos? (bit-and 0x80 b0))
         :opcode (bit-and 0x0f b0)
         :masked? (pos? (bit-and 0x80 b1))
         :len (cond (< len7 126) len7
                    (= len7 126) (let [ext (take-n! fd pending 2)]
                                   (when (= 2 (count ext))
                                     (+ (bit-shift-left (first ext) 8) (second ext))))
                    :else (let [ext (take-n! fd pending 8)]
                            (when (= 8 (count ext))
                              (reduce (fn [acc b] (+ (* acc 256) b)) 0 ext))))}))))

(defn- recv-frame [fd pending]
  (when-let [hdr (recv-header fd pending)]
    (when (and (:fin hdr) (:len hdr) (not (neg? (:len hdr))))
      (let [mask (when (:masked? hdr) (take-n! fd pending 4))
            payload (take-n! fd pending (:len hdr))]
        (when (= (:len hdr) (count payload))
          (let [unmasked (if mask
                           (map-indexed (fn [i b] (bit-xor b (nth mask (mod i 4)))) payload)
                           payload)]
            {:opcode (:opcode hdr) :payload unmasked}))))))

(defrecord Session [fd pending])

(defn make-session [fd] (->Session fd (atom [])))

(defn send!
  "Send a text frame. Returns false when the peer is gone."
  [session s]
  (send-bytes (:fd session)
              (encode-frame 0x1 (map #(bit-and 0xff (long %)) (.getBytes ^String s "UTF-8")))))

(defn send-binary!
  "Send a binary frame (bs: byte seq of unsigned ints)."
  [session bs]
  (send-bytes (:fd session) (encode-frame 0x2 bs)))

(defn close!
  "Send a close frame (no reason payload)."
  [session]
  (send-bytes (:fd session) (encode-frame 0x8 [])))

(defn recv!
  "Blocking read of one message. Pings are answered automatically (control
  frames never surface). Returns {:type :text :data s} | {:type :binary
  :data bs} | {:type :close} — :close also when the peer vanished or the
  connection timed out (a peer close frame is echoed per RFC 6455 §5.5.1)."
  [session]
  (let [fd (:fd session) pending (:pending session)]
    (loop []
      (let [f (recv-frame fd pending)]
        (cond
          (nil? f) {:type :close}
          (= 0x9 (:opcode f)) (do (send-bytes fd (encode-frame 0xA (:payload f))) (recur))
          (= 0x1 (:opcode f)) {:type :text :data (String. (byte-array (map byte (:payload f))) "UTF-8")}
          (= 0x2 (:opcode f)) {:type :binary :data (:payload f)}
          (= 0xA (:opcode f)) (recur)
          (= 0x8 (:opcode f)) (do (send-bytes fd (encode-frame 0x8 (:payload f))) {:type :close})
          :else (recur))))))
