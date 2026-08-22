(ns ring-chez.multipart.bytes
  "Low-level byte, ASCII and charset helpers for the multipart parser."
)
;;; ---------------------------------------------------------------------------
;;; Byte helpers
;;; ---------------------------------------------------------------------------

(defn ba= [^bytes a ^bytes b]
  (and (= (alength a) (alength b))
       (loop [i (dec (alength a))]
         (if (neg? i)
           true
           (if (= (aget a i) (aget b i))
             (recur (dec i))
             false)))))

(defn ba-concat
  "Concatenate byte arrays."
  [& arrays]
  (let [total (reduce + 0 (map (fn [^bytes a] (alength a)) arrays))
        out (byte-array total)]
    (loop [arrays (seq arrays) dest 0]
      (if arrays
        (let [^bytes a (first arrays) n (alength a)]
          (dotimes [i n] (aset out (+ dest i) (aget a i)))
          (recur (next arrays) (+ dest n)))
        out))))

(defn ba-slice
  "Slice ba from start to end with Python-like clamping semantics."
  [^bytes ba start end]
  (let [n (alength ba)
        s (max 0 (min start n))
        e (max s (min end n))
        out (byte-array (max 0 (- e s)))]
    (dotimes [i (- e s)] (aset out i (aget ba (+ s i))))
    out))

(defn ba-eq-at? [^bytes ba ^bytes pat start]
  (loop [j (dec (alength pat))]
    (if (neg? j)
      true
      (if (= (aget ba (+ start j)) (aget pat j))
        (recur (dec j))
        false))))

(defn ba-index-of
  "Index of the first occurrence of pat in ba at or after from; nil if absent."
  [^bytes ba ^bytes pat from]
  (let [n (alength ba) m (alength pat)]
    (when (<= m (- n from))
      (loop [i from]
        (when (<= i (- n m))
          (if (ba-eq-at? ba pat i)
            i
            (recur (inc i))))))))

(defn ba-rindex-of
  "Index of the last occurrence of pat in ba at or after from; nil if absent."
  [^bytes ba ^bytes pat from]
  (let [n (alength ba) m (alength pat)]
    (loop [i (- n m)]
      (if (< i from)
        nil
        (if (ba-eq-at? ba pat i)
          i
          (recur (dec i)))))))

(defn ba-starts-with? [^bytes ba ^bytes prefix]
  (and (>= (alength ba) (alength prefix))
       (ba-eq-at? ba prefix 0)))

(defn str->ba ^bytes [^String s ^String charset]
  (.getBytes s charset))

(defn ba->str ^String [^bytes ba ^String charset]
  (String. ba charset))

(defn ascii-upper [c]
  (let [i (int c)]
    (if (and (>= i 97) (<= i 122)) (char (- i 32)) c)))

(defn ascii-lower [c]
  (let [i (int c)]
    (if (and (>= i 65) (<= i 90)) (char (+ i 32)) c)))

(defn ascii-letter? [c]
  (let [i (int c)]
    (or (and (>= i 65) (<= i 90)) (and (>= i 97) (<= i 122)))))

(defn title-case
  "Python str.title() for ASCII input: capitalize letters that follow a
  non-letter, lowercase everything else."
  [s]
  (apply str
    (map-indexed
      (fn [i c]
        (if (and (ascii-letter? c)
                 (or (zero? i) (not (ascii-letter? (nth s (dec i))))))
          (ascii-upper c)
          (ascii-lower c)))
      s)))

(defn utf8-valid?
  "True if ba is valid UTF-8 (ASCII is a subset)."
  [^bytes ba]
  (let [n (alength ba)
        cont? (fn [b] (let [i (int b)] (and (>= i 128) (<= i 191))))]
    (loop [i 0]
      (if (>= i n)
        true
        (let [b (int (aget ba i))]
          (cond
            (<= b 127) (recur (inc i))
            (and (>= b 194) (<= b 223))
            (if (< (inc i) n)
              (if (cont? (aget ba (inc i))) (recur (+ i 2)) false)
              false)
            (and (>= b 224) (<= b 239))
            (if (< (+ i 2) n)
              (if (and (cont? (aget ba (inc i))) (cont? (aget ba (+ i 2))))
                (recur (+ i 3)) false)
              false)
            (and (>= b 240) (<= b 244))
            (if (< (+ i 3) n)
              (if (and (cont? (aget ba (inc i)))
                       (cont? (aget ba (+ i 2)))
                       (cont? (aget ba (+ i 3))))
                (recur (+ i 4)) false)
              false)
            :else false))))))

;;; ---------------------------------------------------------------------------
;;; Constants
;;; ---------------------------------------------------------------------------

(def crlf (str->ba "\r\n" "US-ASCII"))
(def cr-byte (str->ba "\r" "US-ASCII"))
(def lf-byte (str->ba "\n" "US-ASCII"))
(def dbl-dash (str->ba "--" "US-ASCII"))
(def known-charsets
  #{"utf8" "utf-8" "ascii" "us-ascii" "latin1" "latin-1" "iso-8859-1"})

(defn is-valid-charset? [charset]
  (if (contains? known-charsets charset)
    true
    (try
      (ba->str (byte-array 0) charset)
      true
      (catch Throwable _ false))))
