(ns ring-chez.multipart-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [ring-chez.multipart.core :as m]))

(defn- ba [s] (.getBytes ^String s "UTF-8"))
(defn- bstr [^bytes b] (String. b "UTF-8"))

(defn- parse-events
  "Feed body (a byte array) to a fresh parser in one chunk and return events."
  ([boundary body] (parse-events boundary body {}))
  ([boundary body opts]
   (let [parser (apply m/make-parser boundary (mapcat identity opts))
         [parser events] (m/parse-chunk parser body)]
     (m/close-parser parser)
     events)))

(defn- event-segments [events]
  (keep-indexed (fn [i e] (when (= (first e) :segment) [(quot i 2) (second e)])) events))

(defn- parts->map [parts]
  (into {}
    (map (fn [p]
           [(get-in p [:segment :name])
            {:body (bstr (:body p))
             :filename (get-in p [:segment :filename])}]))
    parts))

(defn- chunked
  "Feed a body in chunks of size n."
  [boundary body n]
  (let [parser (atom (m/make-parser boundary))
        events (atom [])]
    (loop [i 0]
      (if (< i (alength body))
        (let [[p evs] (m/parse-chunk @parser (java.util.Arrays/copyOfRange body i (min (alength body) (+ i n))))]
          (reset! parser p) (swap! events into evs)
          (recur (+ i n)))
        (do (m/close-parser @parser) @events)))))

(defn- assert-error-type [f expected pred]
  (try (f) (is false (str "expected error " expected))
       (catch Throwable e (is (pred e) (str "expected " expected " got " (.getMessage e))))))

;;; ---------------------------------------------------------------------------
;;; Header utils (ported from test_header_utils.py)
;;; ---------------------------------------------------------------------------

(deftest test-header-unquote
  (is (= "foo bar" (m/header-unquote "\"foo bar\"")))
  (is (= "foo\"bar" (m/header-unquote "\"foo\\\"bar\"")))
  (is (= "foo\\bar" (m/header-unquote "\"foo\\\\bar\"")))
  (is (= "äöü" (m/header-unquote "\"äöü\"")))
  (is (= "foo" (m/header-unquote "foo"))))

(deftest test-header-quote
  (is (= "foo" (m/header-quote "foo")))
  (is (= "foo-bar" (m/header-quote "foo-bar")))
  (is (= "\"foo bar\"" (m/header-quote "foo bar")))
  (is (= "\"foo\\\"bar\"" (m/header-quote "foo\"bar")))
  (is (= "\"foo\\\\bar\"" (m/header-quote "foo\\bar"))))

(deftest test-parse-options-header
  (is (= ["text/html" {}] (m/parse-options-header "text/html")))
  (is (= ["text/html" {"charset" "utf-8"}] (m/parse-options-header "text/html; charset=utf-8")))
  (is (= ["text/html" {"charset" "utf-8"}] (m/parse-options-header "text/html; charset=\"utf-8\"")))
  (is (= ["multipart/form-data" {"boundary" "AaB03x"}] (m/parse-options-header "multipart/form-data; boundary=AaB03x")))
  (is (= ["text/html" {"name" "foo bar"}] (m/parse-options-header "text/html; name=\"foo bar\""))))

(deftest test-parse-content-disposition
  (testing "fast path"
    (is (= ["form-data" "field" nil] (m/parse-content-disposition "form-data; name=\"field\"")))
    (is (= ["form-data" "file" "file.txt"] (m/parse-content-disposition "form-data; name=\"file\"; filename=\"file.txt\""))))
  (testing "percent-encoding in fast path"
    (is (= ["form-data" "a\"b" nil] (m/parse-content-disposition "form-data; name=\"a%22b\"")))
    (is (= ["form-data" "a\rb\nc" nil] (m/parse-content-disposition "form-data; name=\"a%0Db%0Ac\"")))
    (is (= ["form-data" "a" "a\"b.txt"] (m/parse-content-disposition "form-data; name=\"a\"; filename=\"a%22b.txt\""))))
  (testing "slow path"
    (is (= ["form-data" "field" "test.txt"]
           (m/parse-content-disposition "form-data; filename=\"test.txt\"; name=\"field\"")))
    (is (= ["form-data" "field" nil] (m/parse-content-disposition "FORM-DATA; name=\"field\"")))
    (is (= ["form-data" "field" nil] (m/parse-content-disposition "form-data ; name=\"field\"")))
    (is (= ["form-data" "field" nil] (m/parse-content-disposition "form-data; name=field"))))
  (testing "ie6 windows path workaround"
    (is (= ["form-data" "Test" "bla.txt"]
           (m/parse-content-disposition "form-data; name=\"Test\"; filename=\"C:\\test\\bla.txt\"")))
    (is (= ["form-data" "Test" "bla.txt"]
           (m/parse-content-disposition "form-data; name=\"Test\"; filename=\"\\\\test\\bla.txt\"")))
    (is (= ["form-data" "Test" "ie.exe"]
           (m/parse-content-disposition "form-data; name=\"Test\"; filename=\"\\\\network\\ie.exe\"")))
    (is (= ["form-data" "Test" "ie.exe"]
           (m/parse-content-disposition "form-data; name=\"Test\"; filename=\"c:\\wondows\\ie.exe\"")))
    (is (= ["form-data" "Test" "täst.txt"]
           (m/parse-content-disposition "form-data; name=\"Test\"; filename=\"täst.txt\"")))))

;;; ---------------------------------------------------------------------------
;;; Push parser basics (ported from test_push_parser.py)
;;; ---------------------------------------------------------------------------

(defn- parse-basic [& {:as opts}]
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary--"))]
    (parse-events "boundary" body opts)))

(deftest test-init
  (is (thrown-with-msg? Exception #"Empty boundary" (m/make-parser "")))
  (is (thrown-with-msg? Exception #"Invalid characters in boundary" (m/make-parser "foo\nbar")))
  (is (thrown-with-msg? Exception #"Boundary too long"
                        (m/make-parser (apply str (repeat 1025 "a")) :strict true))))

(deftest test-simple-form
  (let [events (parse-basic)
        segments (filter #(= :segment (first %)) events)]
    (is (= 1 (count segments)))
    (let [seg (second (first segments))]
      (is (= "form-data" (:disposition seg)))
      (is (= "user" (:name seg)))
      (is (nil? (:filename seg)))
      (is (nil? (:content-type seg)))
      (is (nil? (:charset seg))))))

(deftest test-basic-form
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"pass\"\r\n\r\n"
                      "bob\r\n"
                      "--boundary--"))
        events (parse-events "boundary" body)
        segments (filter #(= :segment (first %)) events)
        bodies (keep #(when (= :body (first %)) (bstr (second %))) events)]
    (is (= ["alice" "bob"] bodies))
    (is (= ["user" "pass"] (map #(get-in % [1 :name]) segments)))))

(deftest test-file-segment
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n"
                      "Content-Type: text/plain\r\n\r\n"
                      "Hello World\r\n"
                      "--boundary--"))
        events (parse-events "boundary" body)
        seg (second (first (filter #(= :segment (first %)) events)))
        payload (bstr (second (first (filter #(= :body (first %)) events))))]
    (is (= "file" (:name seg)))
    (is (= "test.txt" (:filename seg)))
    (is (= "text/plain" (:content-type seg)))
    (is (= "Hello World" payload))))

(deftest test-preamble
  (let [body (ba (str "preamble\r\n"
                      "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary--"))
        events (parse-events "boundary" body)
        segs (filter #(= :segment (first %)) events)]
    (is (= 1 (count segs)))
    (is (= "user" (get-in (second (first segs)) [:name])))))

(deftest test-trailing-data
  (testing "non-strict ignores"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                        "alice\r\n"
                        "--boundary--\r\ntrailing junk"))
          events (parse-events "boundary" body)]
      (is (some #(= :end (first %)) events))))
  (testing "strict raises on data fed after the terminator"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                        "alice\r\n"
                        "--boundary--"))
          parser (m/make-parser "boundary" :strict true)
          [parser _] (m/parse-chunk parser body)]
      (assert-error-type #(let [[_ _] (m/parse-chunk parser (ba "junk"))] nil)
                         "StrictParserError" m/strict-parser-error?))))

(deftest test-incomplete-stream
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice"))]
    (assert-error-type #(parse-events "boundary" body) "ParserError" m/parser-error?)))

(deftest test-chunked-input
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary--"))]
    (doseq [n [1 2 3 5 8 13]]
      (let [events (chunked "boundary" body n)
            bodies (keep #(when (= :body (first %)) (bstr (second %))) events)]
        (is (= ["alice"] (->> bodies (apply str) (vector))) (str "chunk size " n))))))

(deftest test-missing-content-disposition
  (let [body (ba (str "--boundary\r\n"
                      "Content-Type: text/plain\r\n\r\n"
                      "alice\r\n"
                      "--boundary--"))]
    (assert-error-type #(parse-events "boundary" body) "ParserError" m/parser-error?)))

(deftest test-invalid-content-disposition-type
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: inline; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary--"))]
    (assert-error-type #(parse-events "boundary" body) "ParserError" m/parser-error?)))

(deftest test-header-limits
  (testing "header too long"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"x\"\r\n"
                        (str "X-Long: " (apply str (repeat 5000 "a"))) "\r\n\r\n"
                        "alice\r\n--boundary--"))]
      (assert-error-type #(parse-events "boundary" body) "ParserLimitReached" m/parser-limit-reached?)))
  (testing "too many headers"
    (let [hdrs (apply str (map #(str "X-Header" % ": v\r\n") (range 9)))
          body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"x\"\r\n"
                        hdrs "\r\n"
                        "alice\r\n--boundary--"))]
      (assert-error-type #(parse-events "boundary" body) "ParserLimitReached" m/parser-limit-reached?)))
  (testing "max-segment-size"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"x\"\r\n\r\n"
                        "alice\r\n--boundary--"))]
      (assert-error-type #(parse-events "boundary" body {:max-segment-size 3})
                         "ParserLimitReached" m/parser-limit-reached?))))

(deftest test-content-length-segment-header
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"x\"\r\n"
                      "Content-Length: 5\r\n\r\n"
                      "alice\r\n--boundary--"))]
    (let [events (parse-events "boundary" body)]
      (is (some #(= :end (first %)) events))))
  (testing "mismatch raises"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"x\"\r\n"
                        "Content-Length: 6\r\n\r\n"
                        "alice\r\n--boundary--"))]
      (assert-error-type #(parse-events "boundary" body) "ParserError" m/parser-error?)))
  (testing "invalid value raises"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"x\"\r\n"
                        "Content-Length: abc\r\n\r\n"
                        "alice\r\n--boundary--"))]
      (assert-error-type #(parse-events "boundary" body) "ParserError" m/parser-error?))))

(deftest test-header-continuation
  (testing "continuation merges"
    (let [body (ba (str "--boundary\r\n"
                        "Content-Disposition: form-data; name=\"x\"\r\n"
                        "X-Foo: bar\r\n baz\r\n\r\n"
                        "alice\r\n--boundary--"))
          events (parse-events "boundary" body)
          seg (second (first (filter #(= :segment (first %)) events)))]
      (is (= [["Content-Disposition" "form-data; name=\"x\""]
              ["X-Foo" "bar baz"]]
             (:headerlist seg)))))
  (testing "continuation as first header raises"
    (let [body (ba (str "--boundary\r\n"
                        " continued\r\n\r\n"
                        "alice\r\n--boundary--"))]
      (assert-error-type #(parse-events "boundary" body) "StrictParserError" m/strict-parser-error?))))

(deftest test-empty-segment
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"x\"\r\n\r\n\r\n"
                      "--boundary--"))]
    (let [events (parse-events "boundary" body)
          seg (second (first (filter #(= :segment (first %)) events)))]
      (is (= "x" (:name seg)))
      (is (some #(= :end (first %)) events)))))

(deftest test-missing-terminator
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"x\"\r\n\r\n"
                      "alice\r\n--boundary"))]
    (assert-error-type #(parse-events "boundary" body) "ParserError" m/parser-error?)))

;;; ---------------------------------------------------------------------------
;;; High-level API
;;; ---------------------------------------------------------------------------

(defn- make-request [content-type body]
  {:headers {"content-type" content-type} :body body})

(deftest test-parse-multipart
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n"
                      "Content-Type: text/plain\r\n\r\n"
                      "file contents\r\n"
                      "--boundary--"))
        parts (m/parse-multipart "boundary" body)]
    (is (= 2 (count parts)))
    (let [f1 (first parts) f2 (second parts)]
      (is (= "user" (:name f1)))
      (is (nil? (:filename f1)))
      (is (= "alice" (bstr (:bytes f1))))
      (is (= "file" (:name f2)))
      (is (= "a.txt" (:filename f2)))
      (is (= "text/plain" (:content-type f2)))
      (is (= "file contents" (bstr (:bytes f2)))))))

(deftest test-parse-multipart-missing-terminator
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n--boundary"))]
    (assert-error-type #(m/parse-multipart "boundary" body) "ParserError" m/parser-error?)))

(deftest test-is-form-request
  (is (m/is-form-request? "multipart/form-data; boundary=abc"))
  (is (m/is-form-request? "application/x-www-form-urlencoded"))
  (is (not (m/is-form-request? "text/plain")))
  (is (not (m/is-form-request? nil))))

(deftest test-parse-form-data-multipart
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n"
                      "Content-Type: text/plain\r\n\r\n"
                      "file contents\r\n"
                      "--boundary--"))
        result (m/parse-form-data (make-request "multipart/form-data; boundary=boundary" body))]
    (is (= {"user" "alice"} (:params result)))
    (is (= 1 (count (:files result))))
    (is (= "a.txt" (:filename (get-in result [:files "file"]))))
    (is (= "file contents" (bstr (:bytes (get-in result [:files "file"])))))))

(deftest test-parse-form-data-urlencoded
  (let [result (m/parse-form-data (make-request "application/x-www-form-urlencoded"
                                                (ba "user=alice&pass=bob+smith&x=%22quoted%22")))]
    (is (= {"user" "alice" "pass" "bob smith" "x" "\"quoted\""} (:params result)))
    (is (= {} (:files result)))))

(deftest test-parse-form-data-missing-content-type
  (is (= {:params {} :files {}} (m/parse-form-data {})))
  (is (thrown? Exception (m/parse-form-data {} {:strict true}))))

(deftest test-parse-form-data-unsupported
  (is (= {:params {} :files {}} (m/parse-form-data (make-request "text/plain" (ba "hi")))))
  (is (thrown? Exception (m/parse-form-data (make-request "text/plain" (ba "hi")) {:strict true}))))

(deftest test-parse-form-data-multipart-missing-boundary
  (let [body (ba "--boundary\r\n--boundary--")]
    (is (= {:params {} :files {}} (m/parse-form-data (make-request "multipart/form-data" body))))
    (is (thrown? Exception (m/parse-form-data (make-request "multipart/form-data" body)
                                              {:ignore-errors false})))))

(deftest test-streaming
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"user\"\r\n\r\n"
                      "alice\r\n"
                      "--boundary--"))
        read-fn (fn [n]
                  (let [remaining (atom body)]
                    (fn [n]
                      (let [b @remaining]
                        (if (zero? (alength b))
                          nil
                          (let [take-n (min n (alength b))
                                chunk (java.util.Arrays/copyOfRange b 0 take-n)]
                            (reset! remaining (java.util.Arrays/copyOfRange b take-n (alength b)))
                            chunk))))))
        events (m/parse-stream (m/make-parser "boundary") (read-fn 4))]
    (is (= ["alice"] (keep #(when (= :body (first %)) (bstr (second %))) events)))))

(deftest test-multidict-params
  (let [body (ba (str "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"tag\"\r\n\r\n"
                      "a\r\n"
                      "--boundary\r\n"
                      "Content-Disposition: form-data; name=\"tag\"\r\n\r\n"
                      "b\r\n"
                      "--boundary--"))
        result (m/parse-form-data (make-request "multipart/form-data; boundary=boundary" body))]
    (is (= {"tag" ["a" "b"]} (:params result)))))

;; RFC 7578 §4.1 requires the closing --boundary-- delimiter. A body that stops
;; at the delimiter without it is malformed, and the default rejects it; some
;; servers truncate it, and :check-complete false returns the parts that did
;; arrive rather than raising. The part's body is complete either way — only the
;; two bytes saying whether another part follows are missing — so dropping it
;; silently would lose data the parser already has.
(deftest truncated-final-delimiter
  (let [body (ba (str "--XXXX\r\n"
                           "Content-Disposition: form-data; name=\"foo\"\r\n\r\n"
                           "bar\r\n"
                           "--XXXX"))]
    (is (thrown? Exception (m/parse-multipart "XXXX" body)))
    (let [parts (m/parse-multipart "XXXX" body {:check-complete false})]
      (is (= 1 (count parts)))
      (is (= "foo" (:name (first parts))))
      (is (= "bar" (bstr (:bytes (first parts))))))))

;; ring hands the request body as a java.io.InputStream. Both the multipart and
;; the urlencoded branch have to take one, or the library cannot be used from the
;; middleware it exists for.
(deftest input-stream-body
  (testing "multipart"
    (let [body (str "--XXXX\r\n"
                    "Content-Disposition: form-data; name=\"foo\"\r\n\r\n"
                    "bar\r\n"
                    "--XXXX\r\n"
                    "Content-Disposition: form-data; name=\"f\"; filename=\"a.txt\"\r\n\r\n"
                    "data\r\n"
                    "--XXXX--")
          req {:headers {"content-type" "multipart/form-data; boundary=XXXX"}
               :body (java.io.ByteArrayInputStream. (ba body))}
          {:keys [params files]} (m/parse-form-data req)]
      (is (= {"foo" "bar"} params))
      (is (= "a.txt" (:filename (get files "f"))))
      (is (= "data" (bstr (:bytes (get files "f")))))))
  (testing "urlencoded"
    (let [req {:headers {"content-type" "application/x-www-form-urlencoded"}
               :body (java.io.ByteArrayInputStream. (ba "a=1&b=two"))}]
      (is (= {"a" "1" "b" "two"} (:params (m/parse-form-data req)))))))
