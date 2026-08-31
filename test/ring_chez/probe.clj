(ns ring-chez.probe
  "Stress the flake: fibers server with parked keep-alive conns, stop, restart,
  serve a fresh request. Repeated. Prints poller debug-state on failure and
  leakage at the end. Self-contained (raw socket client) so it runs under
  plain: jolt -M -e \"(require 'ring-chez.probe) (ring-chez.probe/-main)\""
  (:require [ring-chez.adapter :as adapter]
            [jolt.io-poller :as poller]
            [jolt.ffi :as ffi]
            [clojure.string :as str]))

(ffi/defcfn t-socket    "socket"    [:int :int :int] :int)
(ffi/defcfn t-connect   "connect"   [:int :pointer :int] :int)
(ffi/defcfn t-close     "close"     [:int] :int)
(ffi/defcfn t-recv      "recv"      [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn t-send      "send"      [:int :pointer :size_t :int] :ssize_t :blocking)
(ffi/defcfn t-setsockopt "setsockopt" [:int :int :int :pointer :int] :int)

(def ^:private macos?
  (str/includes? (str/lower-case (or (System/getProperty "os.name") "")) "mac"))
(def ^:private sol-socket (if macos? 0xffff 1))
(def ^:private so-rcvtimeo (if macos? 0x1006 20))

(defn- sockaddr [port]
  (let [sa (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write sa :uint8 0 i))
    (if macos?
      (do (ffi/write sa :uint8 16 0) (ffi/write sa :uint8 2 1))
      (ffi/write sa :uint8 2 0))
    (ffi/write sa :uint8 (bit-and (bit-shift-right port 8) 0xff) 2)
    (ffi/write sa :uint8 (bit-and port 0xff) 3)
    (ffi/write sa :uint8 127 4) (ffi/write sa :uint8 0 5)
    (ffi/write sa :uint8 0 6)   (ffi/write sa :uint8 1 7)
    sa))

(defn- set-rcvtimeo! [fd ms]
  (let [tv (ffi/alloc 16)]
    (dotimes [i 16] (ffi/write tv :uint8 0 i))
    (ffi/write tv :uint64 (quot ms 1000) 0)
    (if macos?
      (ffi/write tv :uint (long (rem ms 1000)) 8)
      (ffi/write tv :uint64 (long (rem ms 1000)) 8))
    (t-setsockopt fd sol-socket so-rcvtimeo tv 16)
    (ffi/free tv)))

(defn- connect [port]
  (let [fd (t-socket 2 1 0)]
    (when (neg? fd) (throw (ex-info "socket failed" {})))
    (set-rcvtimeo! fd 5000)
    (let [sa (sockaddr port)]
      (when (neg? (t-connect fd sa 16)) (t-close fd) (throw (ex-info "connect failed" {})))
      (ffi/free sa))
    fd))

(defn- send! [fd ^String s]
  (let [buf (ffi/alloc (max 1 (* 4 (count s))))
        n (ffi/write-bytes buf s)]
    (t-send fd buf n 0)
    (ffi/free buf)))

(defn- recv! [fd]
  (let [buf (ffi/alloc 65536)
        n (t-recv fd buf 65536 0)
        s (if (pos? n) (ffi/read-bytes buf n) "")]
    (ffi/free buf)
    s))

(def n-iterations 30)

(defn one-round [i]
  (let [h (fn [req] {:status 200 :headers {"Content-Type" "text/plain"} :body "ok"})
        s1 (adapter/run-server h {:port 8501 :strategy :fibers
                                  :keep-alive-timeout-ms 60000})]
    (Thread/sleep 150)
    (let [fds (mapv (fn [_] (connect 8501)) (range 5))]
      (doseq [fd fds]
        (send! fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (recv! fd))
      (adapter/stop-server s1)
      (Thread/sleep 100)
      (doseq [fd fds] (t-close fd))
      (let [s2 (adapter/run-server h {:port 8501 :strategy :fibers})
            fd (connect 8501)]
        (Thread/sleep 50)
        (send! fd "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
        (let [r (recv! fd)
              ok (str/starts-with? r "HTTP/1.1 200")]
          (when-not ok
            (println "  iter" i "FAIL, got:" (pr-str (subs r 0 (min 60 (count r)))))
            (println "  poller:" (pr-str (poller/debug-state))))
          (t-close fd)
          (adapter/stop-server s2)
          ok)))))

(defn -main [& _]
  (let [results (mapv one-round (range n-iterations))
        fails (count (remove true? results))]
    (println "poller after:" (pr-str (poller/debug-state)))
    (println "rounds:" n-iterations "fails:" fails)
    (when (pos? fails) (throw (ex-info "probe failures" {:n fails})))))
