(ns ring-chez.tls
  "TLS for run-server's :tls option, over the system OpenSSL.

  OpenSSL is used as a codec. Each connection's SSL runs against two memory BIOs,
  and ciphertext moves over whatever the connection's strategy already uses: a
  blocking recv on a worker, a parking one on a fiber. So every bound the plain
  path has (the idle poll, SO_RCVTIMEO, the sweeper's deadlines, write timeouts)
  applies unchanged, and OpenSSL itself never blocks.

  Loaded only when a server asks for it, so a plain server doesn't pull in OpenSSL.

  SSL_get_error reads a per-thread error queue. A fiber sharing its carrier could in
  principle be preempted between an SSL call and that read, and another fiber's
  failure would then be read as this connection's. The window is a few instructions
  and the cost is one dropped connection, so it is left open rather than masked on
  every read."
  (:require [clojure.string :as str]
            [jolt.ffi :as ffi]
            [jolt.socket.native :as native]
            [ring-chez.http :as http]
            [ring-chez.socket :as socket]))

;; Chez resolves a foreign symbol against loaded objects most-recent-first, and on
;; macOS the process itself links LibreSSL, whose structs disagree with OpenSSL's. So
;; these are loaded here, immediately before the bindings resolve, whoever loaded what
;; earlier. No unversioned /usr/lib fallback: macOS aborts the process on loading one.
(def ^:private openssl-candidates
  (let [os (str/lower-case (or (System/getProperty "os.name") ""))]
    (cond
      (str/includes? os "mac")
      {:crypto ["/opt/homebrew/opt/openssl@3/lib/libcrypto.dylib"
                "/usr/local/opt/openssl@3/lib/libcrypto.dylib"
                "/opt/local/lib/libcrypto.3.dylib"]
       :ssl    ["/opt/homebrew/opt/openssl@3/lib/libssl.dylib"
                "/usr/local/opt/openssl@3/lib/libssl.dylib"
                "/opt/local/lib/libssl.3.dylib"]}
      (str/includes? os "win")
      {:crypto ["libcrypto-3-x64.dll" "libcrypto-3.dll"]
       :ssl    ["libssl-3-x64.dll" "libssl-3.dll"]}
      :else
      {:crypto ["libcrypto.so.3" "libcrypto.so.1.1" "libcrypto.so"]
       :ssl    ["libssl.so.3" "libssl.so.1.1" "libssl.so"]})))

(defn- load-first! [candidates]
  (some (fn [path] (when (try (ffi/load-library path) true (catch Throwable _ false)) path))
        candidates))

;; crypto first, so ssl is most recent and wins SSL_*
(let [crypto (load-first! (:crypto openssl-candidates))
      ssl (load-first! (:ssl openssl-candidates))]
  (when-not (and crypto ssl)
    (throw (ex-info "run-server: :tls needs OpenSSL, and none was found"
                    {:key :tls :tried openssl-candidates}))))

(def ^:private WANT-READ 2)
(def ^:private WANT-WRITE 3)
(def ^:private ZERO-RETURN 6)
(def ^:private BIO-CTRL-PENDING 10)
(def ^:private FILETYPE-PEM 1)
(def ^:private CTRL-SET-MIN-PROTO 123)
(def ^:private TLS1-2 0x0303)
;; A client asking to renegotiate is CPU it can spend at will, and no HTTP client
;; needs to.
(def ^:private OP-NO-RENEGOTIATION 0x40000000)
;; ssl.h: SSL_VERIFY_PEER asks for a client certificate and verifies one that comes;
;; SSL_VERIFY_FAIL_IF_NO_PEER_CERT makes not sending one fail the handshake too.
(def ^:private VERIFY-PEER 0x01)
(def ^:private VERIFY-FAIL-IF-NO-PEER-CERT 0x02)
(def ^:private X509-V-OK 0)
;; Once a server verifies its peers, OpenSSL refuses to resume a session that has no
;; id context, so every client that came back would fail instead of resuming. Any
;; constant names this server's sessions; at most SSL_MAX_SID_CTX_LENGTH (32) octets.
(def ^:private SESSION-ID-CONTEXT "ring-chez")

(ffi/defcfn c-TLS-server-method "TLS_server_method" [] :pointer)
(ffi/defcfn c-SSL-CTX-new "SSL_CTX_new" [:pointer] :pointer)
(ffi/defcfn c-SSL-CTX-free "SSL_CTX_free" [:pointer] :void)
(ffi/defcfn c-SSL-CTX-ctrl "SSL_CTX_ctrl" [:pointer :int :int64 :pointer] :int64)
(ffi/defcfn c-SSL-CTX-set-options "SSL_CTX_set_options" [:pointer :int64] :int64)
(ffi/defcfn c-SSL-CTX-use-chain "SSL_CTX_use_certificate_chain_file" [:pointer :pointer] :int)
(ffi/defcfn c-SSL-CTX-use-key "SSL_CTX_use_PrivateKey_file" [:pointer :pointer :int] :int)
(ffi/defcfn c-SSL-CTX-check-key "SSL_CTX_check_private_key" [:pointer] :int)
(ffi/defcfn c-SSL-CTX-load-verify-locations "SSL_CTX_load_verify_locations" [:pointer :pointer :pointer] :int)
(ffi/defcfn c-SSL-load-client-CA-file "SSL_load_client_CA_file" [:pointer] :pointer)
(ffi/defcfn c-SSL-CTX-set-client-CA-list "SSL_CTX_set_client_CA_list" [:pointer :pointer] :void)
(ffi/defcfn c-SSL-CTX-set-verify "SSL_CTX_set_verify" [:pointer :int :pointer] :void)
(ffi/defcfn c-SSL-CTX-get-verify-mode "SSL_CTX_get_verify_mode" [:pointer] :int)
(ffi/defcfn c-SSL-CTX-set-session-id-context "SSL_CTX_set_session_id_context" [:pointer :pointer :uint] :int)
(ffi/defcfn c-SSL-get1-peer-certificate "SSL_get1_peer_certificate" [:pointer] :pointer)
(ffi/defcfn c-SSL-get-verify-result "SSL_get_verify_result" [:pointer] :long)
(ffi/defcfn c-i2d-X509 "i2d_X509" [:pointer :pointer] :int)
(ffi/defcfn c-X509-free "X509_free" [:pointer] :void)
(ffi/defcfn c-SSL-new "SSL_new" [:pointer] :pointer)
(ffi/defcfn c-SSL-free "SSL_free" [:pointer] :void)
(ffi/defcfn c-SSL-set-bio "SSL_set_bio" [:pointer :pointer :pointer] :void)
(ffi/defcfn c-SSL-set-accept-state "SSL_set_accept_state" [:pointer] :void)
(ffi/defcfn c-SSL-accept "SSL_accept" [:pointer] :int)
(ffi/defcfn c-SSL-read "SSL_read" [:pointer :pointer :int] :int)
(ffi/defcfn c-SSL-peek "SSL_peek" [:pointer :pointer :int] :int)
(ffi/defcfn c-SSL-write "SSL_write" [:pointer :pointer :int] :int)
(ffi/defcfn c-SSL-get-error "SSL_get_error" [:pointer :int] :int)
(ffi/defcfn c-SSL-shutdown "SSL_shutdown" [:pointer] :int)
(ffi/defcfn c-BIO-new "BIO_new" [:pointer] :pointer)
(ffi/defcfn c-BIO-s-mem "BIO_s_mem" [] :pointer)
(ffi/defcfn c-BIO-read "BIO_read" [:pointer :pointer :int] :int)
(ffi/defcfn c-BIO-write "BIO_write" [:pointer :pointer :int] :int)
(ffi/defcfn c-BIO-ctrl "BIO_ctrl" [:pointer :int :int64 :pointer] :int64)
(ffi/defcfn c-ERR-clear-error "ERR_clear_error" [] :void)
(ffi/defcfn c-ERR-get-error "ERR_get_error" [] :long)
(ffi/defcfn c-ERR-error-string-n "ERR_error_string_n" [:long :pointer :size_t] :void)

(defn- last-error
  "What OpenSSL last recorded, and an empty queue afterwards."
  []
  (loop [reason nil]
    (let [code (c-ERR-get-error)]
      (if (zero? code)
        (or reason "no detail")
        (recur (or reason
                   (let [buf (ffi/alloc 256)]
                     (try (c-ERR-error-string-n code buf 256)
                          (ffi/ptr->string buf)
                          (finally (ffi/free buf))))))))))

(defn- refuse [msg given]
  (ex-info (str "run-server: :tls " msg) {:key :tls :given given}))

(defn- with-path [path f]
  (let [p (ffi/string->ptr path)]
    (try (f p) (finally (ffi/free p)))))

(defn- check-client-auth
  "Refuse a client-certificate setup that is incomplete or says something it can't
  mean. A trust store alone asks no client for anything, so it is refused rather
  than read as :need: the config would otherwise look like it protects the server."
  [{:keys [client-auth client-ca-file]}]
  (when (and (some? client-auth) (not (#{:need :want} client-auth)))
    (throw (refuse ":client-auth must be :need or :want" client-auth)))
  (when (and client-auth (nil? client-ca-file))
    (throw (refuse ":client-auth needs :client-ca-file, the PEM CAs client certificates must chain to"
                   client-auth)))
  (when (some? client-ca-file)
    (when-not (string? client-ca-file)
      (throw (refuse ":client-ca-file must name a PEM file" client-ca-file)))
    (when-not client-auth
      (throw (refuse ":client-ca-file does nothing without :client-auth :need or :want"
                     client-ca-file)))))

(defn- use-client-auth!
  "Ask every client for a certificate that chains to client-ca-file, naming those CAs
  in the request. :need fails a handshake without one, :want lets it through."
  [ctx {:keys [client-auth client-ca-file]}]
  ;; OpenSSL 1.1 has only SSL_get_peer_certificate, and on macOS that name also
  ;; resolves to the system LibreSSL's, which must never see an OpenSSL SSL
  (when-not (ffi/find-symbol "SSL_get1_peer_certificate")
    (throw (refuse ":client-auth needs OpenSSL 3 (no SSL_get1_peer_certificate)" client-auth)))
  (when-not (= 1 (with-path client-ca-file #(c-SSL-CTX-load-verify-locations ctx % ffi/null)))
    (throw (refuse (str "cannot use client CA file " client-ca-file ": " (last-error)) client-ca-file)))
  (let [names (with-path client-ca-file c-SSL-load-client-CA-file)]
    (when (ffi/null? names)
      (throw (refuse (str "cannot read CA names from " client-ca-file ": " (last-error)) client-ca-file)))
    ;; the ctx owns the list from here
    (c-SSL-CTX-set-client-CA-list ctx names))
  (when-not (= 1 (with-path SESSION-ID-CONTEXT
                   #(c-SSL-CTX-set-session-id-context ctx % (count SESSION-ID-CONTEXT))))
    (throw (refuse (str "SSL_CTX_set_session_id_context failed: " (last-error)) client-auth)))
  (c-SSL-CTX-set-verify ctx (if (= :need client-auth)
                              (bit-or VERIFY-PEER VERIFY-FAIL-IF-NO-PEER-CERT)
                              VERIFY-PEER)
                        ffi/null))

(defn make-context
  "A server SSL_CTX from {:cert-file :key-file}, both PEM; :cert-file may hold the
  whole chain, leaf first. Throws, naming what is wrong, rather than let every
  handshake fail later. TLS 1.2 is the floor. free-context releases it.

  :client-auth :need or :want, with :client-ca-file (PEM CAs), verifies client
  certificates; serve then hands the verified one on as :ssl-client-cert."
  [{:keys [cert-file key-file client-auth client-ca-file] :as opts}]
  (when-not (map? opts) (throw (refuse "must be a map of :cert-file and :key-file" opts)))
  (when-not (string? cert-file) (throw (refuse ":cert-file must name a PEM file" cert-file)))
  (when-not (string? key-file) (throw (refuse ":key-file must name a PEM file" key-file)))
  (check-client-auth opts)
  (doseq [path (cond-> [cert-file key-file] client-ca-file (conj client-ca-file))]
    (when-not (.exists (java.io.File. ^String path))
      (throw (refuse (str "file does not exist: " path) path))))
  ;; the peer certificate reaches the handler as a java.security.cert.X509Certificate,
  ;; which jolt-crypto provides; only a server that asks for one loads it
  (when client-auth (require 'jolt.crypto))
  (let [ctx (c-SSL-CTX-new (c-TLS-server-method))]
    (when (ffi/null? ctx) (throw (refuse (str "SSL_CTX_new failed: " (last-error)) opts)))
    (try
      (c-SSL-CTX-ctrl ctx CTRL-SET-MIN-PROTO TLS1-2 ffi/null)
      (c-SSL-CTX-set-options ctx OP-NO-RENEGOTIATION)
      (when-not (= 1 (with-path cert-file #(c-SSL-CTX-use-chain ctx %)))
        (throw (refuse (str "cannot use certificate " cert-file ": " (last-error)) cert-file)))
      (when-not (= 1 (with-path key-file #(c-SSL-CTX-use-key ctx % FILETYPE-PEM)))
        (throw (refuse (str "cannot use key " key-file ": " (last-error)) key-file)))
      (when-not (= 1 (c-SSL-CTX-check-key ctx))
        (throw (refuse (str key-file " is not the key of " cert-file ": " (last-error)) key-file)))
      (when client-auth (use-client-auth! ctx opts))
      ctx
      (catch Throwable t (c-SSL-CTX-free ctx) (throw t)))))

(defn free-context
  "Drop the server's reference. Connections still open hold their own."
  [ctx]
  (c-SSL-CTX-free ctx))

(defn- open-conn [ctx]
  (let [ssl (c-SSL-new ctx)
        rbio (c-BIO-new (c-BIO-s-mem))
        wbio (c-BIO-new (c-BIO-s-mem))]
    (c-SSL-set-bio ssl rbio wbio)         ; SSL owns both from here
    (c-SSL-set-accept-state ssl)
    {:ssl ssl :rbio rbio :wbio wbio :lock (atom nil) :freed? (atom false)}))

;; An SSL is not safe for two threads at once. An HTTP connection only ever has its
;; owner, and a fiber should not park holding a monitor, so there it takes no lock.
;; A websocket session sends from any thread while another reads, so the upgrade turns
;; the lock on. The session is on a plain thread by then.
(defn- guarded [st f]
  (if-let [l @(:lock st)] (locking l (f)) (f)))

(defn- flush-out!
  "Move the ciphertext OpenSSL produced to the socket; false once the peer is gone."
  [st send!]
  (let [wbio (:wbio st)]
    (or (not (pos? (c-BIO-ctrl wbio BIO-CTRL-PENDING 0 ffi/null)))
        (let [buf (socket/take-recv-buffer!)]
          (try
            (loop []
              (let [n (c-BIO-read wbio buf socket/bufsize)]
                (cond
                  (not (pos? n)) true
                  (send! buf n) (recur)
                  :else false)))
            (finally (socket/give-recv-buffer! buf)))))))

(defn- feed-in!
  "One recv of ciphertext into OpenSSL. recv!'s answer, so non-positive means no more."
  [st recv!]
  (let [buf (socket/take-recv-buffer!)]
    (try
      (let [n (recv! buf)]
        (when (pos? n)
          (guarded st #(when-not @(:freed? st) (c-BIO-write (:rbio st) buf n))))
        n)
      (finally (socket/give-recv-buffer! buf)))))

(defn- accept!
  "Run the handshake; true when it finished."
  [st recv! send!]
  (loop []
    (c-ERR-clear-error)
    (let [ret (c-SSL-accept (:ssl st))
          err (when-not (= 1 ret) (c-SSL-get-error (:ssl st) ret))]
      ;; flushed on failure too: an alert telling the client why is worth a try
      (cond
        (not (flush-out! st send!)) false
        (= 1 ret) true
        (= err WANT-READ) (when (pos? (feed-in! st recv!)) (recur))
        (= err WANT-WRITE) (recur)
        :else (do (c-ERR-clear-error) false)))))

(defn- read!
  "Up to cap octets of plaintext into dst: n, or the recv contract's non-positive
  answer. What OpenSSL already holds comes first, so a pipelined request never waits
  on a socket with nothing more to say."
  [st dst cap recv! send!]
  (loop []
    (let [r (guarded st
              (fn []
                (if @(:freed? st)
                  -1
                  (do
                    (c-ERR-clear-error)
                    (let [n (c-SSL-read (:ssl st) dst cap)
                          err (when-not (pos? n) (c-SSL-get-error (:ssl st) n))]
                      ;; a read can produce output: a KeyUpdate wants an answer
                      (cond
                        (not (flush-out! st send!)) -1
                        (pos? n) n
                        (= err WANT-READ) ::more
                        (= err WANT-WRITE) ::again
                        (= err ZERO-RETURN) 0
                        :else (do (c-ERR-clear-error) -1)))))))]
      (cond
        (= ::more r) (let [n (feed-in! st recv!)] (if (pos? n) (recur) n))
        (= ::again r) (recur)
        :else r))))

(defn- write!
  "Encrypt and send n octets at p; false once the peer is gone."
  [st p n send!]
  (guarded st
    (fn []
      (and (not @(:freed? st))
           (loop [off 0]
             (if (< off n)
               (do
                 (c-ERR-clear-error)
                 (let [wrote (c-SSL-write (:ssl st) (+ p off) (- n off))]
                   (if (and (pos? wrote) (flush-out! st send!))
                     (recur (+ off wrote))
                     (do (c-ERR-clear-error) false))))
               true))))))

(def ^:private nothing-yet -2)

(defn- peer-gone?
  "Whether a client is still there while a stream has nothing to send. Its close_notify
  is ciphertext, so the socket peek ring-chez.socket uses sees bytes and calls that
  talking. What is readable is handed to OpenSSL instead, and SSL_peek says what it
  amounts to, leaving any request for the reader it belongs to."
  [st conn]
  (let [rev (native/poll-one conn native/pollin 0)]
    (cond
      (zero? rev) false
      (neg? rev) true
      (pos? (bit-and rev (bit-or native/pollerr native/pollnval))) true
      (zero? (bit-and rev (bit-or native/pollin native/pollhup))) false
      :else
      (let [n (feed-in! st (fn [buf]
                             (let [[got e] (native/c-recv conn buf socket/bufsize 0)]
                               (if (and (neg? got) (or (native/eagain? e) (native/eintr? e)))
                                 nothing-yet
                                 got))))]
        (cond
          (= nothing-yet n) false
          (not (pos? n)) true
          :else
          (let [b (ffi/alloc 1)]
            (try
              (guarded st
                (fn []
                  (or @(:freed? st)
                      (do
                        (c-ERR-clear-error)
                        (let [p (c-SSL-peek (:ssl st) b 1)]
                          (and (not (pos? p))
                               (not (= WANT-READ (c-SSL-get-error (:ssl st) p)))))))))
              (finally (ffi/free b)))))))))

(defn- close!
  "Say goodbye if the socket will take it right now, then free the SSL. Once. A peer
  that isn't reading doesn't get to hold this up: the close that follows is the answer."
  [st conn]
  (guarded st
    (fn []
      (when (compare-and-set! (:freed? st) false true)
        (try
          (c-ERR-clear-error)
          (c-SSL-shutdown (:ssl st))
          (when (pos? (bit-and (max 0 (native/poll-one conn native/pollout 0)) native/pollout))
            (flush-out! st (fn [p n]
                             (native/c-send conn p n native/msg-nosignal)
                             true)))
          (catch Throwable _ nil))
        (c-ERR-clear-error)
        (c-SSL-free (:ssl st))))))

(defn- blocking-recv
  "One recv on a blocking socket, a signal being no answer. A websocket session's."
  [conn buf]
  (loop []
    (let [[n e] (native/c-recv conn buf socket/bufsize 0)]
      (if (and (neg? n) (native/eintr? e)) (recur) n))))

(defn- peer-certificate
  "The client certificate the handshake verified, as an X509Certificate; nil when the
  client sent none, which is also every connection to a server that did not ask."
  [st]
  (let [x509 (c-SSL-get1-peer-certificate (:ssl st))]
    (when-not (ffi/null? x509)
      (try
        ;; verification failing ends the handshake, so this holds whenever a
        ;; certificate is here at all; checked anyway, since a handler trusts
        ;; :ssl-client-cert as an identity
        (let [v (c-SSL-get-verify-result (:ssl st))]
          (when-not (= X509-V-OK v)
            (throw (ex-info "client certificate did not verify" {:verify-result v}))))
        (let [len (c-i2d-X509 x509 ffi/null)
              _ (when-not (pos? len) (throw (ex-info "i2d_X509 failed" {:len len})))
              buf (ffi/alloc len)
              ;; i2d writes through and advances *out, so it gets a copy of buf
              holder (ffi/alloc (ffi/sizeof :pointer))]
          (try
            (ffi/write holder :pointer buf 0)
            (when-not (= len (c-i2d-X509 x509 holder))
              (throw (ex-info "i2d_X509 failed" {:len len})))
            (.generateCertificate (java.security.cert.CertificateFactory/getInstance "X.509")
                                  (java.io.ByteArrayInputStream. (ffi/read-array buf len)))
            (finally (ffi/free buf) (ffi/free holder))))
        (finally (c-X509-free x509))))))

(defn serve
  "Handshake on conn, then call serve-conn with an io that speaks plaintext over the
  given one, and free everything on the way out. A failed handshake never reaches
  serve-conn: there is no one to answer in HTTP. deadline-ms is the wall clock the
  handshake must finish by, however slowly its bytes trickle in.

  io is a strategy's: :recv!, :idle-recv! if it has one, :write-raw! for octets at a
  pointer, :take-via! for a channel take with a say on whether the peer is gone.
  serve-conn gets the plaintext io and what the handshake learned about the peer:
  {:ssl-client-cert cert} when it presented a verified certificate, else {}."
  [ctx conn io deadline-ms serve-conn]
  (let [st (open-conn ctx)
        raw-recv! (:recv! io)
        raw-idle! (:idle-recv! io)
        raw-send! (fn [p n] ((:write-raw! io) conn p n))
        take-via! (:take-via! io)]
    (try
      (when (accept! st
                     (fn [buf]
                       (if (> (System/currentTimeMillis) deadline-ms)
                         0
                         (raw-recv! conn buf)))
                     raw-send!)
        (serve-conn
         (assoc io
                :recv! (fn [_ buf]
                         (read! st buf socket/bufsize #(raw-recv! conn %) raw-send!))
                :idle-recv! (when raw-idle!
                              (fn [_ buf entry]
                                (read! st buf socket/bufsize #(raw-idle! conn % entry) raw-send!)))
                :send! (fn [_ data]
                         (http/send-all conn data nil (fn [p n] (write! st p n raw-send!))))
                :take! (fn [ch c] (take-via! ch c (fn [_] (peer-gone? st conn))))
                ;; what a websocket session reads and writes through, once the
                ;; socket has gone back to blocking
                :session-io (fn []
                              (reset! (:lock st) (Object.))
                              (let [send! (fn [p n] (http/send-window! conn p n nil))]
                                {:read! (fn [buf]
                                          (read! st buf socket/bufsize
                                                 #(blocking-recv conn %) send!))
                                 :write! (fn [p n] (write! st p n send!))})))
         ;; only a server that asked can have one, and only it needs OpenSSL 3
         (if-let [cert (when (pos? (c-SSL-CTX-get-verify-mode ctx)) (peer-certificate st))]
           {:ssl-client-cert cert}
           {})))
      (finally (close! st conn)))))
