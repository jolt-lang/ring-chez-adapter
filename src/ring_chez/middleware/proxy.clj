(ns ring-chez.middleware.proxy
  "`:remote-addr` behind a reverse proxy, from a *declared* number of trusted
   hops.

       (require '[ring-chez.middleware.proxy :as proxy])
       (def app (proxy/wrap-forwarded-remote-addr handler {:trust-proxy 1}))

   `X-Forwarded-For` is written by the client and appended to by each proxy,
   so it is a list whose left end is attacker-controlled and whose right end
   is not. `ring.middleware.proxy-headers/wrap-forwarded-remote-addr` takes
   the *leftmost* entry — the forgeable one — which is why an IP allow-list or
   a rate limiter keyed on `:remote-addr` behind it can be defeated by sending
   a header.

   Igropyr's rule instead: how many proxies of yours append to that header is
   a deployment fact the operator states, so `:trust-proxy` is that count and
   the address taken is the Nth entry **from the right** — the one the
   outermost proxy you trust actually observed. Everything left of it is
   ignored.

       X-Forwarded-For: 1.2.3.4, 10.0.0.9, 10.0.0.1
       :trust-proxy 0  =>  peer address (the proxy)   — no header trusted
       :trust-proxy 1  =>  10.0.0.9                   — what your edge saw
       :trust-proxy 2  =>  1.2.3.4

   The default of 0 keeps the connection's peer address, which cannot be
   forged. Pointing this at a deployment with no proxy, or with fewer hops
   than declared, hands the client control of its own address — so the count
   has to be the truth about your topology, not a guess."
  (:require [clojure.string :as str]))

(defn nth-from-right
  "The nth comma-separated entry counting from the right (1 = last), trimmed;
  nil when there are fewer than n entries or the entry is empty."
  [header n]
  (when (and header (pos? n))
    (let [parts (str/split (str header) #"," -1)
          idx   (- (count parts) n)]
      (when (and (>= idx 0) (< idx (count parts)))
        (let [v (str/trim (nth parts idx))]
          (when (seq v) v))))))

(defn forwarded-remote-addr
  "The address to trust for this request: the Nth `X-Forwarded-For` entry from
   the right when `trust-proxy` hops are declared and the header has that many,
   otherwise the connection's own `:remote-addr`."
  [request trust-proxy]
  (or (when (pos? (or trust-proxy 0))
        (nth-from-right (get-in request [:headers "x-forwarded-for"]) trust-proxy))
      (:remote-addr request)))

(defn wrap-forwarded-remote-addr
  "Middleware replacing `:remote-addr` with the client address as the outermost
   trusted proxy saw it. opts:

     :trust-proxy  how many proxies of YOURS append to X-Forwarded-For
                   (default 0 — trust nothing, keep the peer address)

   The original peer address stays available as
   `:ring-chez/peer-addr`, since it is the one thing here that cannot be
   forged."
  ([handler] (wrap-forwarded-remote-addr handler {}))
  ([handler {:keys [trust-proxy] :or {trust-proxy 0}}]
   (letfn [(rewrite [request]
             (assoc request
                    :remote-addr (forwarded-remote-addr request trust-proxy)
                    :ring-chez/peer-addr (:remote-addr request)))]
     (fn
       ([request] (handler (rewrite request)))
       ([request respond raise] (handler (rewrite request) respond raise))))))
