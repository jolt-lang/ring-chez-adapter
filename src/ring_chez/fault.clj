(ns ring-chez.fault
  "A ready-made `:on-failure` hook — Igropyr's `make-fault-handler`
   (express.sc) in Ring form.

   When the adapter cannot complete a request it consults `:on-failure` and
   serves whatever response map that returns, on a connection that stays
   keep-alive-usable. That is what makes a fault answer more useful than the
   plain 500: the client can resubmit — changed parameters, carried state —
   on the same connection and get a fresh attempt.

       (adapter/run-server app {:on-failure (fault/fault-handler)})

   The failure's context is on the request, under `:ring-chez/failure`; see
   `ring-chez.adapter/run-server`.")

(defn- envelope
  "The JSON body, built by hand. Every value in it is produced by the adapter
  — :kind is one of a closed set of keywords and :elapsed-ms is a long — so
  there is nothing here to escape and no reason to take a JSON dependency."
  [kind elapsed-ms retryable?]
  (str "{\"fault\":\"" (name kind) "\","
       "\"elapsed-ms\":" (long (or elapsed-ms 0)) ","
       "\"retryable\":" (if retryable? "true" "false") "}"))

(defn fault-handler
  "An `:on-failure` fn answering a small JSON envelope instead of the plain
   500/503:

       {\"fault\":\"timeout\",\"elapsed-ms\":60003,\"retryable\":true}

   `:status` (default 503) sets the status. `:retryable?` is a predicate of
   the failure kind (`:crash`, `:nil-response`, `:timeout`, `:ws-guard`,
   `:ws-session`); the default calls a timeout retryable and a crash not,
   since a handler that threw on this input will throw on it again.

   For anything richer, write your own `(fn [request throwable] ...)` — this
   is a template, not the extension point."
  ([] (fault-handler nil))
  ([{:keys [status retryable?]
     :or   {status 503 retryable? #(= :timeout %)}}]
   (fn [request _throwable]
     (let [{:keys [kind elapsed-ms]} (:ring-chez/failure request)
           kind (or kind :crash)]
       {:status status
        :headers {"Content-Type" "application/json"}
        :body (envelope kind elapsed-ms (boolean (retryable? kind)))}))))
