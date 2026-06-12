# ring-janet-adapter

A [Ring](https://github.com/ring-clojure/ring) adapter for
[Jolt](https://github.com/jolt-lang/jolt), backed by
[spork/http](https://janet-lang.org/spork/api/http.html) — Janet's HTTP
server — through jolt's `janet.*` interop bridge. Synchronous Ring 1.x
handlers.

```clojure
;; deps.edn
yogthos/ring-janet-adapter {:git/url "https://github.com/yogthos/ring-janet-adapter"
                            :git/sha "..."}
janet/spork-http {:jpm/module "spork/http" :jpm/install "spork"}
```

```clojure
(require '[ring-janet.adapter :as adapter])

(defn handler [request]
  {:status 200
   :headers {"Content-Type" "text/plain"}
   :body (str "Hello " (get-in request [:params :name] "world"))})

(def server (adapter/run-server handler {:port 3000}))  ; non-blocking
(adapter/stop-server server)
```

`run-server` returns immediately; connections are served on the janet event
loop. Ring middleware works as-is — see the
[ring-app example](https://github.com/jolt-lang/examples/tree/main/ring-app)
for a full app (ring-core middleware, Selmer templates, config, native
build).

## Divergences from JVM Ring

- `:body` on requests is a `StringReader` shim rather than a
  `java.io.InputStream`; `(slurp (:body req))` drains it, which is what ring
  middleware and most handlers do.
- Response bodies: strings and eager seqs (no streams on this host).
- A throwing handler answers 500 and logs the error to stderr, like other
  ring adapters.

Extracted from the ring-app example once it had soaked there.
