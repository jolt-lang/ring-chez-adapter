# ring adapter for jolt

A minimal Ring HTTP/1.1 server for [jolt](https://github.com/jolt-lang/jolt)
(Clojure on Chez Scheme). It binds BSD sockets directly through `jolt.ffi` — no
jolt built-in, no JVM — and runs synchronous Ring 1.x handlers on loopback.

```clojure
(require '[ring-chez.adapter :as adapter])
(def server (adapter/run-server (fn [req] {:status 200 :body "hi"}) {:port 3000}))
;; ... later ...
(adapter/stop-server server)
```

`run-server` returns a handle and serves on a background thread; the blocking
`accept`/`recv`/`send` calls are bound `:blocking` (collect-safe). `stop-server`
closes the listen socket and the accept loop exits cleanly (no spin).

## Test

```bash
joltc -M:test     # needs joltc on PATH; the test drives the server over HTTP
```
