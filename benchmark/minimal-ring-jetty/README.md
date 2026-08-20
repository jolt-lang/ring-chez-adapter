# minimal-ring-jetty

Minimal Ring benchmark server on [Jetty](https://jetty.org) via
[ring-jetty-adapter](https://github.com/ring-clojure/ring), exposing plain HTTP
routes, Server-Sent Events, and a WebSocket echo endpoint. Same handler and same
endpoints as `../minimal-ring-undertow`, so the two JVM adapters are directly
comparable to each other and to `ring-chez-adapter`.

## Run

```sh
clojure -M:run            # listens on 0.0.0.0:8082, override with PORT=...
```

## Endpoints

- `GET /plaintext` — `Hello, World!` (text/plain)
- `GET /json` — `{"message":"Hello, World!"}` (application/json)
- `GET /sse?interval=1000&events=10` — SSE stream, one `tick` event every
  `interval` ms (default 1s), `events` total (default 10)
- `GET /ws` — WebSocket echo (text and binary)

## Verify

```sh
curl http://localhost:8082/plaintext
curl -sN "http://localhost:8082/sse?interval=200&events=4"
clojure -M scripts/ws_check.clj        # WS echo smoke check
```

## Benchmark

```sh
ab -n 20000 -c 100 http://localhost:8082/plaintext
ab -n 20000 -c 100 http://localhost:8082/json
```

Jetty's connector runs `max(1, cores/2)` selector threads against a shared
`QueuedThreadPool` (`:max-threads` 50 by default); handlers run on pool threads,
so an SSE or WebSocket stream occupies one for its duration. Tune with
`:max-threads`/`:min-threads` on `run-jetty` in `src/bench/server.clj`.

## Implementation notes

- Routing is a `case` on `[request-method uri]`, and there is **no middleware**
  — the query string is parsed in the handler. That is deliberate: the
  benchmark compares adapters, so nothing may sit between the handler and the
  adapter. See `benchmark/README.md`.
- The WebSocket endpoint uses the portable `ring.websocket` listener API from
  ring-core, which the Jetty adapter implements natively.
- SSE streams through `ring.core.protocols/StreamableResponseBody`, flushing
  after each event. Without the flush, small events sit in Jetty's write buffer
  until it fills — the same trap the Undertow server documents.

## Test

```sh
clojure -M:test
```
