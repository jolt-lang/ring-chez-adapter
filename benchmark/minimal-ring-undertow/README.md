# minimal-ring-undertow

Minimal Ring benchmark server on [Undertow](http://undertow.io) via
[ring-undertow-adapter](https://github.com/luminus-framework/ring-undertow-adapter),
exposing plain HTTP routes, Server-Sent Events, and a WebSocket echo endpoint.

## Run

```sh
clojure -M:run            # listens on 0.0.0.0:8080, override with PORT=...
```

## Endpoints

- `GET /plaintext` — `Hello, World!` (text/plain)
- `GET /json` — `{"message":"Hello, World!"}` (application/json)
- `GET /sse?interval=1000&events=10` — SSE stream, one `tick` event every
  `interval` ms (default 1s), `events` total (default 10)
- `GET /ws` — WebSocket echo (text and binary)

## Verify

```sh
curl http://localhost:8080/plaintext
curl -sN "http://localhost:8080/sse?interval=200&events=4"
clojure -M scripts/ws_check.clj        # WS echo smoke check
```

## Benchmark

```sh
ab -n 20000 -c 100 http://localhost:8080/plaintext
ab -n 20000 -c 100 http://localhost:8080/json
```

For SSE/WS throughput use a client that holds connections open (e.g. `hey`,
`wrk` with a script, or `k6`). Every SSE connection occupies a worker thread
for its duration; scale with `:worker-threads`/`:io-threads` passed to
`run-undertow` in `src/bench/server.clj`.

Sample (`ab -n 20000 -c 100`, M-series MacBook, server and client colocated):
~19k req/s plaintext, 0 failed requests — `ab` saturates before the server does.

## Implementation notes

- Routing is a `case` on `[request-method uri]`; middleware is
  `ring-defaults` `api-defaults`. No router or serialization libraries —
  nothing between the handler and the adapter except defaults.
- The WebSocket endpoint uses the portable `ring.websocket` listener API from
  ring-core (`{:ring.websocket/listener {...}}`), not the adapter-proprietary
  callback map.
- SSE extends the adapter's `RespondBody` protocol with a body type that
  writes each event to the exchange's blocking output stream and flushes.
  Returning a plain `InputStream` body does not work for SSE with this
  adapter: it streams via `io/copy`, which never flushes, so small events sit
  in Undertow's write buffer until it fills (verified: 200ms-interval events
  arrived in one burst after stream completion; ~16 KB before the first byte).

## Test

```sh
clojure -M:test
```
