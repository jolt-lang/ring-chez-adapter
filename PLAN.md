# ring-chez-adapter — phased integration plan (SSE, WebSockets, thread pool)

Source projects: **Igropyr** (Chez web framework — streaming/SSE/WS semantics to adopt),
**swish** (Chez — IO/event research only), **Jolt** (host runtime — threads + core.async).
Principle: adopt Igropyr's protocol logic as directly as Clojure allows; use Jolt's
core.async/threads instead of Igropyr's green processes/libuv; prove each phase with
tests before moving on.

## Verified facts (2025-01, jolt 1.11.0-jolt v0.7.9)

- `clojure.core.async` ships with jolt and works: `chan`, `go`, `thread`, `>!!/<!!`,
  `alts!`, `timeout`; a blocking call inside one `go` does not stall other `go` blocks;
  atoms are thread-safe across futures.
- `jolt.crypto` (already a dep) exposes `digest` (SHA-1/SHA-256 via OpenSSL EVP) and
  `random-bytes`; **no base64** — we port a small pure-Clojure base64 codec.
- Adapter today: one accept-loop future, **serial** connection handling, always
  `Connection: close`, always Content-Length. Tests green (`jolt -M:test`).
- Igropyr semantics to adopt (see `$memo_adapter_igropyr_findings`):
  - chunked streaming with **one chunk in flight** (backpressure) + write timeout
  - SSE data-line splitting (CR / LF / CRLF) to prevent field injection
  - RFC 6455 frame codec checks + strict UTF-8 validation (1007)
  - 101 upgrade with leftover-bytes handoff to the ws session

## Design decisions

- **Concurrency model**: thread-per-connection served from a **user-tunable fixed
  worker pool** (Undertow-style). Acceptor loop (1 thread) accepts and dispatches
  the fd to the pool; workers run blocking read/handler/write. Long-lived streams
  (SSE/WS) hold a worker for their lifetime — default pool size
  `(.availableProcessors (Runtime/getRuntime))`, opt `:worker-threads`. Pool built
  from core.async channel + N `a/thread` consumers (blocking Ring handlers park a
  worker thread, never a `go` dispatcher).
- **Streaming API (Ring-compatible)**: response `:body` may be a `core.async`
  channel → adapter streams it as `Transfer-Encoding: chunked` until closed
  (Igropyr `res-begin!/res-write!/res-end!` semantics; backpressure = take/send
  one chunk at a time). Strings/seqs/File stay as today.
- **SSE**: helper ns `ring-chez.sse` — builds the streamed response; `send!`
  frames events with Igropyr's data-line splitting; optional keep-alive comments;
  client-gone detection via write failure (`send!` returns falsey → producer stops).
- **WebSocket**: RFC 6455 codec ported from Igropyr `websocket.sc` (frame
  encode/decode + all validation rules + strict UTF-8), upgrade detection in the
  adapter (GET + `Upgrade: websocket` + `Sec-WebSocket-Key` 16-byte-decodable +
  version 13), 101 handshake, then `(:websocket opts)` session `(fn [ws req])` gets
  the connection: `ws-recv`, `ws-send-text!`, `ws-send-binary!`, `ws-close!`
  (auto ping/pong, fragmentation, close handshake). Reader loop = the worker
  thread; sends from other threads serialize through the same connection lock.

## Phases (each: failing test → implement → green → refactor)

1. **Worker pool + concurrent connections** — tunable `:worker-threads`,
   acceptor/dispatch split, clean shutdown. Tests: 2 slow requests overlap
   (wall-clock), `:worker-threads 1` serializes, stop-server stops promptly,
   old tests still green.
2. **Keep-alive** — HTTP/1.1 persistent connections (respect `Connection: close`),
   multiple requests per connection, idle timeout. Tests via raw-socket client
   helper (`connect` FFI binding) in test ns.
3. **Streaming responses (chunked)** — channel body → chunked TE; one-in-flight
   backpressure; `0\r\n\r\n` terminator; HEAD/bodyless statuses send no framing;
   HTTP/1.0 → close-delimited. Tests: chunk ordering, terminator, interleaved
   producer (channel fed by `go`), write-failure stops producer.
4. **SSE** — `ring-chez.sse`; event framing, multi-line data, id/event fields,
  keep-alive, reconnect (`Last-Event-ID`), injection-safe splitting. Tests: raw
  socket reads parse per the event-stream spec.
5. **WebSocket** — base64 + accept-key; frame codec; upgrade + 101 + session API.
   Tests: echo round-trip (masked client frames), ping/pong, fragmentation,
  invalid UTF-8 → close 1007, close handshake, oversized frame rejection,
  wrong version → 400. Raw-socket ws client in test ns.
6. **Hardening** — write timeouts on streams, idle keep-alive timeout, request
  header/body timeouts, graceful drain on stop (bounded wait for in-flight).

## Status

- [x] Research: adapter, Igropyr http/websocket/express, jolt core.async/crypto (verified by running)
- [ ] swish/jolt-internals researcher reports — refine plan if they surface blockers
- [x] Phase 1: worker pool (`:worker-threads`, unbuffered work chan, kernel backlog)
- [x] Phase 2: keep-alive (Connection parsing, pipelining via leftover carry, idle timeout via SO_RCVTIMEO, `:keep-alive-timeout-ms`)
- [x] Phase 3: streaming (channel body → chunked; HTTP/1.0 close-delimited; 204/304/HEAD no framing; client disconnect closes channel so producers don't hang)
- [x] Phase 4: SSE (`ring-chez.sse`: event-response / format-event / send!, Igropyr-style line splitting)
- [x] Phase 5: WebSocket (`ring-chez.websocket`: base64 + SHA-1 accept token (RFC 6455 golden test), frame codec with masking, ping auto-pong, close echo; adapter `:ws-handler` upgrade takeover)
- [x] Phase 6: hardening (`:max-request-bytes` cap with 413 + close — run-on headers and oversized bodies; README documents the full API)

## Future optimizations (from research, not scheduled)

- Jolt ships an io-poller (fibers park on EAGAIN via a poller under the
  core.async overlay) — the adapter's blocking-worker model could become
  nonblocking/fiber-based later without changing the public API.
- swish's libuv-backed event manager and Igropyr's green processes are not
  portable here (no libuv); their protocol layers were the porting surface.
