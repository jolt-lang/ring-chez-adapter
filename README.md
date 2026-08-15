# ring adapter for jolt

A Ring HTTP/1.1 server for [jolt](https://github.com/jolt-lang/jolt) (Clojure on
Chez Scheme). It binds BSD sockets directly through `jolt.ffi` — no jolt
built-in, no JVM — and runs synchronous Ring handlers on a worker pool.

```clojure
(require '[ring-chez.adapter :as adapter])
(def server (adapter/run-server (fn [req] {:status 200 :body "hi"}) {:port 3000}))
;; ... later ...
(adapter/stop-server server)
```

## Install

Add the git dependency to `deps.edn`:

```clojure
{:deps
 {jolt-lang/ring-chez-adapter
  {:git/url "https://github.com/jolt-lang/ring-chez-adapter"
   :git/tag "v0.3.0"
   :git/sha "9739fbdcc326f0c95e0ebedb7b4ed9fcdec47070"}}}
```

Requires the `jolt` binary (Clojure on Chez Scheme, no JVM); the adapter
binds BSD sockets through `jolt.ffi`, so there is nothing else to install.

## Options

`run-server` takes an opts map:

- `:port` (default 3000)
- `:strategy` (default `:threads`) — concurrency backend for connections:
  - `:threads` — fixed worker pool, one worker thread per busy connection
    (`:worker-threads`, default core count); idle keep-alive connections
    occupy a worker each until they time out
  - `:fibers` — one core.async go block per connection, parked on a single
    shared `poll(2)` io-poller thread; idle keep-alive connections pin no
    thread, so thousands can sit open without extra threads. Blocking work —
    the Ring handler and websocket sessions — still runs on threads, but only
    while actually computing; `:keep-alive-timeout-ms` bounds each parked read.
    Anything other than `:threads`/`:fibers` throws.
- `:worker-threads` (default: core count) — each worker runs one connection
  loop; when all are busy the acceptor parks and the kernel backlog queues
  (`:threads` strategy only)
- `:keep-alive-timeout-ms` (default 30000) — idle keep-alive connections are
  dropped via `SO_RCVTIMEO`
- `:max-request-bytes` (default 1048576) — headers + body combined; over the
  cap the server answers 431 (run-on headers) or 413 (oversized body) and
  closes instead of buffering without bound
- `:ws-handler` — fn of a websocket session, run when an upgrade request
  arrives (below)
- `:on-failure` — fn of `(request throwable)`, consulted for every abnormal
  handler completion: a handler throw, a `nil` response (which Ring defines
  as an error), a `:ws-guard` throw, and post-101 websocket session throws
  (observed after the handshake; the connection still closes). Return a
  response map (with `:status`) to serve it — keep-alive survives — or
  throw/return nil for the plain `500 Internal Server Error`. The hook gets
  one attempt and the worker always survives. `nil` responses arrive tagged
  `{:type :ring-chez/nil-response}` in `ex-data`.
- `:ws-guard` — fn of the upgrade `request`, consulted before the `101` is
  sent. Return truthy non-map to proceed with the handshake; return a
  response map (e.g. `{:status 401 ...}`) to serve it instead — the peer
  never gets the socket and the connection stays keep-alive-usable; return
  nil/false for a bare `403 Forbidden`; a throw routes through `:on-failure`.
- `:write-timeout-ms` (default 30000, `0` disables) — `SO_SNDTIMEO` on every
  blocking send; a peer that stops draining mid-response gets the connection
  abandoned (truncated body = close) instead of pinning a worker forever.
  Applies to the threads strategy and post-upgrade websocket sessions.

All options are validated at boot: bad values (`:port 0`, non-fn
`:on-failure`, negative `:write-timeout-ms`, …) throw before the listen
socket binds, with `{:key :given :expected}` in `ex-data`. Unknown keys
pass through for forward compatibility. Socket/bind failures carry
`:syscall`, `:errno`, and `strerror` text; a port that is already taken
fails with a plain-English message (`port 8080 is already in use — …
stop it or pass a different :port`) and `:errno-name "EADDRINUSE"` in
`ex-data`.

Keep-alive is HTTP/1.1 default, HTTP/1.0 opt-in via `Connection: keep-alive`;
pipelined requests are handled via leftover carry. `204`/`304`/`HEAD` responses
never frame a body. Malformed requests get `400`, unknown HTTP versions `505`,
and a handler-supplied `Connection: close` header is honored (the connection
closes after that response).

## Error handling

Every abnormal handler completion — a handler throw, a `nil` response (an
error per the Ring spec), a `:ws-guard` throw, or a post-`101` websocket
session throw — flows through `:on-failure`, which gets one attempt to
produce a response:

```clojure
(adapter/run-server handler
  {:port 3000
   :on-failure (fn [req ex]
     ;; nil responses arrive tagged {:type :ring-chez/nil-response}
     {:status 500
      :headers {"Content-Type" "text/plain"}
      :body (or (ex-message ex) "handler returned nil")})})
```

Return a map with `:status` and it is served — keep-alive survives the
failure; return anything else (or throw) and the server answers the plain
`500 Internal Server Error`. The worker always survives. Slow peers are
bounded separately by `:write-timeout-ms`: a client that stops draining
mid-response gets the connection abandoned instead of pinning a worker.

## Streaming responses

Return a `core.async` channel as `:body` and the response is sent with
`Transfer-Encoding: chunked` (HTTP/1.0 clients get close-delimited bodies).
Each channel value becomes one chunk; close the channel to end the response. If
the client disconnects mid-stream the channel is closed, so producers see their
next put return `false` instead of hanging.

```clojure
(require '[clojure.core.async :as async])
(fn [req]
  (let [ch (async/chan)]
    (async/go
      (async/>! ch "first\n")   ; chunk 1
      (async/>! ch "second\n")  ; chunk 2
      (async/close! ch))
    {:status 200 :headers {"Content-Type" "text/plain"} :body ch}))
```

## Server-Sent Events

`ring-chez.sse` formats and sends events in the Igropyr style — multi-line
data splits on `\n`, `\r\n`, or `\r` into one `data:` line each:

```clojure
(require '[ring-chez.sse :as sse])
(fn [req]
  (sse/event-response
    (async/go (async/>! events {:id 1 :event "greet" :data "hello\nworld"})
              (async/>! events {:data "bye"})
              (async/close! events))))
```

Each channel value is one event (`:id`, `:event`, `:retry` optional). Responses
carry `Content-Type: text/event-stream` + `Cache-Control: no-cache`, chunked.
`sse/send!` takes a channel and an event map directly for custom loops.

## WebSocket

Pass `:ws-handler`; requests with `Upgrade: websocket` get the RFC 6455
handshake (SHA-1 + base64 accept token, verified against the RFC's golden
test vector) and then your fn owns the connection through a session:

```clojure
(require '[ring-chez.websocket :as ws])
(adapter/run-server handler
  {:port 3000
   :ws-handler (fn [session]
                 (loop []
                   (let [m (ws/recv! session)]
                     (when-not (= :close (:type m))
                       (ws/send! session (:data m))   ; echo
                       (recur)))))})
```

- `ws/recv!` blocks for the next message, answers pings automatically, and
  returns `{:type :text :data s}` / `{:type :binary :data bs}` / `{:type :close}`
  (`:close` covers peer close — echoed per RFC — timeouts, and vanishes)
- `ws/send!` sends a text frame, `ws/send-binary!` a binary frame, `ws/close!`
  a close frame; all return `false` once the peer is gone
- the session runs on the worker thread; when your fn returns the connection
  closes. `:keep-alive-timeout-ms` doubles as the idle read timeout for open
  websocket connections

The handshake is only offered when `:ws-handler` is set; other requests go to
the Ring handler as usual.

## Test

```bash
jolt -M:test   # 214 checks; drives the server over raw sockets + http-client
```
