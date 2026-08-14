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

## Options

`run-server` takes an opts map:

- `:port` (default 3000)
- `:worker-threads` (default: core count) — each worker runs one connection
  loop; when all are busy the acceptor parks and the kernel backlog queues
- `:keep-alive-timeout-ms` (default 30000) — idle keep-alive connections are
  dropped via `SO_RCVTIMEO`
- `:max-request-bytes` (default 1048576) — headers + body combined; over the
  cap the server answers 413 and closes instead of buffering without bound
- `:ws-handler` — fn of a websocket session, run when an upgrade request
  arrives (below)

Keep-alive is HTTP/1.1 default, HTTP/1.0 opt-in via `Connection: keep-alive`;
pipelined requests are handled via leftover carry. `204`/`304`/`HEAD` responses
never frame a body.

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
jolt -M:test   # 55 checks; drives the server over raw sockets + http-client
```
