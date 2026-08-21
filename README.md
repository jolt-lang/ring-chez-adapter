## Install

Add the git dependency to `deps.edn`:

```clojure
{:deps
 {jolt-lang/ring-chez-adapter
  {:git/url "https://github.com/jolt-lang/ring-chez-adapter"
   :git/tag "v0.5.0"
   :git/sha "684a05b0e51521a497a05b9b2e68877430a3ffde"}}}
```

Requires the `jolt` binary (Clojure on Chez Scheme, no JVM); the adapter
binds BSD sockets through `jolt.ffi`, so there is nothing else to install.

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
- `:host` (default `"127.0.0.1"`) — interface to bind, as an IPv4 address.
  `"0.0.0.0"` serves every interface. Parsed by `inet_pton`, so it accepts
  what the platform accepts and anything else fails at boot. The default is
  loopback rather than Igropyr's `0.0.0.0`: this is a library, and a version
  bump should not put a server that was private on the network.
- `:strategy` (default `:threads`) — concurrency backend for connections:
  - `:threads` — fixed worker pool, one worker thread per busy connection
    (`:worker-threads`, default core count); idle keep-alive connections
    occupy a worker each until they time out
  - `:fibers` — one core.async go block per connection, parked on a single
    shared `poll(2)` io-poller thread; idle keep-alive connections pin no
    thread, so thousands can sit open without extra threads. Blocking work —
    the Ring handler and websocket sessions — still runs on threads, but only
    while actually computing; `:keep-alive-timeout-ms` bounds each parked
    read and `:write-timeout-ms` each parked write, so neither a handler nor
    a long stream is bounded by the idle timeout.
    Anything other than `:threads`/`:fibers` throws.
- `:worker-threads` (default: core count) — each worker runs one connection
  loop; when all are busy the acceptor parks and the kernel backlog queues
  (`:threads` strategy only)
- `:keep-alive-timeout-ms` (default 30000) — idle keep-alive connections are
  dropped via `SO_RCVTIMEO`
- `:max-request-bytes` (default 1048576) — headers + body combined; over the
  cap the server answers 413 and closes instead of buffering without bound
- `:max-header-bytes` (default 8192) — the head on its own, capped separately
  because a request may legitimately carry a megabyte of body and a head may
  not. Over it the server answers 431. Clamped by `:max-request-bytes` when
  that is smaller.
- `:request-timeout-ms` (default 60000, `0` disables) — how long one request
  may take to *arrive*, however steadily it dribbles. `:keep-alive-timeout-ms`
  only bounds the gap between segments and re-arms on every one, so without
  this a client sending a byte just inside it holds a worker indefinitely.
  Past the deadline the server answers `408` and closes.
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
  Applies to the threads strategy and post-upgrade websocket sessions; under
  `:fibers` it bounds each park on writability instead (the poller has no
  per-wait timeout, so the connection sweeper enforces it).

All options are validated at boot: bad values (`:port 0`, non-fn
`:on-failure`, negative `:write-timeout-ms`, …) throw before the listen
socket binds, with `{:key :given :expected}` in `ex-data`. Unknown keys
pass through for forward compatibility. Socket/bind failures carry
`:syscall`, `:errno`, and `strerror` text; a port that is already taken
fails with a plain-English message (`port 8080 is already in use — …
stop it or pass a different :port`) and `:errno-name "EADDRINUSE"` in
`ex-data`.

Keep-alive is HTTP/1.1 default, HTTP/1.0 opt-in via `Connection: keep-alive`;
`Connection` is a token list, so `keep-alive, close` means close. Pipelined
requests are handled via leftover carry. Malformed requests get `400`, unknown
HTTP versions `505`, and a handler-supplied `Connection: close` header is
honored (the connection closes after that response).

`1xx`, `204`, `304` and `HEAD` responses never write a body. They still carry
the `Content-Length` a `GET` would have where the RFC allows it — a `HEAD` and a
`304` do, `1xx` and `204` do not — so a client sizing a resource with `HEAD`
learns something. When the length cannot be known without producing the body
(a channel), the field is omitted, which is how RFC 9112 says to say "unknown".

`:uri` is normalized once, here: `//` collapses and `.` / `..` resolve, without
ever escaping the root. A router matches on segments and drops empty ones, so
`//admin/x` routes exactly like `/admin/x` — while a guard written the obvious
way, `(str/starts-with? (:uri req) "/admin")`, compares the raw string and does
not match. Normalizing in the adapter closes that gap for middleware, routing
and static serving alike. The target exactly as it arrived stays available as
`:ring-chez/raw-uri`. Percent-decoding is left to middleware, as Ring expects.

The server owns response framing. `Content-Length` always counts the octets
that actually go on the wire, so a handler that declares a length disagreeing
with its body cannot desynchronise the connection; middleware that sets the
header correctly (`wrap-content-length`) agrees with the count and loses
nothing. Response header names and values carrying CR or LF are dropped
rather than written, so a handler echoing user data into a header cannot
inject headers of its own.

Requests are framed in octets: the socket accumulates raw bytes, and only the
head — which RFC 7230 restricts to ASCII — is decoded (as UTF-8). So a
multibyte body matches its `Content-Length`, and a codepoint split across two
reads survives. A request whose `Content-Length` is missing is treated as
bodyless; one that is not one or more identical non-negative integers gets
`400`. `Transfer-Encoding: chunked` is decoded, with optional trailers
validated and dropped; any other transfer coding gets `501` rather than being
framed by guesswork. `Expect: 100-continue` is answered with the interim
response before the body is collected.

The head is parsed strictly, because a framing decision is only as trustworthy
as the parse behind it. A bare LF, a stray CR, an obs-fold continuation line, a
header name that is not a token, a request declaring both `Content-Length` and
`Transfer-Encoding`, or chunked framing on HTTP/1.0 all get `400` — each is a
case where this server and an intermediary in front of it could read the same
bytes as different messages. Repeated headers coalesce into one comma-joined
value rather than the last one winning.

`:remote-addr` is the peer address `accept(2)` reported, and `:server-name`
the host the client asked for (the `Host` header, port stripped), falling back
to the bind address when the request carries none.

The request `:body` is a `java.io.InputStream` over the body's own octets
(`nil` when the request has no body), so an upload stays byte-exact — an
image, a gzip stream, or text in some charset other than UTF-8 all arrive as
sent. `slurp` it for text (UTF-8 by default), or `clojure.java.io/copy`,
`.readAllBytes`, or `.read` for bytes.

Response bodies work the same way in reverse. A `String` is encoded as UTF-8;
a byte array, `InputStream`, or `File` is served as its own octets; a
seq/vector contributes each element's octets in turn. `Content-Length` counts
what actually goes on the wire, so serving a PNG is just:

```clojure
{:status 200
 :headers {"Content-Type" "image/png"}
 :body (clojure.java.io/file "logo.png")}
```

A `File` body is framed by its length on disk and streamed from there in
bounded chunks, so serving something larger than memory does not require
memory to match. An `InputStream` has no length until it ends: a short one is
buffered and framed with `Content-Length`, and one that does not end within
the first 64 KiB switches to `Transfer-Encoding: chunked` rather than growing
a buffer to whatever the stream turns out to be.

Channel bodies (see below) may yield strings or byte arrays; each chunk's
size line counts its octets. Anything else on a channel throws, which closes
that connection rather than writing garbage into the stream.

## Running servers

`run-server` returns a handle; besides `stop-server` it supports:

```clojure
(adapter/server-stats server)
;; {:connections 12 :active 3 :requests 48122 :uptime-ms 903111}

(adapter/swap-handler! server new-handler)      ; takes effect next request
(adapter/swap-ws-handler! server new-ws-handler)

(adapter/stop-server server)                       ; drains, then closes
(adapter/stop-server server {:drain-timeout-ms 0}) ; closes immediately
```

`stop-server` stops accepting, then waits for in-flight requests to finish
rather than cutting a response off mid-write, bounded by `:drain-timeout-ms`
(default 5000) so a handler that never returns cannot stop it returning either.

`swap-handler!` re-points a running server without a restart, including on
connections already open — the handler is resolved per request, after the read
that waits for it.

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

- `ws/recv!` blocks for the next message, answers pings, reassembles
  fragmented messages, and returns `{:type :text :data s}` /
  `{:type :binary :data bytes}` / `{:type :close}` (`:close` covers peer close
  — echoed with the peer's own status code per RFC 6455 §5.5.1 — timeouts, and
  vanishes). `:binary` data is a byte array.
- `ws/send!` sends a text frame, `ws/send-binary!` a binary frame (byte array,
  or any seq of octets), `ws/close!` a close frame; all return `false` once the
  peer is gone
- the session runs on the worker thread; when your fn returns the connection
  closes. `:keep-alive-timeout-ms` doubles as the idle read timeout for open
  websocket connections

A message that breaks the protocol never reaches your handler: the connection
is failed with the RFC's status code and `recv!` answers `{:type :close}`.
That covers an unmasked client frame, a non-zero RSV bit, an unknown opcode, an
oversized or non-final control frame, a non-minimal length encoding, and a
broken fragmentation sequence (all `1002`); invalid UTF-8 in a text message
(`1007`); and a frame or reassembled message past the caps (`1009`). The caps
are Igropyr's: 1 MiB per frame, 8 MiB per message, 16384 fragments — a declared
length is refused from the header alone, so it is never a memory reservation.

The handshake is only offered when `:ws-handler` is set, and only for a
complete RFC 6455 opening handshake: `GET` over HTTP/1.1, `Upgrade: websocket`,
an `upgrade` token in `Connection`, `Sec-WebSocket-Version: 13`, and a
`Sec-WebSocket-Key` that decodes to 16 bytes. A request that reaches for
websocket without meeting all of that gets `400`, as does an upgrade that also
declares a request body (its octets would otherwise be read as frames). Other
requests go to the Ring handler as usual.

## Test

```bash
jolt -M:test   # 386 checks; drives the server over raw sockets + http-client
```
