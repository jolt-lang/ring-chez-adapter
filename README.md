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
- `:handler-timeout-ms` (default `0`, meaning no deadline) — how long one
  handler call may take. Past it the adapter stops waiting: the request is
  answered through `:on-failure` and then a plain `503`, and the connection and
  its worker go back to serving. Without it a handler that never returns holds
  its worker forever, and `:worker-threads` of them is a dead server.

  **It costs throughput on `:threads`.** A handler cannot be abandoned from its
  own stack, so enforcing a deadline means running it on another thread — a
  handoff per request, measured at 15-25% (`ab -n 20000 -k /plaintext`: 8840 →
  7112 rps at c=10, 8370 → 6611 at c=100). Under `:fibers` the handler is
  already on a thread and the deadline measures free, so turn it on there
  without hesitating. On `:threads` it is a trade: pay ~20% against a failure
  mode you may never hit, or leave it off and make sure your handlers can't
  hang (client timeouts on everything they call).

  Whatever you set, the abandoned handler's *thread* is not reclaimed — jolt
  has no safe thread kill — so keep the value generous enough that only a
  genuinely stuck handler trips it. It bounds handler *execution*, not the
  response: a handler that returns a channel and streams for an hour is
  untouched.
- `:reuse-port` (default false) — bind with `SO_REUSEPORT`, so several
  processes can listen on the same port and the kernel spreads new connections
  across them (Linux; the BSDs allow the bind without the balancing). Off by
  default because a second bind failing is how you find out a server is already
  running.
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
After the drain every live connection is closed, on both strategies — once it
returns, nothing is served, including on keep-alive connections opened before
the stop.

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
`500 Internal Server Error` (`503` for a timeout). The worker always survives.

What went wrong is on the request the hook receives, under
`:ring-chez/failure`:

```clojure
{:kind :timeout        ; :crash :nil-response :timeout :ws-guard :ws-session
 :elapsed-ms 60003}    ; :timeout only
```

`ring-chez.fault/fault-handler` is a ready-made hook (Igropyr's
`make-fault-handler`): it answers a small JSON envelope on a connection that
stays usable, so a client can resubmit on it rather than reconnecting.

```clojure
(require '[ring-chez.fault :as fault])
(adapter/run-server handler {:on-failure (fault/fault-handler)})
;; => 503  {"fault":"timeout","elapsed-ms":60003,"retryable":true}
(fault/fault-handler {:status 500 :retryable? (constantly false)})
```

Slow peers are bounded separately by `:write-timeout-ms`: a client that stops
draining mid-response gets the connection abandoned instead of pinning a
worker. A slow *handler* is bounded by `:handler-timeout-ms`.

## Middleware

Ring's own middleware is pure Clojure and runs here unchanged — params,
cookies, sessions, `ring-defaults`. The exceptions are the ones that reach for
a JVM library; this repo ships those.

### Multipart uploads

`ring.middleware.multipart-params` is written against Apache
commons-fileupload, so it cannot load under jolt and a Ring stack here has no
way to accept a file upload. `ring-chez.middleware.multipart` is the same
middleware over `ring-chez.multipart.core`, an RFC 7578 parser in pure Clojure
that ships with this repo:

```clojure
(require '[ring-chez.middleware.multipart :as multipart])

(def app
  (multipart/wrap-multipart-params
    (fn [{:keys [multipart-params]}]
      (let [{:keys [filename content-type bytes size]} (get multipart-params "file")]
        {:status 200 :body (str filename " (" size " bytes)")}))))
```

`:multipart-params` is added to the request and merged into `:params`, as Ring
specifies. A text field's value is a string; an upload is
`{:filename :content-type :bytes :size}` — Ring's `byte-array-store` shape plus
`:size`. Repeated field names collect into a vector. Requests that are not
`multipart/form-data` pass through untouched, `:body` included.

There is no temp-file store: the parser buffers in memory, so bound uploads
with `:max-request-bytes` rather than assuming a large one spools to disk.

The parser underneath is `ring-chez.multipart.core`, and it is public: call it
directly to stream a body chunk by chunk (`make-parser` / `parse-chunk` /
`parse-stream`), or to read a part's `:charset` and `:headerlist`, which the
Ring shape drops.

```clojure
(require '[ring-chez.multipart.core :as mp])

(mp/parse-form-data request)   ; => {:params {"user" "alice"} :files {"doc" {...}}}
```

### gzip

jolt has no `java.util.zip`, so no existing Ring compression middleware can
load. `ring-chez.middleware.gzip` is Igropyr's policy over zlib bound through
`jolt.ffi`:

```clojure
(require '[ring-chez.middleware.gzip :as gzip])
(def app (gzip/wrap-gzip handler))            ; outermost, so it sees the finished response
(def app (gzip/wrap-gzip handler {:min-size 512 :level 9}))
```

A response is compressed when the client accepts gzip, the body is over
`:min-size` (default 1024 — below that the gzip header costs more than it
saves), the content type is compressible (`:types`, default `text/*` plus JSON,
XML, JavaScript, EDN and SVG), and the handler did not set its own
`Content-Encoding`. `Vary: Accept-Encoding` is added, and an `ETag` is given a
distinct value (`"abc"` → `"abc-gz"`), since a gzipped body is a different
entity.

`Accept-Encoding` is parsed rather than searched: `gzip;q=0` means *no* — a
client sends it precisely because it cannot decode gzip — and an explicit entry
beats a wildcard, so `*;q=0, gzip` still compresses.

In-memory bodies only (a string, a byte array, or a seq of those). A `File`, an
`InputStream` or a channel body passes through uncompressed: compressing a
stream means framing deflate output chunk by chunk, and the case where that
clearly pays — static files — is better served by caching the compressed copy.

zlib is bound lazily, from the running process first (the Chez runtime links
one) and only then from a shared object. Where it cannot be bound the
middleware is a no-op: uncompressed is always a correct answer.

### Static files

`ring.middleware.file` runs under jolt, but it stats and re-opens the file on
every request — and it decides containment with `getCanonicalPath`, which
through jolt 0.7.19 does not resolve symlinks (measured: it only absolutizes),
so a symlink planted inside a served root hands out whatever it points at.
(Fixed upstream in [jolt#693](https://github.com/jolt-lang/jolt/pull/693); this
middleware uses `toRealPath`, which is correct on every version.)

`ring-chez.middleware.static` is Igropyr's static cache in Ring's shape:

```clojure
(require '[ring-chez.middleware.static :as static])
(def app (static/wrap-static handler "public"))
(def app (static/wrap-static handler "public" {:prefix "/assets"
                                               :cache-control "public, max-age=3600"}))
```

A hot file is a map lookup — no `stat`, no read — with the file re-checked at
most once a second (`:stat-window-ms`). Responses carry `Content-Type` and a
weak `ETag`; `If-None-Match` gets a `304`, which for a large file costs no file
operations at all because the validator is in the cached metadata. A gzip copy
is compressed once and cached beside the plain bytes, with its own `-gz` ETag.
Files over `:max-cache-file-bytes` (1 MiB) are handed to the adapter as a
`File` body and streamed in bounded chunks with a real `Content-Length`.

Paths are percent-decoded *before* they are validated (or `%2e%2e` walks past
the `..` check), dotfiles are refused except `.well-known`, and symlinks out of
the root are refused via `toRealPath`. A miss, an unsafe path, or any method
other than GET/HEAD falls through to the wrapped handler, so a traversal
attempt cannot tell a 403 from a 404.

### `:remote-addr` behind a proxy

`X-Forwarded-For` is written by the client and appended to by each proxy, so
its left end is forgeable and its right end is not.
`ring.middleware.proxy-headers/wrap-forwarded-remote-addr` takes the *leftmost*
entry — so an allow-list or rate limiter keyed on `:remote-addr` behind it can
be pointed anywhere by sending a header.

`ring-chez.middleware.proxy` takes Igropyr's approach instead: how many proxies
of yours append to that header is a deployment fact you state, and the address
taken is that many entries from the **right**.

```clojure
(require '[ring-chez.middleware.proxy :as proxy])
(def app (proxy/wrap-forwarded-remote-addr handler {:trust-proxy 1}))

;; X-Forwarded-For: 1.2.3.4, 10.0.0.9, 10.0.0.1
;;   :trust-proxy 0  => the peer address (default: trust nothing)
;;   :trust-proxy 1  => 10.0.0.9   — what your edge actually observed
;;   :trust-proxy 2  => 1.2.3.4
```

A client can prepend as many entries as it likes without moving the one that is
picked. Fewer entries than declared hops falls back to the connection's peer
address, never to a client-supplied value, and that peer address stays on the
request as `:ring-chez/peer-addr`.

This is exactly as safe as the count is true: pointed at a deployment with no
proxy, `:trust-proxy 1` hands the client control of its own address.

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
jolt -M:test   # 507 checks over raw sockets + http-client, plus the parser suite's 103
```

## License

EPL-2.0, except `src/ring_chez/multipart/`, which is a port of
[defnull/multipart](https://github.com/defnull/multipart) and stays under
Apache-2.0 — see `LICENSE-multipart`.
