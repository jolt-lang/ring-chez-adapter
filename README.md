## Install

Add the git dependency to `deps.edn`:

```clojure
{:deps
 {jolt-lang/ring-chez-adapter
  {:git/url "https://github.com/jolt-lang/ring-chez-adapter"
   :git/tag "v0.10.0"
   :git/sha "299c37dce549c4a13d0c9b9a232f60498a6e023e"}}}
```

Requires the `jolt` binary (Clojure on Chez Scheme, no JVM); the adapter
binds sockets through `jolt.socket.native`, so there is nothing else to install.

# ring adapter for jolt

A Ring HTTP/1.1 server for [jolt](https://github.com/jolt-lang/jolt) (Clojure on
Chez Scheme). It runs over jolt's fd-level sockets (`jolt.socket.native`) — no
java.net, no JVM — and runs synchronous Ring handlers on a worker pool. Both
strategies run on macOS, Linux and Windows.

```clojure
(require '[ring-chez.adapter :as adapter])
(def server (adapter/run-server (fn [req] {:status 200 :body "hi"}) {:port 3000}))
;; ... later ...
(adapter/stop-server server)
```

## Options

`run-server` takes an opts map:

- `:port` (default 3000). `0` asks the kernel for any free port; the
  returned handle's `:port` says which.
- `:host` (default `"127.0.0.1"`) — interface to bind. Three accepted
  forms: an IPv4 literal (`"0.0.0.0"` serves every v4 interface), an IPv6
  literal (`"::1"`, `"::"`), or a hostname resolved once at boot with
  `getaddrinfo` — **every** address the name answers with is bound, so
  `"localhost"` serves both `::1` and `127.0.0.1` on the one port. Every
  IPv6 listener is `IPV6_V6ONLY`, so `"::"` is v6-only and dual-stack
  comes from a name that answers in both families. With `:port 0` the
  kernel's pick is unified across every family, so the handle's `:port`
  names one port. A name whose v4 leg collides with a running server
  aborts the whole bind (no half-bound server). The default is loopback
  rather than Igropyr's `0.0.0.0`: this is a library, and a version bump
  should not put a server that was private on the network.
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
    a long stream is bounded by the idle timeout. The poller is kqueue,
    epoll or WSAPoll, whichever the platform has.
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
  arrives (below); the session's `:request` is that upgrade request
- `:on-failure` — fn of `(request throwable)`, consulted for every abnormal
  handler completion: a handler throw, a `nil` response (which Ring defines
  as an error), a `:ws-guard` throw, post-101 websocket session throws
  (observed after the handshake; the connection still closes), and a stream
  body that fails after the head is out, such as a channel chunk that is not
  a string or byte array (observed; the connection closes). Return a
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
  handoff per request, measured at 14-16% (`ab -n 20000 -k /plaintext`: 9854 →
  8434 rps at c=10, 9368 → 7853 at c=100; arming the interrupt that reclaims
  the thread adds nothing measurable on top). Under `:fibers` the handler is
  already on a thread and the deadline measures free, so turn it on there
  without hesitating. On `:threads` it is a trade: pay ~15% against a failure
  mode you may never hit, or leave it off and make sure your handlers can't
  hang (client timeouts on everything they call).

  Whatever you set, a handler that trips the deadline is not merely
  abandoned: the adapter fires jolt's cooperative interrupt at it, and the
  handler unwinds at its next reduction — `finally` blocks run, a catch-all
  cannot swallow the escape (a continuation jump, not a throw), and the
  thread is reclaimed. *Cooperative* is the operative word: a compute-stuck
  handler unwinds promptly, while one blocked in a foreign call or a wait —
  an upstream socket read, a channel take, a promise deref — only observes
  the interrupt when that call returns to Scheme, and holds its thread until
  then. So still keep the value generous enough that only a genuinely stuck
  handler trips it. It bounds handler *execution*, not the response: a
  handler that returns a channel and streams for an hour is untouched.
- `:handler-on-fiber` (default false, `:fibers` only) — run the handler on the
  connection's fiber instead of handing it to a thread. Each handoff wakes a thread
  and then wakes the carrier again, and on a cheap handler that is the larger part
  of what a request costs: plaintext with `ab -k -c 8` goes from 21k to 38k req/s.
  It is about 20 us per request, so a handler doing a millisecond of work won't
  notice.

  The handler has to be one that does not block where the runtime cannot see it.
  Channel ops, `Thread/sleep` and jolt's own socket and subprocess I/O park and are
  fine. A foreign call that blocks, such as a database driver over FFI, holds the
  carrier, and every connection on that carrier waits with it. That shows up as
  errors as well as latency: a new connection placed behind a held carrier is not
  claimed in time and is answered `503` (44 of 64 in a test with 300 ms handlers on
  ten carriers), so this is off by default. Wrap such a call in
  `clojure.core.async/thread` and take the result, which parks. It cannot be combined with `:handler-timeout-ms`, which needs a
  thread to walk away from, and `run-server` refuses the combination.
- `:tls` (default none) — `{:cert-file "cert.pem" :key-file "key.pem"}` serves HTTPS
  and `wss` on this port, under either strategy. Add `:client-auth` and
  `:client-ca-file` to require or accept client certificates. See [TLS](#tls).
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
;; {:connections 12 :active 3 :requests 48122 :uptime-ms 903111
;;  :faults 0 :last-fault nil}

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

`:faults` counts the server's OWN failures — something thrown in the accept
loop, in a worker's take/claim/release, or in a fiber's teardown, where no
response can carry it and no caller is left to catch it, plus one failure that
throws nothing at all: `:unclaimed`, a connection that was accepted, handed to
a worker or a fiber, and then never claimed by either — and `:claimed-silent`,
one that a worker did claim and then never served. A handler's failures
are not here; `:on-failure` answers those, because they have a request to
answer. Every such fault is also printed once to stderr (the first twenty, so
one that repeats per connection cannot become the load itself), and
`:last-fault` keeps `{:kind :error :at}` for the most recent.

It should stay at zero. What it is for is that none of these used to be
visible at all: a throw out of the accept loop ended accepting for the life of
the process, a throw out of a worker's release ended that worker and the pool
never grew back, and the future or thread holding the throwable was never
looked at. A server that had stopped serving therefore looked exactly like a
busy one — it held the port, `server-stats` answered, in-flight requests
finished, and new connections waited in the listen backlog where they cost the
process no fd and showed up in no count. The server now survives all of these
and says so; a nonzero `:faults` is worth a look even though nothing stopped.

`:unclaimed` is the same shape one connection down. The serving path hangs off
the claim at the far end of the accept handoff, so a connection that is handed
on and never claimed is served by nobody, closed by nobody — only an owner may
free an fd number — and, because no guard is crossed, reported by nobody: the
peer waits out its own timeout against a server that is answering everyone
else in a millisecond. A connection still unclaimed 500ms after the handoff is
now taken over by the sweeper, which wins the same claim a late worker or
fiber would have to win, answers the peer with a best-effort `503 Service
Unavailable` and `Connection: close` so it fails fast instead of waiting,
closes it, and counts it here.

`:claimed-silent` is one hop further down: a worker took the connection and
then never entered its serving loop. That connection has an owner, so the
sweeper cannot claim it and may not close it — only the owner frees an fd
number — but it can end it: after four times the same deadline (the margin
covers a GC pause or a descheduled worker that is merely late) it answers the
peer the same 503 and shuts the connection down, so the owner, if it ever
runs, reads EOF and releases the number itself. Both kinds mean the runtime
lost a handoff; neither should appear on a healthy server.

They did appear, one in ~1400 accepts on Apple silicon, and the cause was
not a lost handoff at all: jolt's `compare-and-set!` below
[jolt-lang/jolt#1072](https://github.com/jolt-lang/jolt/pull/1072) could
answer false with the atom still holding the expected value (Chez's
`$record-cas!` is a single `ldxr`/`stxr` on AArch64, and a cleared exclusive
monitor fails the `stxr`), so a worker's claim on a connection it was alone
in claiming was refused and it walked away. Every once-only step in the
adapter is such a claim, and every one now goes through `ring-chez.cas/cas!`,
which believes a false only once it has seen the value be something else —
so the count stays at zero on released jolts too. See benchmark/README.md
for the trace.

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
{:kind :timeout        ; :crash :nil-response :timeout :ws-guard :ws-session :stream
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
middleware over [jolt-lang/multipart](https://github.com/jolt-lang/multipart),
an RFC 7578 parser in pure Clojure that comes in as a dependency:

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

The parser underneath is `multipart.core`, and it is public: call it
directly to stream a body chunk by chunk (`make-parser` / `parse-chunk` /
`parse-stream`), or to read a part's `:charset` and `:headerlist`, which the
Ring shape drops.

```clojure
(require '[multipart.core :as mp])

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

In-memory bodies (a string, a byte array, or a seq of those) are compressed
whole. A channel body — a server-sent event stream, say — is compressed as one
gzip stream: each chunk is deflated with `Z_SYNC_FLUSH`, so the client can
decode it the moment it arrives, and the compressor keeps its window between
chunks, so a chunk that repeats what came before costs a few octets rather than
its own size. That is what makes re-sending a whole rendered page on every
change cheap on the wire: an unchanged 10 KB frame goes out as ~170 octets.
Turn it off with `:stream? false`.

Each open stream holds its compressor for as long as it is open, ~256 KB of
native memory at zlib's defaults. With many long-lived streams, `:window-bits`
(9..15, default 15) and `:mem-level` (1..9, default 8) trade compression for
memory. The window also bounds how far back a match can reach: a frame larger
than 2^window-bits octets (32 KB at the default) can no longer be matched
against the whole of the previous one.

A `File` or an `InputStream` passes through uncompressed; the case where that
would clearly pay — static files — is better served by caching the compressed
copy.

The same streaming compressor is available directly from `ring-chez.zlib`:

```clojure
(require '[ring-chez.zlib :as zlib])
(when-let [d (zlib/deflater {:level 6})]
  (zlib/deflate-chunk! d (.getBytes "first event" "UTF-8"))  ; flushed octets
  (zlib/deflate-chunk! d (.getBytes "second event" "UTF-8"))
  (zlib/deflate-finish! d))                                  ; trailer; releases d
```

zlib is bound lazily, from the running process first (the Chez runtime links
one) and only then from a shared object. Where it cannot be bound the
middleware is a no-op: uncompressed is always a correct answer.

### zstd

`ring-chez.middleware.compress/wrap-compress` offers zstd (RFC 8878) and gzip,
picking whichever the client weighs highest; on a tie zstd wins, so a browser
sending `gzip, deflate, br, zstd` gets zstd. Everything above about gzip
(`:min-size`, `:types`, `Vary`, ETags — `-zst` here — and flushed channel
streams) applies to both.

```clojure
(require '[ring-chez.middleware.compress :as compress])
(def app (compress/wrap-compress handler))
(def app (compress/wrap-compress handler {:zstd {:level -1 :window-log 18}}))
```

zstd matters most on streams. deflate matches at most 258 octets at a time from
at most 32 KB back, so once a re-rendered page outgrows that, each render costs
nearly its full compressed size again. zstd's window reaches back as far as you
let it. Measured re-sending a 98 KB page plus one changed octet on one stream:
gzip 21,301 octets, zstd 25.

Options, under `:zstd`:

- `:level`: from zstd's minimum (negative is faster) up to 19. Default 1.
  Levels 20 and up need windows larger than HTTP allows, so they are refused.
- `:window-log`: 10..23, the window as a power of two. Default 19 (512 KB).
  For streams, aim for 3-4x a typical uncompressed message, so the previous
  message is still in reach. 23 (8 MB) is the most a browser accepts (RFC
  9659). For a whole body the window shrinks to fit it anyway.

Each open stream holds its compressor: ~1.4 MB of native memory at the
defaults, ~1 MB at window-log 17. An invalid setting throws when the middleware
is built. `:encodings` (default `["zstd" "gzip"]`) sets what is offered and in
what order; `wrap-gzip` is `wrap-compress` with `["gzip"]`.

zstd comes from the system libzstd (`libzstd1` on Debian/Ubuntu, `brew install
zstd` on macOS), declared as an optional `:jolt/native`. Without it zstd is
not offered and gzip still is. `ring-chez.zstd` has the compressor directly:

```clojure
(require '[ring-chez.zstd :as zstd])
(when-let [c (zstd/compressor {:level 1 :window-log 19})]
  (zstd/compress-chunk! c (.getBytes "first event" "UTF-8"))   ; flushed octets
  (zstd/compress-finish! c))                                   ; epilogue; releases c
(zstd/compress (.getBytes "whole body" "UTF-8"))               ; one frame, or nil
(zstd/decompress frame-bytes)                                  ; also decodes a flushed prefix
```

Static files (`ring-chez.middleware.static`) still cache and serve only a gzip
copy.

### Static files

`ring.middleware.file` runs under jolt, but it stats and re-opens the file on
every request — and it decides containment with `getCanonicalPath`, which
through jolt 0.7.19 does not resolve symlinks (measured: it only absolutizes),
so a symlink planted inside a served root hands out whatever it points at.
(Fixed in [jolt#693](https://github.com/jolt-lang/jolt/pull/693), released in
jolt 0.7.20; this middleware uses `toRealPath`, which is correct on every
version.)

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
(require '[ring-chez.sse :as sse]
         '[clojure.core.async :as async])
(fn [req]
  (let [events (async/chan)]
    (async/go (async/>! events {:id 1 :event "greet" :data "hello\nworld"})
              (async/>! events {:data "bye"})
              (async/close! events))
    (sse/event-response events)))
```

Each channel value is one event: a map (`:data`, with `:id`, `:event`,
`:retry` optional) or a string already formatted by `sse/format-event`.
Responses carry `Content-Type: text/event-stream` + `Cache-Control: no-cache`,
chunked. `sse/send!` formats an event map and puts it on a channel, blocking,
for loops on a thread or future; it returns false once the client has gone.

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
  peer is gone. `(ws/close! session code)` and `(ws/close! session code
  reason)` put a status code (and a UTF-8 reason of at most 123 bytes) in the
  close frame, which a browser reports as the CloseEvent's `code` and
  `reason`; a code a peer may not receive (such as 1005) throws
- `(:request session)` is the upgrade request (`:uri`, `:query-string`,
  `:headers`, ...), so a handler can read a filter or the vhost the guard saw
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

## TLS

```clojure
(adapter/run-server handler {:port 8443
                             :tls {:cert-file "fullchain.pem"
                                   :key-file  "privkey.pem"}})
```

Both files are PEM, and `:cert-file` may hold the whole chain, leaf first. `run-server`
throws if a file is missing or the key doesn't match the certificate, rather than let
every handshake fail later. Requests carry `:scheme :https`, and websockets work over
it unchanged. Changing a certificate takes a restart.

It needs OpenSSL at runtime (`libssl.so.3` on Linux, Homebrew's `openssl@3` on macOS),
loaded only when `:tls` is set. macOS's system LibreSSL is not used. TLS 1.2 is the
floor, and client-initiated renegotiation is refused. It runs in the suite on macOS and
Linux; on Windows it has not been run.

OpenSSL is only the codec here: ciphertext moves over the same blocking or parking I/O
as plaintext, so `:keep-alive-timeout-ms`, `:request-timeout-ms` and
`:write-timeout-ms` all bound a TLS connection the same way. A handshake must also
finish within the smaller of `:keep-alive-timeout-ms` and 10 seconds as a whole, so a
client feeding it a byte at a time can't hold a worker. A client that doesn't speak TLS
is disconnected with no HTTP answer.

Keep-alive traffic costs little: plaintext with `ab -k -c 8`, an EC P-256 certificate,
went from 73k to 59k req/s on `:threads` and 19k to 18k on `:fibers`. New connections
are where the cost is, at about 500 full handshakes a second, so keep keep-alive on. An
idle TLS connection also holds more native memory than a plain one, for OpenSSL's state.

### Client certificates

```clojure
(adapter/run-server handler {:port 8443
                             :tls {:cert-file      "fullchain.pem"
                                   :key-file       "privkey.pem"
                                   :client-auth    :need
                                   :client-ca-file "client-ca.pem"}})
```

`:client-ca-file` is a PEM file of one or more CA certificates, and a client
certificate has to chain to one of them. Their names go out in the handshake's
certificate request, so a client holding several certificates can pick the right one.
`:client-auth` says what happens without one:

- `:need`: the handshake fails unless the client presents a certificate that verifies.
  The client gets a TLS alert and no HTTP answer.
- `:want`: a certificate is asked for and verified if one comes, but a client may send
  none and still be served.

A certificate that doesn't verify fails the handshake under either setting. Without
`:client-auth` (or with `nil` or `false`) no certificate is asked for, which is how it
was before. Either key on
its own throws at `run-server`, as do a `:client-auth` other than `:need` or `:want`
and a CA file that is missing or that OpenSSL can't read. A `:client-ca-file` alone
would look like it protects the server while asking no client for anything.

When a client did present a verified certificate, the request carries it as
`:ssl-client-cert`, a `java.security.cert.X509Certificate`, the key the Ring spec
names. A websocket session gets it on its `:request` too. Without one the key is
absent rather than nil. A certificate that verified but can't be turned into an
`X509Certificate` gets the client no answer, and the server counts it as a
`:tls-peer-cert` fault (see `:faults` above).

```clojure
(defn handler [request]
  (if-let [cert (:ssl-client-cert request)]
    {:status 200 :body (str "hello " (.getName (.getSubjectX500Principal cert)))}
    {:status 401 :body "no certificate"}))
```

The certificate class comes from [jolt-crypto](https://github.com/jolt-lang/crypto),
which this library already depends on. It is loaded only by a server that sets
`:client-auth`. Revocation (CRLs, OCSP) is not checked.

Not included: SNI with more than one certificate, and ALPN, so clients always get
HTTP/1.1.

## Tuning `:fibers`

Most of what a cheap request costs under `:fibers` is waking OS threads: the poller
when bytes arrive, then the carrier thread that runs the connection's fiber, then a
handler thread unless `:handler-on-fiber` is set. So the number of carriers matters more
than it looks. There is one per core by default, and with fewer, each is more likely to
be awake already with work queued. Plaintext with `ab -k -c 8` on ten cores:

| carriers | `:handler-on-fiber` | CPU per request | default | CPU per request |
|---:|---:|---:|---:|---:|
| 10 (default) | 38k req/s | 114 us | 18k req/s | 248 us |
| 4 | 40k req/s | 85 us | | |
| 2 | 32k req/s | 68 us | 18k req/s | 130 us |
| 1 | 19k req/s | 63 us | | |

Set it before the server starts:

```clojure
(alter-var-root #'clojure.core.async/*fiber-carrier-count* (constantly 4))
```

This only helps while handlers are cheap and don't block. With `:handler-on-fiber`, a
handler doing real CPU work needs the carriers, and a blocking foreign call holds up
more connections when there are fewer of them. Measure with your own handlers. Spinning
carriers or the poller briefly before they sleep was tried and is not worth having:
at c=8 it cost 30-50% of throughput for about 20% at c=1.

## Test

```bash
jolt -M:test   # over raw sockets + http-client
```

## License

EPL-2.0.
