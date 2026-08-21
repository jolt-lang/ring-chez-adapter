# RFC-0011: gzip response compression

- Status: Accepted (wave 2, round 4)
- Prior art: Igropyr `gzip.sc` (zlib via FFI, `deflateInit2` window-bits 31;
  `gzip-acceptable?`'s q-value walk), `express.sc` ~59–80 (`finish!`:
  1 KiB floor, compressible-type prefixes, `Vary`), ~892 (`gzip-etag`)
- Ported from: jolt-lang/http-client `jolt.http.zlib` (EPL-2.0, same
  license), narrowed to compression and made degradable

## Summary

`ring-chez.middleware.gzip/wrap-gzip`, over `ring-chez.zlib` — a lazily
bound, optional zlib. Igropyr's negotiation and policy, unchanged in
substance.

## Motivation

jolt has no `java.util.zip` (measured: `GZIPOutputStream` is absent under
v0.7.19), so every Ring compression middleware in existence fails to load
here. A Ring stack on jolt cannot compress a response at all.

Igropyr answers the same problem the same way — zlib through the FFI — and
its `gzip-acceptable?` is worth porting rather than reinventing, because
the obvious implementations are wrong in ways that matter:

- a substring search for "gzip" compresses for a client that sent
  `gzip;q=0`, which it sent *because it cannot decode gzip*;
- it also fires on an unrelated coding that merely contains those letters;
- and a wildcard has to lose to an explicit entry, so `*;q=0, gzip` still
  compresses.

## Design

### Binding zlib (`ring-chez.zlib`)

Symbols are resolved **from the running process first**, and a shared
object is loaded only if that fails. The Chez runtime links a zlib of its
own (it compresses fasl files with it), so on a normal jolt the symbols are
already present — measured here, `zlibVersion` resolves to 1.2.12 with no
`load-library` at all.

That order is not an optimisation, it is the safe one. Igropyr documents a
FreeBSD build where loading the system libz *alongside* the runtime's own
put two zlibs with identical global names in one process: `deflateInit2_`
reported success but left a `deflate_state` whose `sym_buf` held only the
low 32 bits of `pending_buf`, and `deflate()` then faulted as soon as the C
heap sat above 4 GB. Never loading a second copy avoids that class of bug
outright.

Binding is lazy (a `delay`) and failure is not an error: `available?`
answers false, `gzip` returns nil, and nil means "send it uncompressed" —
which is Igropyr's `gzip-compress` returning `#f`, and which is always a
correct way to answer a request. A throw from the compressor is caught to
the same effect.

### Policy (`wrap-gzip`)

Compress when all of: the client accepts gzip, the body exceeds
`:min-size` (default 1024 — below it the header and trailer cost more than
the compression saves), the content type is in `:types`, the handler did
not set its own `Content-Encoding`, and zlib is available. Then set
`Content-Encoding: gzip`, add `Accept-Encoding` to `Vary`, and give any
`ETag` a distinct value (`"abc"` → `"abc-gz"`): a gzipped body is a
different entity, and sharing a validator lets a cache serve one for the
other.

`Content-Length` is dropped from the map rather than recomputed. The
adapter frames from the octets it actually writes, so a stale length cannot
desynchronise the connection — but leaving a wrong one in the map would
still lie to any middleware between here and the adapter.

**In-memory bodies only.** A string, a byte array, or a seq/vector of
those. A `File`, an `InputStream` or a `core.async` channel passes through:
compressing a stream means framing deflate output chunk by chunk with
`Z_SYNC_FLUSH`, and the one case where that clearly pays — static files —
is better served by caching the compressed copy, which is what Igropyr does
and what round 5 does.

## Alternatives considered

- **Depending on jolt-lang/http-client for `jolt.http.zlib`.** It is the
  same code, but it would pull an HTTP client and a TLS stack into a server
  library for the sake of one file. Ported with attribution instead.
- **Declaring libz in `:jolt/native`.** Loads it for every user of the
  adapter, including those who never compress anything, and reintroduces
  the two-zlib hazard above.
- **Compressing channel bodies.** Deferred; see above.

## Test plan

The oracle is `jolt.http.zlib/gunzip` — a separate implementation, already
a test dependency — and the requests go over a raw socket, because the HTTP
client transparently decodes gzip and would hide exactly what is under
test.

- A compressible body comes back gzipped, smaller, and gunzips to the
  original; a seq body compresses as the octets it concatenates to.
- **Framing**: a second request on the same connection is answered
  correctly, which is what proves `Content-Length` counts the compressed
  octets rather than the plain ones.
- Negotiation, one check each: absent header, `identity`, `gzip`,
  `deflate, gzip`, `gzip;q=0.5`, `gzip;q=0`, `gzip;q=0.0`, `*`,
  `*;q=0, gzip`, `*, gzip;q=0`, and `notgzip`.
- Skips: under the floor, `image/png`, and a body the handler already
  encoded (whose own `Content-Encoding` survives).
- `ETag` gets `-gz` when compressed and is untouched when not.
