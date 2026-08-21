# RFC-0010: multipart/form-data uploads

- Status: Accepted (wave 2, round 3)
- Prior art: Igropyr `express.sc` ~216–440 (`req-form`, `parse-multipart`,
  KMP boundary searcher, line-anchored delimiter matching, RFC 2046 bchar
  validation); `ring.middleware.multipart-params` for the Ring shape
- Depends on: [jolt-lang/multipart](https://github.com/jolt-lang/multipart)
  v0.1.0, an RFC 7578 parser in pure Clojure

## Summary

`ring-chez.middleware.multipart/wrap-multipart-params`: Ring's multipart
middleware, over a parser that loads under jolt. No new parser — the
library exists and this round's real work is proving it end-to-end through
the adapter and dressing its output in Ring's shape.

## Motivation

`ring.middleware.multipart-params` is written against Apache
commons-fileupload, which is a JVM library, so a Ring stack on jolt has no
way to accept a file upload at all. The workaround in the ring-app example
is to call a parser's API directly from the handler, which means the
handler knows about parsing and no middleware in the stack can see the
params.

Igropyr solves this in its framework layer (`req-form`), and its parser is
the reference for what "carefully" means here: KMP rather than naive search
because the boundary is chosen by the sender (at the 1 MiB body limit a
70-byte all-dash boundary against an all-dash body measures ~100 ms versus
~4 ms, and that is 100 ms the single-threaded scheduler spends on nothing
else), delimiters recognised only at a line start and only when followed by
CRLF or `--`, RFC 2046 bchar validation on the boundary itself.

jolt-lang/multipart, ported from `defnull/multipart`, already implements
that class of parser — incremental, limit-bounded, strict-mode-capable. The
gap is not a parser. The gap is Ring.

## Design

```clojure
(def app (multipart/wrap-multipart-params handler))
```

- `:multipart-params` on the request, merged into `:params`, per Ring.
- A text field's value is a string; an upload is
  `{:filename :content-type :bytes :size}` — Ring's `byte-array-store`
  shape plus `:size`, which its temp-file store also carries.
- Repeated names collect into a vector (Ring's `assoc-conj`), including
  across the text/upload split: a form with both a field and a file named
  `x` conjes rather than letting one overwrite the other.
- Anything that is not `multipart/form-data` passes through untouched,
  `:body` included — the body is read only when it is about to be parsed.
- Both handler arities are supported, so the middleware composes in an
  async Ring stack even though this adapter only calls the synchronous one.
- The parser's own extras (`:name`, `:headerlist`, `:charset`) are dropped
  from the Ring-facing value. Code wanting them can call
  `multipart.core/parse-form-data` directly; a middleware's job is to speak
  Ring.

**No temp-file store.** The parser buffers in memory and does not spool, so
the honest bound on an upload is the adapter's `:max-request-bytes`, and
the README says so rather than letting someone assume a 2 GB upload will
find its way to disk.

The dependency rides along the way `jolt-crypto` does: declared in
`deps.edn` so it resolves, required only by this namespace, so a server
that never touches uploads never loads it.

## Test plan

The point of this round is evidence, so the tests are end-to-end over a
real socket rather than unit tests of a parser that has its own suite:

- An upload of all 256 byte values, echoed back by the handler and compared
  byte-for-byte — anything that decodes or re-encodes the payload shows up
  as a mismatch instead of passing by luck. Field, filename, part
  content-type and size checked alongside, and the field checked in
  `:params` as well as `:multipart-params`.
- A 300 KB upload sent with `Transfer-Encoding: chunked` in 8 KB chunks, so
  part headers and boundaries land across read boundaries and across the
  adapter's own chunked decoding. Verified by size and a byte-sum checksum.
  Repeated field names in the same request collect into a vector.
- A truncated body (no closing delimiter): answered rather than hung, and
  the server still serves afterwards.
- A non-multipart POST: no `:multipart-params`, `:body` untouched.
