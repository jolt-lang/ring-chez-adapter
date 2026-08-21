# RFC-0012: static file serving with a hot cache

- Status: Accepted (wave 2, round 5)
- Prior art: Igropyr `express.sc` ~554–966 (the static cache: per-file and
  total caps, 1 s stat window, weak ETag from size+mtime, gzip copy cached
  beside the plain one, large files streamed, dotfile refusal with a
  `.well-known` exception, `safe-static-path`)

## Summary

`ring-chez.middleware.static/wrap-static`: files served from an in-memory
cache keyed by request path, conditional requests answered from metadata,
gzip copies cached, files over the cap streamed from disk as a `File` body.

## Motivation

`ring.middleware.file` works under jolt, so this is not a gap in
capability — it is a gap in cost and in safety.

**Cost.** `wrap-file` resolves, stats and opens on every request. Igropyr's
static path is a hashtable lookup with the mtime re-checked at most once a
second (nginx's `open_file_cache_valid`, default 60 s there, is the same
idea). For the common case — an unchanged `index.html`, `app.css`,
`app.js` — that is the difference between a syscall sequence per request
and a map lookup.

**Safety.** `ring.middleware.file` decides containment with
`getCanonicalPath`. Through jolt 0.7.19 that method does not resolve
symlinks — it only absolutizes. Measured:

```
/tmp/symprobe/root/link.txt -> /tmp/symprobe/secret.txt

  .getCanonicalPath  => /tmp/symprobe/root/link.txt      (unresolved!)
  Path/toRealPath    => /private/tmp/symprobe/secret.txt
```

So the containment check every Ring static middleware is written with
silently does not hold there, and a symlink planted inside a served root
hands out whatever it points at. This middleware uses `toRealPath`, and
the test that proves it plants exactly such a link.

The runtime bug is fixed upstream in jolt#693 (`getCanonicalPath` is
`realpath(3)` now). `toRealPath` stays, because it is correct on every jolt
version and this library supports the released ones.

## Design

- **Cache keyed by request path**, not by resolved file. A hit inside the
  stat window must touch the filesystem *not at all*, and resolving a path
  is itself several syscalls (`isFile`, `canRead`, two real-path
  resolutions) before anything is read. Keying by the file would mean doing
  all of that first, which is the cost the cache exists to avoid.
- **Entry**: `{:file :size :mtime :etag :content-type :bytes :gzip
  :checked-at :stored-at}`. `:bytes` is nil for a file over
  `:max-cache-file-bytes` (1 MiB); its *metadata* is cached anyway, so
  revalidating a large file costs no file operations at all (Igropyr's
  "large-entry, window hit" case).
- **ETag** `W/"<size-hex>-<mtime-hex>"` (Igropyr `etag-of`). Weak, because
  two writes inside one mtime tick are indistinguishable — which is what
  makes it a fine cache key and a poor integrity check.
- **304** keeps the body in the response map. The adapter writes no body
  for a 304 but frames `Content-Length` from it, which is how it reports
  the length a `GET` would have had (RFC 9110 15.4.5 permits this); for a
  `File` that costs one `.length` and no open.
- **gzip** copies are cached beside the plain bytes and compressed on first
  use, with the distinct `-gz` ETag and `Vary: Accept-Encoding`. Compressed
  output is only kept when it actually came out smaller.
- **Eviction is FIFO by `:stored-at`**, not LRU: LRU means writing to the
  cache atom on every hit, and the point of the cache is that a hit is a
  read.
- **Misses are not cached.** A file that appears after a 404 must be
  served, not remembered as absent. An entry whose file has since gone is
  dropped.
- **Fall through, don't answer.** A miss, an unsafe path, or a method other
  than GET/HEAD returns nil and the wrapped handler runs — as `wrap-file`
  does, and so a traversal attempt cannot tell a 403 from a 404.
- **Percent-decode before validating.** Ring leaves decoding to middleware,
  so the path arrives encoded; decoding after the `..` check would let
  `%2e%2e` walk straight past it.
- **Dotfiles refused**, `.well-known` excepted (ACME challenges,
  security.txt) — Igropyr's rule, and the reason is its comment: mounting a
  project directory otherwise serves `.env` and `.git/config`.

## Drawbacks

- `toRealPath` containment is still a TOCTOU check: a symlink swapped
  between the check and the read defeats it. Igropyr's openat walk with
  `O_NOFOLLOW` cannot be raced; jolt has no openat binding here. The check
  runs on a cache miss, not per request, which narrows but does not close
  the window.
- FIFO eviction can drop a hot file kept longer than a cold one.
- No `Last-Modified` / `If-Modified-Since`: the ETag path covers
  revalidation, and adding date parsing for a second validator is not worth
  it until someone needs it.
- No range requests. A large file streams whole.

## Test plan

Against a throwaway tree containing an index, a css file over the gzip
floor, a 1.5 MB binary over the cache cap, a dotfile, and a symlink
pointing outside the root:

- serving, content type, ETag, directory index, nested path; a miss and a
  POST both fall through to the handler
- `If-None-Match` → 304 with no body; a stale validator → 200
- gzip: encoded, `Vary`, `-gz` ETag, gunzips to the original, second hit
  byte-identical (the cached copy), plain when not accepted, untouched
  under the floor
- large file: `Content-Length: 1500000`, full body streamed, content intact
- safety: dotfile, `%2e%2e` traversal, `%2Eenv`, and the symlink are all
  refused while ordinary files still serve
- cache: a file edited past the stat window is re-read
