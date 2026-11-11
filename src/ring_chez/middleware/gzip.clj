(ns ring-chez.middleware.gzip
  "gzip response compression.

   jolt has no `java.util.zip`, so the usual Ring gzip middleware cannot load
   here. This is Igropyr's policy (express.sc `finish!`) over
   `ring-chez.zlib`:

       (require '[ring-chez.middleware.gzip :as gzip])
       (def app (gzip/wrap-gzip handler))

   A response is compressed when the client accepts gzip, the body is over
   `:min-size` (default 1024 — below that the gzip header costs more than it
   saves), and the content type is worth compressing. `Content-Encoding: gzip`
   and `Vary: Accept-Encoding` are set, and an `ETag` is given a distinct
   value, since a gzipped body is a different entity.

   In-memory bodies (a string, a byte array, or a seq/vector of those) are
   compressed whole. A `core.async` channel body — a server-sent event stream,
   say — is compressed as one gzip stream, each chunk flushed as it is written
   so the client sees it at once (`:stream?`, default true). The compressor
   keeps its window across chunks, so a chunk repeating what came before, as
   consecutive renders of one page mostly do, costs a few octets. A `File` or
   an `InputStream` passes through uncompressed: the one case where it clearly
   pays, static files, is better served by caching the compressed copy (see
   `ring-chez.middleware.static`).

   Where zlib is unavailable the middleware is a no-op, which is what
   Igropyr's `gzip-compress` returning #f means: uncompressed is always a
   correct answer.

   This is `ring-chez.middleware.compress/wrap-compress` offering gzip alone;
   use that to offer zstd as well."
  (:require [ring-chez.middleware.compress :as compress]))

(def default-min-size
  "Below this the gzip header and trailer cost more than the compression
  saves (Igropyr gzip-min-size)."
  compress/default-min-size)

(def default-compressible-types
  "Content-type prefixes worth compressing."
  compress/default-compressible-types)

(defn gzip-acceptable?
  "Does this Accept-Encoding value allow gzip? (Igropyr `gzip-acceptable?`.)

   Not a substring search: that would compress for a client that said
   `gzip;q=0` — which it sends precisely because it cannot decode it — and
   would fire on any unrelated coding containing those letters. The most
   specific verdict wins, so `*;q=0, gzip` still compresses, and the wildcard
   only decides when no entry names gzip."
  [accept-encoding]
  (when accept-encoding
    (= "gzip" (compress/choose-coding accept-encoding ["gzip"]))))

(defn gzip-response
  "Compress one response for one request, or return it unchanged. opts as in
  `wrap-gzip`."
  [request response opts]
  (compress/compress-response request response (assoc opts :encodings ["gzip"])))

(defn wrap-gzip
  "Middleware compressing responses the client will accept. opts:

     :min-size  smallest body worth compressing (default 1024)
     :types     content-type prefixes to compress (default
                `default-compressible-types`)
     :level     zlib compression level 1..9 (default 6)
     :stream?   compress channel bodies as a flushed gzip stream (default
                true)
     :window-bits, :mem-level
                zlib state size for each stream (9..15, default 15; 1..9,
                default 8). A stream holds its compressor for as long as it is
                open, ~256 KB at the defaults, which adds up across many open
                event streams; smaller values trade compression for memory

   Place it outermost, so it sees the finished response."
  ([handler] (wrap-gzip handler {}))
  ([handler opts]
   (compress/wrap-compress handler (assoc opts :encodings ["gzip"]))))
