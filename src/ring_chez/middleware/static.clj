(ns ring-chez.middleware.static
  "Static file serving with an in-memory hot cache.

   `ring.middleware.file` works under jolt, but it stats and re-opens the file
   on every request. Igropyr answers a hot file from memory instead — a
   hashtable lookup, no disk read and no `stat` syscall, with the file
   re-checked at most once a second (nginx's `open_file_cache_valid` works the
   same way) — and keeps the gzipped copy beside the plain one. This is that,
   in Ring's shape:

       (require '[ring-chez.middleware.static :as static])
       (def app (static/wrap-static handler \"public\"))
       (def app (static/wrap-static handler \"public\"
                                    {:prefix \"/assets\"
                                     :cache-control \"public, max-age=3600\"}))

   A hit answers with `Content-Type`, a weak `ETag`, and — when the client
   accepts it — a cached gzip copy. `If-None-Match` gets a `304`.

   A miss, a path that is not safe, or any method other than GET/HEAD falls
   through to the wrapped handler, as `wrap-file` does: a traversal attempt
   should not be able to tell a 403 from a 404.

   Files over `:max-cache-file-bytes` are not held in memory; they are handed
   to the adapter as a `File` body, which streams them in bounded chunks with a
   real `Content-Length`. Their metadata is still cached, so revalidating one
   costs no file operations at all."
  (:require [clojure.string :as str]
            [ring-chez.zlib :as zlib])
  (:import [java.io File]))

(def default-mime-types
  "Extension → content type. Igropyr's table plus what a modern asset pipeline
  emits. Anything unknown is `application/octet-stream`, which is what RFC 9110
  says to send when you do not know."
  {"html" "text/html; charset=utf-8"   "htm"  "text/html; charset=utf-8"
   "css"  "text/css; charset=utf-8"    "js"   "application/javascript"
   "mjs"  "application/javascript"     "json" "application/json"
   "map"  "application/json"           "txt"  "text/plain; charset=utf-8"
   "xml"  "application/xml"            "svg"  "image/svg+xml"
   "png"  "image/png"                  "jpg"  "image/jpeg"
   "jpeg" "image/jpeg"                 "gif"  "image/gif"
   "webp" "image/webp"                 "avif" "image/avif"
   "ico"  "image/x-icon"               "pdf"  "application/pdf"
   "woff" "font/woff"                  "woff2" "font/woff2"
   "ttf"  "font/ttf"                   "otf"  "font/otf"
   "wasm" "application/wasm"           "csv"  "text/csv; charset=utf-8"
   "md"   "text/markdown; charset=utf-8"
   "mp4"  "video/mp4"                  "webm" "video/webm"
   "zip"  "application/zip"            "gz"   "application/gzip"})

(def ^:private default-opts
  {:index-files?         true
   :prefix               nil
   :allow-symlinks?      false
   :cache?               true
   :stat-window-ms       1000      ; how stale a cache entry may be
   :max-cache-file-bytes (* 1024 1024)
   :max-cache-bytes      (* 64 1024 1024)
   :max-cache-entries    4096
   :gzip?                true
   :gzip-min-size        1024
   :cache-control        nil       ; Igropyr's own default is "public, max-age=3600"
   :mime-types           default-mime-types})

;; --- paths ------------------------------------------------------------------

(defn- percent-decode
  "Decode %XX, as UTF-8. Ring leaves percent-decoding to middleware, so the
  request path arrives encoded — and it has to be decoded BEFORE the safety
  checks below, or `%2e%2e` walks straight past the `..` test."
  [^String s]
  (if-not (str/includes? s "%")
    s
    (let [out (java.io.ByteArrayOutputStream.)
          n   (count s)]
      (loop [i 0]
        (if (>= i n)
          (String. (.toByteArray out) "UTF-8")
          (let [c (.charAt s i)]
            (if (and (= \% c) (<= (+ i 2) (dec n)))
              (let [hex (subs s (inc i) (+ i 3))]
                (if-let [b (try (Integer/parseInt hex 16) (catch Throwable _ nil))]
                  (do (.write out (int b)) (recur (+ i 3)))
                  (do (.write out (int c)) (recur (inc i)))))
              (do (.write out (int c)) (recur (inc i))))))))))

(defn- extension [^String name]
  (when-let [i (str/last-index-of name ".")]
    (str/lower-case (subs name (inc i)))))

(defn- dotfile-segment?
  "A segment starting with \".\" is refused: mounting a project directory
  otherwise serves .env and .git/config. \".well-known\" is the standard
  exception (ACME challenges, security.txt) and stays reachable (Igropyr)."
  [seg]
  (and (str/starts-with? seg ".")
       (not= seg ".")
       (not= seg ".well-known")))

(defn- safe-segments
  "The decoded path split into segments safe to resolve under a root, or nil.
  Rejects `..`, any dotfile segment, and an embedded NUL — which can truncate
  a path in a lower-level file API and slip past a suffix check
  (\"safe.txt\\u0000.jpg\")."
  [^String rel]
  (let [rel (percent-decode rel)]
    (when-not (str/includes? rel "\u0000")
      (let [segs (remove #(or (= "" %) (= "." %)) (str/split rel #"/"))]
        (when-not (some #(or (= ".." %) (dotfile-segment? %)) segs)
          (vec segs))))))

(defn- real-path
  "The path with symlinks resolved, or nil.

  NOT `getCanonicalPath`: under jolt that only absolutizes — measured, a
  symlink at /tmp/x/link.txt pointing to /tmp/secret.txt canonicalizes to
  itself, while `toRealPath` gives /private/tmp/secret.txt. Every Ring static
  middleware writes its containment check with `getCanonicalPath`, so on jolt
  that check silently does not hold and a symlink inside the root serves
  whatever it points at."
  ^String [^File f]
  (try
    (str (.toRealPath (java.nio.file.Paths/get (.getPath f) (into-array String []))
                      (into-array java.nio.file.LinkOption [])))
    (catch Throwable _ nil)))

(defn- under-root?
  "The resolved file really is inside root, symlinks and all.

  Weaker than Igropyr's openat walk with O_NOFOLLOW, which cannot be raced — a
  symlink swapped between this check and the read would defeat it. It runs on
  a cache miss, not per request. Anything that cannot be resolved is refused."
  [^File root ^File f]
  (let [rp (real-path root)
        fp (real-path f)]
    (boolean (and rp fp (or (= rp fp)
                            (str/starts-with? fp (str rp File/separator)))))))

(defn- resolve-file
  "The File a request path names under root, or nil when it is not a readable
  regular file inside it."
  [^File root ^String rel {:keys [index-files? allow-symlinks?]}]
  (when-let [segs (safe-segments rel)]
    (let [f (reduce (fn [^File acc seg] (File. acc ^String seg)) root segs)
          f (if (and index-files? (.isDirectory f)) (File. f "index.html") f)]
      (when (and (.isFile f)
                 (.canRead f)
                 (or allow-symlinks? (under-root? root f)))
        f))))

(defn- strip-prefix
  "The path under a mount prefix, or nil when the request is not under it.
  \"/assets-private\" must not match the mount \"/assets\" (Igropyr
  static-relative)."
  [prefix ^String uri]
  (cond
    (str/blank? prefix) uri
    (= uri prefix) ""
    (str/starts-with? uri (str prefix "/")) (subs uri (inc (count prefix)))
    :else nil))

;; --- the cache --------------------------------------------------------------
;;
;; Keyed by the REQUEST path, not the resolved file: a hit inside the stat
;; window must not touch the filesystem at all, and resolving the file is
;; itself several syscalls (isFile, canRead, two getCanonicalPath). That is
;; the whole difference from wrap-file.
;;
;; entry: {:file :size :mtime :etag :content-type :bytes :gzip :checked-at
;;         :stored-at}. :bytes is nil for a file too large to cache, which is
;; served as a File body; its metadata is cached all the same, so a
;; revalidation of a large file costs nothing (Igropyr).
;;
;; Eviction is FIFO by :stored-at rather than LRU: LRU means writing to this
;; atom on every hit, and the point of the cache is that a hit is a read.

(defn new-cache
  "A cache, so mounts can share one or be kept deliberately apart."
  []
  (atom {:entries {} :bytes 0}))

(def ^:private shared-cache (new-cache))

(defn- etag-of
  "Weak validator over size and mtime (Igropyr etag-of). Weak because two
  writes inside one mtime tick are indistinguishable — which is also why it is
  a fine cache key and a poor integrity check."
  [size mtime]
  (str "W/\"" (Long/toHexString size) "-" (Long/toHexString mtime) "\""))

(defn- read-all [^File f ^long size]
  (let [buf (byte-array size)]
    (with-open [in (java.io.FileInputStream. f)]
      (loop [off 0]
        (if (>= off size)
          buf
          (let [n (.read in buf off (- size off))]
            (if (neg? n)
              (java.util.Arrays/copyOfRange buf 0 off)
              (recur (+ off n)))))))))

(defn- entry-bytes ^long [entry]
  (if-let [^bytes b (:bytes entry)] (alength b) 0))

(defn- build-entry [^File f now {:keys [mime-types max-cache-file-bytes]}]
  (let [size  (.length f)
        mtime (.lastModified f)]
    {:file         f
     :size         size
     :mtime        mtime
     :etag         (etag-of size mtime)
     :content-type (get mime-types (extension (.getName f)) "application/octet-stream")
     :bytes        (when (<= size max-cache-file-bytes)
                     (try (read-all f size) (catch Throwable _ nil)))
     :gzip         (atom nil)
     :checked-at   now
     :stored-at    now}))

(defn- evict-to-fit
  "Drop oldest-stored entries until the incoming one fits inside both caps."
  [cache incoming {:keys [max-cache-bytes max-cache-entries]}]
  (loop [entries (:entries cache)
         bytes   (:bytes cache)]
    (if (and (<= (+ bytes incoming) max-cache-bytes)
             (< (count entries) max-cache-entries))
      (assoc cache :entries entries :bytes bytes)
      (if-let [[k e] (first (sort-by (comp :stored-at val) entries))]
        (recur (dissoc entries k) (- bytes (entry-bytes e)))
        (assoc cache :entries {} :bytes 0)))))

(defn- store! [cache key entry opts]
  (swap! cache (fn [c]
                 (let [incoming (entry-bytes entry)
                       ;; replacing an entry returns its bytes to the budget
                       c (if-let [old (get-in c [:entries key])]
                           (-> c (update :entries dissoc key)
                                 (update :bytes - (entry-bytes old)))
                           c)
                       c (evict-to-fit c incoming opts)]
                   (-> c
                       (assoc-in [:entries key] entry)
                       (update :bytes + incoming)))))
  entry)

(defn- current?
  "The file behind an entry is still the one it was built from. One stat."
  [entry]
  (let [^File f (:file entry)]
    (and (= (:mtime entry) (.lastModified f))
         (= (:size entry) (.length f)))))

(defn- build-and-store!
  "Resolve and read the file behind a request path, or forget it: a cached
  entry whose file has gone must not outlive it."
  [cache key ^File root rel now opts]
  (if-let [f (resolve-file root rel opts)]
    (store! cache key (build-entry f now opts) opts)
    (do (swap! cache (fn [c]
                       (if-let [old (get-in c [:entries key])]
                         (-> c (update :entries dissoc key)
                               (update :bytes - (entry-bytes old)))
                         c)))
        nil)))

(defn- lookup
  "The entry for a request path, or nil when it names no servable file.

  A hit inside the stat window returns without a single filesystem call,
  which is the point: resolving the file is itself several syscalls (isFile,
  canRead, two getCanonicalPath) before anything is read. Past the window one
  stat decides between renewing the entry and rebuilding it. Misses are not
  cached — a file that appears after a 404 must be served, not remembered as
  absent."
  [cache ^File root rel opts]
  (let [key (str (.getPath root) "\u0000" rel)
        now (System/currentTimeMillis)]
    (if-not (:cache? opts)
      (some-> (resolve-file root rel opts) (build-entry now opts))
      (let [e (get-in @cache [:entries key])]
        (cond
          (nil? e) (build-and-store! cache key root rel now opts)

          (< (- now (:checked-at e)) (:stat-window-ms opts)) e

          (current? e) (do (swap! cache assoc-in [:entries key :checked-at] now)
                           (assoc e :checked-at now))

          :else (build-and-store! cache key root rel now opts))))))

;; --- responses --------------------------------------------------------------

(defn- gzip-etag
  "A gzipped body is a different entity and must not share the plain one's
  validator, or a cache can serve one for the other (Igropyr gzip-etag)."
  [etag]
  (if (str/ends-with? etag "\"")
    (str (subs etag 0 (dec (count etag))) "-gz\"")
    (str etag "-gz")))

(defn- accepts-gzip?
  "The same rule as ring-chez.middleware.gzip's parser, inline: an entry
  naming gzip without q=0. Requiring that namespace here would make static
  depend on it for one predicate."
  [request]
  (when-let [ae (get-in request [:headers "accept-encoding"])]
    (boolean
     (some (fn [entry]
             (let [parts (str/split entry #";")
                   nm    (str/lower-case (str/trim (or (first parts) "")))]
               (and (contains? #{"gzip" "x-gzip"} nm)
                    (not (some (fn [p]
                                 (let [p (str/trim p)]
                                   (and (str/starts-with? (str/lower-case p) "q=")
                                        (try (zero? (Double/parseDouble (str/trim (subs p 2))))
                                             (catch Throwable _ false)))))
                               (rest parts))))))
           (str/split (str ae) #",")))))

(defn- compressible? [content-type]
  (let [ct (str/lower-case (str content-type))]
    (boolean
     (some #(str/starts-with? ct %)
           ["text/" "application/json" "application/javascript" "application/xml"
            "image/svg+xml" "application/wasm"]))))

(defn- gzipped-body
  "The cached gzip copy, compressed once on first use. nil when gzip is not
  wanted, not worth it, or unavailable."
  [entry request {:keys [gzip? gzip-min-size]}]
  (when (and gzip?
             (:bytes entry)
             (> (:size entry) gzip-min-size)
             (compressible? (:content-type entry))
             (accepts-gzip? request)
             (zlib/available?))
    (or @(:gzip entry)
        (when-let [^bytes gz (zlib/gzip (:bytes entry))]
          ;; only when it actually saved something — a compressed file that
          ;; slipped past the type check must not be served larger
          (when (< (alength gz) (:size entry))
            (reset! (:gzip entry) gz))))))

(defn- entry-response [entry request opts]
  (let [gz      (gzipped-body entry request opts)
        etag    (cond-> (:etag entry) gz gzip-etag)
        headers (cond-> {"Content-Type" (:content-type entry)
                         "ETag"         etag}
                  gz (assoc "Content-Encoding" "gzip" "Vary" "Accept-Encoding")
                  (:cache-control opts) (assoc "Cache-Control" (:cache-control opts)))
        ;; a file too large to cache is handed over as a File: the adapter
        ;; streams it in bounded chunks with a real Content-Length rather than
        ;; holding it in memory to write
        body    (or gz (:bytes entry) (:file entry))]
    (if (= etag (get-in request [:headers "if-none-match"]))
      ;; the body stays on a 304 and the adapter writes none of it — that is
      ;; how it reports the Content-Length a GET would have had (RFC 9110 15.4.5
      ;; permits it, and for a File it costs one .length and no open)
      {:status 304 :headers headers :body body}
      {:status 200 :headers headers :body body})))

(defn file-response
  "The Ring response for one file — cached, conditional and gzip-aware. Public
   so a handler can serve a file directly with the same caching."
  ([^File f request] (file-response f request {}))
  ([^File f request opts]
   (let [opts  (merge default-opts opts)
         cache (or (:cache opts) shared-cache)
         key   (str "\u0000file\u0000" (.getPath f))
         now   (System/currentTimeMillis)
         e     (get-in @cache [:entries key])
         entry (cond
                 (and e (< (- now (:checked-at e)) (:stat-window-ms opts))) e
                 (and e (current? e)) (do (swap! cache assoc-in [:entries key :checked-at] now)
                                          (assoc e :checked-at now))
                 :else (store! cache key (build-entry f now opts) opts))]
     (entry-response entry request opts))))

(defn static-request
  "The response for a static request, or nil to fall through to the handler."
  [request root opts]
  (let [opts (merge default-opts opts)
        root (if (instance? File root) root (File. (str root)))]
    (when (contains? #{:get :head} (:request-method request))
      (when-let [rel (strip-prefix (:prefix opts) (:uri request))]
        (when-let [entry (lookup (or (:cache opts) shared-cache) root rel opts)]
          (entry-response entry request opts))))))

(defn wrap-static
  "Middleware serving files under `root` before the handler sees the request.
   GET and HEAD only; anything not found, not safe, or not a file falls
   through. opts:

     :prefix               mount point to strip, e.g. \"/assets\" (default: none,
                           the whole :uri is the path under root)
     :index-files?         serve index.html for a directory (default true)
     :allow-symlinks?      follow links out of the root (default false)
     :cache?               keep files in memory (default true)
     :stat-window-ms       how stale a cache entry may be (default 1000)
     :max-cache-file-bytes largest file to hold in memory (default 1 MiB;
                           larger files stream from disk)
     :max-cache-bytes      total cache cap (default 64 MiB)
     :max-cache-entries    entry-count cap (default 4096)
     :gzip?                cache and serve a gzip copy (default true)
     :gzip-min-size        smallest file worth compressing (default 1024)
     :cache-control        value for the header (default: none set)
     :mime-types           extension → content type map
     :cache                a `new-cache` to use instead of the shared one"
  ([handler root] (wrap-static handler root {}))
  ([handler root opts]
   (fn
     ([request] (or (static-request request root opts) (handler request)))
     ([request respond raise]
      (if-let [response (static-request request root opts)]
        (respond response)
        (handler request respond raise))))))
