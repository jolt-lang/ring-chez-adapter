# RFC-0015: a streaming response notices a client that left while it was quiet

- Status: Accepted
- Kind: bug fix
- Related: RFC-0009 (handler deadline), RFC-0014 (fd ownership)

## Summary

A channel body only discovered its client was gone by failing to write to it.
An application that had nothing to send — the normal state of an SSE stream
between events — never wrote, so it never discovered anything: the connection
stayed in `CLOSE-WAIT` for the life of the process, the worker stayed parked in
the channel take, and the application kept a producer alive for a reader that
had gone home. Under `:threads`, one such connection per worker takes the
server down.

`stream-body` now waits for the next chunk with a bounded take, and each time
the wait expires it asks whether the far end is still there. When it is not,
the body channel is closed and the connection released, exactly as if a write
had failed.

## Motivation

Two clients open a stream against a server with two workers, read the first
event, and disappear. The application, having nothing new to publish, writes
nothing. Measured on `main`:

```
  baseline page:                                200
  open channels the app still holds:            2
  sockets in CLOSE-WAIT:                        2
  page after 2 abandoned idle streams:          000 in 4.001699s   (timeout)
  3s later, page:                               000 in 4.001133s   (timeout)
  channels closed by the adapter?               [true true]        (still open)
```

The server never answers again. Under `:fibers` no worker is pinned, so pages
still serve, but the connection, the fd and the application's channel leak just
the same — and an SSE application that renders on every state change goes on
rendering for an audience of nobody.

The existing coverage misses this because it tests the *loud* case:
`test-stream-client-disconnect-aborts` has the handler produce every 150ms, so
a write fails almost immediately. A stream is not obliged to be loud.

## Design

`socket/peer-gone?` answers the question without consuming anything:

```clojure
(defn peer-gone? [fd] ...)   ; poll(2) with a zero timeout, then a peeking recv
```

- `POLLHUP` / `POLLERR` / `POLLNVAL` is conclusive: the peer is gone.
- `POLLIN` is **not** conclusive. A client that pipelines its next request
  while a stream runs is readable and very much present, so readability is
  settled by a `recv(..., MSG_PEEK)`: `0` is end of stream, anything positive
  is a peer that is still talking — and the peek leaves those bytes queued for
  the reader they belong to.
- `EINTR` is not an answer either way, so it asks again.

The strategies decide how to wait, because that is the only part that differs:

```clojure
;; threads
:take! (fn [ch conn]
         (loop []
           (let [[v p] (async/alts!! [ch (async/timeout stream-idle-check-ms)])]
             (cond (= p ch)                 v
                   (socket/peer-gone? conn) http/peer-gone
                   :else                    (recur)))))
;; fibers: the same with alts!, so the fiber parks rather than the thread
```

`stream-body` treats `http/peer-gone` the way it already treats a failed write:
close the channel, return false, let the caller retire the connection.

`stream-idle-check-ms` is 1000. That is one `poll(2)` per idle stream per
second — the price of not holding a worker, an fd and a producer for a client
that has already left.

## Results

Same scenario, this branch:

```
  sockets in CLOSE-WAIT:                        0
  page after 2 abandoned idle streams:          200 in 0.001040s
  3s later, page:                               200 in 0.009110s
  channels closed by the adapter?               [false false]      (closed)
```

Identical under `:fibers`.

Three tests cover it, run under both strategies where it matters:

- `test-stream-idle-client-disconnect-reclaims` — with `:worker-threads 1`, so
  the follow-up request only succeeds if the abandoned stream let go.
- `test-stream-idle-client-disconnect-reclaims-fibers` — same, on the poller.
- `test-stream-pipelined-request-is-not-a-disconnect` — a client that pipelines
  mid-stream keeps its stream, which is what the `MSG_PEEK` is for.

## Note on the test dependency

The suite could not run at all while the `http-client` pin was v0.0.5: on jolt
0.8.1 that release raises `UnknownHostException` for every host, `127.0.0.1`
included, before it reaches a socket — it writes sockaddr bytes with the
pre-0.8.0 `ffi/write` argument order. v0.0.6 carries the fix, and main had
already moved the pin there. This is a test-only dependency; nothing in `src/`
uses it.
