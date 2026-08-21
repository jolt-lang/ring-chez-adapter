# RFC-0009: handler execution deadline and failure context

- Status: Accepted (wave 2, round 2)
- Prior art: Igropyr `http.sc` ~1860 (`stuck-ms`, default 30000, and
  `check-ms` — the supervisor kills a worker stuck past the threshold and
  answers through `on-failure` with `kind=stuck`), `express.sc` ~499
  (`make-fault-handler`: a 503 envelope on a kept-alive connection, so the
  client can resubmit)

## Summary

New opt `:handler-timeout-ms` (default 60000, `0` disables): a Ring handler
that has not returned within it is abandoned, and the request is answered
through the existing failure path — `:on-failure` first, then a plain
`503`. Adds the context Igropyr's `info` alist carries, as
`:ring-chez/failure` on the request the hook receives, and
`ring-chez.fault/fault-handler` as the ready-made hook.

## Motivation

Every other way a request can stall is bounded: `:keep-alive-timeout-ms`
and `:request-timeout-ms` bound the read, `:write-timeout-ms` bounds the
write. The handler itself is unbounded, and it is the one part of the
system the adapter does not write.

- **`:threads`** — the handler runs inline on the worker. One that never
  returns holds that worker forever; `:worker-threads` of them and the
  server is dead with the listen socket still accepting.
- **`:fibers`** — quieter and worse. `connection-loop` deliberately sets
  the sweeper deadline to `no-deadline` before invoking the handler (a
  slow handler must not be reaped as a slow *peer*), so a handler that
  never returns leaks its fd, its fiber and its `async/thread` thread, and
  nothing reaps any of them.

Igropyr's answer is a supervisor that kills the worker at `stuck-ms` and
replaces it. We cannot kill: jolt has no safe thread kill, and a Ring
handler holds no supervisor-visible state. What we can do is stop *waiting*
for it — which reclaims everything except the thread.

## Design

### Abandoning the handler

`io` grows `:run-timed!`, the per-strategy "run this, but not past ms":

```clojure
;; threads
(fn [f ms]
  (if (zero? ms)
    (f)
    (let [ch (async/thread (f))
          [v p] (async/alts!! [ch (async/timeout ms)])]
      (if (= p ch) v ::timeout))))
```

and the same shape with `alts!` inside the fiber's go block. The port, not
the value, decides: a handler that legitimately returns `nil` delivers
`[nil ch]`, which is the existing nil-response path, not a timeout.

With a deadline configured the handler no longer runs on the worker thread
— it cannot be abandoned from its own stack. Under `:fibers` this costs
nothing: `:run!` already moved it to `async/thread`. Under `:threads` it is
a thread handoff per request, which is why `0` is a supported answer and
why the number is measured in the test plan rather than assumed.

### Why keep-alive survives a timeout

The abandoned handler cannot interfere with the connection: it was handed a
`ByteArrayInputStream` over a body already fully read, and it writes
nothing — it returns a map, to a channel nobody is reading any more. So the
503 goes out through the normal response path and the connection stays
usable, which is what makes Igropyr's fault envelope useful: the client
resubmits on the same connection.

The thread is not reclaimed. That is the honest cost of not being able to
kill one, it is documented in the README, and it is why the default is a
minute rather than a second: a deadline that fires on merely-slow handlers
would leak threads for a living.

### Only one hook call per request

A handler that throws *just* as the deadline fires would otherwise call
`:on-failure` twice for one request — once from the handler thread's catch,
once from the timeout path. A per-request claim (`compare-and-set!`) gates
the hook invocation: whoever gets there first calls it, the loser's
response is discarded along with the rest of the abandoned work. The client
is always answered by the timeout path, because the worker has moved on.

### Failure context

`:on-failure` keeps its `(request throwable)` shape — existing hooks are
2-arity and there is no reason to break them. The context goes on the
request instead, which is where an adapter is expected to add things:

```clojure
{:ring-chez/failure {:kind :timeout      ; :crash :nil-response :timeout
                                          ; :ws-guard :ws-session
                     :elapsed-ms 60003}}
```

`:kind` is derived from the ex-data type when the call site does not name
one, so a plain handler throw is `:crash` without anyone having to tag it.
The fallback status follows the kind: `503` for `:timeout` (the request may
well succeed on retry), `500` for everything else, as today.

`ring-chez.fault/fault-handler` is Igropyr's `make-fault-handler`: an
`:on-failure` that answers a small JSON envelope
(`{"fault":"timeout","elapsed-ms":60003,"retryable":true}`) at a
configurable status instead of the plain text. Every value in it is
produced by the adapter, so the string is built without a JSON dependency.

## Alternatives considered

- **Watchdog that closes the connection, handler left inline.** Cheaper —
  no handoff — and it frees the client, but it does not free the worker,
  which is the whole problem on `:threads`. Rejected.
- **`Thread/interrupt` the stuck worker.** Only helps a handler blocked in
  an interruptible call; a CPU spin or a blocking FFI call ignores it, and
  the worker's state afterwards is undefined. Rejected.
- **Igropyr's retry ring (`max-retries`).** Re-running a handler that has
  already touched a database is worse than a 500, and Ring gives us no
  "has it answered yet" token to gate on. The fault envelope is the half of
  the ring that is ours to offer.
- **Third argument on `:on-failure`.** Breaks every existing hook to carry
  what a namespaced request key carries for free.

## Drawbacks

- A thread handoff per request on `:threads` whenever the deadline is on.
- An abandoned handler's thread is never reclaimed. A server whose handlers
  routinely hang will run out of threads — later than it would have run out
  of workers, and with the difference that it keeps answering until then.
- A legitimately slow handler (a report, a big export) now needs
  `:handler-timeout-ms` raised or set to `0`.

## Test plan

- Handler that never returns: client gets `503` within a margin of the
  deadline, and the *next* request on a one-worker server is served — the
  worker was reclaimed. Both strategies.
- `:handler-timeout-ms 0`: the same handler hangs the request (bounded by
  the test's own client timeout), proving the option is what does it.
- A handler slower than a short deadline but faster than the client
  timeout, with the deadline raised, still answers normally — the deadline
  cuts nothing it should not.
- `:on-failure` sees `{:kind :timeout :elapsed-ms n}` and its response is
  served in place of the 503.
- Hook called exactly once when a handler throws at the deadline boundary.
- SSE/streaming: a handler that returns a channel immediately and streams
  for longer than the deadline is not cut off (the deadline covers handler
  *execution*, not the stream).
- `fault-handler` produces the documented envelope.
- Benchmark `/plaintext` with the deadline on and off, and report the
  delta in the PR.
