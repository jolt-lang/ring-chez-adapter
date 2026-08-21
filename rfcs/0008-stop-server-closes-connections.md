# RFC-0008: stop-server closes live connections on both strategies

- Status: Accepted (wave 2, round 1)
- Kind: bug fix
- Prior art: the fibers strategy's own `conn-close!` sweep, added with the
  poller work; Igropyr `http-shutdown!` (drain the pool, then stop)

## Summary

`stop-server` closes every live connection on the fibers strategy and none
of them on the threads strategy. Register accepted connections on the
threads path too and close them all in the one place, through the same
idempotent close the fibers path already uses.

## Motivation

Measured, against `main`:

```
:threads  first response: "HTTP/1.1 200 OK"
:threads  stop-server returned; served so far: 1
:threads  AFTER STOP got: "HTTP/1.1 200 OK"  served: 2     <-- still serving
:fibers   AFTER STOP got: "<nothing>"        served: 1
```

A keep-alive connection opened before the stop keeps being served after
`stop-server` returns. `connection-loop` never looks at `running?`, and the
threads path only closes the work channel and the listen fd — neither of
which touches a connection a worker already owns. The worker stays on it
until the peer leaves or `:keep-alive-timeout-ms` (default 30 s) fires, so
"the server is stopped" and "the server answers requests" are true at the
same time, and a test or a REPL session that stops a server to start
another still has up to N worker threads serving the old one.

There is a second, quieter leak on the same path: a connection accepted but
not yet claimed by a worker sits in the (unbuffered) work channel. Closing
the channel drops it — the fd is never closed by anyone.

## Design

Give the threads strategy the registry the fibers strategy already has, and
close through one function:

```clojure
(defn- conn-close! [conns entry]
  (when (compare-and-set! (:closed? entry) false true)
    (socket/c-shutdown (:conn entry) 2)
    (socket/c-close (:conn entry))
    (when (:poller? entry) (poller/forget! (:conn entry))))
  (swap! conns disj entry))
```

- **Register at accept, not at claim.** The acceptor already increments
  `(:open stats)` for every connection it takes; the entry goes in beside
  it. That covers the conn-in-the-channel leak: it is registered before it
  is ever handed to a worker, so the stop sweep reaches it whether or not a
  worker ever did.
- **The worker's `finally` deregisters through the same call** instead of
  its own `c-shutdown` / `c-close` pair. This is the part that has to be
  idempotent: with two closers, the loser would `close(2)` an fd number the
  kernel has already handed to a different socket — the exact hazard the
  `closed?` CAS was introduced for on the fibers path.
- **`stop-server` sweeps both strategies.** The drain (wait for
  `:active` to reach zero, bounded by `:drain-timeout-ms`) is unchanged and
  still runs first, so the sweep only ever closes connections that are
  between requests — an in-flight response is not cut off. The threads
  path keeps closing the work channel so idle workers exit their take loop.
- `poller/forget!` stays fibers-only, flagged on the entry. It is the
  poller's registration that the threads path never makes.

No option, no API change, no change to the fibers path's observable
behavior.

## Why not a `running?` check in connection-loop

It reads like the obvious fix and it is not enough on its own: a worker
parked in `idle-poll-recv!` or blocked in `recv` is not executing the loop,
so it would keep the connection (and the thread) for up to the idle timeout
regardless. Closing the fd is what wakes it. Once the fd is closed the
check is also redundant — the next read fails and the loop ends — so it
would be a second mechanism for something already handled, on a function
whose argument list is long enough already (it gets grouped into a config
map in round 2, where the next option lands).

The one case it would catch that the sweep does not: a *pipelined* request
already buffered when the drain releases. The client's observable outcome
is the same either way — the connection closes without a response for it —
so it does not earn a mechanism.

## Drawbacks

- A client that has a connection open when the server stops now gets it
  closed rather than being served for another 30 seconds. That is the
  point, and it is what the fibers strategy has always done.
- The threads path grows a per-connection entry (a map and an atom) and a
  set membership operation per connection. Both are off the request hot
  path — once per connection, not once per request.

## Test plan

- `stop-closes-live-connections`, run for **both** strategies: connect,
  request, get a 200; `stop-server`; request again on the same connection;
  the client reads EOF rather than a response, and the handler's call count
  has not moved.
- `stop-server` still returns promptly with idle workers parked (existing
  `test-stop-is-prompt`, unchanged).
- The existing fibers stop/restart stress (`ring-chez.probe`, 30 rounds)
  still passes — it is the regression guard for double-close and for stale
  poller registrations.
- Existing drain test (an in-flight request finishes rather than being cut
  off) still passes.
