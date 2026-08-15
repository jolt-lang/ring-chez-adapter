# RFC-0005: write timeout on server sends

- Status: Accepted (phase 5)
- Prior art: Igropyr `http.sc` ~105 (`default-write-timeout-ms 30000`:
  "the kernel refuses can expire, which means the peer's receive window
  has been shut the whole time"; `abandon-write!` semantics), swish
  `http.ss` ~403 (error write with 100ms short timeout "in case the
  original failure was a timeout waiting for the connection")

## Summary

New opt `:write-timeout-ms` (default 30000, 0 disables). Threads strategy:
`SO_SNDTIMEO` on every accepted socket, mirroring the existing
`SO_RCVTIMEO` idle-read bound — a blocking `send` to a peer whose receive
window stays shut returns `-1`/EAGAIN after the timeout, `send-all`
already treats that as peer-gone (false → close). Fibers strategy:
documented caveat (below), no behavior change.

## Motivation

The adapter bounds every read (SO_RCVTIMEO / sweeper deadline) but no
write. A client that requests a large body and stops reading parks a
worker thread in `send` *forever* — the kernel buffers what it can, then
blocks. Igropyr's analysis is exact: a live consumer drains a chunk in
milliseconds; a window shut for 30s means the peer is gone or hostile, and
whatever feeds the stream keeps queueing behind it. One stalled peer per
worker, repeated, drains the pool — the same starvation shape the
keep-alive retirement fix addressed for reads.

swish's contribution is the *scope*: the fallback-500 write after a
handler failure is the write you make when something is already wrong —
it must not inherit an unbounded stall from the failing connection.
`SO_SNDTIMEO` bounds *every* send, which covers the error path with zero
extra machinery.

## Design

```clojure
(def ^:private so-sndtimeo (if macos? 0x1005 21))

(defn- set-sndtimeo! [fd ms] ;; mirror of set-rcvtimeo!
  ...)
```

- Applied in the threads-strategy accept path, next to wherever
  `SO_RCVTIMEO` is applied today — one more `setsockopt` on an fd we
  already touch; no new lifecycle point, no new close path.
- `send-all` needs **no change**: on timeout the blocking `send` returns
  -1, which is not EINTR/EAGAIN-with-waiter → existing `:else false` →
  caller closes. The EAGAIN case in `send-all` only parks when
  `wait-write!` is supplied (fibers); threads passes nil.
  - Subtlety: `SO_SNDTIMEO` makes a *blocking* send return EAGAIN on
    timeout. Threads-strategy `send-all` passes `wait-write! = nil`, so
    `(and (neg? sent) wait-write! (poller/eagain?))` is false → falls to
    `:else false` → close. Correct by construction; a comment at that
    cond arm documents it.
- Fibers strategy: sockets are O_NONBLOCK; `SO_SNDTIMEO` does not apply
  to nonblocking sockets (they EAGAIN immediately, and the fiber parks in
  `wait-write!` on the poller). Bounding that park means racing the
  parker with a timeout — the exact anti-pattern that produced the 10%
  stop/restart flake (waker registering after close+forget, stale poller
  entries misdirecting wakeups). We do not reintroduce it. Caveat
  documented in README: on the fibers strategy a write-stalled peer parks
  its fiber (no thread), until the kernel's own buffers fill and the peer
  closes or the sweeper's deadline fires on the next idle window.
- Validation: RFC-0002 slot — non-neg int, 0 disables.

## Alternatives considered

- **Per-send deadline with poll-based writability checks (full Igropyr
  `do-write` + `abandon-write!` port).** Rejected for the threads
  strategy: SO_SNDTIMEO is one line and the kernel implements the
  deadline; the poll-based version reimplements it in user space with
  more syscalls per send on the hot path.
- **Bounding fibers writes via a racing alts!/timeout waker.** Rejected —
  documented anti-pattern (see Design); measured 10% flake under
  stop/restart stress in this repo's own history.

## Drawbacks

- A write to a slow-but-live consumer longer than the timeout is cut off;
  30s default matches Igropyr's judgment of "gone in practice".
- Fibers strategy retains the unbounded-park caveat (documented, bounded
  in practice by kernel buffers + sweeper).

## Test plan

- Test client sets a tiny `SO_RCVBUF` (e.g. 2048) before connecting,
  requests a ~1 MB body, and never reads. Server runs with
  `:write-timeout-ms 300`. Within a generous margin (~3s) the server
  closes: client `recv` sees EOF/reset rather than a full body, and the
  worker is free (a subsequent normal request on a new connection is
  served immediately).
- `:write-timeout-ms 0` leaves behavior unchanged (long body to a
  reading client succeeds — this is also the regression guard that the
  option doesn't break normal sends).
- Default boot still serves a large body to a normally-reading client.
