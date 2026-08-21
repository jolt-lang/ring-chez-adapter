# RFC-0014: a connection's fd is released only by its owner

- Status: Accepted (wave 2, follow-up)
- Kind: bug fix
- Supersedes the teardown design in RFC-0008
- Prior art: the `fiber-recv!` comment about an earlier waker design whose
  "stale poller entry misdirected a reused fd number's wakeups"

## Summary

`conn-close!` closed a connection's fd and then called `poller/forget!`,
which *resumes* the fiber parked on that fd. `close(2)` had already returned
the fd number to the kernel, so the resumed fiber's retry `recv` could land
on a number that now belonged to a different, live socket. Split teardown in
two: `conn-down!` ends the connection with `shutdown(2)` and keeps the number
reserved, and only the worker or fiber that owns the connection calls
`close(2)`.

A second, narrower hole in the same lifecycle: the acceptor read `running?`
*before* it registered the connection, so one accepted as `stop-server` swept
was never taken down at all. Register first, check after.

## Motivation

`fiber-recv!` parks on the poller when a read would block, and its docstring
states the contract teardown is supposed to honour:

> deadlines are enforced by the connection sweeper, whose close+forget! wakes
> this parked fiber and its retry recv answers negative (EBADF) → :closed

That holds only while nothing else has claimed the fd number. Teardown was:

```clojure
(socket/c-shutdown (:conn entry) 2)
(socket/c-close (:conn entry))                          ; number is now free
(when (:poller? entry) (poller/forget! (:conn entry)))  ; ...and this resumes the fiber
```

A resumed fiber does not run until the go-block scheduler reaches it, so the
window between the `close` and the retry `recv` is unbounded. Any acceptor in
the process can be handed that number inside it. The stale fiber then reads a
**live connection belonging to somebody else** and answers it out of its own,
already-stopped server's handler.

Measured on `main`, 25 rounds of "start a server, open 8 connections, stop it,
start another and request from it", every server stamping its own token into
every response body:

```
:fibers   5 of 200 responses served by an already-stopped server
:threads  0 of 200            (only the fibers path parks on the poller)
```

with the failures naming themselves — a connection to `SRV-B-2` answered
`SRV-B-1`. Removing *only* the `c-close` line, so fd numbers are never
recycled, takes it to `0 of 200`: that isolates the cause to freeing the
number at that point.

This is what was behind the intermittent suite failures that had been filed as
unexplained. They looked unrelated to each other — a handler that never ran
while the client still read a 200, an empty response where a body was
expected, `EPIPE` on a websocket frame right after a successful handshake —
because the symptom depends on what the stale reader happened to consume and
which server's handler answered. One of them reproduced as
`FAIL fibers stop: handler not re-entered — expected 1 got 0`: the client got
a valid `HTTP/1.1 200` that its own server's handler had never produced.

They are **not** EINTR, which was the previous hypothesis. Probing the actual
signal dispositions in a jolt process settles it: apart from `SIGINT`/`SIGQUIT`
(Chez's `^C` handlers) every signal is `SIG_DFL`, `SIGPIPE` is `SIG_IGN`, and
the thread mask is empty — so in an automated run no signal with a handler is
ever delivered and EINTR is unreachable. The EINTR retries kept elsewhere in
this branch are correct defensive code that happened to fix nothing.

## Design

Three once-only steps, three atoms on the conn entry, and a rule about who
may perform each.

```clojure
(defn- conn-down! [entry]            ; sweeper + stop-server
  (when (compare-and-set! (:down? entry) false true)
    (socket/c-shutdown (:conn entry) 2)
    (when (:poller? entry) (poller/forget! (:conn entry)))))

(defn- claim-close! [entry]          ; exactly one caller wins
  (compare-and-set! (:owned? entry) false true))

(defn- conn-release! [conns entry stats]   ; the owner only
  (conn-down! entry)
  (when (compare-and-set! (:released? entry) false true)
    (when (:poller? entry) (poller/forget! (:conn entry)))
    (socket/c-close (:conn entry))
    (swap! (:open stats) dec))
  (swap! conns disj entry))
```

- **`shutdown` instead of `close` for everyone who is not the owner.** It
  makes every pending and subsequent read answer EOF and every write `EPIPE`,
  so the holder unwinds on its own — and it leaves the fd number allocated.
  That is the entire fix: the number stays reserved until the last user is
  done with it.
- **`forget!` after `shutdown` is safe** in a way `forget!` before `close`
  never was. RFC-0008 ordered close-then-forget because a forget-first waker
  "retries recv on the still-open fd, sees EAGAIN, re-parks on a dead
  registration and hangs". After a shutdown the retry sees **EOF, not
  EAGAIN**, so it cannot re-park.
- **Ownership by CAS.** The worker claims when it takes the conn off the work
  channel; the fiber claims as its go block starts; `stop-server` claims
  whatever nobody did. A caller that loses the CAS must not touch the fd
  again — the winner may already have released the number. This is what makes
  the ownerless cases (a conn still in the work channel, a fiber never
  scheduled) safe without racing a worker that took the same conn.
- **`conn-release!` forgets before closing**, the opposite order, safe for the
  opposite reason: the owner is unwinding, so no reader is left to re-park.
  Forgetting *after* the close would race the freed number into another
  socket's registration — which is also how a registration was being leaked
  (see below).
- **The sweeper only downs.** Reaping an idle keep-alive conn no longer frees
  its fd; the shutdown wakes the fiber, whose read answers EOF and whose
  `finally` does the release.

No option, no API change. `stop-server`'s observable contract is
unchanged, and now actually holds in the one case where it did not:
connections are dead before it returns, and none is left behind.

## The second hole: a conn registered after the sweep

`serve-loop` decided whether to serve by reading `running?` in its `cond`,
*before* `serve!` published the entry into `conns`:

```
acceptor:     accept() returns, reads @running? -> true
stop-server:  running? false; drain; sweep @conns    (conn not in it yet)
acceptor:     serve! -> register-conn! -> entry lands AFTER the sweep
```

Nobody ever takes that connection down. On `:fibers` a fiber goes on serving
it for a server that has stopped; on `:threads` it goes into a closed work
channel and the fd leaks. This is the case RFC-0008 set out to eliminate,
still open — and it is the reason `stop-server` could not be said to leave
nothing behind.

The fix is ordering, not locking: **publish, then check**. `serve!` registers
the entry first and reads `running?` afterwards, which interlocks with
`stop-server` clearing `running?` before it sweeps.

| | outcome |
|---|---|
| register lands before the sweep | the sweep sees it and takes it down |
| register lands after the sweep | `running?` was cleared before that sweep, so the read here returns false and the acceptor cleans up |
| both race | `claim-close!` decides; the loser does nothing |

Airtight given the two orderings that create it: `stop-server` sets
`running?` before sweeping (it is the first statement), and `serve!`
registers before reading it. The `running?` branch left in `serve-loop`'s
`cond` is now only an early-out that avoids the work of `nonblock!` and
`register-conn!` on an already-stopped server.

The threads arm folds the channel put into the same condition, because
`>!!` answers false on a channel `stop-server` has already closed and a conn
nobody can take off it needs exactly the same cleanup. It also unwinds
`pending`, which the worker's `(io :claim!)` would otherwise have done —
leaving it high for the life of the server.

### Measuring it

The window is the handful of instructions between the check and the
register, so it does not show up under ordinary stress: a storm of 20
stop-during-connect rounds never hit it. Widening it with a `Thread/sleep 5`
between the two makes it plain, and shows the fix closes the window rather
than narrowing it — the delay stays in for the "after" column:

```
                          before      after
:fibers   12 rounds       9 leaked    0 leaked
:threads  12 rounds       --          0 leaked
```

## Fixing the leaked poller registration

The same run surfaced the existing stress gate catching
`fd 13 {:read {:ready false, :waiters 1}}` — a fiber registered, parked, and
never resumed. That is the same lifecycle bug from the other end: teardown
called `forget!` once, but a fiber that was *running* rather than parked at
that moment could re-park afterwards, re-registering an fd that nothing would
forget again. `conn-release!` forgetting immediately before the close closes
that: the owner is past all its parks by then.

## Alternatives

- **Have `stop-server` wait for every owner to release before returning.**
  Makes the fd table exactly empty on return, at the cost of stop latency
  bounded by the slowest handler. Rejected: the connections are already dead
  once shut down, and holding an fd for a few more milliseconds is invisible.
  What mattered was never releasing it *early*.
- **Reference-count the entry instead of naming an owner.** Equivalent
  safety, more moving parts, and every syscall site would have to hold a
  reference. Ownership is a count of one that the existing `finally` blocks
  already delimit.
- **Generation-tag the fd and re-check before each syscall.** Detects the
  race rather than preventing it, and every check is itself racy — the number
  can be reused between the check and the syscall.

## Drawbacks

- An owner may still be unwinding when `stop-server` returns, so
  `(:connections (server-stats s))` can lag briefly after a stop. It was
  already only a snapshot.
- A conn whose handler is genuinely wedged now holds its fd until that
  handler returns, where teardown used to close it out from under it. That
  close is the bug, so this is the trade being made deliberately; the drain
  bound (`:drain-timeout-ms`) is unchanged and still governs how long
  `stop-server` waits.
- Three atoms per connection instead of one. Off the request hot path — once
  per connection, not once per request.

## Test plan

- `stop-leaves-nothing-behind`, both strategies: connections arriving in a
  throttled storm while `stop-server` runs, with the clients held **open**
  afterwards so a conn that missed the sweep keeps `:connections` above zero
  rather than being reaped for going away. It holds the invariant; it is not
  a reproducer, for the reason measured above.
- `serves-only-own-conns`, run for **both** strategies: 16 rounds of start
  server A, open 8 connections, stop it, start server B and request from it;
  every server stamps its own token into every body, and no response may
  carry another server's. Fails 8 of 128 on `:fibers` before the fix, 0 after;
  `:threads` is 0 either way and is there to hold the invariant for both.
- `stop-closes-live-conn` gains an "answered by this server" check on the
  response it already reads, so the cheap path catches a cross-serve too.
- The existing fibers stop/restart stress gate (poller table clean after each
  round) still passes — it is the regression guard for the leaked
  registration above.
- Existing drain test (an in-flight request finishes rather than being cut
  off) and the stats tests still pass.
