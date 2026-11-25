# RFC-0017: idle keep-alive connections are parked without a worker (threads strategy)

- Status: Proposed
- Kind: design change (threads strategy)
- Builds on: RFC-0014 (a conn's fd is released only by its owner)
- Follow-up to: the `ka c=100` regression on branch `fix/ka-pressure-retires-everything`

## Summary

On the threads strategy a worker that has answered a request keeps its
connection and blocks in `idle-poll-recv!` watching that one socket for the
next request. The strategy can therefore serve only as many idle keep-alive
connections as it has workers. The pressure machinery (`retire-for-pressure!`,
the 2 s grace in `idle-poll-recv!`) has been hiding this by closing idle
connections to free a worker, which costs clients their keep-alives and, as
measured below, was itself feeding on its own output.

Change: a worker whose connection goes idle **parks** it on a server-wide set
and returns to the pool. One poller thread watches the parked set with
`poll(2)`; a parked conn that becomes readable is put back on the work channel
and any free worker serves it. The keep-alive timeout applies on the parked
set. A worker is held only while a request is actually being read or answered.

Plaintext connections only. A TLS connection's session lives inside
`serve-tls` for the duration of the worker's call, so it keeps the current
worker-bound model and the retirement valve.

## Motivation

`ab -k -N 20000 -c 100` against the bench handler, default pool of 10, jolt
0.8.30, macOS. Three runs per row, each within 0.4 req/s of the others.

```
adapter                       rps      keep-alives   p99    max      faults
main                          2.4-3.2k ~16k/20000     13ms   6-8 s    0
+ retire grace (this branch)  1505     19953/20000    0ms    13.3 s   0
+ retire grace, 20 workers    1666     19959/20000    1ms    12.0 s   0
+ retire grace, 50 workers    2426     19974/20000    2ms     8.2 s   0
+ retire grace, 100 workers   103k     20000/20000    4ms       8ms   0
```

The last row is the whole argument: with one worker per connection there is
no starvation at all and the cell reads 103k. With fewer workers than idle
connections, the connections nobody is watching wait for a parked worker to
give up its own conn after the 2 s grace, one at a time. Throughput is
bounded by the worker count, not by the work.

The keep-alive count tracks the worker count exactly (19953 at 10, 19959 at
20, 19974 at 50, 20000 at 100): the "16k/20000" of the red cell is the server
keeping `workers` connections and churning the rest.

Two things the retirement machinery was doing, measured in a 2000-request
burst with counters on `retire-for-pressure!`:

- the response path answered "retire" 497 times, every one at
  `pending=1 idle=0 retiring=0`. A worker that has just answered is about to
  park and take the queued conn within microseconds, but the pool does not
  count it yet, so one queued conn read as pressure and cost a live client
  its keep-alive. Each retirement is a reconnect, each reconnect is a queued
  conn. 497 retirements, 526 parks that spent one.
- a conn the idle path gave up is closed with no response: `read-request`
  returns `:closed`, which has no branch in `serve-connection`'s `cond`, so a
  request the client had already sent on it is lost and the client reads EOF.

The retire grace already on this branch fixes the first (the burst test went
from 4 of 6 conns keeping keep-alive to 6 of 6) and cannot fix the second,
because the second is the strategy's limit rather than a race.

This is not a jolt regression and not an adapter commit. The same adapter
source flips verdict with the jolt beneath it and the same jolt flips with
adjacent adapter commits (`.cache/bench/sweep/{cross,repeat}.txt`); jolt
>= 0.8.24 only makes a latent race near-deterministic.

## Design

### States of a connection

Today a threads-strategy conn is in exactly one of: queued (in the work
channel, `:pending?`), owned (a worker is in `connection-loop` for it), or
released. This adds **parked**: idle, readable-watched by the poller, owned
by the parked set.

```
accept -> queued --(worker takes)--> owned --(response sent, conn idle)--> parked
                 ^                              |                                  |
                 |                              v                                  v
                 +----(poller: readable)---------+            (poller: ka timeout, or stop-server)
                                                              --> shutdown, released by owner
```

### Ownership is handed over, never shared

RFC-0014's rule is that exactly one caller releases a conn's fd, decided by
the `:owned?` CAS. Parking keeps that rule by making the parked set an owner:

- **Park.** `serve-connection` calls `idle-recv!` only when it has no
  buffered bytes (`read-request` line 419: `have` is zero). The threads
  `idle-recv!` puts the entry on the parked set with its deadline
  (`now + ka-ms`) and returns a new status, `:parked`. `serve-connection`
  sees it, leaves the loop, and `connection-loop` returns a value saying the
  conn was parked rather than finished.
- **The worker's `finally` must not release a parked conn.** `worker`
  currently runs `conn-release!` in a `finally` around `connection-loop`.
  It becomes: release unless the loop reported `:parked`. The entry's
  `:owned?` stays true throughout, so `stop-server`'s "release the ones
  nobody ever claimed" pass still skips it, exactly as it skips a conn a
  worker is serving.
- **Unpark.** The poller takes the entry off the parked set and puts it on
  the work channel. It stamps `:pending?`/`:queued-at` exactly as the
  acceptor does, so the pressure count and the grace see it. A worker takes
  it, `claim-close!` must find `:owned?` already true from the park, so the
  take-time claim becomes "re-adopt": assert the entry is in the `:parked`
  state and clear that flag, rather than a fresh CAS.
- **Release.** Whoever is the owner when the conn ends: the worker that
  served the last request (release in `finally` as today), or the poller for
  a parked conn that timed out or was stopped.

That last case is the one new close path, and it follows the house rule:
`conn-down!` (shutdown SHUT_RDWR) then `conn-release!` (close), through the
same functions, with the `:released?` CAS making it once-only.

### The poller

One thread per server, started with the workers:

1. Snapshot the parked set. Empty: wait on a `promise`/`java.util.concurrent`
   semaphore the park side signals. Otherwise: `alloc-pollfds n`,
   `init-pollfd!` each `pollin`, `c-poll pfds n timeout` where `timeout` is
   the ms until the earliest deadline (bounded to a slice, 250 ms, so a set
   that changes only by new parks is still re-read).
2. For each index with `revents & poll-readable`: remove from the parked set
   and `>!!` it onto `work`. The put is a rendezvous: if no worker is free
   the poller would block, and that stalls every other parked conn, so the
   put happens on a **separate hand-off**: the poller moves readable entries
   to an "unparked" queue and a hand-off loop (the poller itself, after
   finishing the batch) puts them on `work` one at a time, accepting that a
   full pool makes it wait. A parked conn that is readable but cannot be
   handed on is still owned by the server, so nothing leaks; it is just
   served in queue order.
3. For each entry past its deadline: take it off the set and release it
   (`conn-down!` + `conn-release!`).
4. `stop-server`: `running?` false, the sweep shuts every conn down, which
   makes `poll` on a parked fd report `POLLHUP|POLLIN` at once. The poller
   sees `running?` false at the top of its loop, releases nothing (the sweep
   and the owners do), and exits. `stop-server` waits for it as it waits for
   the acceptors.

`poll(2)` is O(n) per call, as in the fibers strategy's kqueue/epoll
difference: fine for the hundreds-to-low-thousands of idle keep-alives a
threads-strategy server is for; a server that needs tens of thousands of
idle conns is what `:fibers` is for, and the README says so.

### What is removed

The retire paths become dead for plaintext conns: nothing is parked *with* a
worker, so there is no worker to retire. `retire-for-pressure!`, the `:retiring`
pool count and the 2 s grace in `idle-poll-recv!` stay in place for the TLS
arm, which keeps the worker-bound model, and are now only reachable from
there. Removing them for TLS too is a follow-up once TLS sessions can be
carried on the entry.

`:idle-recv!` on the threads arm therefore has two behaviours: plaintext
park, TLS poll-and-retire. The branch is on `(:tls? entry)`.

### Fairness and races

- A request arriving on a parked conn between "poll says readable" and "put
  on `work`" is simply there when a worker reads it; the conn is not read
  until a worker owns it. No bytes are consumed by the poller.
- A client closing a parked conn: `poll` reports readable (`POLLIN` on EOF),
  the poller unparks it, a worker's `read-request` gets `:closed` on the
  first recv and releases it. One wasted take per client close, bounded by
  the close rate. (`POLLHUP` without `POLLIN` is treated the same way; the
  recv decides.)
- Thundering herd: none. `poll` wakes one thread and it hands each readable
  conn to exactly one worker.
- Double unpark: the entry leaves the parked set by CAS on a `:parked?` flag
  before the put, so a conn readable in two consecutive snapshots is put once.
- A parked conn's fd number is never freed while parked (owner = parked set),
  so RFC-0014's fd-reuse hazard does not arise.

## Alternatives

- **Workers >= expected idle keep-alives (tuning).** Works (103k above) and
  needs no code, but the default is 10 and a threads-strategy server cannot
  know its client's connection count. Rejected as the fix; it stays valid
  guidance.
- **Keep retiring, but only after the grace, and answer `Connection: close`
  when a parked conn is given up.** Fixes the lost-request half and is what
  the branch has now. Throughput stays bounded by workers (1.5k), because
  every unwatched conn still costs a retirement. Rejected: it prices the
  limit, it does not lift it.
- **Poll every idle conn from each worker before parking (no poller
  thread).** N workers each polling M conns is N x M wakeups per slice and a
  take race between workers on every readable conn. Rejected.
- **Use `jolt.io-poller` for the parked set, as the fibers strategy does.**
  Attractive (kqueue/epoll, already a dependency) but its API resumes a
  parked fiber; a worker thread is not a fiber and cannot be resumed by it.
  Deferred until the threads strategy can wait on the poller from a thread.
- **Move every threads-strategy conn onto fibers.** That is the fibers
  strategy; this RFC keeps a pure-threads option that does not starve.

## Drawbacks

- One more thread per server (the poller), idle when nothing is parked.
- One extra channel hop per request on a parked conn (poller -> `work` ->
  worker) versus today's direct read. The 103k row above is with no parking
  at all; the cost on the parked path is to be measured in the test plan and
  recorded in the README.
- `poll(2)` is linear in the parked set. Acceptable at the scale the threads
  strategy targets (see above).
- TLS conns do not benefit and keep the retirement valve. Stated, and the
  README note says so.
- The conn state machine gains a state and the worker's release rule gains
  a condition. Both are spelled out in `worker`'s docstring and tested.

## Test plan

- **Red first:** `test-idle-conns-beyond-worker-count-are-served-at-once`:
  6 conns, 2 workers, a request on every conn after the burst is served;
  every conn answers on the same conn within 250 ms and no conn is closed.
  Fails today (4 of 6 read EOF).
- `test-parked-conn-keep-alive-timeout`: a parked conn is closed after
  `:keep-alive-timeout-ms`, the fd is released once, `:connections` returns
  to 0.
- `test-parked-conn-client-close`: a client closing a parked conn costs one
  take and no fault.
- `test-stop-server-closes-parked-conns`: conns parked at stop are shut down
  before `stop-server` returns; `:connections` reaches 0; the poller thread
  exits (no thread leak across 20 start/stop rounds).
- `test-tls-keep-alive-still-retires-under-pressure`: the TLS arm still
  answers and still retires (the valve is unchanged for it).
- The existing pressure tests (`one-queued-conn-retires-one-keepalive`,
  `handoff-with-idle-workers-is-not-pressure`, `lost-handoff-does-not-retire-keepalive`,
  `burst-of-conns-keeps-keep-alive`, `keep-alive-fairness`, `pipelined-under-pressure`)
  stay green. `one-queued-conn-retires-one-keepalive` and
  `queued-conns-are-served-by-idle-retirement` describe the worker-bound
  model and are retargeted to TLS or restated for the parked model.
- Full suite `jolt -M:test`.
- Benchmark: `ab -k -c 100` default pool before/after, plus `c=10` and the
  plain cells to show no regression; README table updated from raw files.
