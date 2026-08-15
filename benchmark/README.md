# Benchmarks

Compares this adapter against a minimal [Undertow](https://undertow.io/) Ring
server (`benchmark/minimal-ring-undertow/`) running the identical handler:
`GET /plaintext` and `GET /json`, both returning fixed `Hello, World!`
bodies. Workload is [ApacheBench](https://httpd.apache.org/docs/2.4/programs/ab.html)
(`ab`), colocated with the servers.

## Run

```sh
benchmark/run.sh                     # full matrix, :threads strategy (default)
STRATEGY=fibers benchmark/run.sh     # fibers strategy
WORKERS=200 benchmark/run.sh         # threads with :worker-threads 200
N=2000 C_LIST="10" benchmark/run.sh  # quick smoke run
```

Requires `ab`, `curl`, `timeout`, `lsof`, the Clojure CLI, and `jolt`.
Defaults: undertow on :8080, adapter on :8081, `N=20000` requests,
concurrency 10 and 100, plain and keepalive (`-k`) modes. Each line prints
req/s plus p50/p99 latency; `TIMEOUT` or a `<- client errors (stall?)`
marker means the run hung or dropped connections — that is a finding, not
a script failure. Server logs go to mktemp files named at startup.

For a single server by hand: `jolt -M:bench` from the repo root
(`PORT`, `STRATEGY`, `WORKERS` envs), or
`cd benchmark/minimal-ring-undertow && clojure -M:run` (`PORT` env).

## Findings (Aug 2026, M-series Mac, colocated)

Representative `ab -n 20000` results on `/plaintext`:

```
undertow         plain  c=10      ~21-22k req/s   p50 0ms   p99 1ms
undertow         plain  c=100     ~22k req/s      p50 0ms   p99 1ms
undertow         ka     c=10      ~55-78k req/s   p50 1ms   p99 3ms
undertow         ka     c=100     ~93-107k req/s  p50 1ms   p99 4ms

chez :threads    plain  c=10      ~16-17k req/s   p50 0ms   p99 2ms
chez :threads    plain  c=100     ~17-18k req/s
chez :threads    ka     c=10      ~17-18k req/s   p99 2ms
chez :threads    ka     c=100     STALLS          (see below)

chez threads-200 ka     c=10      ~19k req/s      p99 4ms
chez threads-200 ka     c=100     ~12-13k req/s   p99 41ms   (unstable)

chez :fibers*    plain  c=10      ~6k req/s
chez :fibers*    plain  c=100     ~2k req/s       p99 149ms
chez :fibers*    ka     c=10      ~1.9k req/s
chez :fibers*    ka     c=100     ~1.5k req/s     p50 67ms   zero errors

chez :fibers     ka     c=10      ~9.8k req/s     io-poller (post-migration)
chez :fibers     ka     c=50      ~9.8k req/s     io-poller, flat across c

json endpoint, plain c=100: undertow ~21.4k, chez :threads ~13k req/s
```

`*` pre-migration numbers for the old hand-rolled poll(2) poller, kept for
history — see "Adopted: :fibers runs on jolt.io-poller" below for the
current implementation.

What the numbers say:

- **Plain connections: same ballpark as Undertow.** ~16-18k vs ~21-22k
  req/s. The adapter's per-connection cost (thread hand-off from the
  acceptor to the worker pool) costs roughly 20-25%, not a multiple.
- **Keepalive: Undertow pulls 5x ahead.** Its NIO event loop pipelines
  requests on an existing connection at ~100k req/s. Our threads strategy
  parks one worker thread per connection, so keepalive throughput is
  capped by worker scheduling, not sockets (~18k).
- **`:fibers` is a scalability feature, not a throughput feature.** Every
  request parks on the single poll(2) poller thread and hops through two
  core.async channels, so raw RPS is 3-10x below threads — but idle
  keepalive/WebSocket/SSE connections pin no threads at all. It ran the
  entire matrix with zero errors. See "Why fibers lose under load" and
  "Idle-connection density" below.
- **Worker count is not a tuning knob for throughput.** 200 workers
  collapses plain-connection throughput to ~2.6k req/s (thread contention
  on accept/close churn) and destabilizes keepalive at c=100.

## Why fibers lose under load (threads vs fibers, same session)

`ab -n 20000` on `/plaintext`, both strategies on the same server binary:

```
threads  plain c=1     3986 req/s   p99 1ms
fibers   plain c=1     2339 req/s   p99 1ms     <- 1.7x slower at zero contention
threads  plain c=100  16524 req/s   p99 87ms
fibers   plain c=100   1825 req/s   p99 137ms   <- 9x slower, does not scale
threads  ka    c=100  16762 req/s   p99 89ms
fibers   ka    c=100   1491 req/s   p99 106ms   <- 11x slower
```

Two separate effects, both measured:

- **Fixed per-request overhead (the c=1 row).** A fiber request costs, on
  top of the same recv/send the thread does: a delivery chan alloc, a
  timeout chan alloc (`alts!` vs `async/timeout`), a wake-pipe write to
  interrupt the poller, a poller wakeup that rebuilds the entire pollfd
  array from the regs atom, the poll syscall itself, a pipe read to drain
  the wake byte, a revents scan over every slot, `put!` delivery, a go
  park/unpark — then `:run!` hops the handler to `async/thread` and back
  through another channel. "Cheaper switching" never enters the picture:
  the threads strategy performs no switch at all per request (a blocking
  recv parks the kernel thread; the wake is direct).
- **Serialization through one poller (the c=100 rows).** Every
  registration and every wakeup funnels through a single poller thread
  whose per-cycle work is O(N) (rebuild N+1 pollfd slots, 2 FFI writes
  each; scan 2(N+1) revents fields). Adding concurrency makes fibers
  *slower* (2339 -> 1825 req/s from c=1 to c=100) while threads scales
  4x. Within a single fibers run, throughput declines as steady-state
  registration churn builds (3716 -> 1089 req/s across 50k requests).

Fibers win where threads are the scarce resource: thousands of mostly
idle connections (SSE/WebSocket fleets) that would otherwise pin one
thread each. Under compute-style load the poller is the bottleneck by
construction. The fix is adopting `jolt.io-poller` — see
"Recommended: adopt jolt.io-poller" below.

## Idle-connection density: where fibers win (measured)

1000 concurrent keep-alive connections opened, one request each, left
idle 6s, then reused. Same server binary per strategy, default workers
(N = available processors = 10), `ulimit -n 10240`:

```
                             :fibers            :threads
connect 1000 conns           3.9s, 0 errors     12.8s, 0 errors
established after open       1000               10  (rest retired)
fresh request mid-churn       13ms              663ms (accept backlog)
fresh request once idle       12ms              12ms
reusable after 6s idle       1000/1000            9/1000
RSS                          214MB (+133MB)     101MB (+19MB)
```

What the numbers say:

- Threads no longer stalls (the pressure-retirement fix holds: all 1000
  requests served, zero errors) but it pays with the fleet — ~99% of
  idle keep-alive connections are retired with `Connection: close`, and
  a fresh request during the churn window waits 663ms behind the accept
  backlog. Keep-alive reuse drops to under 1%.
- Fibers holds all 1000 parked — one parked go block per connection, no
  thread pinned — keeps 100% of them reusable, and answers fresh
  requests at full speed throughout. Cost is memory: ~133MB RSS delta
  for 1000 parked connections (parked continuation stacks plus Chez
  heap growth).
- The same shape extends to streaming, structurally: an SSE response or
  WebSocket session on :threads pins one worker for the stream's whole
  lifetime (`take!` is `<!!`), so ~10 concurrent streams exhaust the
  pool; on :fibers the go block parks per chunk. Not measured here.

Verdict: `:threads` for raw request throughput, `:fibers` for
connection-dense or streaming workloads.

## How mature runtimes do readiness (research)

- **Go netpoller** — one stateful interest set per process (epoll,
  edge-triggered, one registration per fd covering both directions). A
  goroutine that would block parks; the poller, woken through an
  eventfd, unparks it when the fd is ready. Nothing re-registers per
  read.
- **Java virtual threads (Loom)** — sockets are non-blocking under the
  hood; on would-block the virtual thread parks and a small set of
  poller threads (fd -> parked-thread map, `sun.nio.ch.Poller`) unpark
  it on readiness. Blocking-style API, event-loop efficiency.
- **libuv / nginx** — a single-threaded event loop per worker; every
  network fd non-blocking; epoll/kqueue stateful interest sets; poll()
  only as a fallback where nothing better exists.

Shared invariants: (1) non-blocking fds everywhere, (2) the kernel
holds the interest set — never rebuilt per call, (3) readiness parks a
userland schedulable (goroutine / virtual thread / fiber) rather than
an OS thread, (4) writes park on EAGAIN too.

Our poller breaks (2) — poll(2) is stateless, so every wake rebuilds
the entire pollfd array — and mostly (1): sockets are blocking, so
readiness is a pre-check paid on every read instead of an EAGAIN
fallback, and even a read whose data is already waiting goes through
the channel/wake/poll dance.

## Adopted: :fibers runs on jolt.io-poller (measured)

`jolt.io-poller` is a process-wide poller thread over kqueue (macOS) /
epoll (Linux) with persistent kernel registrations, a control pipe that
closes registration races, and `wait-ready`, which parks the current
fiber and resumes it on readiness. Jolt's own `jolt.socket` uses exactly
the target shape (`io-call`: run the syscall on a non-blocking fd; on
EAGAIN `wait-ready` and retry; on EINTR retry) — the adapter now does too.

What shipped (src/ring_chez/adapter.clj):

- `O_NONBLOCK` on accepted fds (`poller/nonblock!`).
- `fiber-recv!` is `io-call`-shaped: plain `c-recv`; EAGAIN parks the
  current fiber via `wait-ready`; EINTR retries. A read with data ready
  never touches the poller — the per-request tax left the hot path.
- Sends park on writability the same way (`send-all` + `wait-ready
  :write` on EAGAIN).
- Idle keep-alive deadline enforcement moved OUT of the read path: each
  conn carries a deadline atom (reset when a request's read starts;
  `Long/MAX_VALUE` after a websocket takeover), and a 100ms sweeper
  closes conns past it. The close (shutdown → close → `poller/forget!`)
  wakes the parked fiber; its retry recv answers negative and the loop
  ends as :closed.
- Every close path does shutdown → close → forget!. forget! AFTER close:
  it wakes any fiber still parked on the fd, and the woken read must see
  EBADF — forget-first lets it see EAGAIN, re-park on a dead
  registration and hang. This refines the shutdown-before-close rule.

Design note — the idle timeout is NOT an `alts!` race. The first
implementation used `(alts! [(go (wait-ready fd :read)) (timeout ka)])`
per read; under stop/restart stress it failed 10% of rounds. Two leaks:
a waker go that registered after close+forget parked forever, and its
stale poller entry made the kernel wakeup for a REUSED fd number go to
a dead waiter (`debug-state` showed `{fd {ready false waiters 1}}`
persisting across server restarts, plus an ~8M `:ev-errors`/`:waits`
spin). The stdlib's own socket layer never spawns a waker — the fiber
itself parks — so the deadline moved to the sweeper and the waker is
gone. After the fix: 0/30 stress rounds, `:fds {}` (no stale entries),
waits bounded (~550 per 30 rounds vs millions).

Measured (same session, colocated, `ab -k /plaintext`, M-series Mac):

- chez :fibers (io-poller) ka c=10  ~9.8k req/s, 0 failures
- chez :fibers (io-poller) ka c=50  ~9.8k req/s, 0 failures — flat across
  concurrency (the old hand-rolled poll(2) fibers fell 1.9k → 1.5k from
  c=10 → c=100 and hit ~2k at plain c=100)
- chez :threads (same session) ka c=50 ~15.6k req/s

So fibers went from ~10% of threads to ~63%. Idle density re-verified
post-migration: 300/300 idle keep-alive conns parked and all 300 still
usable after idling (connect 1.5s, reuse sweep 0.1s, rss ~110MB flat). Remaining gap is the per-request handler hop
(`async/thread` for the sync Ring handler) and channel ops — candidate
future work, not a poller problem.

Alternatives considered: making the old poll(2) loop persistent with an
incrementally maintained local array — still a stateless O(N) kernel
interface plus the per-read channel dance; rejected. Writing a
kqueue/epoll layer from scratch — reinvents a stdlib that jolt already
tests (registration-storm gate); rejected.

## Resolved: keepalive stall at c=100 (threads strategy)

`ab -k -c 100` used to complete ~19k of 20k requests and then hang one
connection until its 60s timeout. Root cause was starvation by design,
not a socket bug: with N workers and 100 keepalive clients, the first N
connections pin every worker in an idle keepalive recv, the acceptor
parks on the unbuffered work channel, and the remaining ~90 established
connections sit in the kernel backlog until the client times out.
(Fixed alongside: a worker could die permanently on any escaping
Throwable, and the threads path closed sockets without shutdown() —
the 791d5c4 Linux FIN family.)

The fix: a worker stamps `Connection: close` on its response whenever
another connection is accepted but unclaimed (accept pressure), freeing
the worker for the backlog instead of parking it on an idle keepalive —
but never while a pipelined request is already buffered (serving that
costs no park), and pressure is measured at claim-time so a conn already
picked up by a worker doesn't read as pressure. Idle first reads wait in
poll(2) slices (250ms) instead of a full-timeout recv, so a connection
parked before pressure arrived is retired too — but only after a 2s
grace period of quiet, never instantly, so a client mid-reuse never
races a reset. The worker body is also exception-guarded (an escaping
Throwable used to kill the worker and shrink the pool permanently) and
does shutdown-before-close.

Verified with repeated `ab -k -c 100 -n 50000` runs: 50,000 of 50,000
complete, zero failures, no resets, no stray fds, ~16k req/s with
healthy keepalive reuse (~16k of 50k). At c=10 there is no pressure, so
keepalive is never declined and throughput holds ~19k. Before the fix,
c=100 keepalive stalled at ~19k of 20k and hung until the client's 60s
timeout.
