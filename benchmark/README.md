# Benchmarks

Compares this adapter against two minimal JVM Ring servers running the identical
handler: [Undertow](https://undertow.io/) (`benchmark/minimal-ring-undertow/`)
and [Jetty](https://jetty.org) (`benchmark/minimal-ring-jetty/`). Endpoints are
`GET /plaintext` and `GET /json`, both returning fixed `Hello, World!` bodies.
Workload is [ApacheBench](https://httpd.apache.org/docs/2.4/programs/ab.html)
(`ab`), colocated with the servers.

All three servers run the **bare handler with no middleware**. That is the point
of the comparison: what is measured is the adapter, and a middleware stack on one
side and not the other would put its cost in the adapter's column. (Until Aug 20
2026 the Undertow server wrapped its handler in `ring-defaults api-defaults`
while the chez server ran bare, so the "identical handler" claim above was not
true and Undertow's numbers carried a per-request tax the others did not.)

## Run

```sh
benchmark/run.sh                       # full matrix, :threads strategy (default)
STRATEGY=fibers benchmark/run.sh       # fibers strategy
WORKERS=200 benchmark/run.sh           # threads with :worker-threads 200
SERVERS="jetty chez" benchmark/run.sh  # subset of undertow/jetty/chez
JSON=1 benchmark/run.sh                # add a /json pass
N=2000 C_LIST="10" benchmark/run.sh    # quick smoke run
```

Requires `ab`, `curl`, `timeout`, `lsof`, the Clojure CLI, and `jolt`.
Defaults: undertow on :8080, adapter on :8081, jetty on :8082, `N=20000`
requests, concurrency 10 and 100, plain and keepalive (`-k`) modes. Each line
prints req/s plus p50/p99 latency; `TIMEOUT` or a `<- client errors (stall?)`
marker means the run hung or dropped connections — that is a finding, not a
script failure. Server logs go to mktemp files named at startup.

For a single server by hand: `jolt -M:bench` from the repo root
(`PORT`, `STRATEGY`, `WORKERS` envs),
`cd benchmark/minimal-ring-undertow && clojure -M:run` (`PORT` env), or
`cd benchmark/minimal-ring-jetty && clojure -M:run` (`PORT` env).

## Findings (2026-08-20, M-series Mac, colocated, `ab -n 20000`)

Three runs of the whole matrix per strategy, same machine, same sitting, so
every cell below is a RANGE rather than one sample. That matters here: a single
run of `ab -k -c 100` against this adapter can read anywhere from 4k to 34k
req/s, and a point value would have been noise dressed as a result. `/plaintext`;
req/s, with the typical p99 in the last column.

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 | p99 |
|---|---:|---:|---:|---:|---|
| undertow | 21.4-23.2k | 68-91k | 22.9-23.7k | 94-110k | 1 / 0 / 5 / 3 ms |
| jetty | 19.7-21.7k | 70-79k | 21.5-22.8k | 75-104k | 1 / 0 / 35 / 4 ms |
| chez `:threads` | 16.8-18.6k | 47-54k | 18.4-19.2k | **4-34k** | 2 / 1 / 65 / 35 ms |
| chez `:fibers` | 15.0-15.5k | 15.4-16.0k | 18.6-18.9k | 12.9-13.1k | 2 / 2 / 50 / 40 ms |

`/json`, plain, one run — the body is 27 bytes instead of 13 and nothing else
differs, which is the control that says the numbers above are adapter cost and
not body cost:

| server | c=10 | c=100 |
|---|---:|---:|
| undertow | 22688 | 22531 |
| jetty | 21233 | 24028 |
| chez `:threads` | 18688 | 17729 |

What the numbers say:

- **Plain connections: the adapter is in the same class as both JVM servers.**
  16.8-19.2k against Undertow's 21.4-23.7k and Jetty's 19.7-22.8k — roughly 80%
  of Undertow and 88% of Jetty. The gap is the per-connection cost of handing
  off from the acceptor to the worker pool, and it is a fraction, not a
  multiple.
- **Keepalive at low concurrency: the adapter reaches ~50k req/s.** That is the
  row that has moved most since the io-poller and keepalive work; it used to be
  ~18k, capped by worker scheduling. Undertow and Jetty are still ahead — their
  NIO event loops pipeline requests on an established connection without a
  per-request thread hand-off — but the shape is no longer "5x behind", it is
  ~0.65x of both.
- **Keepalive at c=100 does not converge on `:threads`, and that is the finding.**
  Six samples came out 34.0k, 32.0k, 30.9k, 7.0k, 6.8k and 4.2k — bimodal, not
  spread: the run either sustains keepalive reuse or collapses to roughly a
  fifth of it, with nothing in between and no client errors either way. The
  mechanism is the accept-pressure retirement: with 100 keepalive clients and N
  workers, a worker that sees an unclaimed accepted connection stamps
  `Connection: close` and frees itself for the backlog rather than parking on an
  idle keepalive. Whether the run lands in the high mode or the low one is
  decided by how that race resolves in the first moments and never recovers.
  It is the right trade — before it, this cell hung until the client's 60s
  timeout (see the last section) — but "~16k req/s" as previously recorded here
  is a single sample from inside a bimodal distribution, not a throughput
  figure. Both JVM servers scale UP in this cell instead (75-110k), because
  pipelining gets cheaper as connections stay hot.
- **`:fibers` trades peak throughput for predictability, and now wins the cell
  `:threads` is worst in.** Every fibers cell is tight — the widest is 3% —
  where threads swing 8x at ka c=100. Fibers match threads at plain c=100
  (18.6-18.9k vs 18.4-19.2k) and beat the threads MEDIAN at ka c=100
  (12.9-13.1k, stable, against a bimodal 4-34k), because a fiber parks per
  connection instead of pinning a worker, so accept pressure never builds and
  the retirement never fires. They remain behind at low concurrency (15.0-15.5k
  vs 16.8-18.6k plain c=10; 15.4-16.0k vs 47-54k keepalive c=10), where a
  thread's blocking recv costs nothing and the fiber still pays its per-request
  channel hop. That completes the trajectory recorded below: the old
  hand-rolled poll(2) fibers ran at ~10% of threads, io-poller took it to ~63%,
  and it now leads in the cell threads is least predictable in.
- **Undertow vs Jetty.** Undertow leads on plain connections at both
  concurrencies, by about 6%. Keepalive at c=10 is a wash once the spread is
  taken into account (68-91k vs 70-79k). At ka c=100 Undertow is both faster and
  steadier (94-110k vs 75-104k). The sharpest difference is tail latency on
  plain c=100: Undertow p99 5-6ms, Jetty p99 35ms across all three runs, which
  is consistent enough to be a property of the connector rather than noise.
- **Body size is not a factor.** `/json` tracks `/plaintext` within a few percent
  for all three servers, so nothing here is measuring serialization.


## Why fibers cost more at low concurrency (threads vs fibers, same session)

At c=10 a `:threads` request performs no context switch at all — a blocking recv
parks the kernel thread and the wake is direct. A `:fibers` request pays, on top
of the same recv/send: a delivery channel, the park/unpark, and the handler hop
to `async/thread` and back for a synchronous Ring handler. That is the whole of
the 15.4k-vs-18.2k gap at plain c=10, and it is fixed cost per request, so it
stops mattering once the threads strategy starts contending.

Under the OLD hand-rolled poll(2) poller this fixed cost was much larger and,
worse, it did not amortise: every registration and wakeup funnelled through one
poller thread whose per-cycle work was O(N) — rebuild N+1 pollfd slots, two FFI
writes each, scan 2(N+1) revents fields — so adding concurrency made fibers
*slower* (2339 -> 1825 req/s from c=1 to c=100) while threads scaled 4x. Those
numbers are kept here because they are why the poller was replaced, not because
they describe the current implementation. With `jolt.io-poller` the interest set
lives in the kernel and a read whose data is already waiting never touches the
poller at all, which is why the c=100 columns above now favour fibers.


## Idle-connection density: where fibers win (measured)

Measured when the io-poller migration landed and **not** re-run for the
2026-08-20 table above — the numbers here are a separate experiment, not part of
that matrix.

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

Measured when this landed (same session, colocated, `ab -k /plaintext`,
M-series Mac):

- chez :fibers (io-poller) ka c=10  ~9.8k req/s, 0 failures
- chez :fibers (io-poller) ka c=50  ~9.8k req/s, 0 failures — flat across
  concurrency (the old hand-rolled poll(2) fibers fell 1.9k → 1.5k from
  c=10 → c=100 and hit ~2k at plain c=100)
- chez :threads (same session) ka c=50 ~15.6k req/s

So fibers went from ~10% of threads to ~63% at the time. Idle density
re-verified post-migration: 300/300 idle keep-alive conns parked and all 300
still usable after idling (connect 1.5s, reuse sweep 0.1s, rss ~110MB flat).
The 2026-08-20 table above supersedes the throughput rows — fibers now match
threads at plain c=100 and beat them under keepalive pressure. The remaining
low-concurrency gap is the per-request handler hop (`async/thread` for the sync
Ring handler) and channel ops — candidate future work, not a poller problem.

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
complete, zero failures, no resets, no stray fds. Before the fix, c=100
keepalive stalled at ~19k of 20k and hung until the client's 60s timeout, so
the liveness property this section is about does hold.

The throughput figure originally recorded here (~16k req/s) does not. Re-measured
2026-08-20, this cell is **bimodal** — 34.0k / 32.0k / 30.9k / 7.0k / 6.8k /
4.2k across six runs — so ~16k was one sample from a distribution with nothing
near its middle. See the Findings section: the retirement keeps the server live
under keepalive pressure, but what it costs in throughput is decided by a race
early in the run and does not converge.
