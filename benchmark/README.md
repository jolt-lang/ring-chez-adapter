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

chez :fibers     plain  c=10      ~6k req/s
chez :fibers     plain  c=100     ~2k req/s       p99 149ms
chez :fibers     ka     c=10      ~1.9k req/s
chez :fibers     ka     c=100     ~1.5k req/s     p50 67ms   zero errors

json endpoint, plain c=100: undertow ~21.4k, chez :threads ~13k req/s
```

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
  entire matrix with zero errors.
- **Worker count is not a tuning knob for throughput.** 200 workers
  collapses plain-connection throughput to ~2.6k req/s (thread contention
  on accept/close churn) and destabilizes keepalive at c=100.

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
