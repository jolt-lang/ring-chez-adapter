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
SETTLE=0 benchmark/run.sh              # don't wait for TIME_WAIT between cells
```

Requires `ab`, `curl`, `timeout`, `lsof`, the Clojure CLI, and `jolt`.
Defaults: undertow on :8080, adapter on :8081, jetty on :8082, `N=20000`
requests, concurrency 10 and 100, plain and keepalive (`-k`) modes. Each line
prints req/s plus p50/p99/max latency, and a `ka` line also prints how many of
the requests ab counted as keep-alive: every one short of `N` opened a new
connection. Before each cell the script waits (up to `SETTLE_MAX`, 90s) for the
machine's TIME_WAIT entries to drain below `SETTLE_TW` (500); see "Ephemeral
ports" for why. `TIMEOUT` (no completion within
`AB_TIMEOUT`) or a `<- client errors (stall?)` marker means the run hung or
dropped connections — that is a finding, not a script failure. `INCOMPLETE`
means `ab` itself gave up, and the marker beside it says which: `CLIENT-PORTS`
is the client running out of ephemeral ports, a limit of the machine and not of
the server (see below). Server logs go to mktemp files named at startup.

On an `AB-FAILED` or `TIMEOUT` cell, `run.sh` then diagnoses it rather than
leaving the reader to: it prints the server's own `fault:` lines (or says there
were none) and, for a `plain` cell, re-runs the same workload through
`probes/plain-drop-probe.py`, a raw-socket client that classifies every
connection on its own, and says which of the three answers it got: every
connection served (the server was healthy throughout and ab aborted over
something only ab saw), a connection accepted and never answered (a real
drop), or only connects that never landed (the client's port table, not the
server). The probe is capped by `AB_TIMEOUT` like the ab run itself, and
hitting that cap is the fourth answer: the server stopped answering this
client too, which is the real stall a dropped connection is not. `DIAGNOSE=0`
skips the whole step.

## Probes

`probes/` holds the two scripts that turned the Mac `plain` failure from "the
server stalls" into "one connection in a few thousand is never served". Both
are standalone and take `PORT`, `N`, `C` from the environment.

- `plain-drop-probe.py` — N one-shot `Connection: close` requests over C
  threads, each connection classified on its own. Exit status 0 when every
  connection was answered, 1 when one was accepted and never answered (the
  drop), 2 when the only failures were connects that never landed (the client
  port table, not the server), so a harness can branch on it; `run.sh` does.
- `plain-drop-hunt.sh` — a fresh server per round (the drop was only ever seen
  in the window after a start) running the probe until a round loses a
  connection, then leaving the server up and printing the server's faults, the
  connection states on the port, the fd count and the thread state.

For a single server by hand: `jolt -M:bench` from the repo root
(`PORT`, `STRATEGY`, `WORKERS` envs),
`cd benchmark/minimal-ring-undertow && clojure -M:run` (`PORT` env), or
`cd benchmark/minimal-ring-jetty && clojure -M:run` (`PORT` env).

## Ephemeral ports: read a `plain` failure here before blaming the server

In `plain` mode every request is its own connection and the SERVER closes it,
so each one leaves a TIME_WAIT entry for 2*MSL against the server's fixed port.
The 4-tuple is therefore pinned by the client's ephemeral port, and a run of N
requests needs N distinct ones inside that window — the run is far too fast for
any of them to have expired.

That is a client limit and it bites well below the default `N=20000`:

| | ephemeral ports | TIME_WAIT | loopback reuse |
|---|---:|---:|---|
| macOS (stock) | 16384 (49152-65535) | 30 s (`net.inet.tcp.msl` 15000) | no |
| Linux (stock) | 28232 (32768-60999) | 60 s | yes (`tcp_tw_reuse=2`) |

So on stock macOS the `plain` cells **cannot** complete as configured, whatever
the server does: 2000 warmup + 20000 = 22000 connections against 16384 ports.
Linux gets through it because the range is larger and the kernel recycles
loopback TIME_WAIT entries for new outgoing connections — one full matrix run
measured here left 14115 of them behind, which is half the range.

`run.sh` now prints `INCOMPLETE ... CLIENT-PORTS` rather than `TIMEOUT` when
`ab` gives up connecting, because the two used to be indistinguishable in the
output and they are not the same finding. Check it with `netstat -an | grep -c
TIME_WAIT` during a run. To measure the server instead of the port table:
lower `N`, raise the range (`sysctl -w net.inet.ip.portrange.first=16384` on
macOS), or read the `ka` cells, which reuse their connections and barely touch
it.

The same entries hurt a cell that runs after them even when it does not run out
of ports. The server closed those connections, so the TIME_WAIT side of each
4-tuple is the server's and the client's side is already free: the client
kernel hands the port to the next cell's connect, whose SYN lands on the
server's TIME_WAIT entry and waits out a retransmit before it gets through. At
20000 requests one such connect is most of the run. A chez `:threads` `ka c=100`
cell run straight after a `plain c=100` one measured 7.6k req/s with a 2018 ms
max, against 31-35k with a 16-34 ms max in the same session, p99 unchanged in
both. `run.sh` now lets the entries expire before each cell (`SETTLE`).

## Findings (M-series Mac, jolt 0.8.30, colocated, `ab -n 20000`, 2026-11-24)

Undertow and Jetty are the same server in all six runs (their behaviour does
not depend on the adapter's strategy), so their cells span six samples; each
chez row spans the three runs of its own strategy. Every cell ran after the
TIME_WAIT entries from the one before it had drained.

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 |
|---|---:|---:|---:|---:|
| undertow | 20.1-24.1k | 59.6-75.1k | 22.4-25.2k | 90.6-109.3k |
| jetty | 18.6-21.6k | 56.9-76.1k | 22.3-23.6k | 93.0-105.3k |
| chez `:threads` | 21.6-23.4k | 75.8-101.9k | 22.8-26.4k | 2.2-3.2k |
| chez `:fibers` | 18.4-19.3k | 23.7-24.6k | 21.1-22.2k | 22.3-23.1k |

The JVM servers are where they were on 0.8.13, within noise. Every chez cell but
one got faster. The ten plain and `/json` cells gained 1.2-1.5x (by the
midpoint of each range), chez `:fibers` `ka c=100` gained 1.1x, and chez
`:threads` `ka c=10` nearly doubled, from 42.9-46.2k to 75.8-101.9k, ahead of
both JVM servers on that cell (59.6-76.1k). chez `:threads` now leads both JVM
servers on four of the six cells it shares with them.

The exception is the regression of this round. chez `:threads` `ka c=100` read
40.8-43.5k on 0.8.13 and reads 2.2-3.2k here, in all three runs and again in
two isolated runs on a fresh server, with a 6-8 s longest request and 15.6-16.0k
of 20000 requests kept alive. The server logged no fault, and the cell is clean
in the sense the rest of this file uses: no abort, no stall mid-run, no dropped
connection.

The cause is worker starvation, and the pool size shows it. The same cell
against the same server with `:worker-threads` 100 instead of the default 10
measured 98.6k and 110.7k req/s with every request on keep-alive and a 7-10 ms
longest request. With 10 workers, ab's 100 connections split so that some
workers park on connections the client is not using while others sit idle and
new connections wait in the backlog; the work channel is unbuffered, so a
connection waiting there is never counted as pending and nothing retires the
parked ones. `retire-for-pressure!` exists for exactly this and is no longer
firing. It is not yet known whether the change is in the adapter (20 commits
touch `src/ring_chez` since the last round) or in jolt (0.8.13 to 0.8.30);
until it is found, a default-pool server under more keep-alive connections than
workers should be read as degraded, and `:worker-threads` at or above the
expected connection count avoids it.

`/json`, plain, all six runs — the body is 27 bytes instead of 13 and nothing
else differs, so body size is again not a factor:

| server | c=10 | c=100 |
|---|---:|---:|
| undertow | 18.8-24.9k | 24.0-24.6k |
| jetty | 22.2-23.3k | 22.3-24.6k |
| chez `:threads` | 20.9-24.4k | 25.1-26.1k |
| chez `:fibers` | 18.9-19.2k | 22.1-22.4k |

The six matrices put 732,000 requests through the adapter (2000 warmup plus
six 20000-request cells each) and it logged no drop.
