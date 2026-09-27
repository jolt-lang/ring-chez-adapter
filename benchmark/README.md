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
prints req/s plus p50/p99 latency. `TIMEOUT` (no completion within
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
macOS), or read the `ka` cells, which reuse a handful of connections and never
touch it.

## Findings (M-series Mac, jolt 0.8.13, colocated, `ab -n 20000`, 2026-09-27)

Undertow and Jetty are the same server in all six runs (their behaviour does
not depend on the adapter's strategy), so their cells span six samples; each
chez row spans the three runs of its own strategy.

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 |
|---|---:|---:|---:|---:|
| undertow | 17.6-21.4k | 55.0-84.8k | 18.0-22.3k | 79.3-110.5k |
| jetty | 15.7-18.4k | 49.7-74.7k | 17.3-20.6k | 52.9-104.3k |
| chez `:threads` | 12.5-17.1k | 39.4-47.0k | 15.6-16.0k | 7.6-32.1k |
| chez `:fibers` | 11.0-13.4k | 17.0-18.7k | 14.9-15.7k | 20.1-20.2k |

The chez `:threads` `ka c=100` cell is the one unstable spot: its three runs
came in at 28.9k, 7.6k and 32.1k, a 4.3x spread where the other eleven chez
cells stay within 1.4x. All three runs completed with no drop and no server
fault, and the slow run's latency (p50 0 ms, p99 17 ms) is no worse than the
fast run's (p99 10 ms), so it is run-to-run variance in that cell rather than
a stall.

`/json`, plain, all six runs — the body is 27 bytes instead of 13 and nothing
else differs, so body size is again not a factor:

| server | c=10 | c=100 |
|---|---:|---:|
| undertow | 18.6-20.6k | 18.0-20.5k |
| jetty | 18.3-20.9k | 17.4-20.6k |
| chez `:threads` | 14.6-16.1k | 14.1-15.3k |
| chez `:fibers` | 13.1-14.3k | 15.8-17.5k |

The six matrices put 732,000 connections through the adapter (2000 warmup plus
six 20000-request cells each) and it logged no drop.

