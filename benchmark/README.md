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

## Findings (2026-09-20, M-series Mac, jolt 0.8.10, colocated, `ab -n 20000`)

Same Mac as the 2026-08-20 table below, now on jolt 0.8.10 with the
server-faults hardening and the unclaimed-connection sweeper merged. Three
runs of the whole matrix per strategy, `/json` pass on the first. The
ephemeral-port range was widened to 16384-65535 for the plain cells
(`sysctl -w net.inet.ip.portrange.first=16384`), so they complete on the
stock client for the first time.

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 |
|---|---:|---:|---:|---:|
| undertow | 19.0-26.3k | 61.2-79.3k | 21.5-24.2k | 92.4-106.1k |
| jetty | 17.3-21.1k | 60.2-74.6k | 19.3-21.9k | 92.6-102.1k |
| chez `:threads` | 1.9-3.4k | 1.8-3.1k | 3.2-3.3k | 3.2-3.3k |
| chez `:fibers` | 3.1-13.3k | 15.9-16.6k | 3.1-13.6k | 15.3-16.5k |

The fibers `plain` range is bimodal, not spread: 3.1-3.5k on two servers
that lost connections, 13.3-13.6k on the one that did not.

`/json`, plain, one run — same story as `/plaintext`, so body size remains
irrelevant:

| server | c=10 | c=100 |
|---|---:|---:|
| undertow | 20549 | 21661 |
| jetty | 21874 | 19004 |
| chez `:threads` | 2745 | 3495 |

Undertow and Jetty land in their 2026-08-20 classes (ka cells squarely;
plain cells a touch wider, 17.3-26.3k vs 19.7-23.7k before), which says the
machine is healthy and the comparison is fair. The chez cells did not, and
the reason is not the adapter: it is the Mac `plain` drop of PR #39
now amplified by the sweeper that reclaims it.

What happened, verified step by step on this sitting:

- **Every adapter commit between Aug 31 and today benches 14-16k req/s on
  `plain c=10` at `N=3000`** — swept one by one from the jolt-0.8.0
  migration (`23d1fbc`) through main (`6a53b6f`), single server, no
  diagnosis machinery. There is no adapter regression in that window.
- **At `N=20000` the same main build falls to ~2.8k req/s with `unclaimed
  fault:` lines in the server log.** Drop counts per matrix run (42000
  connections: 2000 warmup plus the two 20000-request plain cells): 20, 20,
  20 on the three `:threads` servers — one per ~2100 connections, PR #39's
  rate — and 5, 5, 0 on the `:fibers` ones. Each drop
  now costs five seconds of sweeper claim-deadline before the fd is
  reclaimed. Against `ab -c 10`, whose ten slots have ~13 slot-seconds of
  real work at 15k req/s, twenty drops contribute ~100 slot-seconds of
  waiting: the cell reads ~2.8k.
- **The `ka` collapse on `:threads` is the same drop read sideways.** An
  unclaimed connection counts in `pending` for its whole 5s window, and
  `idle-poll-recv!` (adapter.clj:355) retires every idle keepalive once
  `pending` is positive past a 2s grace — so with drops arriving every few
  hundred milliseconds somewhere in a 20k run, keepalive on `:threads`
  never engages. That is why `ka` cells sit at the same ~3k as `plain`
  ones, ~15x below the 47-54k of 2026-08-20.
- **`:fibers` ka is untouched at 15.3-16.6k, and its one drop-free server
  ran `plain` at 13.3-13.6k** — 2026-08-20's class (15.0-15.5k). The
  sweeper's keepalive retirement applies to the threads strategy; a dropped
  fiber conn costs its own 5s on one connection, and every other connection
  keeps keepalive. Plain cells collapse on both strategies whenever drops
  happen — drops are per-connection, and every plain request is its own
  connection. Three servers per strategy is a small sample, but threads
  dropped on 3/3 and fibers on 2/3 at a quarter of the count, which points
  at the `>!!` claim hop rather than the poller handoff.
- **The 5s backstop itself is working as designed**: fd counts stay flat,
  no CLOSE_WAIT residue, and every drop is counted and reported as
  `:unclaimed` in `server-stats` rather than leaking silently. What
  changed since 2026-08-20 is that the Mac drop acquired a visible,
  throughput-scale price tag.

What it does not say: that the adapter got slower. A jolt-side fix for the
lost handoff (the still-open question from PR #39 — why the claim/handoff
hop loses a connection once in a few thousand, only on this machine) would
restore the 2026-08-20 numbers on every cell, because the commits in
between were proven clean above. Until then, on this Mac the honest matrix
reads as the table above: JVM references unchanged, chez cells priced by
the drop.

## Findings (2026-09-20, same Mac, branch `keepalive`: the drop no longer prices the matrix)

The sitting above ended in a fix rather than a shrug, and the numbers above
are now historical. Three changes, all adapter-side, all TDD'd on the
branch:

- **Pressure leaves at the rendezvous, not at the claim.** A conn whose
  `>!!` handoff answered is past the queue — nobody is ever going to take
  it again — so it stops counting as accept pressure the moment the
  rendezvous completes (once-only, shared with the worker's take-time claim
  through the same CAS). A lost conn therefore retires nobody's keep-alive:
  the ka collapse is gone, not mitigated.
- **A lost conn is answered, not silenced.** The sweeper writes a
  best-effort `503 Service Unavailable` with `Connection: close` before it
  releases the fd — the answer Undertow gives an exchange it cannot serve.
  One zero-timeout poll for writability, one send, failure ignored: the
  answer is a courtesy, the release is the guarantee.
- **The backstop is 500ms, and it covers the second loss class too.** A
  conn claimed by a worker whose serving loop never started — the "claim
  hop or below" class from PR #39, which once held an ab run for its full
  30s poll timeout — gets the same answered close after four sweep
  deadlines (the margin covers an owner that is merely late), reported as
  `:claimed-silent` in server-stats, its own fault kind. That conn has an
  owner, so the sweeper shuts it down and leaves the fd number for the
  owner to release rather than closing it under them. The sweep deadline
  renamed from claim-deadline to sweep-deadline to say what it now measures.

The same matrix, same machine, same jolt, after (two runs per strategy;
before, from the table above, in parentheses):

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 |
|---|---:|---:|---:|---:|
| chez `:threads` | 9.1-11.8k (1.9-3.4k) | 37.8-40.2k (1.8-3.1k) | 9.8-11.8k (3.2-3.3k) | 14.2-23.4k (3.2-3.3k) |
| chez `:fibers` | 12.4-13.6k (3.1-13.3k) | 15.2-16.2k (15.9-16.6k) | 13.0-13.6k (3.1-13.6k) | 15.2-16.1k (15.3-16.5k) |

Every cell completed — no ab aborts in eight matrices' worth of chez runs,
where the 2026-09-20 sitting had one and the 2026-08-20 one had several —
and the raw-socket probe's verdict on the one cell that did abort
mid-sitting (`ok=20000 dropped=0`) confirmed every connection now gets an
answer. Drop counts are unchanged (20 and 20 per threads matrix server,
0-6 on fibers): the fix does not stop the jolt-side loss, it stops the loss
from costing anything but itself. The arithmetic of what remains is exact:
at ~20 drops per 42000-conn matrix and 500ms per answered drop, plain
c=10's ten slots lose ~10 slot-seconds of a ~140s run — the observed
9-12k against the drop-free 13-16k class is that and nothing else.

Keep-alive against the JVM, finally readable: threads ka c=10 lands at
37.8-40.2k against undertow's 64-78k and jetty's 64-75k on this machine —
half to two-thirds of the JVM servers rather than a twentieth, with the
remaining gap the per-request worker-dispatch cost. ka c=100 reads
14.2-23.4k against 92-106k; that gap is dispatch again, plus a pool of
default 40 workers against 100 hot connections. The structural floor —
one OS thread per request under a blocking Ring handler — is the honest
ceiling on this design, and it is the one thing an adapter-side change
cannot lift.

## Findings (2026-09-20, same Mac: the drop was a weak compare-and-swap, and it is gone)

The jolt-side loss that PR #39 left open and the section above priced is
found and fixed. It was never in the channel or the poller: the worker's
`(compare-and-set! owned? false true)` — the claim every serving path hangs
off — answered **false with the atom still false**. jolt's
`compare-and-set!` sat on Chez's `$record-cas!`, which on AArch64 is a
single `ldxr`/`stxr` (`s/arm64.ss` `asm-cas`); the `stxr` fails whenever the
core's exclusive monitor was cleared between the two — a context switch, or
another core storing into the same reservation granule, which the acceptor
writing `:claim-by` beside `:owned?` is — and the primitive reports failure
with the value untouched. The worker read that as "stop-server owns this
conn" and walked away; the sweeper found it unowned 500ms later. Both
strategies claim through the same CAS, so both dropped; x86's `cmpxchg`
cannot fail spuriously, so Linux CI never saw it. Traced by stamping the
worker's steps on every entry the sweeper reaped: `<!!` returned it at
t=0, `claim-close!` lost at t=0, and 500ms later the same CAS won for the
sweeper. Reproduced without the adapter: 50-130 spurious refusals per 3M
single-owner CASes under neighbour stores (jolt `test/chez/cas-test.ss`).

Two fixes, either sufficient: jolt-lang/jolt#1072 makes `sa-record-cas!`
strong (retry while a re-read still holds the expected value), and the
adapter's every once-only claim now goes through `ring-chez.cas/cas!`,
which does the same over the released primitive until `:jolt/min-version`
carries the fix.

Same cells, same machine, stock jolt 0.8.10 plus the adapter change — but a
plainer harness than `run.sh`: `ab -n 20000 -q` straight at a
`(fn [_] {:status 200 :body "x"})` handler, no warmup and no `/json` pass, so
the numbers are the drop's absence, not a re-run of the matrix (two to five
runs per cell; the `keepalive` matrix numbers above in parentheses):

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 |
|---|---:|---:|---:|---:|
| chez `:threads` | 15.5-16.2k (9.1-11.8k) | 34.5-34.9k (37.8-40.2k) | 15.4-15.7k (9.8-11.8k) | 27.1-29.9k (14.2-23.4k) |
| chez `:fibers` | 13.4-13.7k (12.4-13.6k) | 15.8-16.1k (15.2-16.2k) | 14.4-15.4k (13.0-13.6k) | 16.8-16.9k (15.2-16.1k) |

**Zero drops** — 0 `:unclaimed`, 0 failed requests — over 260k connections
across every cell, on both strategies, where the same runs on the fixed jolt
alone read the same (0 in 180k) and stock jolt without the adapter change
read 12-29 per 20k. The plain cells land in the drop-free class the
previous section predicted from arithmetic (13-16k); the ka cells sit where
the `keepalive` matrix put them, harness differences aside. What separates
threads ka c=10 from 2026-08-20's 47-54k is not answered here.

One thing this sitting saw that is NOT the drop, recorded so it is not
mistaken for it: on `:threads` ka c=100 — a hundred hot keep-alive
connections over the default ten-worker pool — about one run in seven takes
2.7s instead of 0.7s and ab reports ~5 `Length` failures (0-byte responses)
with `server-stats` at 0 faults. That is a single 2-second stall plus a few
connections closed on a peer mid-reuse, and it sits in the accept-pressure
retire path (`idle-poll-recv!`'s 2s grace): with `:worker-threads 100` the
same cell ran 8/8 clean. Oversubscription policy, not a lost handoff; its
own issue.

## Findings (2026-09-20, 4-core Linux 6.8, jolt 0.8.10, colocated, `ab -n 20000`)

A second machine and a much smaller one, recorded because the matrix had never
run on Linux at all — `run.sh` died at startup on `mktemp -t undertow-bench`,
which is the BSD spelling of an argument GNU coreutils reads as a template and
refuses. Chez only; three runs of the whole matrix per strategy.

| server | plain c=10 | ka c=10 | plain c=100 | ka c=100 |
|---|---:|---:|---:|---:|
| chez `:threads` | 5.31-5.47k | 6.86-6.98k | 5.30-5.37k | 6.91-6.97k |
| chez `:fibers` | 4.60-4.63k | 6.11-6.17k | 4.87-4.88k | 6.06-6.08k |

Re-run after the server-fault hardening (same machine, same sitting):
threads 5.05-5.20k plain / 6.56-6.59k ka, fibers 4.32-4.66k plain / 5.81-5.97k
ka — every cell complete, and the guards cost nothing measurable.

What is worth saying about them:

- **No stall, anywhere.** 40 further cells across `:worker-threads` 1/2/4/16 and
  concurrency 10/50/200/etc, a 200k-request plain soak, and 100k requests
  against a deliberately allocation-heavy handler (~3000 maps per request, to
  make the collector run constantly) all completed with zero failures and a
  flat fd and thread count. The `plain`-mode hang reported against jolt 0.8.9
  on macOS does not reproduce here.
- **Keepalive is only ~1.3x plain, not ~3x.** With four workers and ten or more
  clients there is essentially always a connection waiting for a worker, so the
  accept-pressure retirement stamps `Connection: close` on nearly every
  response and keep-alive never engages. That is the documented trade-off doing
  what it says, but it means the `ka` cells on a small machine measure roughly
  the same thing the `plain` cells do.
- **Throughput barely scales with the pool**: 3.45k at one worker, 5.00k at
  two, 5.55k at four, 4.54k at sixteen. Four cores, so the last is
  oversubscription; the first three are the interesting shape, and 1.6x from
  four times the workers says most of a request is spent somewhere serialized
  rather than in the workers.

One data point from the Mac that reported the `plain`-mode stall, re-run with
this branch on jolt 0.8.10 (`N=5000`, well under its 16384-port table): both
`plain` cells still end `AB-FAILED` with `apr_pollset_poll` timeout — ab was
connected and the response never came — while both `ka` cells either side of
them ran at full speed. The server log carries no `fault:` line and no
GC-rendezvous line, and the port table held ~2.4k TIME_WAIT entries against
~16k available. So on that machine it was none of the three known causes: not a
server-level throw, not a whole-process GC stall (the server kept serving
between failures), not the client port table.

It was one dropped connection. A raw-socket client over the identical plain
workload (`probes/plain-drop-probe.py`) completed 5000/5000 in 0.7s
against both strategies, and across fresh-server runs it caught what ab was
reporting: roughly one connection in a few thousand was accepted, handed on,
and then claimed by nobody — connect succeeded, the request was never
answered, and the connection froze as `CLOSE_WAIT` on the server against
`FIN_WAIT_2` on the client, with nothing having recv'd, polled or shut it
down. ab aborts its *entire* run over one of those, which is why a whole
`plain` cell read `AB-FAILED` while every other client and every other cell
saw a healthy server.

The adapter no longer leaves such a connection lying there: the connection
sweeper now runs on both strategies and enforces a claim deadline as well as
an idle one, so a connection nobody has claimed within five seconds of the
handoff (500ms, and answered with a 503, since the `keepalive` findings above)
is taken over by the sweeper, closed, and counted as an `:unclaimed`
server fault (`server-stats`). That is a backstop and not a root cause — why
the handoff loses a connection at all, once in a few thousand and only on that
machine, is still open — but it bounds what one costs: the peer is closed on
after five seconds instead of waiting on silence forever, the fd comes back
instead of sitting in `CLOSE_WAIT` for the life of the process, and the loss
is a number a caller can read rather than nothing at all. It does not make the
cell pass: a dropped connection is still a connection ab never got an answer
for, and ab still aborts over it. See the `lost-handoff` / `lost-spawn` tests
in the suite.

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
