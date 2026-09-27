#!/usr/bin/env bash
# ab benchmark matrix: minimal Undertow and Jetty references vs this adapter.
#
#   benchmark/run.sh                     # full matrix, :threads strategy
#   STRATEGY=fibers benchmark/run.sh     # fibers strategy
#   WORKERS=200 benchmark/run.sh         # -> :worker-threads 200
#   N=2000 C_LIST="10" benchmark/run.sh  # quick smoke run
#   SERVERS="jetty chez" benchmark/run.sh  # subset of undertow/jetty/chez
#
# All three servers run the SAME bare handler with no middleware, so what is
# measured is the adapter and not a middleware stack.
#
# Requires: ab (ApacheBench), curl, timeout, lsof, clojure CLI, jolt.
# Results print to stdout; server logs go to mktemp files named on startup.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UNDERTOW_DIR="$ROOT/benchmark/minimal-ring-undertow"
JETTY_DIR="$ROOT/benchmark/minimal-ring-jetty"

UNDERTOW_PORT="${UNDERTOW_PORT:-8080}"
CHEZ_PORT="${CHEZ_PORT:-8081}"
JETTY_PORT="${JETTY_PORT:-8082}"
SERVERS="${SERVERS:-undertow jetty chez}"
STRATEGY="${STRATEGY:-threads}"
WORKERS="${WORKERS:-}"
N="${N:-20000}"
C_LIST="${C_LIST:-10 100}"
AB_TIMEOUT="${AB_TIMEOUT:-90}"   # per-run cap; a stalled run is reported as TIMEOUT
DIAGNOSE="${DIAGNOSE:-1}"        # on an ab failure, ask the server directly (0 to skip)
SETTLE="${SETTLE:-1}"            # wait for TIME_WAIT to drain before each cell (0 to skip)
SETTLE_TW="${SETTLE_TW:-500}"    # ...down to this many entries
SETTLE_MAX="${SETTLE_MAX:-90}"   # ...for at most this many seconds
PROBE="$ROOT/benchmark/probes/plain-drop-probe.py"

LABEL="chez-${STRATEGY}${WORKERS:+-$WORKERS}"
# `mktemp -t PREFIX` is the BSD spelling: GNU coreutils reads the same argument
# as a TEMPLATE and refuses it ("too few X's in template"), so on Linux this
# script died at startup before it ran a single request. A full template under
# TMPDIR is the spelling both accept.
UNDERTOW_LOG="$(mktemp "${TMPDIR:-/tmp}/undertow-bench.XXXXXX")"
JETTY_LOG="$(mktemp "${TMPDIR:-/tmp}/jetty-bench.XXXXXX")"
CHEZ_LOG="$(mktemp "${TMPDIR:-/tmp}/chez-bench.XXXXXX")"
UNDERTOW_PID=""
JETTY_PID=""
CHEZ_PID=""

wants() { case " $SERVERS " in *" $1 "*) return 0;; *) return 1;; esac; }

log_for_port() {
  case "$1" in
    "$UNDERTOW_PORT") echo "$UNDERTOW_LOG";;
    "$JETTY_PORT")    echo "$JETTY_LOG";;
    "$CHEZ_PORT")     echo "$CHEZ_LOG";;
  esac
}

# ab aborts its ENTIRE run over a single connection that is never answered, so
# `AB-FAILED` on its own cannot tell a wedged server from a healthy one that
# dropped one connection in thousands — and those are not the same finding.
# Ask the server the same question without ab: its own fault counter (a
# server-level failure prints one `fault:` line), then a raw-socket client over
# the identical plain workload, which classifies every connection on its own
# and so separates the three answers ab collapses into one: the server served
# everything (ab aborted over something it saw alone), a connection was
# accepted and never answered, or only connects failed (the client's port
# table). See benchmark/probes/plain-drop-probe.py.
diagnose() { # port mode
  local port="$1" mode="$2" log faults probe rc result verdict
  [ "$DIAGNOSE" = "1" ] || return 0
  log="$(log_for_port "$port")"
  if [ -n "$log" ] && [ -f "$log" ]; then
    # grep -c exits 1 on a zero count, which `set -e` would take as an error
    faults=0
    if grep -q "fault:" "$log" 2>/dev/null; then faults="$(grep -c "fault:" "$log")"; fi
    if [ "$faults" -gt 0 ]; then
      printf '%22s server log: %s fault line(s); last: %s\n' "" "$faults" \
        "$(grep "fault:" "$log" | tail -1 | head -c 100)"
    else
      printf '%22s server log: no fault lines — the server threw nothing\n' ""
    fi
  fi
  if [ "$mode" = "plain" ] && [ -x "$PROBE" ] && command -v python3 >/dev/null 2>&1; then
    # capped like every ab run, and for a sharper reason: against the wedged
    # server this is here to rule out, every connection waits out its own
    # TIMEOUT before the next one starts, so an uncapped probe costs
    # N/C * TIMEOUT — nearly three hours at the default N — and the diagnosis
    # of one failed cell hangs the rest of the matrix behind it.
    probe="$(PORT="$port" N="$N" C=10 timeout "$AB_TIMEOUT" python3 "$PROBE" 2>&1)" && rc=0 || rc=$?
    result="$(grep "^RESULT" <<<"$probe" || tail -1 <<<"$probe")"
    case "${rc:-0}" in
      0) verdict="the server served every connection; ab aborted on its own";;
      1) verdict="connections really were accepted and never answered";;
      2) verdict="only connects failed — the CLIENT ran out of 4-tuples, not a server fault";;
      124) result="no completion within ${AB_TIMEOUT}s"
           verdict="the server stopped answering this client too — a real stall, not one dropped conn";;
      *) verdict="the probe itself failed to run";;
    esac
    printf '%22s non-ab probe: %s -> %s\n' "" "$result" "$verdict"
  fi
  return 0
}

kill_port() {
  lsof -ti tcp:"$1" 2>/dev/null | xargs kill 2>/dev/null || true
}

cleanup() {
  [ -n "$UNDERTOW_PID" ] && kill "$UNDERTOW_PID" 2>/dev/null || true
  [ -n "$JETTY_PID" ] && kill "$JETTY_PID" 2>/dev/null || true
  [ -n "$CHEZ_PID" ] && kill "$CHEZ_PID" 2>/dev/null || true
  kill_port "$UNDERTOW_PORT"
  kill_port "$JETTY_PORT"
  kill_port "$CHEZ_PORT"
}
trap cleanup EXIT INT TERM

wait_up() { # port label logfile
  local port="$1" label="$2" log="$3" i
  for i in $(seq 1 60); do
    if curl -s -m 1 "http://127.0.0.1:$port/plaintext" >/dev/null 2>&1; then
      echo "$label up on :$port (log: $log)" >&2
      return 0
    fi
    sleep 1
  done
  echo "$label did not start; log: $log" >&2
  return 1
}

# Every connection a cell opens and the server closes leaves a TIME_WAIT entry
# on the SERVER side of the loopback 4-tuple for 2*MSL. The client's side of it
# is already CLOSED, so the client kernel happily hands the same ephemeral port
# to the next cell's connect, whose SYN then lands on the server's TIME_WAIT
# entry and waits out a retransmit (1-2s) before it gets through. One such
# connect is the whole tail of a 20000-request run: it measured a chez ka c=100
# cell at 7.6k req/s against 31-35k in the same session, with p99 unchanged and
# the max at 2018ms. Waiting for the entries to expire keeps each cell from
# paying for the one before it.
time_wait_count() {
  if command -v ss >/dev/null 2>&1; then
    ss -Htan state time-wait 2>/dev/null | wc -l | tr -d ' '
  else
    netstat -an 2>/dev/null | grep -c TIME_WAIT || true
  fi
}

settle() {
  local i tw
  [ "$SETTLE" = "1" ] || return 0
  for i in $(seq 1 "$SETTLE_MAX"); do
    tw="$(time_wait_count)"
    [ "${tw:-0}" -le "$SETTLE_TW" ] && return 0
    sleep 1
  done
  printf '%22s (still %s TIME_WAIT entries after %ss; the next cell may stall on a reused port)\n' \
    "" "$tw" "$SETTLE_MAX"
}

run_ab() { # label port mode c [path]   (mode: plain|ka)
  local label="$1" port="$2" mode="$3" c="$4" path="${5:-/plaintext}" out rc rps p50 p99 max ka note=""
  local flags=(-n "$N" -c "$c")
  if [ "$mode" = "ka" ]; then flags+=(-k); fi
  settle
  out=$(timeout "$AB_TIMEOUT" ab "${flags[@]}" "http://127.0.0.1:$port$path" 2>&1) && rc=0 || rc=$?
  if [ "$rc" -eq 0 ]; then
    if grep -qE "apr_socket_recv|timed out|Connection reset|Broken pipe" <<<"$out"; then
      note="  <- client errors (stall?)"
    fi
    rps=$(awk '/Requests per second/{print $4}' <<<"$out")
    p50=$(awk '$1=="50%"{print $2}' <<<"$out")
    p99=$(awk '$1=="99%"{print $2}' <<<"$out")
    max=$(awk '$1=="100%"{print $2}' <<<"$out")
    # in a ka cell, requests ab did NOT count as keep-alive each opened a new
    # connection: a server declining keep-alive shows up here and nowhere else
    ka=""
    if [ "$mode" = "ka" ]; then
      ka="  ka=$(awk '/Keep-Alive requests/{print $3}' <<<"$out")/$N"
    fi
    printf '%-18s %-5s c=%-4s %10s req/s  p50=%-4sms p99=%-5sms max=%-5sms%s%s\n' \
      "$label" "$mode" "$c" "$rps" "$p50" "$p99" "$max" "$ka" "$note"
  elif [ "$rc" -eq 124 ]; then
    printf '%-18s %-5s c=%-4s %10s  <- no completion within %ss\n' \
      "$label" "$mode" "$c" "TIMEOUT" "$AB_TIMEOUT"
    diagnose "$port" "$mode"
  else
    # ab gave up on its own, which is NOT the same finding as a server that
    # stopped answering — and the two used to print identically. The common
    # one in `plain` mode is the CLIENT running out of 4-tuples: every request
    # is a new connection the server closes, so each leaves a TIME_WAIT entry
    # against the server's fixed port for 2*MSL, and a run of N requests needs
    # N distinct ephemeral ports inside that window. macOS ships 16384 of them
    # (49152-65535) and no loopback TIME_WAIT reuse, so the stock `N=20000`
    # plain cells cannot complete there whatever the server does. See the
    # "Ephemeral ports" section of benchmark/README.md.
    if grep -qE "apr_socket_connect|Can.t assign requested address|Address already in use" <<<"$out"; then
      note="CLIENT-PORTS"
    else
      note="AB-FAILED"
    fi
    printf '%-18s %-5s c=%-4s %10s  <- %s (ab exit %s): %s\n' \
      "$label" "$mode" "$c" "INCOMPLETE" "$note" "$rc" \
      "$(grep -m1 -E "^(apr_|Test aborted|socket:)" <<<"$out" | head -c 120)"
    if [ "$note" = "AB-FAILED" ]; then diagnose "$port" "$mode"; fi
  fi
}

echo "n=$N  strategy=$STRATEGY  workers=${WORKERS:-default}  c_list='$C_LIST'  servers='$SERVERS'"

PORTS=""
if wants undertow; then
  (cd "$UNDERTOW_DIR" && PORT="$UNDERTOW_PORT" clojure -M:run >"$UNDERTOW_LOG" 2>&1) &
  UNDERTOW_PID=$!
  PORTS="$PORTS $UNDERTOW_PORT"
fi
if wants jetty; then
  (cd "$JETTY_DIR" && PORT="$JETTY_PORT" clojure -M:run >"$JETTY_LOG" 2>&1) &
  JETTY_PID=$!
  PORTS="$PORTS $JETTY_PORT"
fi
if wants chez; then
  (cd "$ROOT" && PORT="$CHEZ_PORT" STRATEGY="$STRATEGY" WORKERS="$WORKERS" jolt -M:bench >"$CHEZ_LOG" 2>&1) &
  CHEZ_PID=$!
  PORTS="$PORTS $CHEZ_PORT"
fi

if wants undertow; then wait_up "$UNDERTOW_PORT" undertow "$UNDERTOW_LOG"; fi
if wants jetty; then wait_up "$JETTY_PORT" jetty "$JETTY_LOG"; fi
if wants chez; then wait_up "$CHEZ_PORT" "$LABEL" "$CHEZ_LOG"; fi

# warmup every server before timing
for port in $PORTS; do
  ab -n 2000 -c 10 "http://127.0.0.1:$port/plaintext" >/dev/null 2>&1 || true
done

printf '%-18s %-5s %-8s %12s\n' SERVER MODE CONC 'REQ/S'
for c in $C_LIST; do
  for mode in plain ka; do
    if wants undertow; then run_ab undertow "$UNDERTOW_PORT" "$mode" "$c"; fi
    if wants jetty; then run_ab jetty "$JETTY_PORT" "$mode" "$c"; fi
    if wants chez; then run_ab "$LABEL" "$CHEZ_PORT" "$mode" "$c"; fi
  done
done

if [ -n "${JSON:-}" ]; then
  echo
  echo "--- /json ---"
  for c in $C_LIST; do
    if wants undertow; then run_ab undertow "$UNDERTOW_PORT" plain "$c" /json; fi
    if wants jetty; then run_ab jetty "$JETTY_PORT" plain "$c" /json; fi
    if wants chez; then run_ab "$LABEL" "$CHEZ_PORT" plain "$c" /json; fi
  done
fi
