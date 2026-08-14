#!/usr/bin/env bash
# ab benchmark matrix: minimal Undertow reference vs this adapter.
#
#   benchmark/run.sh                     # full matrix, :threads strategy
#   STRATEGY=fibers benchmark/run.sh     # fibers strategy
#   WORKERS=200 benchmark/run.sh         # -> :worker-threads 200
#   N=2000 C_LIST="10" benchmark/run.sh  # quick smoke run
#
# Requires: ab (ApacheBench), curl, timeout, lsof, clojure CLI, jolt.
# Results print to stdout; server logs go to mktemp files named on startup.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UNDERTOW_DIR="$ROOT/benchmark/minimal-ring-undertow"

UNDERTOW_PORT="${UNDERTOW_PORT:-8080}"
CHEZ_PORT="${CHEZ_PORT:-8081}"
STRATEGY="${STRATEGY:-threads}"
WORKERS="${WORKERS:-}"
N="${N:-20000}"
C_LIST="${C_LIST:-10 100}"
AB_TIMEOUT="${AB_TIMEOUT:-90}"   # per-run cap; a stalled run is reported as TIMEOUT

LABEL="chez-${STRATEGY}${WORKERS:+-$WORKERS}"
UNDERTOW_LOG="$(mktemp -t undertow-bench)"
CHEZ_LOG="$(mktemp -t chez-bench)"
UNDERTOW_PID=""
CHEZ_PID=""

kill_port() {
  lsof -ti tcp:"$1" 2>/dev/null | xargs kill 2>/dev/null || true
}

cleanup() {
  [ -n "$UNDERTOW_PID" ] && kill "$UNDERTOW_PID" 2>/dev/null || true
  [ -n "$CHEZ_PID" ] && kill "$CHEZ_PID" 2>/dev/null || true
  kill_port "$UNDERTOW_PORT"
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

run_ab() { # label port mode c   (mode: plain|ka)
  local label="$1" port="$2" mode="$3" c="$4" out rps p50 p99 note=""
  local flags=(-n "$N" -c "$c")
  if [ "$mode" = "ka" ]; then flags+=(-k); fi
  if out=$(timeout "$AB_TIMEOUT" ab "${flags[@]}" "http://127.0.0.1:$port/plaintext" 2>&1); then
    if grep -qE "apr_socket_recv|timed out|Connection reset|Broken pipe" <<<"$out"; then
      note="  <- client errors (stall?)"
    fi
    rps=$(awk '/Requests per second/{print $4}' <<<"$out")
    p50=$(awk '$1=="50%"{print $2}' <<<"$out")
    p99=$(awk '$1=="99%"{print $2}' <<<"$out")
    printf '%-18s %-5s c=%-4s %10s req/s  p50=%-4sms p99=%-5sms%s\n' \
      "$label" "$mode" "$c" "$rps" "$p50" "$p99" "$note"
  else
    printf '%-18s %-5s c=%-4s %10s\n' "$label" "$mode" "$c" "TIMEOUT"
  fi
}

echo "n=$N  strategy=$STRATEGY  workers=${WORKERS:-default}  c_list='$C_LIST'"

(cd "$UNDERTOW_DIR" && PORT="$UNDERTOW_PORT" clojure -M:run >"$UNDERTOW_LOG" 2>&1) &
UNDERTOW_PID=$!
(cd "$ROOT" && PORT="$CHEZ_PORT" STRATEGY="$STRATEGY" WORKERS="$WORKERS" jolt -M:bench >"$CHEZ_LOG" 2>&1) &
CHEZ_PID=$!

wait_up "$UNDERTOW_PORT" undertow "$UNDERTOW_LOG"
wait_up "$CHEZ_PORT" "$LABEL" "$CHEZ_LOG"

# warmup both before timing
for port in "$UNDERTOW_PORT" "$CHEZ_PORT"; do
  ab -n 2000 -c 10 "http://127.0.0.1:$port/plaintext" >/dev/null 2>&1 || true
done

printf '%-18s %-5s %-8s %12s\n' SERVER MODE CONC 'REQ/S'
for c in $C_LIST; do
  run_ab undertow "$UNDERTOW_PORT" plain "$c"
  run_ab undertow "$UNDERTOW_PORT" ka "$c"
  run_ab "$LABEL" "$CHEZ_PORT" plain "$c"
  run_ab "$LABEL" "$CHEZ_PORT" ka "$c"
done
