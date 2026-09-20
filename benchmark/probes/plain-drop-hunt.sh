#!/usr/bin/env bash
# Hunt the intermittent plain-mode connection drop: run plain-drop-probe.py
# against a FRESH server, round after round, until one round loses a
# connection — then leave the scene standing (server still up, log kept) and
# print what the kernel and the process were doing at that moment.
#
#   benchmark/probes/plain-drop-hunt.sh                    # 8 rounds, fibers
#   ROUNDS=20 STRATEGY=threads benchmark/probes/plain-drop-hunt.sh
#
# Fresh server per round on purpose: the drop was only ever seen in the window
# after a start, so a long-lived server does not reproduce it.
#
# Exit 0: every round clean. Exit 7: a round dropped a connection, and the
# server it happened on is still running (pid and log printed). Exit 1: the
# server would not start, or the client ran out of 4-tuples (the probe's own
# exit 2 — that is the machine's limit, not a finding about the server).

set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PROBE="$HERE/plain-drop-probe.py"

PORT="${PORT:-8081}"
STRATEGY="${STRATEGY:-fibers}"
ROUNDS="${ROUNDS:-8}"
# TIME_WAIT from the previous round has to drain before the next one, or the
# client runs out of 4-tuples and the round fails for a reason that is not the
# server's (see the "Ephemeral ports" section of benchmark/README.md).
SETTLE="${SETTLE:-22}"

export PORT STRATEGY
export N="${N:-5000}" C="${C:-10}"

for round in $(seq 1 "$ROUNDS"); do
  lsof -ti tcp:"$PORT" 2>/dev/null | xargs kill 2>/dev/null
  sleep "$SETTLE"

  log="$(mktemp "${TMPDIR:-/tmp}/chez-hunt-$round.XXXXXX")"
  (cd "$ROOT" && jolt -M:bench) >"$log" 2>&1 &
  pid=$!

  up=""
  for _ in $(seq 1 60); do
    curl -s -m 1 "http://127.0.0.1:$PORT/plaintext" >/dev/null 2>&1 && { up=1; break; }
    sleep 0.5
  done
  if [ -z "$up" ]; then
    echo "round $round: server did not start; log: $log"
    kill "$pid" 2>/dev/null
    exit 1
  fi

  out="$(python3 "$PROBE" 2>&1)"
  rc=$?
  echo "== round $round ($STRATEGY): $(tail -n1 <<<"$out")"

  if [ "$rc" -eq 0 ]; then
    kill "$pid" 2>/dev/null
    continue
  fi
  if [ "$rc" -ne 1 ]; then
    echo "round $round: the probe could not measure the server (exit $rc); log: $log"
    kill "$pid" 2>/dev/null
    exit 1
  fi

  echo "FAILURE in round $round — freezing the scene (server pid $pid, log $log)"
  sed -n '1,20p' <<<"$out"
  echo "--- server faults ---"
  grep -n "fault:" "$log" || echo "(none — nothing was thrown)"
  echo "--- connections on :$PORT ---"
  (netstat -an 2>/dev/null || ss -an 2>/dev/null) | grep ":$PORT" | sort | uniq -c | sort -rn | head -20
  echo "--- fds held ---"
  lsof -p "$pid" 2>/dev/null | grep -c TCP
  echo "--- threads ---"
  (sample "$pid" 2 -mayDie 2>/dev/null || cat "/proc/$pid/status" 2>/dev/null) | head -40
  exit 7
done

echo "no drop in $ROUNDS rounds"
exit 0
