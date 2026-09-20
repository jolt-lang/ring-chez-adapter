#!/bin/bash
for round in $(seq 1 8); do
  lsof -ti tcp:8081 2>/dev/null | xargs kill 2>/dev/null; sleep 22
  ulimit -n 10240
  PORT=8081 STRATEGY=fibers jolt -M:bench > /tmp/chez-hunt-$round.log 2>&1 & SPID=$!
  for i in $(seq 1 60); do curl -s -m 1 http://127.0.0.1:8081/plaintext >/dev/null 2>&1 && break; sleep 0.5; done
  OUT=$(python3 /tmp/plainclient2.py 2>&1)
  echo "== round $round: $OUT"
  if echo "$OUT" | grep -q "fails=0"; then
    kill $SPID 2>/dev/null
  else
    echo "FAILURE in round $round — freezing scene, SPID=$SPID"
    echo "SPID=$SPID" > /tmp/hunt-spid
    exit 7
  fi
done
exit 1
