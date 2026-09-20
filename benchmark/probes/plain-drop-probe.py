#!/usr/bin/env python3
"""Plain-mode connect churn against one server, WITHOUT ab.

ab aborts its entire run over a single connection that is never answered, so
an `AB-FAILED` cell says nothing about whether the server is wedged or whether
it dropped one connection out of thousands. This asks the same question
without ab: N one-shot `Connection: close` requests over C threads, each
connection classified on its own as connect-fail, response-fail or ok.

    PORT=8081 N=5000 C=10 TIMEOUT=5 benchmark/probes/plain-drop-probe.py

Exit status, so a harness can branch on it: 0 when every connection was
answered, 1 when a connection was accepted and then never answered (the drop),
and 2 when the only failures were connects that never landed — the client
running out of ephemeral ports or fds, which is a limit of the machine and not
of the server (see the "Ephemeral ports" section of benchmark/README.md). The
last line is machine-readable:
`RESULT ok=<n> dropped=<n> connect-fail=<n> n=<n> secs=<f> rps=<n>`.
"""
import os
import socket
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "8081"))
N = int(os.environ.get("N", "5000"))
C = int(os.environ.get("C", "10"))
TIMEOUT = float(os.environ.get("TIMEOUT", "5"))
URI = os.environ.get("URI", "/plaintext")

REQUEST = ("GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n"
           % (URI, HOST)).encode()


def one(i):
    """Classify one connection: the connect and the response fail differently.

    A connect that succeeds and a response that never comes is the drop this
    probe exists to find — the server accepted the connection and nothing ever
    served it. A connect that fails is the client's own limit (ephemeral ports,
    fd ceiling) or a listen queue that is full, which is a different finding.
    """
    t0 = time.time()
    try:
        s = socket.create_connection((HOST, PORT), timeout=TIMEOUT)
    except Exception as e:
        return (i, "connect-fail:%s" % type(e).__name__, time.time() - t0)
    try:
        s.sendall(REQUEST)
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = s.recv(4096)
            if not chunk:
                return (i, "response-fail:eof-before-head", time.time() - t0)
            buf += chunk
        return (i, "ok", time.time() - t0)
    except Exception as e:
        return (i, "response-fail:%s" % type(e).__name__, time.time() - t0)
    finally:
        s.close()


def main():
    t0 = time.time()
    with ThreadPoolExecutor(C) as ex:
        results = list(ex.map(one, range(N)))
    secs = time.time() - t0
    fails = [r for r in results if r[1] != "ok"]
    dropped = [r for r in fails if r[1].startswith("response-fail")]
    connect = [r for r in fails if r[1].startswith("connect-fail")]
    ok = len(results) - len(fails)
    rps = ok / secs if secs else 0
    print("%d/%d ok in %.2fs = %d req/s; dropped=%d connect-fail=%d"
          % (ok, N, secs, rps, len(dropped), len(connect)))
    if fails:
        print("  by kind:", dict(Counter(r[1] for r in fails)))
        print("  first 8 (index, kind, seconds):",
              [(i, k, round(d, 3)) for i, k, d in fails[:8]])
    print("RESULT ok=%d dropped=%d connect-fail=%d n=%d secs=%.2f rps=%d"
          % (ok, len(dropped), len(connect), N, secs, rps))
    if dropped:
        return 1
    return 2 if connect else 0


if __name__ == "__main__":
    sys.exit(main())
