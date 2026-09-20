import socket, time, concurrent.futures
N, C = 5000, 10
results = []
# fix: simpler classify below
def one(i):
    t0 = time.time()
    try:
        s = socket.create_connection(("127.0.0.1", 8081), timeout=5)
    except Exception as e:
        return (i, "connect-fail", round(time.time()-t0,3))
    try:
        s.sendall(b"GET /plaintext HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        buf = b""
        while b"\r\n\r\n" not in buf or len(buf.split(b"\r\n\r\n",1)[1]) < 13:
            buf += s.recv(4096)
        s.close()
        return (i, "ok", round(time.time()-t0,3))
    except Exception as e:
        return (i, f"resp-fail:{type(e).__name__}", round(time.time()-t0,3))
t0 = time.time()
with concurrent.futures.ThreadPoolExecutor(C) as ex:
    results = list(ex.map(one, range(N)))
dt = time.time() - t0
oks = [r for r in results if r[1]=="ok"]
fails = [r for r in results if r[1]!="ok"]
print(f"{len(oks)}/{N} ok in {dt:.2f}s = {len(oks)/dt:.0f} req/s; fails={len(fails)}")
from collections import Counter
print(Counter(r[1] for r in fails))
if fails:
    print("first 8 fails (idx, kind, dur):", fails[:8])
    print("last 3 fails:", fails[-3:])
    print("max ok idx:", max((r[0] for r in oks), default=None))
