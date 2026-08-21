# RFC-0013: SO_REUSEPORT, and `:remote-addr` behind a proxy

- Status: Accepted (wave 2, round 6)
- Prior art: Igropyr `http.sc` ~1875 (`(reuseport . #t)` → `UV_TCP_REUSEPORT`),
  `middleware.sc` ~130–175 (`trust-proxy`, `client-key`, `nth-from-right`)

## Summary

Two small, independent operational adoptions:

- `:reuse-port` on `run-server` — bind with `SO_REUSEPORT` so several
  processes can share the port, kernel-balanced.
- `ring-chez.middleware.proxy/wrap-forwarded-remote-addr` — `:remote-addr`
  from a *declared* number of trusted proxy hops, counted from the right.

## `:reuse-port`

One `setsockopt` before `bind`, opt-in and defaulting to false. Off, a
second bind on a live port fails, and that failure is how you discover a
server is already running — the adapter already turns it into a
plain-English `EADDRINUSE` message, and losing that by default would be a
bad trade.

On Linux the kernel spreads new connections across every socket bound to
the port, which is the reason to want it: N processes, one port, no
front-end. The BSDs allow the bind under the same name without the
balancing, so the README says Linux.

Constants differ by platform (macOS `0x0200`, Linux `15`), like the
`SO_REUSEADDR` pair already in `socket.clj`. Setting it *before* bind
matters — after, it does not apply to the binding.

## Trusted-proxy `:remote-addr`

`ring.middleware.proxy-headers/wrap-forwarded-remote-addr` takes the
**leftmost** `X-Forwarded-For` entry. That entry is written by the client.
Anything keyed on `:remote-addr` behind it — an IP allow-list, a rate
limiter, an audit log — can be pointed anywhere by sending a header.

Igropyr's framing is that the number of proxies that append to that header
is a fact about the deployment, which only the operator knows, so it is
declared:

```
X-Forwarded-For: 1.2.3.4, 10.0.0.9, 10.0.0.1
  :trust-proxy 0  =>  peer address        (default: trust nothing)
  :trust-proxy 1  =>  10.0.0.9            (what your edge observed)
  :trust-proxy 2  =>  1.2.3.4
```

Counting from the right means a client can prepend as many entries as it
likes without moving the one that is picked. Fewer entries than declared
hops falls back to the connection's peer address — never to a
client-supplied value. The peer address stays on the request as
`:ring-chez/peer-addr`, since it is the one thing here that cannot be
forged.

This is exactly as safe as the declared count is true: pointed at a
deployment with no proxy, `:trust-proxy 1` hands the client control of its
own address. The docstring says so in those words.

## Alternatives considered

- **Trusting `X-Forwarded-For` by default.** No: it is a client-supplied
  header, and the default has to be the address the kernel reports.
- **A trusted-networks list (nginx `set_real_ip_from` style)** — strictly
  more expressive, and it needs CIDR parsing plus a policy for chains that
  do not match. A hop count covers the deployments people actually have
  behind one CDN or one load balancer; the list can come later if someone
  needs it.
- **Parsing RFC 7239 `Forwarded:`.** Same rules would apply, and almost
  nothing emits it. Later, if asked.

## Test plan

- Two servers bind the same port with `:reuse-port true` and the port
  serves; without it the second bind is refused with `:errno-name
  "EADDRINUSE"` in ex-data.
- `:trust-proxy 1` picks the entry the trusted hop saw, and a client
  forging a four-entry chain cannot move that pick; an empty header falls
  back to the peer; `:trust-proxy 0` ignores the header entirely;
  `:ring-chez/peer-addr` is preserved.
- `nth-from-right` at the boundaries: first, last, past the end, nil.
