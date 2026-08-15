# RFC-0004: websocket upgrade guard

- Status: Accepted (phase 4)
- Prior art: Igropyr `http.sc` ~1610 (`#(ws-reject status text)`: "an auth
  guard refused the upgrade — answered before any handshake, so an
  unauthenticated peer never gets the socket"), `auth.sc` (`token-guard`,
  `session-guard` composing with `app-ws`)

## Summary

New `run-server` opt `:ws-guard` — `(fn [request])` consulted when a
websocket upgrade request arrives, *before* the 101 is sent:

- returns truthy-and-not-a-map → proceed with the upgrade as today
- returns a Ring response map → that response is served instead; no 101,
  no session, and the response goes through the normal send path so the
  connection stays keep-alive-usable
- absent → proceed (backward compatible: every existing `:ws-handler`
  setup upgrades exactly as before)

## Motivation

Today the upgrade decision is binary: `:ws-handler` configured + valid
upgrade request → 101, always. An app that wants to authenticate upgrade
requests (subprotocol negotiation, origin checks, rate limiting) has no
hook before the socket is handed over — after 101 the only possible
answers are ws close frames, and the peer already "got the socket".
Igropyr's `ws-reject` exists precisely so "an unauthenticated peer never
gets the socket"; our equivalent moves the decision to the request layer
where a plain HTTP response is still a valid answer.

## Design

```clojure
;; inside connection-loop's upgrade branch, replacing the bare
;; (and ws-handler (upgrade-request? request)) gate:
(let [guard (:ws-guard svc)]
  (cond
    (not (and ws-handler (upgrade-request? request))) ...normal path...
    (nil? guard) ...upgrade...
    :else (let [v (guard request)]
            (if (map? v) ...send v as the response, keep-alive as usual...
                          ...upgrade...))))
```

- The guard runs inside the same try boundary as the handler (a guard
  throw is a request failure → RFC-0003's failure path, `:on-failure`
  included). Deliberate: guard bugs are handler bugs.
- A guard response map is sent via the existing `send-response` — nothing
  new on the wire path; `Connection: close` handling, keep-alive pressure
  stamping, and Content-Length all apply as for any handler response.
- The guard sees the full parsed Ring request (headers available for
  `Sec-WebSocket-Protocol`, `Origin`, cookies).
- Igropyr's `#(ws-reject status text)` shaped value is deliberately *not*
  introduced: a Ring response map is the richer, idiomatic equivalent —
  `{:status 401, :body "..."}` vs a status+text tuple. Port the semantics,
  not the container (RFC-0000 principle).
- Composition with Igropyr's `token-guard` style: guards that need
  secrets close over them, `(fn [req] (and (authorized? req) ...))`;
  returning the request itself is a fine truthy accept (not a map? a
  request IS a map! — so the accept contract is "truthy and not a
  response-shaped decision"... see Drawbacks).

**Accept/reject contract (precise):** the guard returns

- a map containing `:status` → treated as a reject-with-response. A Ring
  request map never contains `:status`, so requests and responses are
  unambiguous.
- anything else truthy → accept the upgrade.
- nil/false → reject with the RFC-0003 fallback 500? **No** — nil/false is
  a *reject*, and a bare reject answers `403 Forbidden` (plain text). A
  guard that declines without a response means "not allowed", not "server
  error".

## Alternatives considered

- **Reuse `:ws-handler`'s return value** (Igropyr's single-function shape
  where the handler/guard wrapper returns either a session or a reject).
  Rejected: `:ws-handler` receives a live *Session*, not a request — by
  then the 101 is already sent; the reject decision must precede the
  takeover, so it needs its own entry point.
- **`{:guard f :session f}` map as the `:ws-handler` value.** Rejected:
  breaks every existing caller for a composability win nobody asked for.

## Drawbacks

- Truthy-vs-map duck typing is slightly clever; mitigated by the precise
  `:status`-presence contract, one README table, and tests for both
  shapes.
- One extra opt key to document.

## Test plan

- No guard: existing ws tests unchanged (101 + session).
- Guard returns true → 101, session runs (echo round-trip).
- Guard returns `{:status 401 ...}` → client receives the 401 response,
  no 101, connection reusable for a subsequent normal request
  (keep-alive preserved — raw-socket client, send upgrade then a GET on
  the same conn).
- Guard returns nil/false → 403 plain response, no upgrade.
- Guard throws → failure path (500, or `:on-failure` response if
  configured).
