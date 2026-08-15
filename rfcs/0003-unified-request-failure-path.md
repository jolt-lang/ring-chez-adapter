# RFC-0003: unified request failure path (`:on-failure` + nil → 500)

- Status: Accepted (phase 3)
- Prior art: Igropyr `http.sc` ~1230 (on-failure hook: "A raise inside the
  hook is caught (the worker survives...) and falls back to the plain
  500"), ~1904 (the `'on-failure` opt); swish `http.ss` ~1093
  (`http:process-handler`: catch → report → **re-raise**); Ring spec
  (nil response = 500, per ring-jetty behavior)

## Summary

All per-request abnormal completions funnel through one failure path:

1. the Ring handler throws, or
2. the handler returns `nil` (wrapped as an ex-info with
   `{:type :ring-chez/nil-response}`), or
3. a websocket session throws after the 101 (observability only, below)

For (1) and (2): if `:on-failure` is configured, call
`(on-failure request throwable)`; a response map it returns is served
through the normal send path (keep-alive preserved); nil or a throw inside
the hook falls back to the plain 500. Without `:on-failure` the behavior
is today's canned 500 — the default adds observability without changing
wire behavior.

## Motivation

Today a handler exception produces a canned 500 and the exception is 100%
swallowed — no hook, no log, no ex-data. The operator of a broken handler
sees "Internal Server Error" and nothing else. Igropyr's `on-failure` is
exactly the observability + customization point: logging, Sentry-style
reporting, custom error pages, or fail-fast responses. And a nil response
currently throws *inside* `response->string` (NPE-ish), escaping into
`connection-loop`'s catch and closing the connection — Ring's contract
says 500.

## Design

### The failure function

```clojure
(defn- handle-failure
  "All abnormal handler completions land here. Returns a response map."
  [on-failure request t]
  (or (when on-failure
        (try (on-failure request t) (catch Throwable _ nil)))
      {:status 500 :headers {"Content-Type" "text/plain"}
       :body "Internal Server Error"}))
```

Semantics ported from Igropyr's `run-task` failure branch: the hook gets
one attempt by construction; a throw inside it is caught (the worker
survives) and the plain 500 answers. swish's re-raise-for-supervisors does
not apply — there is no supervisor above us; the hook *is* the supervisor's
observation point. That is the adaptation, and it keeps the worker alive
(Igropyr: "the worker survives, so the supervisor never retries it" — our
analogue: the worker thread must never die, an invariant the adapter
already defends in `worker`'s catch).

### Wire-in at the handler boundary

```clojure
(let [resp (try (handler request)
                (catch Throwable t (handle-failure on-failure request t)))]
  (if (map? resp)
    resp'
    (handle-failure on-failure request
                    (ex-info "handler returned nil"
                             {:type :ring-chez/nil-response}))))
```

The existing inline `#(try (handler request) (catch ...))` inside
`connection-loop` is replaced by this; the result flows into the existing
keep-alive-pressure stamping and `send-response` unchanged. Because the
hook's response goes through `send-response`, keep-alive survives a
failure — an Igropyr requirement ("answered through the normal response
path... enabling a fail-fast retry loop on one connection").

### Websocket sessions

Post-101 there is no response to serve — Igropyr's post-start rule
applies: close is the truncation signal. The hook is still invoked for
observability, with the *original upgrade request* and the throwable, and
its return value is ignored:

```clojure
((:run! io) #(try (ws-handler (ws/make-session conn))
                  (catch Throwable t
                    (when on-failure
                      (try (on-failure request t) (catch Throwable _ nil))))))
```

The session's own finally already closes the fd (`connection-loop` /
`conn-close!` own teardown), so no new close path is introduced —
respecting the every-close-path rule.

### Internal refactor: the per-server config map

`connection-loop` currently takes `[conn handler port ka-ms ws-handler
max-bytes io deadline]` — eight positional args; this phase adds
`on-failure` (and phase 4 `ws-guard`). Group the per-server, per-request
policy into one map threaded through instead:

```clojure
{:handler h :port port :ka-ms ka :ws-handler w :on-failure f :max-bytes m}
```

`worker` and `fiber-serve` build it once; `connection-loop` destructures.
Purely internal; no public signature changes (`run-server`, `stop-server`
unchanged).

## Alternatives considered

- **`prn` the throwable to stderr when no hook is configured** (swish
  re-raise analogue). Rejected as a default: libraries that print are a
  nuisance in production; the RFC's contract is "opt in to observation via
  `:on-failure`".
- **Igropyr's 404 for nil.** Rejected: Ring/Jetty says 500 for nil; we
  follow Ring (see RFC-0000 non-adopted).
- **Distinguishing hook-served from handler-served responses in
  ex-data/headers.** Rejected: leaks the hook's existence to clients.

## Drawbacks

- Channel-body responses from the hook are streamed with the same rules as
  handler responses (chunked) — powerful but easy to misuse; documented in
  README rather than restricted.
- The nil-response marker is a synthetic exception; hooks must check
  `(:type (ex-data t))` if they care to distinguish. Documented.

## Test plan

- Handler throws, no hook → 500 "Internal Server Error" (existing test
  keeps passing unchanged).
- Handler throws, hook returns `{:status 503 ...}` → 503 served; the
  throwable the hook received `=` the thrown exception (identity, captured
  via atom).
- Hook returns nil → 500. Hook itself throws → 500 (worker survives; a
  second request on a *new* connection succeeds).
- Hook response keep-alive: two requests on one keep-alive connection,
  first triggers the hook, second is normal — both served, connection
  reused (raw-socket client).
- Handler returns nil, no hook → 500 (not a dropped connection; the client
  receives a complete response).
- Handler returns nil, hook present → hook receives ex-info with
  `:type :ring-chez/nil-response`.
- ws-handler throws, hook present → hook invoked with the upgrade request;
  connection closed by the server (client recv sees EOF); worker survives.
