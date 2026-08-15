# RFC-0002: boot-time option validation

- Status: Accepted (phase 2)
- Prior art: Igropyr `http.sc` ~1895 (body-limit validated at
  `http-listen`: "A bad value must crash HERE, at boot — deferred to
  request time it raises inside the reader and the connection just
  drops"), swish `http.ss` ~143 (`arg-check` on `http:configure-server`
  inputs); existing adapter precedent: `:strategy` is already validated at
  boot

## Summary

`run-server` validates every typed option before binding the listen socket
and throws one `ex-info` naming the bad key, the given value, and the
expected shape.

## Motivation

Today `{:port "abc"}` dies deep inside `make-sockaddr` bit arithmetic or
worse, `{:worker-threads 0}` silently boots a pool of zero workers and the
server accepts connections that nobody ever serves, and a negative
`:keep-alive-timeout-ms` configures an instant-retirement loop. Each fails
far from the cause. Igropyr's rule: bad configuration is a boot-time
defect; the boot path is the only place with the caller's attention.

## Design

A single private validator, run first in `run-server` (before
`:strategy`, which keeps its existing specific error):

```clojure
(defn- validate-opts! [opts]
  ;; each rule: nil-or-valid; throw names key, given value, expected shape
  )
```

Rules (all "if the key is present, it must satisfy"):

- `:port` — int in 1..65535
- `:worker-threads` — pos int
- `:keep-alive-timeout-ms` — pos int
- `:max-request-bytes` — pos int
- `:on-failure` — ifn (validated from phase 3; the validator lands the
  slot now, rejecting non-ifn values even before the feature ships, since
  a non-ifn value can only be a mistake)
- `:ws-guard` — ifn (slot from phase 4, same reasoning)
- `:write-timeout-ms` — non-neg int, 0 = disabled (slot from phase 5)

Failure shape:

```clojure
(throw (ex-info "run-server: :port must be an integer in 1..65535"
                {:key :port :given "abc" :expected "int 1..65535"}))
```

One error per throw (the first failing key), not an accumulated report —
configuration mistakes are fixed one at a time in practice, and first-error
keeps the helper trivial. Unknown keys are *not* rejected (forward
compatibility with future opts, matches current behavior).

Validator is pure (no I/O) so it is directly unit-testable without
binding ports.

## Alternatives considered

- **Accumulate all failures into one report.** Rejected: complexity for a
  rare path; `:given` already identifies the mistake precisely.
- **Reject unknown keys.** Rejected: breaks forward compat and the
  legitimate "pass the whole config map" pattern.

## Drawbacks

- None identified; cost is a dozen lines plus tests.

## Test plan

- Each rule: `(run-server h {key bad-value})` throws with `:key`,
  non-nil `:given`, message naming the key; no listen socket bound (a
  subsequent clean boot on the same port succeeds).
- Defaults and valid values still boot and serve (guard against
  over-validation).
- `:strategy` error unchanged (existing behavior).
