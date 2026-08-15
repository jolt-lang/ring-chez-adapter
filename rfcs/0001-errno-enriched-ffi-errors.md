# RFC-0001: errno-enriched FFI errors

- Status: Accepted (phase 1)
- Prior art: swish `src/swish/errors.ss` (tagged error taxonomy,
  `errno->english` = strerror via `osi_get_error_text`), Igropyr errno
  discipline; Linux thread-local errno lesson from ring-chez-adapter CI work

## Summary

Every FFI failure in the boot path (`socket`, `setsockopt`, `bind`,
`listen`) throws an `ex-info` whose ex-data carries the failing syscall,
the errno number, and the strerror text. Today `"bind() failed on port N"`
with empty ex-data makes EADDRINUSE indistinguishable from EACCES or
EINVAL — the single most common boot failure (port in use) is also the
least diagnosable.

## Motivation

Swish's error architecture puts errno in every syscall reason tuple
(`#(listen-tcp-failed address port who errno)`) and renders it through one
formatter; the observable contract is "every I/O failure names the syscall
and the OS's own explanation of it." The adapter is a library — Jolt's
native equivalent of a tagged reason is `ex-info` + ex-data; the renderer
is whatever the *caller*'s REPL prints. The contract to keep is the data,
not swish's formatter.

## Design

### Reading errno

`jolt.io-poller` already exports an `errno` reader (it maintains
`eagain?`/`eintr?` internally, which require errno). Prefer it — no new
native symbol declarations. If probing shows it is not callable from
adapter context, fall back to declaring in `deps.edn` `:jolt/native`:

- macOS: `__error` — `(ffi/defcfn c-errno-loc "__error" [] :pointer)`,
  dereference for the int
- Linux: `__errno_location`, same shape

Strerror: `(ffi/defcfn c-strerror "strerror" [:int] :pointer)` (needs a
`deps.edn` entry) and read the returned C string. If jolt.ffi has no
string-from-pointer reader, fall back to a static errno→keyword map for the
common socket errnos (EADDRINUSE, EACCES, EBADF, EINVAL, EMFILE, ENFILE,
ENOMEM, ENOTSOCK, EPROTONOSUPPORT) — the map is compile-time data, never
wrong for the errnos it names, and `:errno` is always present for the rest.

### Staleness rule (from the Linux CI lesson)

errno is thread-local and *stale across syscalls*: a successful call leaves
the previous error in place. Rules:

- Read errno only immediately after a call returned a failure value on the
  same thread, before any other FFI call.
- Never reset-and-hope (reading errno "before" a syscall to clear it buys
  nothing; the only safe read window is after failure).

In `listen-socket` every read follows a `neg?` check on the same line —
the window is already correct; the RFC makes it a stated invariant rather
than an accident.

### Shape

```clojure
;; every boot-path FFI failure:
(throw (ex-info (str "bind() failed on port " port ": " strerror
                     " (errno " errno ")")
                {:syscall "bind" :errno errno :strerror strerror :port port}))
```

Message stays human-readable (it is what a bare REPL prints); ex-data is
machine-readable (what middleware and tests assert on). All four sites in
`listen-socket` get the same treatment; resource cleanup (close fd, free
sockaddr) is unchanged and still runs before the throw.

Not in scope: runtime accept/recv/send failures inside `serve-loop` and
`send-all`. Those are handled as peer-gone conditions today (their control
flow is close-connection, not signal-caller); enriching them would change
runtime behavior, which this phase deliberately does not.

## Alternatives considered

- **A `ring-chez.errors` namespace (swish `errors.ss` analogue).** Rejected
  for now: one helper fn and four call sites don't earn a namespace; the
  adapter keeps all FFI bindings and their failure shapes in one file. If
  the taxonomy grows (runtime syscalls, ws errors), split then.
- **Tagged keywords only (`:errno/eaddrinuse`) without strerror.** Rejected:
  the string is the thing operators read; swish ships it for that reason.

## Drawbacks

- One extra native symbol (`strerror`) in deps.edn (skipped entirely if the
  static map is chosen).

## Test plan

- Boot two servers on the same port; the second `run-server` throws
  `ex-info` whose ex-data has `:syscall "bind"`, positive integer `:errno`
  (48 on macOS / 98 on Linux — assert `pos?`, not the value), and a
  non-empty `:strerror` string; message contains "Address already in use"
  on both OSes.
- The first server still serves and stops cleanly after the failed boot
  (no fd leak into a broken state).
