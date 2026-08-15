# RFC-0006: string Content-Length framing + friendly port-in-use error

- Status: Accepted
- Discovered: running the jolt-lang/examples ring-app in a browser — every
  keep-alive page load hangs until client timeout; separately, booting on a
  taken port reports `bind() failed on port 8080: Address already in use
  (errno 48)` with no hint of what to do.

## Problem 1 — string Content-Length produces unframed keep-alive responses

`ring-defaults` (and most ring middleware) sets `Content-Length` as a
*string*. `response->string` honored that value verbatim and passed it to
`head->string`, which (a) suppresses the handler's own Content-Length to
avoid duplicates and (b) only emits framing for `(number? framing)`. A
string value is not a number, so the header was suppressed and nothing was
added: an HTTP/1.1 persistent response with neither Content-Length nor
Transfer-Encoding. Such a response has no legal body terminator — the
client reads the body and then blocks waiting for more bytes until timeout.

Fix: normalize the honored value (`parse-long` when string) in
`response->string` before the framing decision. Unparseable values fall
through to the computed UTF-8 byte length, which is always correct for the
string body being sent.

## Problem 2 — EADDRINUSE boot error is accurate but unactionable

`errno 48` is correct, but a user who hits it needs: the port number (have
it), that another process holds it, and the two ways out. Keep errno data
(`:syscall`, `:errno`, `:strerror`, `:port`), add `:errno-name
"EADDRINUSE"` to ex-data, and special-case the message:

```
port 8080 is already in use — another process is listening on
127.0.0.1:8080 (Address already in use, errno 48); stop it or pass a
different :port
```

Non-EADDRINUSE bind failures keep the existing message unchanged.

## Tests

- wire test: handler sets `Content-Length "5"` (string), keep-alive on —
  response must carry `Content-Length: 5` and the connection must serve a
  second request (legal framing).
- boot test: second `run-server` on a bound port throws with
  `:errno-name "EADDRINUSE"`, message naming the port and suggesting
  `:port`.
