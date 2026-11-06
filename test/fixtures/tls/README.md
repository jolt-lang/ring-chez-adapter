Self-signed test certificate and keys, valid for 100 years and used only by the
test suite. other-key.pem does not match cert.pem, which one of the tests needs.
Not secrets; never use them for anything else.

client-hello.bin is one recorded TLS ClientHello, for a client that starts a
handshake and then goes silent.
