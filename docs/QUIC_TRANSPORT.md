# Optional QUIC transport

Enable `quic` to use `native::quic::NativeQuicSession` with the existing
`secure::SecureSession` and `native::transport::NativePeerTransport` contracts:

```sh
cargo test --locked --offline --features quic --test quic
```

TCP/TLS remains the default. QUIC is a construction-time provider choice, using
pinned `quinn-proto =0.11.19`, `bytes =1.12.1` and the existing Rustls/ring
credentials. The protocol engine runs inside caller polls; it starts no async
runtime, reactor or thread.

## Construction and ownership

The host binds one dedicated `std::net::UdpSocket` for each peer pair endpoint,
then calls `NativeQuicSession::client` on one side and `::server` on the other.
Both constructors consume the socket and take:

- A reference to `NativeTlsConfig`, with trusted roots, local certificate and key.
- `QuicSessionOptions`, containing the exact local recovered node/store session,
  construction-authorized `TlsPeer`, fixed remote socket address, fresh local
  connection generation and `SessionLimits`.
- The initial caller-supplied `MonoTime`.

Constructors set the supplied socket nonblocking. Constructor rejection drops
the consumed socket. They perform no network I/O; the caller drives both peers
with `SecureSession::poll(now, budget)`. `local_addr` reports the bound address.
`next_deadline` reports the next protocol/handshake deadline in the caller's
initial monotonic domain. The host must arrange readiness and timer wakeups and
continue polling through handshakes, flow control, retransmission and close.
The receive budget must have at least 1200 bytes available for a datagram read;
smaller budgets make no receive progress. A write budget smaller than a pending
datagram similarly defers transmission. A one-call budget alternates reads and
writes across polls.

Before Ready, mutual TLS authentication, the exact peer leaf certificate pin,
QUIC ALPN `voteboat-quic/1`, and the authenticated `VBSESS01` node/store/session
hello must all succeed. The hello selects existing message wire format 1. Remote
addresses are routing inputs, not membership authority. Foreign source addresses
are discarded before protocol admission. Migration, early data, bidirectional
streams and unreliable application datagrams are disabled.

Once Ready, pass the session to `NativePeerTransport` or
`NativeTransportFactory` with the same codec, outbound queue and limits used for
TCP/TLS. Authentication does not grant voting, membership or durability evidence.
Restart still needs a fresh persisted store session and a host-reserved fresh
connection generation; QUIC connection IDs do not replace those identities.

## Reliable channel and bounds

The byte channel concatenates ordered, reliable unidirectional stream chunks.
Each accepted plaintext write is capped by `write_buffer_bytes` and finishes its
stream. At most one outgoing chunk awaits acknowledgement, and the peer permits
one incoming stream at a time. Reading a FIN ends that chunk, not the logical
byte channel. The application and frame codec see ordinary partial reads/writes.
This deliberately conservative flow-control scheme has not been benchmarked.

Each poll bounds socket calls and bytes. The fixed UDP payload/initial MTU is
1200 bytes, with MTU discovery and segmentation offload disabled. One pending
encrypted datagram survives socket backpressure without being overwritten by
receive-side protocol output. Stream/connection windows, crypto buffering,
handshake byte totals and handshake deadlines are bounded. Protocol event visits
are bounded too; these are resource controls, not a measured total RSS ceiling.

`is_flushed` waits for acknowledgement of the outgoing stream chunk and release
of pending local packet output. This is stronger than merely draining the local
socket. It is still neither remote application consumption nor a durable Raft
acknowledgement. The framed transport retains the original outbound batch until
its usual exact terminal completion is consumed.

Close stops new writes and waits for accepted output before protocol shutdown.
Clean peer close retains already received plaintext for draining. A lost peer
with unacknowledged output fails instead of remaining Closing forever. Invalid
poll budgets reject before changing session state; time reversal, authentication
failure, truncation and revocation latch failure. Failed sessions do no further
I/O; dropping them releases their sockets and protocol buffers. QUIC's current
default idle timeout also applies to established sessions.

## Current integration and evidence

This slice exposes the session backend and its existing framed transport
integration. `NativePeerConnector`, `NativeStartup`, `NativeNode` aliases and
the counter service CLI still use TCP/TLS. There is no shared UDP listener,
QUIC connector, address discovery or CLI QUIC flag yet. A socket must not be
shared by independently polling sessions: one session would consume another's
packets. Bounded establishment and service selection are the next integration
slice.

Nine Linux loopback tests cover authenticated partial ordered I/O, exact pins and
identities, small-budget fairness, clocks/deadlines, foreign datagrams, handshake
limits, revocation, clean close, loss/retransmission, failure during close,
framed outbound ownership, and three-replica election/commit/application over
actual QUIC. That last history uses the host log provider and its exact durable
tickets; it is not a native-file crash history. Existing secure, transport,
startup and TCP service regressions also pass. These finite checks make no
macOS execution, remote deployment, production readiness or performance claim.
