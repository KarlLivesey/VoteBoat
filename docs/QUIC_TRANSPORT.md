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
I/O; dropping them releases their sockets and protocol buffers. Established
sessions use a five-second idle timeout so an unreachable peer releases its old
route for reconnect. Active Raft heartbeats normally keep healthy connections
live; hosts must continue polling.

## Shared connector and service selection

`native::quic_connect::NativeQuicConnector` implements the same `PeerConnector`
contract. Construct it with `NativeConnectConfig`, TLS configuration, a map of
exact peer IDs to `(SocketAddr, TlsPeer)` pairs, an explicitly bound UDP socket,
and initial monotonic time. Failed construction returns the supplied socket.

One shared native socket routes packets by configured source address into session
leases. At most eight 1200-byte packets are queued per live peer; full queues drop
packets for QUIC retransmission. Unknown sources allocate no mailbox. Every
consumed packet is charged to the visiting session's I/O budget, including packets
queued for another peer or discarded. Queued reads are conservatively charged
again. Each read visit performs at most one OS read. Sharing creates no additional
reader or background task. Independently cloning/polling a dedicated socket is
still unsupported; use this connector for sharing.

An accepted connection ticket retains its request slot through exactly one
terminal poll, including cancellation, expiry and unpolled Ready results. Fresh
generations increase per peer. Identity, address and deadline rejection returns
the original request before transfer. Fair handshake visits use the per-session
I/O budget; terminal outputs have a separate completion ceiling. Zero completion
budget retains terminal slots. `next_deadline` includes protocol timers, attempt
expiry and immediately pollable terminal results. There are no TCP dial or
anonymous-preface slots.

Only one live lease may own a peer address. Cancellation drops an owned handshake
and clears its queued packets; a transferred session retains its lease until
dropped. A reconnect is overloaded while that old lease remains live. Generation
checks prevent stale cleanup from removing a replacement route. Connector close
cancels owned attempts and releases its socket reference; transferred sessions
remain usable and keep the shared socket alive. Final session drop releases it.
A fresh protocol engine/connection ID and crypto handshake reject old encrypted
packets; application identity/generation checks remain unchanged. There is no
migration or dynamic discovery.

Call `NativeStartup::open_with_protocol(NativePeerProtocol::Quic, app, wake, now)`
to bind one UDP socket and use the same verified native WAL/snapshot recovery
path as TCP. This starts two storage workers and no dial worker. Select TcpTls
for TCP through the same method. Original `NativeStartup::open` and default
`NativeNode<A>` retain TCP/TLS. `NativeNode<A, C>` and `NativeNodeParts<A, C>`
also accept an explicitly selected connector. `NativeServiceConnector` forwards
the chosen provider and returns boxed authenticated sessions through the same
public contract; its `into_dialer` returns None for drained QUIC and a reclaimed
worker for TCP. Failed startup returns the application and existing cleanup
handle; it is not file rollback.

Build the counter with `--features quic`, then append `--transport quic` to each
serve create/recover command, after any peer file. Peer ports BASE+1..3 use UDP;
local command ports remain TCP. Omit the flag or select `--transport tcp` for
TCP/TLS. A build without QUIC rejects its flag before creating a data store. See
[the service quickstart](COUNTER_SERVICE.md).

## Evidence

Nine Linux loopback tests cover authenticated partial ordered I/O, exact pins and
identities, small-budget fairness, clocks/deadlines, foreign datagrams, handshake
limits, revocation, clean close, loss/retransmission, failure during close,
framed outbound ownership, and three-replica election/commit/application over
actual QUIC. That last history uses the host log provider and its exact durable
tickets; it is not a native-file crash history.

Four downstream connector tests cover simultaneous peers through one socket per
node, one-call/one-visit fairness, transferred sessions surviving connector
close/drop, fresh-generation reconnect, exact cancellation, retained terminal
slots, expiry, invalid budgets/time, identity/address rejection and failed
constructor socket return. An internal real-UDP test checks queue/drop bounds,
charged routed/unknown packets and discarded old queues on lease replacement.

A three-process QUIC service history uses native files and workers. It checks
election, write/read/dedup, abrupt leader loss, replacement writes, recovered-peer
catch-up, checkpoints, shutdown/join, restart and further reads/retries. Late
failed QUIC startup releases its socket and joins started storage workers before
the WAL is reopened. TCP/TLS regressions also pass. These finite Linux checks
make no macOS execution, remote deployment, production readiness or performance
claim.
