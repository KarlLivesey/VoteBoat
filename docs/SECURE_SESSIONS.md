# Authenticated sessions, contract version 1

`secure::SecureSession` separates channel authentication and bounded nonblocking
I/O from Raft delivery. `NativeTlsSession` implements it using Rustls 0.23.45 with
its explicit ring provider. The optional `tls` feature enables the provider and
implies `native`; defaults include both. Core/host-only and native-without-TLS
builds have no third-party runtime dependencies. Cargo.lock pins the dependency
closure. No global crypto provider or hidden executor is installed.

## Identity and compatibility

Each side authenticates the other's certificate against host-supplied roots.
Client connections also verify the configured DNS name. Both sides require an
exact leaf certificate pin associated with the trusted peer node and stable
store identity. Native sessions require TLS 1.3 and ALPN `voteboat/1`. Client
resumption, early data and server resumption tickets are disabled. Production
credentials, trust roots and pin mappings are host configuration, not discovery
results supplied by an untrusted hello.

After TLS authentication, both sides exchange a 52-byte encrypted hello:

| Offset | Bytes | Meaning |
| --- | --- | --- |
| 0 | 8 | `VBSESS01` |
| 8 | 2 | little-endian wire version 1 |
| 10 | 2 | zero reserved flags |
| 12 | 8 | node ID |
| 20 | 16 | store ID |
| 36 | 8 | store incarnation |
| 44 | 8 | recovered store session |

All IDs must be nonzero. Node and stable store identity must match the trusted
mapping. The recovered peer store session is authenticated channel data. It is
not evidence that any remote log index is durable. The hello must finish and
its local ciphertext must drain before `binding` and `Ready` become visible.
No application plaintext can be written or read before this transition.
Unknown hello versions and flags fail closed. This slice supports wire version
1 only; version negotiation is an explicit future extension.

A connection generation is supplied by its owner. Allocate strictly fresh
`SecureSessionGeneration` values within the local recovered store session; after
restart, the persisted new StoreSession disambiguates old generations. Generation
reuse within a session is a host contract violation, not something the channel
can detect globally. Never reuse a connection's retained frames on a new binding.
The binding supplies incoming/outgoing `WireScope`s for the codec. Core term,
configuration, group/store incarnation and context validation still apply.

## Progress, budgets and ownership

Constructors consume one stream; TCP constructors make it nonblocking. Generic
streams must already be nonblocking and make writes advance the stream without
another host flush. Polling accepts injected monotonic time and per-call I/O,
read-byte and write-byte budgets. Zero budgets are valid. Short I/O, Interrupted
and WouldBlock retain progress. A blocked direction is not repeatedly spun
within the same poll. Time going backward fails the connection. Handshake time
and each direction's cumulative ciphertext bytes have independent ceilings;
I/O is clamped before exceeding those byte limits. No failed session is reusable.

Defaults are 8 I/O calls and 64 KiB per direction per poll, 10 seconds and 1 MiB
per direction for a handshake, and a 16 KiB application write buffer. Construction
validates configurable bounds. Credential inputs have finite count, per-object
and total DER limits before provider construction. The authenticated hello uses
two fixed 52-byte buffers.

Rustls `set_buffer_limit` bounds its application send buffering. It does **not**
cap all TLS handshake/control allocation or total RSS. Rustls separately limits
record/handshake parsing and applies incoming plaintext backpressure. The pinned
provider's limits and configured credential limits are part of the resource
model. Hosts separately budget socket buffers, concurrent sessions, receive
frames, encoded output, decoded ingress and retained effects. This session does
not claim to impose a process-wide memory limit.

Plaintext writes may accept only a prefix. The frame owner retains the rest.
`is_flushed` means the local channel has no pending ciphertext, never remote
receipt or a Raft acknowledgement. Close stops writes, sends close_notify and
drains accepted ciphertext; already decrypted input remains readable after clean
closure. Abrupt EOF without close_notify fails as `Truncated`. Revocation latches
failure and blocks further plaintext I/O. Accepted external writes remain
uncertain after failure. A host must not retry a command under a new operation ID
merely because the connection failed.

The session owns no listener, reactor, pool or process singleton. Terminal
`take_io` returns its stream exactly once. Rotation creates a new connection and
generation using the host's updated roots/pins; there is no live trust hot reload
or automatic revocation service. `require_authenticated` rejects simulator-only
providers and any provider that is not Ready with a binding. A host replacement's
security capability is trusted host attestation, not independent verification of
its implementation.

## Integration boundary

The native channel and codec are exercised together over a real loopback TCP
connection. Partial reads/writes and channel backpressure are tested separately
with real Rustls over bounded in-memory streams. The three-node/100-group durable
native WAL history now also traverses real TCP/TLS using the public
`PeerTransport` frame driver. Production effect staging, peer roster/reconnect
policy and asynchronous snapshot workers remain pending. Do not describe this
channel provider as a complete networked Raft service.
