# Bounded authenticated connection establishment

`connect::PeerConnector` is the public connection-establishment contract. It
accepts exact roster `ConnectTicket`s with a dial/accept direction and a supplied
monotonic deadline. It returns only authenticated, Ready `SecureSession`s or
terminal failures. Endpoint and session types belong to the selected provider;
a downstream host can supply its own implementation without native socket/TLS
imports in the core. No connection result is quorum participation, remote log
progress or a durability token.

With the `tls` feature, `native::connect::NativePeerConnector<D>` implements this
contract over a selected public `PeerDialer<Endpoint=SocketAddr, Channel=TcpStream>`
and the existing Rustls/ring TLS implementation. Its default type parameter is
`NativeTcpDialer`, but construction always requires an explicit dialer. It also
consumes an optional caller-created TCP listener, TLS configuration, the recovered
local identity, authorized peer/store/certificate pins, session limits and
connection limits. It starts no listener, thread or global runtime itself. The
listener is set nonblocking and transfers exclusive accept responsibility; do
not race another acceptor on a clone. A failed constructor returns the dialer and
listener for caller cleanup/reuse. It checks local binding, quiescent dial work,
limits, peer-map keys, certificate/name sizes and server-name syntax before use.

The fixed peer map has a 1024-peer ceiling, no self-peer, and at most 1 MiB of
retained public certificate/name capacity. Request and anonymous-socket ceilings
are independently configurable up to 1024, defaulting to 16 each. Attempts retain
slots through dialing, preface transfer, TLS, and unpolled terminal results.
Only terminal polling releases an attempt slot. One attempt per peer is allowed;
accepted generations increase per peer, so another peer's newer generation can
be submitted first. Hosts reserve disjoint ranges across simultaneous/replacement
owners within a persisted local store session. Restart uses a fresh recovered
store session. No network owner persists or invents these identities.

## Native routing protocol

The native connector writes a fixed 16-byte preface before TLS: eight bytes
`VBCONN01`, followed by the dialing node's nonzero u64 ID in little-endian order.
There is no untrusted length or allocation request. This format selects an
already pending accept ticket in the fixed authorized map. It grants no identity,
store session, voter membership or service authorization. Unknown/zero IDs,
invalid magic and hints without a pending accept are dropped without consuming
an authorized ticket. Duplicate hints cannot replace a live handshake.

Incoming sockets have separately bounded fixed preface slots and deadlines.
Partial reads retain only the fixed array. After a complete valid hint, the
socket moves to its reserved accept attempt and TLS verifies the expected exact
certificate pin, root trust and authenticated node/store/session hello. A forged
hint paired with another otherwise trusted certificate fails. No session reaches
the caller until `require_authenticated` and exact local, peer/store, generation
and wire-version checks succeed. The roster independently checks attachment and
remote store-session rollback. TLS 1.3 and ALPN/wire version 1 remain mandatory.
This preface belongs to the connector's protocol; direct `NativeTlsSession`
users still supply their own established streams. There is no insecure/raw-TLS
fallback on the connector listener.

The host chooses which side dials. The native cluster histories use lower node
ID to dial and higher ID to accept, avoiding simultaneous duplicate connections.
Both sides need pre-reserved roster tickets. Discovery/address hints cannot add
authorized peers. An attacker may occupy anonymous slots or claim a valid hint
and then fail authentication; budgets/timeouts bound retained work, not denial of
service availability. No unbounded retry or accept loop is hidden in the provider.

## Progress, deadlines and shutdown

`poll(now, budget)` rejects invalid budgets or reversed time before I/O. Attempt
and anonymous expiry scans have fixed construction-time capacities. I/O visits
rotate over peers and anonymous slots. Accept/preface read/write calls share one
socket-call budget; TLS has a per-visited-handshake call/byte budget.
First access to that shared socket budget rotates between anonymous prefaces,
authorized attempts and accepts. Exhausted credit preserves unserved socket
cursors; polls without socket credit do not consume a scheduling turn. Thus an
incomplete anonymous stream cannot monopolize every positive-budget poll.
The maximum TLS work per poll is visits times that session budget. Dial receipt
ingestion and terminal output each obey the completion budget. Zero budgets cannot transfer
completions or perform the corresponding I/O. Socket option setup has constant
work per accepted socket. Scheduling readiness/deadline wakeups remains the
host's responsibility; dial completions use the supplied dialer's wake handle.
`next_deadline` includes anonymous/attempt expiry and immediately pollable
terminal results. A canceled dial awaiting its actual receipt has no busy deadline.

The accepted deadline must be later than submission time and no farther than the
configured timeout (default 10000 ms, hard ceiling 60000 ms). Pass the exact live
`PeerRoster::attempt_deadline(ticket)`, rejecting a missing/stale ticket. The
connector enforces this end-to-end deadline before further I/O, including queued
dial time. TLS gets the smaller of its configured handshake timeout and remaining
attempt time. Dialing uses the smaller of the selected per-call timeout and
remaining time. Expiry cancels the dial, but its slot remains until the provider
actually stops and reports terminal completion. A native active OS connect may
finish later than the logical deadline. Queue cancellation and wall-clock bounds
are described in [dialing](DIALING.md).

`cancel(ticket)` matches the full ticket. Preface/TLS sockets and unpolled Ready
sessions are closed, and exactly one failure remains to poll. An already latched
failure is preserved. Close stops admission, drops the owned listener and
anonymous sockets, and cancels all attempts; polling still drains actual dial
receipts and terminal outcomes. Successfully transferred sessions belong to the
caller and survive connector shutdown. Once closed and drained, `into_dialer`
returns the selected provider; a native caller explicitly joins it with
`try_finish`. Drop closes owned streams and relies on the selected dialer's drop
contract for bounded cleanup, abandoning observation. No host reactor is stopped.

## Optional QUIC provider

With `quic`, `native::quic_connect::NativeQuicConnector` implements the same
PeerConnector contract over one supplied UDP socket, fixed pinned peer addresses
and fresh tickets. Peer leases bound packet queues and prevent a replacement
session from consuming an old session's packets. Close cancels owned handshakes;
transferred sessions retain their leases. There is no dial worker or anonymous
routing preface. NativeStartup::open_with_protocol and the counter's --transport
flag explicitly select it; TCP remains the default. See
[QUIC construction, accounting and evidence](QUIC_TRANSPORT.md).
Explicit `NativeQuicConnector::new_with_discovered_dials` permits validated
replacement Dial addresses through `DiscoveryConnector`. Static construction and
Accept addresses remain provisioned. Pinned identities and one live lease per
peer prevent endpoint hints from granting trust or accumulating sessions.
See [discovered endpoint refresh](DISCOVERY.md).

The owner checks injected dialer identity, immutable limits, outstanding count,
receipt bounds and exact tickets. Provider violations stop admission and return
`ProviderViolation`; an alien/lost receipt cannot release the real ticket. A
broken host provider may prevent orderly draining, requiring observation to be
abandoned and that provider repaired. No such receipt can establish Raft progress.
Other errors distinguish pre-admission rejection, accepted dial/TLS/I/O failure,
timeout, cancellation and reversed time. Rejections return the original request.

Request sockets, anonymous sockets, fixed arrays, metadata, public pin clones and
TLS per-session limits are bounded. Kernel socket buffers, returned sessions,
transport frames, decoded ingress, outbound staging and application results need
separate budgets. TLS handshake-byte ceilings bound traffic and the existing
Rustls adapter's buffering assumptions; no exact RSS bound is claimed.

## Evidence and remaining work

`tests/connect.rs` implements a downstream connector/session type and injects a
held host dialer into the native owner. Tests exercise three peers on shared
listeners with tiny fair budgets, fragmented/invalid routing, anonymous caps and
expiry, forged hints versus pins, scope/time/generation rejection, cancellation
of stalled and unpolled Ready sessions, retained dial credits, failed-construction
resource return, and mis-scoped host receipts. The three-node/100-group native
histories now use a long-lived connector per node for initial mesh and fresh
peer reconnection, then continue actual WAL/snapshot/replication/read/recovery
work. Shutdown closes/drains connectors and joins each dial worker explicitly.
`tests/connect/fairness.rs` holds anonymous TCP streams open at fixed virtual
time while authorized incoming/outgoing peers complete with one socket call per
poll. It covers saturated anonymous slots, visits equal to capacity and
interleaved zero-I/O polls. Completion retains the original authenticated ticket;
returned sessions remain authenticated after connector shutdown.

Bounded decoded ingress is now supplied by [IngressRouter](INGRESS.md), with
connector/session/transport/ingress coordination in [PeerDriver](PEER_DRIVER.md).
Client/read admission and local application/WAL driving use the service owners
and ReplicaDriver. Node assembly, physical WAL reclamation, membership changes,
recursive responsibilities and selected native split/merge paths now exist.
Shared receive/control fairness, broader overload and recovery combinations,
and platform acceptance remain open. These finite Linux tests are not a full kernel
fault matrix, macOS execution, power-cut evidence or a liveness proof.
