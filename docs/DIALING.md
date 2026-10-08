# Bounded TCP dialing

`dial::PeerDialer` is the connection-address execution boundary. It has associated
endpoint and channel types; a host can supply a local provider without importing
native socket types into the core. `native::dial::NativeTcpDialer` implements it
with `SocketAddr` and `TcpStream`. This is an untrusted channel establishment
mechanism. Success grants no authenticated identity, quorum authority, send
acknowledgement or durability evidence. TLS and its authenticated store-session
hello remain mandatory before constructing a production peer transport.

Construction supplies the recovered local node/store/session, a finite authorized
peer/store map, limits and a shared `WorkerWake`. There is no DNS or implicit
listener, credential source, executor or process-wide state. The map has a hard
1024-peer ceiling and cannot include the local node. Invalid construction starts
no thread. The native provider explicitly starts one serial blocking connect
worker, independent of the Raft/application owners. A completed native stream is
nonblocking with TCP_NODELAY enabled, suitable for the existing TLS provider.

Each `DialRequest` contains an existing `ConnectTicket`, a fixed-size socket
address hint and a bounded connect-call timeout. The ticket must match the local
identity and authorized peer/store. Admission rejects zero ports, unspecified
addresses, excessive timeouts, reused generations, concurrent requests for the
same peer and aggregate capacity overflow. Rejection returns the original
request and consumes no generation or slot. Accepted generations increase per
peer, so independent peers may be submitted in a different generation order.
Host-reserved generation ranges must not overlap within a recovered store
session, including across replacement dialers/rosters. Restart obtains a fresh
persisted store session. Dialing persists no state and reconstructs no authority.

The request ceiling includes queued work, the active connect call and completed
but unpolled channels. Both channels and correlation metadata remain reserved
until terminal polling. Native submission and polling never block. Polling
returns at most the requested limit and at most the configured request ceiling;
zero polling transfers nothing. One terminal event contains the exact accepted
ticket and either the channel or a typed failure. Only that event releases the
slot. The caller must validate that its roster attempt remains live before
attaching a resulting authenticated transport. A successful dial can become
obsolete while queued, connecting or awaiting observation.

`cancel(ticket)` matches the entire pending ticket. A stale ticket cannot cancel
newer work. Queued canceled work never calls connect. Rust's blocking OS connect
cannot be interrupted through this implementation: cancel retains its slot until
that bounded call returns and its stream is closed. Cancellation also suppresses
an already completed, unpolled success and closes the socket before releasing
its slot. A successful result already transferred to the caller belongs to that
caller and is unaffected by later dialer shutdown. Multiple dialers can share a
wake handle; stopping one does not stop another or its host reactor.

The default ceiling is 16 requests and a 1000 ms timeout per connect call; hard
ceilings are 1024 requests and 10000 ms. The timeout excludes queue delay. A full
serial queue can delay a request by preceding connect timeouts. The future
connection owner must cancel on its end-to-end roster deadline; this dialer
does not replace that deadline with a wall-clock read in the deterministic core.
Close cancels the whole queue, so shutdown waits for at most the active connect
call plus OS scheduling, rather than running every queued connect. Metadata and
fixed-size request addresses are bounded; socket count is bounded by outstanding
requests. Kernel socket buffers, transferred streams, TLS handshake buffers and
framed transport/ingress buffers need their own budgets. No exact RSS or liveness
bound under arbitrary OS scheduling is claimed.

`close` stops admission and marks all accepted requests canceled. Continue polling
until `is_drained`, then use native `try_finish` to join without blocking the
owner. It returns false while work/thread shutdown remains, rejects an open
worker and reports a panicked thread. A disconnected worker produces one
`WorkerFailed` completion per remaining accepted ticket and stops admission.
Drop abandons observation, cancels queued work and closes retained channels;
the detached worker exits after its active bounded connect returns. Use explicit
close/drain/join when deterministic shutdown is required. Wake is only a
nonblocking scheduling hint, with the same host obligations as WAL workers.

`tests/dial.rs` supplies an independent downstream host provider/channel type and
exercises real loopback connection success, nonblocking I/O, rejection ownership,
scope/generation/timeout checks, refusal, late cancellation, socket closure and
independent instance shutdown. Internal deterministic worker tests hold one
active call to verify queued cancellation, retained credits, drop cleanup and
panic completion accounting. The three-node/100-group TCP/TLS histories now
dial through the same public contract before authentication, framing and actual
WAL/snapshot work. Listener acceptance and TLS-driving still use fixture setup;
a production listener/routing/handshake owner and full node admission remain
unfinished. These tests are Linux evidence, not macOS execution or a complete
network-failure/liveness proof.
