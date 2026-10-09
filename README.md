# VoteBoat

VoteBoat is a Rust consensus runtime under development, licensed under
[RPL 1.5](LICENSE). Its design separates recursive application responsibilities,
quorum topology, execution lanes, and physical WAL lanes. Native providers use
public interfaces that host applications can implement themselves.

Initial target platforms are **Linux and macOS**. Windows is deferred.

The immediate milestone is an **embeddable Rust library and a runnable networked
service**, starting with static membership. The library, local counter demo and
initial three-process TCP/TLS counter service now run. The service shares a typed
startup API with the Rust embedding example and accepts configured peer addresses.
The local client supports automatic leader selection for writes and reads. See the
[service quickstart and embedding guide](docs/COUNTER_SERVICE.md). Online
reconfiguration, recursive responsibilities and split/merge follow that milestone.
See the [first usable milestone](docs/IMPLEMENTATION.md#first-usable-milestone--priority-updated-8-october-2026).

The working baseline now includes a **static-configuration Raft core** with
durable elections, replication, ordered commitment, conflict repair and
quorum-backed read barriers, plus a three-replica counter demo with durable
application checkpoints, pinned logical compaction and follower snapshot catch-up.
Group configuration and voter store identities are
persisted with the native WAL. Operation retries return the original application
result without repeating their effect. Restart, partition, corruption and
storage-failure tests exercise native providers and host replacements. The
default `tls` feature adds pinned Rustls/ring for authenticated channels. Native
storage without TLS and the core/host-only build have no third-party runtime
dependencies.

The optional `quic` feature adds a caller-polled authenticated UDP session backend
through the same framed transport API. The service selects it with
`--transport quic` when built with `--features quic`; three-process tests cover
replication, leader loss and native-file recovery. See
[QUIC construction and limits](docs/QUIC_TRANSPORT.md).

A release-mode [native performance harness](docs/PERFORMANCE.md) measures
committed/applied writes through three real TCP/TLS or QUIC replicas, retains raw
latencies, and verifies WAL recovery and exact retries. Results are finite local
baselines; measured tuning and broader P7 validation remain work.

Shared runtime components now schedule many groups through bounded ready queues
and explicit deadlines. The 100-group history uses one WAL per node, batches
persistence across groups, and demonstrates progress while one group's durable
completion is delayed. Public scheduler, timer, clock and election-jitter seams
support host replacements. `TimedShard` automatically manages election and
heartbeat deadlines, including stale queued expirations and overload retries.
The asynchronous WAL worker and authenticated framed peer driver are implemented.
The native three-node/100-group durable history now runs through actual loopback
TCP/TLS connections, including leader replacement and restart. The bounded
`EffectOwner` now reserves output space before execution and retains exact effect
leases through rejection, persistence, application and read completion. Peer
connection coordination is now bounded by `transport::PeerRoster` and driven by
`runtime::PeerDriver`; `runtime::Node` now owns both drivers and selected providers
with coordinated shutdown and fenced recovery. An explicit
asynchronous snapshot worker now preserves publication, WAL durability and
application-installation dependencies, and supports local checkpoints and logical
compaction while both storage workers retain their handles.

`LogStore::reclaim` now rewrites the exact live WAL state through durable native
file selection, freeing superseded history after logical compaction. It requires
drained store transitions. `Node::reclaim` now submits bounded maintenance to the
existing selected WAL worker while the node stays live; poll its exact result
with `Node::poll_reclaim`. See [live worker maintenance](docs/WORKER_MAINTENANCE.md) and
[physical WAL cleanup](docs/WAL_RECLAMATION.md).

Native filesystem/provider assembly, incremental WAL cleaning and online reconfiguration remain under
development; this is not a production consensus release.

The [durable configuration journal](docs/CONFIGURATION_JOURNAL.md) now validates
learner, joint and final records through host/native storage, recovery and suffix
rollback. [Configuration-aware snapshots](docs/CONFIGURATION_SNAPSHOTS.md) now
preserve membership and operation identities across compaction and recovery.
Default/static assemblies keep configuration admission disabled. Explicit
[learner enrollment/recovery](docs/LEARNER_RECOVERY.md) now
accepts a committed exact-store learner assignment with unchanged bootstrap
voters, restores verified checkpoints, and runs without election timers. It does
not enable online promotion or administration. Explicit
[dynamic member recovery](docs/MEMBER_RECOVERY.md) now restores a voter or learner
from authorized durable joint/final state and verified application data, retaining
the accepted predicate and historical ballot through restart/rollback.
The [owned administration API](docs/CONFIGURATION_ADMINISTRATION.md) now connects
typed proposals, execution-time host authorization, committed receipts and
cancellation through Node. The counter's explicit `recover-member` assembly now
supports bounded trusted startup plans, exact deployment stores, offline checkpoint
enrollment and validated configuration replication over TCP or QUIC. Executable
tests start from bootstrap, add/enroll/promote a new store, retire an absent voter,
and recover survivor checkpoints with preserved retries. See
[trusted plans and lifecycle evidence](docs/COUNTER_SERVICE.md#trusted-startup-administration-plan).
Selected interrupted-delivery and repair histories now pass. General public
configuration mutation endpoints remain gated; this is not a production certification.

The [responsibility routing foundation](docs/RESPONSIBILITY_ROUTING.md) now provides
checked Single/Partitioned/Delegated manifests, bounded native/host cache and
partition-policy seams, and local committed-owner checks. Cached child lookup
works without a parent cache entry. The [replicated directory application](docs/DIRECTORY_APPLICATION.md)
now publishes fixed-bootstrap manifests through ordinary Raft commands and
recovers retry history through native WAL replay, checkpoints and snapshot install.
The opt-in [group and fresh namespace creation path](docs/GROUP_CREATION.md)
now binds assigned bootstrap to committed metadata, journals fresh independent
namespace publication and requires target activation before service. Selected
TCP/QUIC histories cover partial provisioning, checkpoint/reopen and operation
with metadata offline. Inserting children into existing covered selectors still
requires fencing and transfer; creation cannot replace an existing owner.
Opt-in [recursive deletion](docs/DELETION.md) now reserves original manifests and
publishes retained tombstones after owner fences and child deletion facts.
Application/checkpoint/native journal conformance passes; native deletion
composition remains in progress.
The [routed application wrapper](docs/ROUTED_APPLICATION.md) now checks ownership
at admission and apply, preserves semantic retries and enforces a durable local
fence. Native TCP/QUIC child writes and recovery work with every parent replica
stopped and unchanged ancestor logs. Ordinary metadata publication cannot activate
or transfer ownership; split/merge remains lifecycle work.
The [scope data adapter](docs/SCOPE_APPLICATION.md) now supplies a public optional
export/import seam and a bounded per-bucket counter that preserves retry outcomes
and pending outbox instructions. It supplies the data path for P6; ownership
fencing, publication and activation remain separate lifecycle work.

The [durable transfer intent journal](docs/TRANSFER_INTENTS.md) records proposed
splits/merges and reserves local targets across recovery. The optional
[source guard](docs/SOURCE_FENCING.md) now commits a fence and preserves exports
at its exact boundary. The [target guard](docs/TARGET_IMPORTS.md) now stages and
durably imports data while remaining non-serving. Publication and activation
remain pending.

## Run

For three independent networked processes, start with the
[native service quickstart](docs/COUNTER_SERVICE.md). Each process runs the same
embeddable Node facade, with durable files, quorum reads and automatic elections.


Install Rust 1.98.1 (pinned in `rust-toolchain.toml`), then:

```sh
cargo fetch --locked
cargo test --locked --offline
cargo test --locked --offline --no-default-features
cargo test --locked --offline --no-default-features --features native
cargo clippy --locked --offline --all-targets -- -D warnings
```

The first fetch needs network access. TLS uses ring's native build toolchain;
Linux/macOS need their normal C compiler and build tools. The TLS integration
test opens a local loopback TCP listener.

Run the shared-runtime histories, including three actual WAL files:

```sh
cargo test --locked --offline --test runtime
```

Try the three-replica counter in a fresh directory:

```sh
cargo run --example replicated_counter -- /tmp/voteboat-counter 1 7
# operation=1 outcome=Value(7) retry_duplicate=true
# linearizable_value=7
# All three replicas report value=7, with committed/applied boundaries matching.
# Each replica publishes checkpoint=3 and compacted_through=3.
cargo run --example replicated_counter -- /tmp/voteboat-counter 1 7
# Restore checkpoint 3 and retry operation 1: value remains 7.
cargo run --example replicated_counter -- /tmp/voteboat-counter 2 3
# A new operation adds 3: value becomes 10.
```

The demo uses three real WALs and host-driven in-process message delivery.
It explicitly elects node 1 and submits each operation twice to demonstrate
deduplication. The leader then uses a fresh quorum-backed read barrier and waits
for application before printing `linearizable_value`. Per-replica `value` lines
are explicitly local applied-state diagnostics. Its counter accepts signed i64 deltas
encoded as eight little-endian bytes. Application/deduplication capacity is
bounded; service embeddings use the ClientRouter below to reserve capacity before
proposals. This older demo retains its direct host-driven API.

Each replica publishes a checkpoint containing the full counter and retry state.
Recovery verifies its configuration and index/term against the durable Raft log,
then restores the application and replays only the committed tail. Publication
uses two alternating files and a synchronized atomic manifest. Each demo replica
pins its checkpoint before durably removing the covered logical log prefix.
Lagging followers can install a leader's snapshot and replay its later entries;
acknowledgements wait for durable storage and application restore. Physical WAL
bytes remain until the later cleaner is implemented. Hosts can replace snapshot storage,
platform I/O, encoding and application serialization through public contracts.

Compacted replicas recover through `snapshot::recover_replica`, which verifies
the pinned image and restores application state before returning a usable core.
Snapshot transfers currently own one bounded image in the in-process transport;
wire snapshot chunking and shared transport assembly remain to be implemented.

`runtime::Shard` owns its registered cores. Admit an owned event, poll a scoped
visit, then call `step_next`. Drive returned effects through bounded host workers,
using `with_core` to deliver exact admissions/completions and existing snapshot
or read helpers. Finish the visit once its dependencies resolve; other groups
can run while it is suspended. Rejected admission returns the original event.
Input credits remain charged until the visit finishes. These limits cover
ingress retention; the host still supplies separate outbound and application
budgets. The runtime creates no threads or stores.

Wrap a quiescent shard with `runtime::TimedShard`, a timer service, seeded
election entropy and `TimerConfig` to manage deadlines automatically. Pass local
monotonic time when polling, stepping and delivering completions. Its visit
access updates timers after valid leader contact, durable vote grants and role
changes. Deadlines trigger protocol work; reads still require a fresh quorum.

For asynchronous WAL work, construct `native::worker::NativeLogWorker` with the
quiescent selected `LogStore`, an explicit worker generation, limits and a shared
`WorkerWake`. Recover cores before moving the store to the worker. Submit exact
`Effect::Persist` batches using `worker::submit_for_shard` (or its timed variant),
then route polled events through `apply_to_shard`/`apply_to_timed` on the owner.
Written admission releases no dependent effects; only the exact durable barrier
does. Queue rejection returns the batch. Request, unit and retained capacity-byte
credits include reserved control space and remain charged until terminal delivery.
The caller budgets effects after delivery. Close, drain and `try_reclaim` return
the store; dropping observation cannot cancel accepted writes. The older snapshot
helpers require quiescent synchronous store access; asynchronous installation uses the
separate snapshot worker.

For bounded output coordination, wrap a quiescent `TimedShard` in
`runtime::EffectOwner`. It reserves effect capacity before each event, holds one
external lease per group, preserves rejected persistence batches and checks exact
written/durable delivery. Control work has reserved visit and byte capacity.
Application and one-use read completions stay on the serialized owner; transferred
sends move into separately bounded outbound queues. Failure fences service while
external leases remain charged until returned. See [the effect-owner contract](docs/EFFECT_OWNER.md)
for budgets, shutdown and remaining node assembly. Its real-file 100-group history
also uses native WAL workers and actual TCP/TLS connections:

```sh
cargo test --locked --offline --test effect_owner
```

`runtime::ApplicationRouter` now reserves bounded committed receipt capacity before
application and retains it through consumer completion. Native `Counter` and host
applications implement the public `BoundedStateMachine`/`ApplicationReceipt`
capability. Exact lease/log/boundary checks precede application; malformed output
fences the owner for recovery. The native 100-group histories use this same path.
See [application results](docs/APPLICATION_RESULTS.md).

`runtime::ClientRouter` now reserves command/reply capacity and uses public
`ProposalAdmission` to validate syntax and dedup capacity before admission and
again immediately before proposal execution. Scoped tracked inputs correlate
each invocation with its exact applied group/index/term/operation result.
Cancellation and leadership changes report Unknown without rolling back commands.
The native 100-group histories use this path, including quorum loss and recovery.
See [client ownership](docs/CLIENTS.md). `runtime::ReadRouter` now reserves query
and result capacity before consuming an original one-use read barrier, and holds
opaque output credits until consumer completion. Counter and host applications
share `BoundedReadableStateMachine`; native multi-group histories use this path.
See [read result ownership](docs/READ_RESULTS.md). `runtime::ReadRequests` now
owns original queries and reply reservations before Read admission, correlates
exact steps/barriers, and drains cancellation after quorum loss or leadership
changes. It composes an explicitly selected ReadRouter; the native histories
use both. See [read invocation ownership](docs/READ_REQUESTS.md).
`runtime::ReplicaDriver` now coordinates local WAL/snapshot completions, tracked
owner steps, application/client/read execution and bounded rejected-effect
retries over explicitly selected public providers. The native histories compose
this library driver with PeerDriver for network coordination. See
[local replica assembly](docs/REPLICA_DRIVER.md).
`runtime::PeerDriver` now owns the selected connector, transport factory, roster
and ingress router, driving exact connection lifetimes, bounded rejected-send
staging, local send completion and decoded ingress. The native histories use both
library drivers; only test fault injection stays in the harness. See
[peer reactor assembly](docs/PEER_DRIVER.md). `runtime::Node` now owns both drivers,
offers proposal/read/admin entry points and coordinates shutdown with original
reply and worker ownership. Native aliases select the same public contracts;
hosts explicitly open/recover providers and join reclaimed workers. See the
[owning node facade](docs/NODE.md).

`snapshot_worker::SnapshotWorker` supplies asynchronous publication and pinned
loads. `native::snapshot_worker::NativeSnapshotWorker` owns selected snapshot
handles on one explicit thread, with request/byte limits and control reserves.
Owner-side helpers validate the original effect and exact admission envelope.
Installation produces Persist after snapshot publication, SnapshotInstalled after
WAL durability, and SnapshotAck after verified application restore. The real-file
history composes both native workers with the effect owner and checks recovery
before WAL submission and after lost application completion.
`runtime::SnapshotRouter` now retains accepted effect leases, reserves loaded-image
space and rejects duplicate/stale completions. The three-node/100-group TCP/TLS
history installs snapshots into a fresh follower, checks retries and fresh reads,
and recovers actual files before further replicated writes. Native timers drive
heartbeat traffic; elections remain explicit in that history. See the
[snapshot-worker](docs/SNAPSHOT_WORKER.md) and [snapshot-routing](docs/SNAPSHOT_ROUTER.md)
contracts. A separate native 100-group network history uses timer-driven
elections, partition replacement and healing. Admit `Event::Checkpoint` as bounded
background work for a group with new committed/applied entries. Route
CheckpointRequired and CheckpointCompacted through the same snapshot router:
publication/pinning precedes WAL compaction, and verified retention reconciliation
finishes maintenance. The native network history repeats this cycle before and
after file recovery. Full node assembly remains in progress.

Use `outbound::OutboundQueue` (native provider `NativeOutbound`) to retain
same-peer batches of `Effect::Send` under node and peer budgets. Admission
returns ownership on rejection. Poll hands accepted batches to the transport;
credits remain charged until `complete` consumes them after buffer release.
Control capacity is reserved, snapshots have a separate ceiling, and the native
queue fairly visits peers and traffic classes. Local send success carries no
Raft acknowledgement. The host drives polling and budgets retained rejected
effects, encoded buffers and receive queues separately. This queue creates no
sockets; `PeerTransport` consumes its dispatched batches for framed channel I/O.

An explicitly shared [encoded-frame pool](docs/BUFFERS.md) can protect control
send capacity with `NativeBufferPool::new_with_control_reserve` and
`NativeTransportFactory::with_buffers`. Bulk frames cannot consume that byte/lease
headroom; receive frames remain bulk and per-owner fairness remains separate.

`NativeOutbound::with_policy` selects the public [admission policy](docs/ADMISSION.md)
for additional shareable bulk credits. Mandatory ceilings run first and control
bypasses optional policy. Outbound contract2 carries an owned lease with each
policy-admitted batch; it survives transport work and queue destruction until
the last owner releases it. The original constructor selects the native provider.

`wire::WireCodec` supplies a public bounded framing seam. The native
`NativeWireCodec` implements [wire formats 1–6](docs/WIRE_FORMAT.md).
The default constructor retains format 1 for current static-configuration peers;
`with_membership` explicitly selects format 2 for configuration entries and
membership-aware snapshots. `with_authority` selects format 3 for
[direct promoted-replica witness authorization](docs/REPLICATION_AUTHORITY.md).
`with_readiness` selects format 4; `with_learner_repair` selects format 5 for
bounded multi-batch pre-election learner catch-up. Format 6 adds historical
committed checkpoint repair. Retained format-5/6 repair can replace different-term
uncommitted learner tails while protecting committed and same-term overlap;
[native fault checks](docs/MEMBERSHIP_CORE.md) cover that restricted path.
These capabilities do not
release general online configuration ingress.
Native TLS/QUIC startup defaults to format 1; select an exact supported version
with `NativeTlsConfig::with_wire_version` before constructing it. Startup uses
that version for its codec, roster and authenticated sessions.
Default configuration ingress remains disabled; the
[accepted-log core audit](docs/MEMBERSHIP_CORE.md) records quorum integration
and the remaining activation/recovery gates. Validate its fixed prefix before
allocating a receive frame, then decode one exact frame with the connection's
trusted `WireScope`. Size, shape and decoded retention checks precede payload
allocations. Checksums provide integrity only; the transport must authenticate
the peer and supply separate encoded-buffer and ingress budgets.

`secure::SecureSession` supplies the authenticated channel seam. With the default
`tls` feature, `native::tls::NativeTlsSession` uses TLS 1.3 mutual certificate
authentication, exact peer certificate pins, and an authenticated node/store
session hello. The host supplies credentials, trusted node/store mappings,
nonblocking streams, connection generations, monotonic time and reactor wakeups.
No listener or executor is created implicitly. Polls limit external I/O calls
and bytes, and handshake progress has byte and time ceilings. Production
composition must use `require_authenticated` before delivering Raft traffic;
simulator providers are rejected. See [the channel contract](docs/SECURE_SESSIONS.md)
for lifecycle and buffer-accounting details.

`transport::PeerTransport` supplies a bounded connection-level send/receive
contract. Construct `native::transport::NativePeerTransport` with a ready
authenticated session, selected codec, outbound queue and transport limits.
Submit a dispatched outbound batch; rejection returns it. Drive `poll` with
separate session-I/O and plaintext budgets. Short writes retain the original
batch and encoded frame until local channel output drains. Consume `take_send`
through the original queue's `complete` to release its retained credits. One
completed receive batch blocks further frame reads until `take_received` moves
it into separately bounded ingress. Neither event proves Raft durability.
Transport contract 2 exposes immutable `received_info` so
`runtime::IngressRouter` can reserve count/byte/control credits before taking a
decoded frame. It retains full charges through partial admission, rotates past
overloaded groups, and discards still-held input from retired connections.
Native 100-group TCP/TLS histories now use this path. See
[decoded ingress](docs/INGRESS.md); committed application results use the router above.
`transport::PeerRoster` coordinates construction-authorized peers, fair visits,
bounded connection reservations, retry deadlines and fresh connection generations.
It preserves accepted send ownership across failure and rejects obsolete input.
`runtime::PeerDriver` now coordinates establishment, ingress and rejected-send
staging over explicitly selected providers. The host supplies readiness/wakeups
and monotonic time. The native connection provider below participates in the
100-group TCP/TLS histories. The checkpoint history
reconnects a peer pair before further replicated work. See the
[transport](docs/TRANSPORT.md) and [peer-roster](docs/PEER_ROSTER.md) contracts for
ownership, failure and shutdown details.

`dial::PeerDialer` provides bounded asynchronous address execution.
`native::dial::NativeTcpDialer` uses one explicit worker and checked roster
tickets, returning nonblocking TCP streams. Queued, active and unpolled work
retain slots; cancellation closes a late successful socket before releasing its
slot. The TCP/TLS histories now use this provider for dialing. A dial result
still needs TLS authentication. See [dialing](docs/DIALING.md) for timeout, cancellation and
close/drain/join behavior.

`connect::PeerConnector` supplies authenticated establishment over exact roster
tickets and deadlines. `native::connect::NativePeerConnector` consumes an explicit
dialer, optional listener and pinned TLS configuration. It bounds anonymous
prefaces and handshakes, checks identity/generation before returning a session,
and retains canceled/expired dialing until actual completion. Native 100-group
histories use one long-lived connector per node and explicitly drain/join their
dial workers. See [connection establishment](docs/CONNECTIONS.md). Embedded hosts can select
[bounded peer discovery](docs/DISCOVERY.md) through `DiscoveryConnector` and
[responsibility-authority discovery](docs/AUTHORITY_DISCOVERY.md) through
`ManifestDiscovery`/`resolve_discovered`, without
changing provisioned peer trust or committed membership. Local
client/read/result admission and peer driving are composed by the owning node
facade. Native filesystem/provider setup remains explicit.

Embedding hosts submit original queries through `runtime::ReadRequests`, feed
shared owner steps back to it, drive ReadProbe/ReadAck messages, and dispatch
ReadReady leases through its execute method. Lower-level embeddings can use
`Event::Read` with `ReadRouter`, or
`application::read_at_barrier` under their own query/result budget.
Each group allows one outstanding read (including an unconsumed ready barrier).
Request IDs increase within a store session; cancellation frees the slot. A
barrier is for its original invocation only and cannot authorize a later read.

The original standalone voting example remains available:

```sh
cargo run --example durable_vote -- /tmp/voteboat-demo 2 1
# term=1 candidate=2 granted=true
cargo run --example durable_vote -- /tmp/voteboat-demo 3 1
# term=1 candidate=3 granted=false
cargo run --example durable_vote -- /tmp/voteboat-demo 3 2
# term=2 candidate=3 granted=true
```

Every invocation opens or recovers the same native store. The example's fixed
three-voter membership is an explicit local demo configuration. It sends no
network traffic and does not elect a leader or replicate application data.

## Implementation status

See [the implementation record](docs/IMPLEMENTATION.md) for the roadmap,
validation evidence, contracts, durability assumptions and remaining work.
The local `VoteBoat-Design-Pack/` is intentionally Git-ignored; it is a design
reference, not a shipped dependency. The committed implementation record
preserves the decisions needed to continue development without that pack.

The Linux/macOS CI template is in `ci/platform-feedback.yml`. The current GitHub
token lacks workflow-write permission, so it has not been activated. When
enabled, CI will provide background feedback without a required merge gate.
Development proceeds using relevant local checks.

The evolving split/merge path now includes [verified metadata publication](docs/TRANSFER_PUBLICATION.md)
after source fencing and target imports, plus [durable target activation](docs/TARGET_ACTIVATION.md)
that serves without metadata access and preserves retries across restart.
Bounded [retirement](docs/RETIREMENT.md) preserves fences and lineage after an
explicit host retention release. Broader lifecycle recovery remains in progress;
the static service is usable independently.

The [split recovery guide](docs/SPLIT_RECOVERY.md) describes trusted host resumption
and the tested TCP/QUIC phase/reopen ledger. The [merge recovery guide](docs/MERGE_RECOVERY.md)
covers compatible two-source merge, partial fencing, offline sources and collision
refusal. [Repeated transfers](docs/REPEATED_TRANSFERS.md) let activated targets
become durably fenced sources for later moves. [Delegated coordination](docs/DELEGATED_TRANSFERS.md)
now has selected TCP/QUIC WAL/checkpoint split → merge → split recovery evidence,
including parent outages and final writes with ancestors stopped. Abandoned
reservation recovery, general retention and broader platform/fault validation
remain unfinished.

[Initial learner placement](docs/PLACEMENT_PLANNING.md) exposes host/native
recommendations through checked placement authorization and ordinary membership
admission. It preserves voting policy and does not perform automatic rebalancing.

[Application deployment envelopes](docs/APPLICATION_ENVELOPES.md) expose enforced
whole-lifetime bounds through the public StateMachine contract. Configuration
execution and learner readiness refuse unavailable or understated envelopes.

Opt-in authenticated configuration submission uses `--remote-admin-plan` and
`configure OPERATION_ID` for provisioned intents. See [the command contract](docs/COUNTER_SERVICE.md#authenticated-submission-of-provisioned-configuration-intents)
for phase receipts, retries and remaining general-ingress limits.

`--remote-admin-policy` accepts bounded client-supplied `configure-record` targets
without predeclared operation intents, retaining session, placement and application
checks. [Target grammar and retry limits](docs/COUNTER_SERVICE.md#client-supplied-configuration-targets)
include explicit refusal when compaction removed payload comparison history.
