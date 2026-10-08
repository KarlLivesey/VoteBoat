# VoteBoat

VoteBoat is a Rust consensus runtime under development, licensed under
[RPL 1.5](LICENSE). Its design separates recursive application responsibilities,
quorum topology, execution lanes, and physical WAL lanes. Native providers use
public interfaces that host applications can implement themselves.

Initial target platforms are **Linux and macOS**. Windows is deferred.

The working baseline now includes a **static-configuration Raft core** with
durable elections, replication, ordered commitment, conflict repair and
quorum-backed read barriers, plus a three-replica counter demo with durable
application checkpoints, pinned logical compaction and follower snapshot catch-up.
Group configuration and voter store identities are
persisted with the native WAL. Operation retries return the original application
result without repeating their effect. Restart, partition, corruption and
storage-failure tests exercise native providers and host replacements. There
are no third-party runtime dependencies.

Shared runtime components now schedule many groups through bounded ready queues
and explicit deadlines. The 100-group history uses one WAL per node, batches
persistence across groups, and demonstrates progress while one group's durable
completion is delayed. Public scheduler, timer, clock and election-jitter seams
support host replacements. This is a caller-driven integration boundary;
automatic timer management, background workers and transport assembly remain.

Transport, physical WAL reclamation and online reconfiguration remain under
development; this is not a production consensus release.

## Run

Install Rust 1.98.1 (pinned in `rust-toolchain.toml`), then:

```sh
cargo test --locked --offline
cargo test --locked --offline --no-default-features
cargo clippy --locked --offline --all-targets -- -D warnings
```

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
bounded; the later service admission layer must reserve capacity before commits.

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
network framing and shared outbound buffer/admission assembly remain to be implemented.

`runtime::Shard` owns its registered cores. Admit an owned event, poll a scoped
visit, then call `step_next`. Drive returned effects through bounded host workers,
using `with_core` to deliver exact admissions/completions and existing snapshot
or read helpers. Finish the visit once its dependencies resolve; other groups
can run while it is suspended. Rejected admission returns the original event.
Input credits remain charged until the visit finishes. These limits cover
ingress retention; the host still supplies separate outbound and application
budgets. The runtime creates no threads or stores.

Embedding hosts admit a read with `Event::Read`, drive its `ReadProbe`/`ReadAck`
messages, then consume `Effect::ReadReady` through `application::read_at_barrier`.
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
