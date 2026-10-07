# VoteBoat

VoteBoat is a Rust consensus runtime under development, licensed under
[RPL 1.5](LICENSE). Its design separates recursive application responsibilities,
quorum topology, execution lanes, and physical WAL lanes. Native providers use
public interfaces that host applications can implement themselves.

Initial target platforms are **Linux and macOS**. Windows is deferred.

The working baseline now includes a **static-configuration Raft core** with
durable elections, replication, ordered commitment and conflict repair, plus a
three-replica counter demo. Group configuration and voter store identities are
persisted with the native WAL. Operation retries return the original application
result without repeating their effect. Restart, partition, corruption and
storage-failure tests exercise native providers and host replacements. There
are no third-party runtime dependencies.

Transport, read barriers, snapshots and online reconfiguration remain under
development; this is not a production consensus release.

## Run

Install Rust 1.98.1 (pinned in `rust-toolchain.toml`), then:

```sh
cargo test --locked --offline
cargo test --locked --offline --no-default-features
cargo clippy --locked --offline --all-targets -- -D warnings
```

Try the three-replica counter in a fresh directory:

```sh
cargo run --example replicated_counter -- /tmp/voteboat-counter 1 7
# operation=1 outcome=Value(7) retry_duplicate=true
# All three replicas report value=7, with committed/applied boundaries matching.
cargo run --example replicated_counter -- /tmp/voteboat-counter 1 7
# Restart and retry operation 1: value remains 7.
cargo run --example replicated_counter -- /tmp/voteboat-counter 2 3
# A new operation adds 3: value becomes 10.
```

The demo uses three real WALs and host-driven in-process message delivery.
It explicitly elects node 1 and submits each operation twice to demonstrate
deduplication. Its reads are local applied-state diagnostics; no distributed
linearizable-read API is advertised yet. Its counter accepts signed i64 deltas
encoded as eight little-endian bytes. Application/deduplication capacity is
bounded; the later service admission layer must reserve capacity before commits.

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
