# VoteBoat

VoteBoat is a Rust consensus runtime under development, licensed under
[RPL 1.5](LICENSE). Its design separates recursive application responsibilities,
quorum topology, execution lanes, and physical WAL lanes. Native providers use
public interfaces that host applications can implement themselves.

Initial target platforms are **Linux and macOS**. Windows is deferred.

The first working slice is **durable voting**, not a complete Raft engine.
It provides checked identities, validated recursive/weighted quorum predicates,
a deterministic RequestVote gate, and a bounded native term/vote WAL. A ballot
is released only after its exact storage dependency is durable. Restart,
corruption and storage-failure tests exercise the real provider and injected
host implementations. There are no third-party runtime dependencies.

## Run

Install Rust 1.98.1 (pinned in `rust-toolchain.toml`), then:

```sh
cargo test --locked --offline
cargo test --locked --offline --no-default-features
cargo clippy --locked --offline --all-targets -- -D warnings
```

Try the persistent voting example in a fresh directory:

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
