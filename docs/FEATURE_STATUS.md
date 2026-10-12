# Feature status

The goal is **all P0–P8 functionality, qualified on Linux and macOS**. P8 remains
incomplete. Performance is P9, dedicated security work is P10, and Windows is
deferred. Stop after P8.

| Capability | Evidence |
| --- | --- |
| Durable Raft, quorum reads, retries and restart | Native WAL/checkpoints, Rust embedding and networked services; Linux/macOS acceptance records |
| TCP/TLS and QUIC | Native providers through public session, connector and transport contracts |
| Shared Multi-Raft, recursive quorums, routing and split/merge | Implemented paths with protocol, durability and native acceptance records |
| Broader transactions and striped single-group storage | Selected Linux/macOS matrices318/321 |
| Physical-voter relays | Static-tree TCP/QUIC and relay-backed logical storage on both hosts |
| Subgroup-backed logical voters | Imported/Joint/Unknown recovery and both repeated-replacement protocols qualify on both hosts; deep cold recovery remains below |
| Independent diagnostic clock | Native and host injection qualified on Linux and macOS401 |

Remaining:

1. **Mac QUIC deep cold recovery.** Full Mac TCP402 now passes1/0 with all27
   owners cleaned up. Linux402 passes all9/default7 nested tests. Its
   checkpoint-only90s watchdog retains actual durable-frontier and cleanup
   assertions. Both old Mac398 cold failures under15s remain preserved.
2. **Final source and requirements reconciliation.** The71-row audit records
   implemented evidence and these remaining checks; it is not a completion claim.

Both Mac398 repeated-replacement histories and the exact Mac native-only100-group
recovery test now pass with original owner cleanup.
Formatting and strict all-target Clippy remain zero for all four configurations,
enforced by the enabled pre-push hook. CI provides background feedback.

See the [current mini plan](IMPLEMENTATION.md#current-mini-plan403),
[requirement audit](../validation/baseline/slice403/README.md),
[checkpoint qualification](../validation/baseline/slice402/README.md), and
[Mac repeated-replacement results](../validation/baseline/slice398/mac/README.md).
