# Feature status

The goal is **all P0–P8 functionality, qualified on Linux and macOS**. P8 remains
incomplete. Performance is P9, dedicated security work is P10, and Windows is
deferred. Stop after P8.

| Capability | Evidence |
| --- | --- |
| Durable Raft, quorum reads, retries and restart | Native WAL/checkpoints, Rust embedding and networked services; selected Linux/macOS acceptance records |
| TCP/TLS and QUIC | Native providers through public session, connector and transport contracts |
| Shared Multi-Raft, recursive quorums, routing and split/merge | Implemented paths with protocol, durability and native acceptance records |
| Broader transactions and striped single-group storage | Selected Linux/macOS matrices318/321 |
| Physical-voter relays | Static-tree TCP/QUIC and relay-backed logical storage qualified on both hosts |
| Subgroup-backed logical voters | First replacement and imported/Unknown recovery qualified on both hosts; deeper composition and remaining lifecycle checks below |

Remaining:

1. **Mac deep composition and cleanup.** Complete twenty-seven-owner cold histories
   pass Linux TCP/QUIC on frozen392. Mac394 TCP reaches same-store recovery and
   retry, then fails final physical shutdown with retained work. QUIC and repeated
   replacement qualification are running; remaining failed ownership must be
   resolved and all original joins/rebinds completed.
2. **Membership and startup closure.** Linux393 passes all fourteen logical
   activation tests. Mac Joint qualification is prepared. Startup395 passes
   default42/0 and all-feature56/1; the remaining QUIC membership-drain failure
   follows a source-only cleanup that parks surviving voters. Published396 keeps
   those voters polled; its complete startup qualification is running.
3. **Final requirement and platform qualification.** Frozen Linux384 passes all
   default tests2124/0; all features2436/2 fail in startup fixtures subsequently
   corrected. Core/native profiles are pending. Reconcile these historical cuts
   with current-source checks and finish the P0–P8 requirement audit; this is not
   a whole-current-source acceptance claim.

Formatting and strict all-target Clippy remain zero for all four supported
configurations, enforced by the enabled pre-push hook. CI runs in the background.
Historical failures and exact source/runtime evidence remain recorded.

See the [current mini plan](IMPLEMENTATION.md#current-mini-plan372),
[requirement map](BASELINE_ACCEPTANCE.md), and
[frozen392 recovery evidence](../validation/baseline/slice392/README.md).
