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

1. **Mac deep recovery and repeated replacement.** Joint recovery and complete
   startup targets pass on both hosts. Linux397/398 and default400 cover the
   complete logical targets. Combined Mac398 is running; its cold TCP case failed
   waiting for the new checkpoint to publish, with original snapshot work still
   owned. The old and new failures remain recorded.
2. **Separate diagnostic wall clock (C12).** The final audit found this required
   public operation missing. Slice401 adds an independent injectable contract
   and native implementation; local four-profile runtime tests and native controls pass, with matching Mac
   checks pending after398 hands back the machine.
3. **Final requirement and source audit.** Broad Linux384 remains historical:
   default2124/0, core1028/0, native1511/0, all2436/2. Corrected startup and all
   affected logical fixture targets pass. Their399 reconciliation predates401's
   additive production clock change. Finish the source-specific Linux/macOS
   evidence map before declaring P8 complete.

Formatting and strict all-target Clippy remain zero for all four supported
configurations, enforced by the enabled pre-push hook. CI runs in the background.
Historical failures and exact source/runtime evidence remain recorded.

See the [current mini plan](IMPLEMENTATION.md#current-mini-plan401),
[requirement map](BASELINE_ACCEPTANCE.md), and
[checkpoint recovery evidence](../validation/baseline/slice397/README.md), and
[repeated-replacement evidence](../validation/baseline/slice398/README.md).
