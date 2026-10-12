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

1. **Mac deep composition and cleanup.** Linux397 passes both complete cold
   histories and all nine nested-owner tests. Linux398 passes both complete
   repeated-replacement histories. Matching Mac checks are prepared; Mac394's
   three final-cleanup failures remain preserved.
2. **Startup and Joint recovery are qualified.** Linux396 passes all-feature57/0
   and default42/0; Mac396 passes the same complete targets. Mac393 also passes
   original Joint cold recovery over TCP and QUIC, including all old and restored
   owners' cleanup. Remaining advanced membership work is the Mac repeated path.
3. **Final configuration and requirement audit.** Broad Linux384 is terminal:
   default2124/0, core1028/0, native1511/0, all features2436/2. Its old startup
   failures remain failed records. All sixteen later changed inputs are fixtures
   in three targets; production is unchanged. Their replacement all-feature
   checks pass; current default logical targets pass7/0 and8/0. Complete the Mac
   gates and source-specific P0–P8 evidence review before declaring completion.

Formatting and strict all-target Clippy remain zero for all four supported
configurations, enforced by the enabled pre-push hook. CI runs in the background.
Historical failures and exact source/runtime evidence remain recorded.

See the [current mini plan](IMPLEMENTATION.md#current-mini-plan372),
[requirement map](BASELINE_ACCEPTANCE.md), and
[checkpoint recovery evidence](../validation/baseline/slice397/README.md), and
[repeated-replacement evidence](../validation/baseline/slice398/README.md).
