# Feature status

**P0–P8 capabilities are implemented for Linux and macOS.**
P9 implementation is now explicitly authorized; see [the P9 plan](P9.md).
The user confirmed that shutdown speed is a performance requirement, not an
unfinished capability. P9 is performance, P10 is dedicated security work;
Windows remains deferred. Do not start those phases automatically.

| Capability | Evidence |
| --- | --- |
| Durable Raft, quorum reads, retries and restart | Native storage, embedding and network-service histories on both platforms |
| TCP/TLS and QUIC | Native provider contracts and network histories on both platforms |
| Shared Multi-Raft, recursive quorums, routing and split/merge | Protocol, durability, ownership and recovery histories |
| Broader transactions and striped single-group storage | Linux/macOS matrices318/321 |
| Physical-voter relays | Static trees and relay-backed logical storage on both platforms |
| Subgroup-backed logical voters | Membership, repeated replacement and recovery histories on both platforms |
| Independent diagnostic clock | Native and host injection on both platforms401 |

The Mac402 deep QUIC test passed its recovery assertions but exceeded the
fixture's15-second final-close watchdog. **That test failed; it is not reported
as passing.** A fixed shutdown-speed target is deferred to P9. The test-only405
change permits an already-flushed original QUIC close to reach its actual
protocol deadline while still requiring real resource release. No production
optimization was made. Linux full nested suites pass15/15 and13/13; Mac shared
close controls6/6 and both real timer controls pass. The full Mac405 rerun now passes1/1 (918.62s), including actual cleanup;
the Linux routed regression passes192/192. Neither establishes a15-second
shutdown speed requirement. [Final records](../validation/baseline/slice405/mac-final/README.md).

Formatting and strict Clippy are zero across all four feature configurations,
enforced by the enabled pre-push hook. This is a feature-completion boundary,
not a claim of exhaustive proof, a clean new full-platform test matrix, measured
performance, or a security audit.

See [the implementation record](IMPLEMENTATION.md),
[the completion evidence](../validation/baseline/slice405/README.md), and
[the preserved Mac timeout](../validation/baseline/slice402/mac_quic/README.md).
