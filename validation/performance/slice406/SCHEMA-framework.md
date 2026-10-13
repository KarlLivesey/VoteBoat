# P9 framework scaffold schema

Purpose: attribute real public deterministic Raft and logical storage contract cost before optimizing it. Not a claimed matched OpenRaft campaign.

Pinned inspiration: OpenRaft e88bb40707f2e511d8ecc44d31edcb1d814a11a8; benchmarks/minimal README, src/bin/bench.rs, src/store.rs, src/network.rs inspected.

Shape: three original replicas, each with public Raft, volatile logical LogStore, and constant-size applied-index-only application. One bounded queue carries actual Message values. Closed-loop logical clients retain at most one original operation; receipts resolve only from the leader's actual Committed effect after its application frontier advances. Empty command payload. Bootstrap/election and warmup excluded from measurement; original retained history is never silently cleared.

State: bootstrapped -> elected -> warmup applied/drained -> measured clients queued/admitted -> quorum commitment/application -> original result -> client reissues -> final all-original-messages drain. Persistence effects go through persist_effect and exact accepted tickets plus barrier; volatile barriers model completion and promise no disk or crash durability.

Ownership: one cooperative driver owns all three cores, stores, client map, and message queue. No threads, network, platform I/O or alternate consensus implementation hidden in the example.

Bounds/failure: clients<=4096; finite operations+warmup must fit explicitly configured retained-entry bound (default16384); message bound proportional to configured clients. Unexpected effects, missing/duplicate original receipts, unsupported batch>1, history exhaustion, and no-progress all fail. No compaction/snapshot reset added to obscure retained-history scaling. Report identifies cooperative single-thread scheduling versus upstream1 client/16 server workers, absent snapshots, and retained history; OpenRaft gate is always unmatched until those differences are resolved and same-host campaign runs.

Checks: one-client receipt count and all-three contiguous applied frontiers; multi-client one-outstanding identity/count; batch4 refusal; history refusal before measurement; exact ticket/provider behavior; strict Clippy/fmt. Functional smoke only, no timed throughput claims during concurrent development.
