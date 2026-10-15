# Slice468 Linux: rejected heap wait-buffer reuse

Original1699 ended0/ALL_TIMING_END. All24 workload exits and both final result validators pass, with exact completed originals and three final frontiers. Useful repeatable gain was not established; retain all mixed trials. No retune, repeated cases or Mac468 performance campaign.

| Cell | Variant | Completed originals/s median | Request p99 median ms |
| --- | --- | ---: | ---: |
| bulk | baseline | 7,051,114.970 | 3.950913 |
| bulk | candidate | 7,100,719.879 | 3.816851 |
| bulk | on | 7,111,496.676 | 3.668212 |
| bulk | openraft | 3,223,711.700 | 7.614435 |

bulk OFF paired rate ratios: 1.007035, 0.908855, 1.059598; p99 ratios: 1.041547, 1.185037, 0.846961.

| singleton | baseline | 101,929.544 | 0.020408 |
| singleton | candidate | 104,893.145 | 0.020017 |
| singleton | on | 91,233.146 | 0.021000 |
| singleton | openraft | 20,377.810 | 0.127420 |

singleton OFF paired rate ratios: 1.052869, 0.987459, 0.998511; p99 ratios: 0.933193, 1.022099, 0.976115.

Every original retains an independent pinned Tokio oneshot, ordered await and exact128-bit ID check. The experiment reuses only an exclusive client's empty heap receiver Vec, bounded by configured batch width; cold clones allocate on their first timed write. Source tests establish cleanup/reuse/bounds, not global allocator call counts, RSS or physical-memory plateau.

One group, three voters, empty payload, volatile stores and in-process RPC; client1/server16 workers. Bulk requests20M and completes19,988,480 originals; singleton completes1,048,576; each has1024 singleton warmups. Default active retention/catch-up is retained. Throughput ends after all client joins, before latency flatten/sort, follower verification and shutdown. API envelopes are equivalent4096 requests/16384 originals at batch4, ordinary replication1024; upstream append capacity is a soft merge threshold. VoteBoat historical snapshots and OpenRaft current-state snapshots differ.

Both artifacts use identical Linux1.98.1 compiler/tool hashes, asserted opt3/debug2/overflow+assertions/thin-LTO/codegen1, and four actual fresh:false root/executable/upstream/minimal records with verbose/CargoJSON proof. Candidate exported inventory1452 is retained verbatim: functional projection1443 plus9 configuration inputs; its historical compiled/supplemental classifier is not extra functional coverage. Cargo records establish actual compiled roots. Committed API/control documents are separately proven, not relabeled compiled source. Mac has a different compiler and no468 campaign. Current source may be rolled back independently of immutable captured artifacts.

Attribution ON elapsed scopes29/4/15 are nested/overlapping and not additive CPU time. ON/OFF and upstream raw results, tails, logical queues/retained entries, persistence/checkpoint/snapshot-install counters remain in READOUT/raw JSON. No separately reported framework refusal counter, offered-load headroom, RSS or cache physical-memory maximum exists; unavailable is not zero. No causal allocator or native-service speed claim follows.

This archive includes all24 commands/stdout/stderr/exit/shell-time records, both case checkers and pre/post identity guards, setup/readiness/scripts/control documents, original unpublished guard refusals, actual fresh artifact/source/mode/tool/compiler/oracle/CLI metadata and relevant qualification/layout evidence. Historical PREPARATION and SETUP receipts remain labeled before timing. No binary, debug sidecar, build tree, generated WAL, cache bytecode or bulk source tree is copied. Original raw tool files are copied unchanged; COPY_PROOF lists their identities. SHA256SUMS seals every regular payload except itself.
