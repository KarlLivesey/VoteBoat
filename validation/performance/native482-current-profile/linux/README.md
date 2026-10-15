# 482 Linux current480 loaded CPU diagnostic

One original handle 11490 actually terminated 0 (final tool chunk d4fe2c): perf, native, resource owner, exact postguard and frozen native/storage/publication validator all passed. Three quorum-durable voters share the guarded Btrfs Sabrent NVMe. Published 480 source f7b0ef12709bb7e7aad4f0279851aa035b508e9e and its unchanged fresh native opt3/debug0/assertions/overflow/defaultLTO+codegen artifact 8d3530a6… run TCP/TLS, WitnessLogIo, ready64, 16000 offers at 2000/s, window4096, one group, 64 warmups and 2s active maintenance. No recapture, build, policy change, browser or SVG.

Profiled diagnostic: 8219 completed originals, 7781 window refusals, all other refusals/unknown/pending zero. Completed 715.425 originals/s; during-offer 515.375/s. Backlog at horizon and max inflight 4096; drain 3.488282s. Recovery 8283=64+8219, retries/joins verified; 3 checkpoints, 9 reclaims, no maintenance skips. This is loaded, unlike 479's max 65 pending; profiles are not paired throughput acceptance.

| Applied-original latency | p50 ms | p95 ms | p99 ms | max ms |
| --- | ---: | ---: | ---: | ---: |
| intended | 4399.116 | 6448.260 | 6522.608 | 6556.992 |
| service | 4394.511 | 6448.222 | 6522.458 | 6556.968 |
| dispatch | 0.095 | 0.879 | 2.115 | 6.182 |

Intended/service distributions cover 8219 actual applied originals; dispatch covers all 16000 actual offers, including refused ones. Percentiles use nearest-rank ceil(n*p/100)-1. All original offers and raw summaries are preserved.

| Exact physical-frame union | Sample-count share | Perf-period share |
| --- | ---: | ---: |
| Counter_validate_proposal | 44.746% | 57.003% |
| OperationId_set_insert | 28.021% | 35.999% |
| Counter_applied_map_get | 11.295% | 13.878% |
| pending_client_btree | 17.947% | 19.867% |
| client_reconcile | 18.246% | 19.528% |
| check_ready_execution | 4.507% | 5.083% |
| ready_collect | 2.797% | 3.289% |
| core_raft | 1.602% | 1.554% |

These inclusive unions overlap and cannot be added. Counter validation is the largest observed named work stage: 57.003% native-period/44.746% counts. Its exact check_submit-iterator instantiation covers 55.257% period; physical ClientRouter::submit/propose_maintenance ancestry covers 55.643%. check_submit is present in a generic type name, not a captured physical frame. Direct caller of Counter validation leaf work is 97.823% ClientRouter::submit. Among OperationId-set insertion leaf periods 94.874% directly name the Counter submission validation instantiation; Counter applied-map get has 93.655% that caller. Source application.rs225 rebuilds reserved BTreeSet across pending after dedup checks; clients.rs327 invokes it separately during submission on log.chain(queued). This source structure fits ancestry, without measuring call/allocation counts or proving a future optimization gain.

Native-exec branch 2669021.1 has 3683 unit-weight samples and period sum 17333009497; all samples match raw perf leaves one-to-one by TID/ordinal and timestamps within 0.655us. No preexec Python samples occurred in this native PID. Whole perf launch has 3730 samples/17421684805 period, including wrappers. Counts and periods differ; neither is elapsed CPU, exact cycles, blocked I/O time or per-original cause. Whole native launch includes startup/warmup/maintenance/drain/recovery/cleanup; there is no exact measurement-only CPU boundary. Worker names do not identify leader ownership.

Perf reports zero lost samples, but its libdw text conversion has 98.301% native-period leaf-only. Headless saved-data Samply import recovers depths up to 29, leaving6 single-frame samples; no second capture. Native debug0 still limits inlines/source, and no zero-truncation or complete-stack claim is made. Final unresolved leaves:131 samples/0.728% period; synthetic fun_<hex> leaves 266 samples/7.482% period are separate unnamed identities. Many outer stacks contain synthetic names; those are not actual function labels.

The exact 480 ELF buildID ad4c6b0ce94d237f9aa7f3ca04fd28de1747eb7b matches imported codeId and symbol requests; no 476 maps used. Raw addresses, imported profile, symbol request/response JSON, all converter reports, folded count/period data and warnings remain inspectable. The temporary 127.0.0.1 JSON-only Samply symbol server was owned, stopped and joined 0; no browser. Original policies remain 2/2/516 before/after. One read-only source-path observation error (nonexistent src/application/counter.rs; actual source application.rs read subsequently) is recorded, without inventing a raw file.

Whole native-child resources: user 3.419650s, system 0.662162s over 12.718069s, peak RSS 29454336 bytes (raw Linux KiB converted). This boundary includes verification/cleanup and differs from the 11.488s original timer. Block counters are operations, not bytes; inclusive/elapsed storage scopes are not additive CPU.

Raw perf.data remains ignored at /home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native482-current-profile/linux/capture/perf.data, SHA256 26de351dbcc4adbbf1e1627af2d5c02a84cc5d49d421a4b8fbd7d406ad3c6675, 61970732 bytes. Archive omits binaries, buildtrees, source trees, caches and generated WAL. Frozen source inventories/modes, exact artifact/root/compiler/protoc/provenance proofs, native metadata CSVs, all raw stdout/stderr, commands, exits and errors are retained losslessly. Text/stdout/stderr/log files gzip-packed upfront; PACKING/SELECTION preserve original hashes/lengths/modes and SHA256SUMS seals every payload.

Linux loaded TCP and Mac moderate QUIC answer different questions: do not compare their percentages or profiled rates cross-host. This evidence selects submission validation for a bounded design review; it does not change code/defaults or satisfy full P9 sustained/headroom/fairness/placement/composition or pending ext4/XFS acceptance.
