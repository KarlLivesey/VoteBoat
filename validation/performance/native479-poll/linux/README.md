# 479 Linux native Counter CPU diagnostic

Original handle 28000 actually terminated with exit 0; native, perf, resource owner and frozen validator passed. One TCP/TLS one-group three-voter capture uses the unchanged published 476 native opt3/debug0/assertions/overflow/default LTO+codegen artifact, WitnessLogIo, ready64, 8192 offers at 1024/s, window4096, 64 warmups and 2s active maintenance. No recapture, build, policy change, browser or SVG.

This profile was offer-capped: 8192 applied, zero refusals/unknown/pending, 1022.769 completed originals/s and 1022.625 during/s, intended p99 45.110 ms, 9.629 ms drain, max 65 inflight and 11 queued at horizon. Recovery 8256, retries and joins verify; 3 checkpoints and 9 reclaims complete. Source/artifact/compiler/runtime/tool and before/after 2/2/516 policy guards pass. The current file resides on the same guarded Btrfs volume/device as 478. The captured run is not the thousands-pending 478 regime and is not evidence of an unprofiled improvement.

| Exact physical-frame context | Native sample-count share | Native perf-period share |
| --- | ---: | ---: |
| client_reconcile | 11.827% | 14.306% |
| pending_client_btree | 9.462% | 11.863% |
| Counter_validate_proposal | 2.529% | 2.937% |
| core_raft | 5.832% | 7.461% |
| ready_collect | 0.856% | 0.896% |

These are overlapping inclusive unions, not an additive CPU partition. Final Samply native-exec branch 1008406.1 contains 2452 unit-weight samples. Its events map one-to-one to native perf leaves by TID/ordinal and every timestamp difference<=1.1µs; the corresponding period denominator is 5017802056. Five pre-exec Python samples with total period 668 are excluded. Raw whole-launch perf has 2506 samples and approximate period sum 5123877853, including the env/Python resource owner. Sample counts and periods are separate views; neither is elapsed CPU time, exact cycles, call counts, disk waiting or per-original service latency.

Pending-client B-tree work is supported by real ancestry: 11.138% of all native-exec period lies in the intersection of pending B-tree and ClientRouter::reconcile frames. Among pending B-tree leaf periods, direct callers are 68.089% reconcile and 22.007% cleanup. The earlier type-only perf view could not establish that assignment. Named Counter admission costs 2.529% of samples/2.937% of periods here; low demand and missing inlines prevent extrapolating to 478 backlog. The separate Mac capture supplies different, loaded context and must retain its own denominator.

Perf reports zero lost samples; all converter stdout/stderr are preserved. Its build options report DWARF and libdw-unwind ON, libunwind OFF. Nevertheless, its native PID view has only one frame for 92.961% of period weight. Saved-data Samply import of the same 16KiB raw DWARF stacks recovers depths up to 29 and only 7/2452 single-frame samples. The missing perf ancestry was converter-dependent; debug0 alone was not an adequate explanation. No recording was repeated to obtain this improvement.

Final Samply symbolication retains 114 unresolved leaf samples (4.649% of counts) plus 200 synthetic fun_<hex> leaf names (8.157%) separately. Synthetic labels can recover physical ancestry without supplying an actual function name. Many stacks contain an unnamed frame; no complete-stack or zero-truncation claim is made. Native debug0 still limits source/inline detail. Thread names do not identify leader ownership. Raw stack addresses, saved imported JSON, full symbol request/response JSON, exact ELF buildID identities and both converter qualities remain available. The temporary 127.0.0.1 JSON-only symbol server was owned and stopped with exit 0; no browser was opened.

The perf callgraph text can omit the sampled IP when DWARF unwinding yields no frames. Final perf analysis joins explicit perf script -G leaves to the callgraph dump by exact pid/tid/time/period, preferring the real leaf and removing only an identical first unwind address. Initial failed frame assertion, the incorrect callchain-only reference and the Rust tuple/DSO parser split are retained under analysis-* with original outputs; final parser splits the final DSO suffix. These were analysis corrections on the same saved data. The current final Samply full-frame readout is the useful ancestry source.

Raw perf.data stays at the ignored location with SHA256 8e9546d08c2f77aa81f441be6155a94c924a4c7a63ade374651299e9edd2f9e5 and 41611836 bytes. ARCHIVE_PROOF and readout provenance record that location; the 40MiB raw capture, native executable, production source/buildtrees, caches and generated WAL are excluded from Git. All text/CSV/JSON, imported profile, scripts, commands, exits, errors and warnings are lossless; large text is gzip-packed with original hashes/lengths. Whole-native-child resources include warmup/recovery/cleanup: user 1.043700s/system 1.275591s over 8.263755s, peak RSS 21409792 bytes (Linux raw KiB converted); block counters are not bytes.

This diagnostic identifies reconciliation work in this offered regime and confirms that Linux absence cannot disprove loaded Counter admission cost. It does not choose or validate a source change, promote defaults, close real ext4/XFS or satisfy full P9 sustained throughput/headroom/fairness/placement/composition requirements.

Two small original stdout files are also losslessly gzip-packed after the staged whitespace check reported raw blank/trailing spaces. Their exact bytes are preserved; no raw output was stripped or rewritten. Prior seal metadata and original files remain under the ignored target whitespace-repack directory.
