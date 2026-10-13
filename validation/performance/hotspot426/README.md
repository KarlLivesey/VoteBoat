# 426 CPU sampling diagnostics

Published426 artifacts and original source/build identities are reused. These
recordings are separate from unprofiled capacity acceptance. Both engines' normal
original outcomes remain; only VoteBoat is sampled here. Active default retention,
client runtime1/server runtime16, empty payload, three voters and per-original
quorum/applied completion are unchanged. Bulk requested20M rounds to19,988,480;
singleton uses1,048,576; both warm up1024.

Linux initially used official checksum-verified samply0.13.1. Its own policy gate
refused perf_event_paranoid2. User authorized a temporary1; recording then failed
at mmap startup. Auto-restoration returned2, verified. No benchmark result was
produced by that failed attempt. kptr_restrict was never changed. The subsequent
buffer-change request was superseded by the user's instruction to leave kernel
protections unchanged; perf_event_mlock_kb remains516.

Signed official Arch perf7.2.10-1 and libpfm4.14.1-1 are extracted locally, not
installed system-wide. Both signatures verify against the installed official
Arch keyring after format conversion. perf uses cycles:u,1000Hz, DWARF16384-byte
stack snapshots and128 mmap pages. Only the profiler inherits a local library
path; the benchmark unsets it. Before/after protections remain paranoid2/kptr2/
mlock516. Both recordings exit0. The first Linux attempt may overlap Mac archive
transfer; its raw output remains. The repeated perf-quiet run original42542 is
terminal0 and has no concurrent team build/check/copy/benchmark.

Linux flat sample periods identify libc comparison instructions1ae49e/1ae4a5
at43.81%/23.29% of sampled user-cycle weight. Caller records identify retained
LogEntry equality. Whole-process profiles include setup/warmup/latency sorting/
shutdown; these percentages are not the earlier leader's wall intervals. The initial perf/libdw text export has a load-offset decoding error and shallow
caller output. Direct raw Samply JSON validation supersedes that limitation:
27,485 bulk and18,980 singleton samples contain full physical call paths, with
only0.156%/0.232% at depth<=2 and maximum depths25/22. No additional capture is
needed. Text-export attempts remain as historical derived evidence; corrected
inclusive analysis reads stackTable.prefix directly and resolves symbols.
Flat leaf ranking from perf -G remains usable independently of caller decoding.
Packed DWP input is available and hashed; attaching it does not change binary
bytes. No allocator, cache-miss or contention cause is established.

Mac official samply0.13.1 launches directly, with no privilege/codesigning/security
change. Both profiles exit0, contain18 threads and matching source/dSYM symbols.
Raw profiles and symbol sidecars are under ../hotspot426/mac. Mac CPUdelta weights
span the prior interval but use the current sampled stack, so short work bursts
ending in waits bias wait-stack attribution. Do not describe this as CPU spent
waiting. Native physical-function totals are primary: the0.13.1 sidecar shares a
representative inline chain across addresses of one native symbol. Inline/source
line displays therefore do not establish precise per-instruction attribution.

FlameGraph rendering uses official scripts pinned41fee1f99f9276008b7cd112fca19dc3ea84ac32.
Graphs and collapsed stacks are generated locally. Automatic approval review
rejected external Firefox Profiler navigation due to possible disclosure of local
paths/symbols/process metadata. No profile was uploaded; inspection stays local.

P9 remains active. These profiles do not establish sustained floors, real storage
performance, fairness/headroom or any428 improvement.

## Profiled diagnostic results

| Host/cell | Completed originals | Profiled originals/s | Request p99 ms |
| --- | ---: | ---: | ---: |
| Linux c4096/b4 | 19,988,480 | 1,236,961 | 15.852268 |
| Mac c4096/b4 | 19,988,480 | 3,088,076 | 6.037041 |
| Linux c1/b1 | 1,048,576 | 89,167 | 0.023755 |
| Mac c1/b1 | 1,048,576 | 57,078 | 0.037334 |

These are single profiled diagnostics, with differing per-platform sampling
mechanisms. They are not paired or sustained acceptance rates. Exact originals,
frontiers, active checkpoints and source/artifact/tool guards pass.

[Linux bulk compact JSON](linux/bulk-symbolicated/SUMMARY.json) and
[subset JSON](linux/bulk-symbolicated/SUBSETS.json) are the analysis interface.
Corrected full-path bulk sample weight is27,479: retained entry equality62.89%,
checkpoint entry equality40.41%, and persistence-update suffix equality19.13%.
These overlapping inclusive weights differ from perf user-cycle periods.
[Mac compact JSON](mac/READOUT.json) records the distinct CPU-delta method and
its sampling limitations. Historical SVGs/text-decoder outputs remain archived.

Raw perf buffers and large collapsed-stack text are losslessly compressed.
RAW_PERF.json records original hashes/sizes; every compressed item was
independently decompressed and hash-verified. Official tool versions/source
pins/checksums/signature evidence are retained; executables/packages and system
keyrings are not checked into the repository.

## Analysis interface

The user explicitly requires direct compact text/JSON analysis. No browser or
image inspection is permitted unless the user explicitly instructs it. Existing
SVGs are historical derived outputs, not the analysis interface. JSON_STACK_VALIDATION.json
records independently checked full raw stack depth. FINDINGS.json is a compact
machine-readable index; corrected symbolicated inclusive rankings follow from
those existing raw records.
