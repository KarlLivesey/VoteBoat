# Linux elapsed diagnostic: 415 versus 417 off/on

Nine rotating cases pass every original/request/frontier/histogram and phase
check. Each completes 1,048,576 originals in 262,144 four-original requests, after
1,024 singleton warmups; all three final frontiers are 1,049,601. Startup envelopes
remain 4096/1024 and default snapshot/retention maintenance remains enabled.
Before/after source and artifact guards pass. No rebuild, mutation or copying
occurred during the quiet timing window.

| Variant | Median originals/s | Three trials | Median request p99 |
| --- | ---: | --- | ---: |
| Frozen 415 | 861,766 | 864,391 / 794,084 / 861,766 | 22.36 ms |
| Frozen 417, attribution off | 857,111 | 923,159 / 775,711 / 857,111 | 25.66 ms |
| Frozen 417, attribution on | 872,289 | 880,768 / 872,289 / 838,715 | 22.05 ms |

Off is about 0.54% below the unchanged baseline median. Trial ranges overlap; the
higher on median establishes no observer speedup or precise overhead bound. Bash
whole-process median user/system times are 2.128/0.066 seconds for baseline,
2.152/0.074 for off, and 2.093/0.057 for on. RSS is unavailable. These process totals
do not assign CPU cost to phases. This remains a diagnostic, with no sustained
capacity or minimum-margin qualification.

The leader's instrumented whole-lifetime median is 1,221.5 ms, including campaign,
warmup, measured writes, follower drain and shutdown. Per-phase medians are:

| Leader phase | Calls | Elapsed total |
| --- | ---: | ---: |
| Maintenance tick, inclusive | 9,060 | 487.2 ms |
| Persistence helper, inclusive | 4,692 | 315.1 ms |
| Host admission/correlation/reservation | 2,337 | 97.5 ms |
| Original-result delivery | 2,353 | 86.2 ms |
| Direct Core calls | 9,061 | 23.5 ms |
| Application | 2,353 | 1.48 ms |
| Send enqueue | 6,675 | 2.79 ms |
| Host select wait | 9,061 | 209.3 ms |
| Store append, nested in helper or maintenance | 4,896 | 148.6 ms |
| Store barrier, nested | 4,896 | 0.119 ms |

Maintenance includes no-op checks and completed checkpoint/compaction work; the
leader publishes 204 checkpoints in every trial. It includes store/Core helper
work and any snapshot effects. The store append total covers both persistence
and maintenance, so subtracting it from the persistence helper would mix scopes.
Likewise direct Core excludes continuation inside helpers. Do not sum these
inclusive/nested intervals, sum replicas, or call them CPU time or critical-path
measurements. Follower wait intervals are residence awaiting input and include
drain; they establish no transport bottleneck.

The useful priority is checkpoint/compaction and persistence-helper work before
changing FIFO result storage. Current source confirms `compact_replica` verifies
the log, pins/loads/validates the checkpoint, stages Core compaction, persists it
and reconciles retention; `persist_effect` includes exact admission, barrier and
Core completion. Attribution does not yet isolate which of those suboperations
dominates. A focused representation/ownership audit or finer bounded intervals
within these existing seams should precede a redesign. Host admission plus result
delivery remains a smaller, measurable candidate; no duplicate/original checks
should be removed. The no-op application and sender enqueue are small here.

Candidate source is exact 355cfa6244e9e4d04cb12d7ba1d44204b07ec215. Both actual
candidate root-library and harness compiler records are fresh:false, optimization
3, assertions and overflow checks enabled, thin LTO and one codegen unit. They
match baseline compiler identity, independent lock and profiles. The only
production source difference from 415 is the native Counter wrapper outside this
matched Core path. `artifacts-linux` retains commands, compiler paths, hashes and
all 1,378 source inputs; executable/checkouts are excluded from tracked evidence.
`diagnostic1048576` preserves every command, stdout, stderr, exit, Bash process
time, guards and validation. PERFORMANCE-EVIDENCE.json hashes all 67 records.
