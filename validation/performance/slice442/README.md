# Native442: admission hypothesis postponed; publication waiting is material

Published source590f6e78 is unchanged. This slice gathers current three-voter,
disk-backed Counter evidence on Linux and M2 Max, separate from the volatile
OpenRaft framework. It changes the next action; it claims no optimization gain.
[Plan](SCHEMA.md), [Linux](linux/README.md), [Mac](mac/README.md).

| Host | Profiled TCP/TLS window32 | Request p99 | Originals | Window1 attempt |
| --- | ---: | ---: | ---: | --- |
| Linux | 187.789 completed PUTS/s | 455.628494ms | 15,000 |120-second progress timeout;938 completed, operation1003 unresolved; cleanup also timed out |
| M2 Max | 271.967 completed PUTS/s | 161.817500ms | 15,000 |120-second progress timeout;1,456 completed, one unresolved; cleanup also timed out; no final rate claimed |

Both successful bulk runs also complete64 warmups, reopen the logs, verify first/
last original retries and join workers. Current ready quantum16/Core64 and the
32-original inflight window stay unchanged. The source preflight corrected20,000
to15,000 before invoking either workload, preserving the existing16,384-entry
limit and control reserve. Both singleton failures remain failures; there was no
retry, deadline extension, limit bypass or discarded slow case.

Linux's actual bulk width is16×937 plus8×1. The leader makes1,875 WAL syncs and
manifest publications during79.876685 seconds. WAL sync reports24.586224 seconds,
manifest publication51.781757 seconds, with file sync25.465129 and directory sync
25.886266 inside publication. These are elapsed wall intervals; nested and
cross-worker attribution cannot be added as CPU time. Direct publication waiting
is material in this workload, irrespective of incomplete CPU attribution.

Mac reports1,876 leader WAL syncs (11.936541 seconds) and manifest publications
(37.396576 seconds); publication includes16.932494 seconds of file sync and
17.178438 seconds of directory sync. Its observed widths match Linux's exactly.
Mac's resolved Counter-validation stack union is0.113670% of observed thread CPU
deltas, split into ready0.059862% and ingress0.053808%; unresolved ancestry and
wait/wakeup bias prevent treating those lower bounds as full instruction costs.

Saved headless profiles rank physical self and inclusive unions separately.
Linux has77.004% empty caller chains and18.362% unresolved leaf weight in its
whole-process bulk capture. The resolved pending-client tree lookup/range leaves
are substantial; Counter union values are lower bounds, not proof of zero total
admission cost. Mac's observed thread CPU deltas attributed to sampled stacks
also have wait/wakeup-stack bias. Captures include startup, warmup, verification,
recovery or failed cleanup; do not call them measurement-only CPU profiles or
compare their units across hosts. No browser, SVG, profiler installation or
kernel-policy change was used.

Decision: postpone the optional Counter admission-context interface. Source
complexity and incomplete caller attribution do not justify it as the largest
current drain. Preserve the publication/damage-detection protocol and default
quantum16. Next, make the existing configured64-command ready quantum measurable
with a bounded64-original window (current CLI cap32 prevents64-wide batches), then
measure actual proposal widths, publications per original, completed rates and
latency on both hosts. Compare policies at the same window and native profile;
report configuration changes separately from software gains. Existing421 singleton
regressions still prohibit changing defaults without sustained fairness/headroom
and hot/cold evidence. This is batching behind the current barrier, with no storage
format migration or weakened durability.

Fresh asserted native profiling artifacts retain default LTO/codegen and use
debug2 for symbols; acceptance native builds use debug0. Original/copy/source/tool/
profile identities, raw errors, lost/unresolved limits and partial outcomes remain
in the host archives; raw large profiles remain at recorded locations and hashes.
No full test suite or old successful framework benchmark/profile was repeated.
No refusal counter, physical RSS/debt series, healthy framework minimum or full
P9 completion is inferred from these diagnostics.

Root independently verifies all119 Linux and112 Mac archived payload hashes and
all1,405 common native source bytes/executable bits. Source physical modes differ
(644/755 Linux export versus664/775 committed Mac tar headers), and each host
preserves its recorded modes before/after; all seven private fixture keys remain
0600. The additional32 Mac selected inputs belong to the unbuilt matched workspace.
[Source review](ROOT_SOURCE_REVIEW.json) retains the initially over-strict root
cross-host mode assertion and the actual mode maps; no source/artifact/capture
was changed to satisfy it. This does not alter within-host comparison guards.
