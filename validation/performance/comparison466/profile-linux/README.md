# Actual466 headless CPU diagnostic

Both original31284 captures ended0. All processing is terminal and the verified
owned localhost symbol server74816/PID2103896 stopped0. Current466 ELF c8d489c4…
and packed DWP0acd9e4c… were rehashed; source is frozen92454c26, independent of
the later main-worktree rollback. Policies remain2/2/516. No browser, image,
source/build change, tool installation or recapture was used.

Bulk selects6,235 on-CPU sample weights: wrapper self819 (13.136%), append_for
self503 (8.067%), oneshot send452 (7.249%), EntryIter::next207 (3.320%),
iterator Vec materialization151 (2.422%). The materialization context union
is161 (2.582%);157 (2.518%) have the concrete borrow_completion→persist helper
call chain. Range coalescing union163 (2.614%) overlaps that work.

Singleton selects23,174 weights: EntryHistory::get self4,550 (19.634%), largest
named self function. Actual per-address source bindings include partition_point
inside get; its source/inlined frame counts overlap. Explicit caller buckets
include LogMetadata::from_log under reserve/rollback and provider apply paths.
Resolved entry-backing Arc contexts are134 (2.149%) bulk and1,061 (4.578%)
singleton; these are overlapping sampled contexts, not refcount/allocation calls.
No separately resolved descriptor-clone/growth samples establish zero such work.

The old slice-to-Vec native symbol has only2 bulk self samples, versus651/8,395
in the separate455 capture. This does not prove eliminated copying: the current
iterator materialization and lookup/outbound work are recorded under different
functions. READOUT.json keeps455's own identity and capture denominator separate;
no old address response is reused or sample percentage translated into speedup.
No new optimization or allocator-cause claim follows from this readout.

Perf reports6,241 bulk/23,187 singleton samples. Bulk lost1 chunk; no singleton
loss was reported. Null selected stacks0; explicitly unresolved leaves113
(1.812%)/326 (1.407%). Raw synthetic FDE names remain separately disclosed.
Shallow physical depth≤2 weights38/57. Finite16KiB unwinds may truncate, but an
exact truncation count is unavailable. Bulk import's zero-timestamp ordering
warning and all raw capture/tool stderr remain unchanged. Inclusive contexts,
native functions, inline source frames and nested stages cannot be added.

Profiled diagnostics complete19,988,480/1,048,576 originals, with frontiers
19,989,505/1,049,601 on all replicas. Rates6,478,110/s and80,061/s, p993.758411ms
and0.024626ms, belong to perturbed whole-launch captures, not paired capacity
acceptance. Thread names do not identify leader/follower ownership. Raw perf.data
remains ignored locally with hashes/locations in RAW_PERF_LOCATIONS.json.

Current JSON address responses, physical/inline summaries and folded stacks are
under symbolication/. Existing converters were reused; client-context textual
historical labels were retargeted without predicate changes. Focused ownership
predicates operate only on saved current responses. Original sandbox query,
bundled-loader and stop-inspection failures are retained in processing-attempt1/
and symbolication/PROCESSING_FAILURES.json; no workload was rerun for them.
