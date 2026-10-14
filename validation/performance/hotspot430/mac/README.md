# Mac430 saved CPU diagnostic

Original remote PID13810 completed0/DONE; bulk and singleton recording exits0,
all before/after source/artifact/tool/executable/dSYM guards0. Existing Samply0.13.1
SHA dcf042115dd8a9e8582510f403365cbee1a2025405d0f3ebe95162f31943123b, unchanged
qualified430 executable6cdbc5a1ce976a0a34aeff6d788e0cb76b098a6f91a13c7e5140d93046db768c,
matching dSYM UUID C18FB2E2-D783-381F-8C5E-5F33E5C7801E. Adjacent dSYM symlink
only resolves the known presymbolicate lookup issue; no artifact bytes changed.
No build, browser, SVG, new tool or settings change. No competing Mac work during
recording. Existing426 converter SHA b4d85920f686bd055816184afeae04dd6bc240737ed49c3d6947fd7ca1bf35a1.

Requested bulk20M normalizes by existing clients*batch rule to19,988,480 completed
originals/4,997,120requests, frontiers[19,989,505;3]. Singleton completes1,048,576,
frontiers[1,049,601;3]. Raw requests and actual counts retained. Profiler-perturbed
rates4,226,449 and54,085 originals/s, p995.031ms/0.039292ms, are not paired capacity
acceptance. Both use1024 warmups, default maintenance/upstream envelopes,
attributionoff, runtime1/16,1000Hz save-only+presymbolicate. Recordings include
startup/warmups/latency sorting/follower verification/shutdown, unlike throughput.

Initial postprocessing reused a power-of-two checker expecting request=actual and
failed. POSTPROCESS.initial.stderr/POSTPROCESS_FAILURE.json retain that failure.
validate-profiles.py first proves actual requested/normalized counts, then runs
other exact invariants on a deep copy with normalized request solely for that
checker. Raw files unchanged; six forged request/completion/frontier controls
rejected. No recording rerun. COPY_CONTEXTS.initial.json retains initial broad
receive_append substring classification, which also included receive_appended;
copy-contexts.py uses exact names and preserves final corrected classification.

Observed selected-thread CPU delta: bulk9,587,073µs; singleton36,765,790µs across18
threads. Unresolved leaf6.511%/2.687%; any unresolved stack9.762%/5.180%; no detected
null/malformed stack CPU. Physical native self shares sum to observed delta;
inclusive functions overlap. Bulk self HashMapinsert4.414%, memmove4.340%,
LogEntryclone3.313%, Actor effects3.122%, DefaultHasherwrite2.668%, hash_one2.642%.
Bulk inclusive Actorstep28.045%, effects25.027%, requests14.373%, persist_effect
11.707%, oneshot send7.796%; do not sum these as costs.

Sampled wait leaves dominate (cvwait38.951% bulk/70.933%singleton). CPU deltas are
interval observations attributed to sampled stacks, strongly biased toward waits;
these are not actual CPU time spent blocking, instruction costs, elapsed wall
percentages or allocator causes. Native aliases and inline/source chains are
representative after symbol-address coalescing, not per-RVA precise. An Actor<true>
symbol label does not override the verified OFF runtime setting. Generic server
thread names do not distinguish leader/followers. Unknown unwind truncation remains
unquantified; unresolved/null/malformed observations are retained.

COPY_CONTEXTS.json uses physical native caller frames. Bulk memmove under complete
1.779%, exact receive_append0.008%, Ownedapply1.365%, PreparedOwnedLoginstall2.531%
of observed delta (overlap). LogEntryclone under complete1.633%, receive_append
0.652%. Singleton memmove under complete0.746%, receive_append0.500%, Ownedapply
0.890%; clone under complete/receive_append each0.020%. No separate physical
plan_copy frame found; inlined work and431 eligibility remain unproven by this
absence. Full physical self/inclusive rankings, raw gzipJSON/symbol sidecars,
commands/exits/guards and limits remain. No sustained floor/durability/P8 claim.

Original63 file hashes verified after collection. ArchiveSHA
d6b90f8220fd50523405b9c289150780be7061e0d181b7fed2a63838ebfa737b. Four large derived
folded text outputs are losslessly gzipped; LOSSLESS_CONTAINERS.json records their
original byte hashes, verified by decompression. Raw recording/symbol bytes are
unchanged. Regular files with SHA256SUMS; no failures, tails or regressions removed.
