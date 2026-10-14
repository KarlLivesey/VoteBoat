# Mac431 saved CPU diagnostic

Original remote PID75637 actually completed0/DONE. Both record exits0, all
before/after source/artifact/tool/executable/dSYM guards0; raw stderr retained
("Stopping profile." / "All tasks terminated."). No build, browser, SVG, new
profiling tool or settings change. No competing Mac activity during recording.
Qualified431 source ebcacbbb755d8069a084837fdc0f4920426c61b7; executable
SHA a211af458233d4d719298436ba4bb138af4230eddb1ef7b26615332307ccf400;
fresh binary/dSYM/profile UUID DC3E912F-6F96-3C10-AFD4-78542A3D1CF3. Existing
Samply0.13.1 SHA dcf042115dd8a9e8582510f403365cbee1a2025405d0f3ebe95162f31943123b
and unchanged converter SHA b4d85920f686bd055816184afeae04dd6bc240737ed49c3d6947fd7ca1bf35a1.
Adjacent dSYM symlink resolves known presymbolicate lookup; no artifact bytes
changed. New raw sidecars belong to431; no430 symbol addresses reused.

Bulk requested20M normalizes to19,988,480 completed originals/4,997,120requests,
frontiers[19,989,505;3]. Singleton completes1,048,576, frontiers[1,049,601;3].
Both use1024warmups, default maintenance/upstream envelopes, attributionoff,
runtime1/16,1000Hz save-only+presymbolicate. Profiled rates4,436,427 /54,754
originals/s, p994.556041 /0.039292ms are perturbed diagnostics, not capacity
acceptance. Recording includes warmup/startup/latency sorting/verification/shutdown;
throughput timer boundaries remain unchanged. Normalized validator proves raw
request, actual counts and frontiers before adapting only a deep-copy request to
the frozen power-of-two checker; six forged controls rejected. No recording or
validation rerun. Offline analyses all0.

Selected18thread observed CPU deltas: bulk8,958,575µs, singleton36,185,088µs.
Unresolved leaf6.551%/2.727%; any unresolved stack9.875%/5.281%; null/malformed
stack CPU zero. Bulk physical self: cvwait38.328%, HashMapinsert6.149%,
mutexwait4.860%, memmove4.239%, effects3.253%, hasherwrite2.911%, hash_one2.872%,
LogEntryclone2.565%, Clientwrite2.038%. Inclusive application shares: Actorstep
26.572%, effects24.681%, requests16.899%, persist_effect10.932%, HashMapinsert
8.541%, oneshotsend8.487%, client_loop8.249%, Raftcomplete6.306%, Clientwrite6.032%.
Inclusive shares overlap; do not sum as costs. Singleton cvwait70.561% and
cvsignal6.455% dominate, memmove2.503%, LogEntryclone0.039%.

COPY_CONTEXTS.json uses exact physical names, separating receive_append from
receive_appended. Bulk clone2.565% occurs under complete1.442%, receive_append
0.356%, Ownedapply0.053%; memmove4.239% under complete2.013%, receive_append0.011%,
Ownedapply1.318%, Preparedinstall2.784%. These contexts overlap. Singleton clone
under complete0.018%, receive_append0.012%; memmove under complete0.746%,
receive_append0.436%, Ownedapply0.942%. No physical plan_copy frame; absence does
not establish absent inlined work or optimization eligibility. No exact empty
payload allocation cause can be inferred from clone labels.

ALLOCATION_CONTEXTS.json retains identifiable resolved growth leaves0.168%bulk/
0.141%singleton and unresolved malloc-library leaves5.496%/2.140%. Bulk unresolved
malloc-library context: Clientwrite2.721%, client_loop3.220%, Actorrequests1.377%,
append_for0.190% (overlap). Library identity cannot distinguish allocation/free or
identify operation allocation counts. Initial derived unresolved label omitted
library identity, yielding zero; focused corrected classifier matches SUMMARY
library-weight totals. ALLOCATION_CONTEXT_CLASSIFIER.json retains that correction;
raw profiles are unchanged. No allocator causal claim.

CPU deltas are intervals attributed to sampled stacks, strongly biased toward
waits. They are not actual CPU time blocking, instruction costs, elapsed wall
percentages or allocation counts. Native aliases/representative inline chains
coalesced by symbol-start address are not per-RVA precise. Generic server thread
names do not identify leader/followers. Actor<true> alias does not override proven
OFF runtime controls. Unknown unwind truncation remains unquantified.

Original60 collected regular file hashes verified. Complete remote archive SHA
595d70ef3d3efa473578c13f76dabfaa84af1fcad80f39735e2424d5f02b9d2a remains target-only.
Four large derived folded outputs are losslessly gzipped, with original hashes
and decompression checks in LOSSLESS_CONTAINERS.json. Raw gzip profiles and plain
symbol sidecars unchanged. This archive contains regular files and SHA256SUMS;
no original errors, tails or regressions removed. P9 floors/durable/P8 acceptance
remain unachieved.
