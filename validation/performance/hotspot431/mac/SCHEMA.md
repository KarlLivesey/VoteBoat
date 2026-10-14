# Mac431 save-only sampled CPU diagnostic

Reuse the existing verified Samply0.13.1 binary (SHA dcf04211...) and unchanged
426 offline converter (SHA b4d85920...). Qualified published431 executable SHA
a211af45... and its complete exact original/copied dSYM inventories are guarded by
the actual430/431 pair checker. Existing compiler1.98.0/profile proof remains.
Before recording, adjacent dSYM metadata symlink points only to the qualified
bundle, UUID DC3E912F-6F96-3C10-AFD4-78542A3D1CF3. This preserves the known426
presymbolicate resolution workaround without changing any source/artifact bytes.
No builds, browser, SVG, new tools or kernel/settings changes.

Original detached runner executes bulk20M clients4096/batch4 and singleton1048576
clients1/batch1, both1024 warmups/maintenance default/upstream envelopes/attribution
off/runtime1+16,1000Hz save-only+presymbolicate, sequentially. Source/artifact/dSYM
and tool/executable hashes checked outside each recorded case; quiet host verified.
Record originalPID/status/stage/commands/rawstdout/stderr/exits and preserve any
failure. No archive/copy/check/build on the same host while recording. Root must
coordinate READY→START; no recording launched by preparation.

After actual terminal, validate every completed original/frontier/capacity and
collect raw gzipJSON plus exact symbol sidecar as regular files/hashes. Offline
converter keeps CPU threadCPUDelta microseconds and wall sample weights separate,
counts null/zero/unresolved/malformed observations and excluded threads. Physical
native self weights sum to observed selected-thread CPU denominator; inclusive
functions count once per stack, overlap and must not sum. Inline/source chains
are representative (Samply0.13.1 symbol-address coalescing), not precise per-RVA.

CPU deltas are interval observations attributed to sampled stacks, potentially
biased toward sampled wait stacks; not instruction-level cost, total machineCPU,
wall percentages, allocator cause or throughput-timer attribution. Profiles cover
launch/initialization/warmups/latency sorting/follower verification/shutdown too.
No leader/follower identity from generic matched-server thread names, no full
unwind completeness guarantee. Profiler-perturbed rates are diagnostics, not
capacity acceptance or sustained floor proof. Report limitations and physical
self/inclusive rankings without inventing causal certainty or summed costs.

Fresh431 artifact/dSYM/sidecars only; no430 symbol-address reuse. Existing requested20M normalizes to19,988,480 completed originals at4096*4. Validate original requested and actual counts explicitly; retain raw bytes/normalization controls without recording rerun.
