# Paired delta409 performance diagnostics on M2 Max

Reference production is committed6c404559; only its native staging benchmark
harness is replaced with the exact frozen409 append-mode harness. Candidate is
the complete previously qualified409 frozen working-tree snapshot, including
new delta modules. All framework/native harness files match between variants.
Neither variant reads upcoming410 live source. Each has its own SHA256 manifest.

Actual hardware is queried without serial identifiers: chip, CPU core counts,
memory, OS, Rust/Cargo/Node. Build all features in release opt3 with assertions
and overflow checks enabled. Cargo compiler-artifact JSON must confirm profiles
for both executables and library. Copy and hash originals immediately after
each variant's build. Use separate target directories, require non-fresh
compilation and distinct artifact identities; the preserved first attempt
showed that shared fingerprints could select reference artifacts for candidate.
No Cargo,
functional tests or parallel workload runs overlap timed cases on this Mac.

96 cases reproduce the exact Linux409 workloads/order: framework clients
1/64/256/1024, batches1/4, 4096 measured +1024 warmup, 3 alternating pairs;
native unrelated0/8, initial histories256/4096, append/replace512 +16 warmup,
3 alternating pairs. Every framework case checks4096 completions, all three
applied frontiers5121 and unmatched comparison label. Every native CSV checks
512 rows, exact mode and state/recovery verification. Preserve raw argv,
stdout/stderr, exits, JSON/CSV and final source/artifact guards.

Scope is volatile/unmatched CPU diagnostics. These are not durable filesystem
benchmarks, OpenRaft matched comparisons, or evidence that any advertised floor
was exceeded. Report all workload results, including regressions, honestly.
