# macOS Core batching qualification

Frozen 1319-input source snapshot. All source checks match before and after.
All-feature focused controls: **442 passed, zero failed** (library282, reclaim11,
staging2, native log12, provider29, Raft79, barriers8, striped9, framework7,
staging diagnostic3). Formatting and all four strict Clippy configurations pass
with zero diagnostics. Every runner stage exits0; terminal failures=0.

These are correctness checks, not macOS performance measurements.
