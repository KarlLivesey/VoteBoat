# Checkpoint compact gap observer qualification

Isolated benchmark-only six-path overlay of exact published 75064ac523beea6ca496468821e8011fc3ec103a. The guarded base map contains 1407 inputs derived from the qualified 41 source map; configs and supplemental documents are included, so this is not a claim that every input compiled. Every unmodified mapped file retained its exact base hash. Main production, public helpers, provider traits, dependencies and original helper outcomes are unchanged.

The final standalone run passed 32 controls, format check and strict all-target Clippy with zero diagnostics. Added controls cover exact ordinal/gap/maxima accounting, missing/stale/backward marks, missing prefix, extra calls, failed selected methods, atomic checked overflow, disabled poisoned stamps/counters, and an actual corrupt pinned image preserving its original Storage(Corrupt) helper error and pin ownership. Existing original-result, snapshot follower, retention and cleanup controls remained enabled.

Actual final debug off/on CLI histories both completed 12000 originals and 3000 requests, with all three frontiers 12001 and two compactions per replica. ON retained 27 phase intervals/four scoped provider groups plus three fixed gap reports per replica, each with two successful observations; OFF omitted attribution. Debug rates emitted by the existing executable are not performance evidence. No release timing or CPU/allocator claim was made.

Raw first lint failure (chunks_exact_to_as_chunks) and reporting-contract failure (existing not_pure_Core label assertion) remain alongside the focused fixes and final passing logs. The former was corrected to fixed-size array chunks; the latter retained the original exact label token while clarifying the remaining residual excludes reported gaps.

Provider methods reuse their actual existing start/end observations. Three boundary gaps are inclusive elapsed intervals; pinned load is nested, maxima cover complete successful observations across the actor lifetime, and the intervals do not claim pure Eq, Core CPU or critical path. Failed helper observations skip gap certification and preserve original errors; malformed successful diagnostics are deferred until otherwise successful owned shutdown.

## Exact qualification commands

The source working directory was target/p9/compact-gaps/source/benchmarks/matched; CARGO_TARGET_DIR was target/p9/compact-gaps/checks.

- cargo +stable test --locked --offline
- cargo +stable clippy --locked --offline --all-targets -- -D warnings
- cargo +stable fmt --all -- --check
- cargo +stable build --locked --offline
- checks/debug/voteboat-matched-framework --engine voteboat --clients 1 --batch 4 --operations 12000 --warmup 0 --maintenance default-retention --envelopes upstream-default --attribution false|true --campaign-timeout-secs 60

The fresh asserted release and observer-impact timing remain separate next steps after parent promotion and coordinated quiet grants.
