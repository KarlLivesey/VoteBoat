# Slice191 — explicit placement planning and execution

The offline placement CLI emits immutable administration records for the
existing runtime. Exact replica store bindings reject deployment substitution.
Planning snapshots and projected future memberships are informational inputs;
they cannot establish durability, reserve capacity or authorize promotion.

Five selected all-feature tests pass in3.77s; four default-feature cases pass
in3.02s. The TCP/QUIC histories use real generated voter and replacement plans,
stop before a new store exists, verify no premature promotion, enroll it and
recover the same saved records through finalization and checkpoint restart.
Other cases check recursive policy preservation, exact stores, malformed/stale
input, reused operation IDs, unknown voters and placement violations. The three
examples in docs/PLACEMENT.md were executed; output artifacts are retained here.

Initial compile/fixture corrections and an invalidated concurrent-feature run
are retained, not counted as passing validation. The final full regression and
strict-check logs contain the terminal outcomes. source.sha256 identifies the
implementation checked. No new platform, full-roadmap or performance acceptance
claim is made; current macOS, separate-host, broader faults and P7 remain open.

Final sequential all-feature regression:70 counter cases pass in65.06s,
8 placement/administration cases pass and13 planning cases pass. Formatting,
strict all-target Clippy for default/all/core-only and95-entry inventory pass.
No warnings were suppressed or thresholds changed.
