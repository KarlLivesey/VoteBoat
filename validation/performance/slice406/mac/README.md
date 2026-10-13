# Mac406 functional qualification

Frozen1317-input source; original first P9 touched-group staging plus both
baseline diagnostic examples. All stages0, before/terminal source checks match.
Focused all-feature suite431/0: library274, reclamation11, staging2, native log12,
provider29, Raft79, shared barriers8, striped9, framework4, store diagnostic3.
Formatting and all four strict all-target Clippy configurations are zero.
This is correctness/platform qualification, not Mac performance measurement.

No production OS-specific durability, authentication or feature change. Public
storage tickets/atomic batch, core quorum and original crash/recovery controls
remain tested. Native store common source matches the Linux measured candidate.
