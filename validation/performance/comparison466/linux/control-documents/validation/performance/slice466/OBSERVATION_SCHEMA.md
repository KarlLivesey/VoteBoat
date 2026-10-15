# Slice466 retained-entry observations

Before-edit extension to the ownership schema. New shared/partial backing must
be measured before retention; logical entry counts and process RSS alone cannot
identify pinned history allocation debt.

Use the existing attribution-only Recorder (TRACE=true) to record checked
per-owner EntryHistory::allocation_bytes after successful volatile store apply,
after successful actor persistence completion and after maintenance. Serialize
observation count, last charge and maximum observed charge for Core and store
separately. This includes entry capacity, immutable backing/control, descriptor
capacity, payloads and hidden partial spans, charging each owner in full. It
excludes other Core metadata, pending effects, app/snapshot/queue memory, kernel
allocations and transient coalescing; the named points are not a continuous
whole-process peak. Setup/warmup/measurement/drain/shutdown follow existing
attribution lifetime. No sharing discount or sum-as-RSS/CPU inference.

No new provider API, phase, clock, sampling task, lock, queue, timer or durability
receipt. OFF uses a compile-time false branch and does not inspect histories or
update counters. Core/store outcomes complete first; observation overflow/None
only poisons the final attribution report, never changes protocol progress or
substitutes a storage error. Restart starts fresh diagnostic cells.

Controls: disabled poison and lazy observation are untouched; checked counter
updates are atomic on diagnostic failure; both final owner reports keep scope;
actual maintained three-voter ON controls have nonzero observations, exact final
entry charges and maximum>=last. OFF report remains absent. Existing count,
frontier, phase/scope and original outcome assertions remain. Fresh artifact and
paired performance proofs remain later; this document claims no improvement.
