# Slice 419 read-only implementation map

Current source is slice 418 plus the parent task's validation/documentation changes.
No production edit, build, test, copy or timing was performed for this proposal.

## Concrete movement paths

| Owner | Current movement | Proposed movement |
| --- | --- | --- |
| Core persist_update | input suffix -> planned clone; input -> normalized Pending clone; original -> public effect | borrow-validate metadata; one Pending normalized owned copy; original -> unchanged public effect |
| Core completion | planned suffix -> canonical; Pending copy dropped | exact owned transition update.suffix -> canonical |
| Written overlap | first access uses Pending.update, later matching_entry uses next.Delta.append | both use the ONE sealed Pending update under the same exact ticket/context guard |
| OwnedLogState | consumed input -> borrowed plan clones; consumed original dropped | consumed input -> validated/normalized sealed unit -> canonical after whole batch prepares |
| Native append | original borrowed codec encoding; accepted plan clones; original consumed/dropped | unchanged original codec encoding; then consumed validated/normalized sealed units held through I/O -> accepted |
| Native barrier | accepted tail -> canonical | same movement, including deferred trim from 418 |
| Full fallback | cloned retained state plus new entry clones | preserve exact old validator; do not claim these copies removed by ordinary-append work |

Core cannot move one buffer into both Pending and the public owned provider effect.
Retaining the existing single normalized Pending copy preserves the raw public effect
and its original allocation-budget/provider behavior. No public effect signature needs
change. OwnedLogState's public get/apply_batch/into_states API stays unchanged.

## Files and exact responsibilities

- `src/log/transition.rs`, optional private owned-transition child module:
  factor append admission into a metadata-only validator; keep the borrowed NextLog
  oracle adapter for differential controls if useful. Seal normalized owned input with
  that metadata under one private type. Keep exact content equality and exhaustive
  field coverage, consuming preparation/install, Full and 418 Compact fallbacks.
- `src/log/transition/view.rs`: preserve exact trimmed accepted view from 418;
  no new canonical storage representation is needed.
- `src/raft.rs`: Pending owns the sealed update and its plan together, with accessors
  replacing redundant update/next storage. persist_update validates raw input before
  the one necessary normalized Pending copy. Preserve membership/cancellation/error
  behavior and complete ticket checks. Provider Effect keeps the original input.
- `src/raft/admitted_replication.rs`: both admitted_append and matching_entry read the
  same owned suffix. Preserve exact request context/range/ticket fences, no durability
  inference from Written, and empty aliases. Inspect other Pending.update/next users
  before edits; do not leave one reader pointing at a dropped duplicate allocation.
- `src/log/owned.rs`: owned preparation consumes mutation units, with the original
  order/duplicate/creation error behavior; whole batch correlates before committing.
- `src/native/log_store.rs`: codec still borrows originals first; prepare then consumes
  them. Obtain control classification/ticket fields before ownership moves, without
  cloning payloads. Hold prepared units through I/O, preserve fencing/bookkeeping.
- `src/native/log_store/accepted.rs`: prepare owned transitions from exact Clean,
  Plain(trimmed durable + accepted tail), and Full views. Move entries into overlays;
  no accidental materialization for ordinary append after accepted compaction.
- `examples/framework_benchmark/store.rs`: ideally no production-like host change:
  its existing public OwnedLogState.apply_batch already consumes the owned units.
  Check its original-result/one-outstanding/durability-model contracts unchanged.

## Reuse focused controls

- `src/log/transition/tests`: full oracle, retained pointers, slack capacities, altered
  payload with identical metadata, compact correlation and configured fallback.
- `src/log/owned/tests`, `tests/log_owned.rs`: moved incoming pointers, original raw
  rejection precedence where applicable, malformed later atomic unit and identities.
- `src/raft/proposals/tests`: Pending/effect allocation count and capacity identity,
  exact original results; configured and checkpoint histories; stale summaries.
- `src/raft/admitted_replication/tests`: early Written entries, exact/duplicate/delayed
  ACKs, retry ranges and aliases with the single owned update.
- `src/native/log_store/accepted/tests`: incoming pointer moves, multiple unpublished
  revisions, all 418 trimmed paths and exact subset publication.
- `tests/log_store`, `tests/snapshot`: codec refusal ordering, native control budget,
  actual power-loss frontiers, old-session/generation tickets, all snapshot proofs.

Add assertions to these fixtures rather than introduce a new provider/benchmark.
The private sealing API must not accept unchecked scalar plans from public callers;
original content equality is checked wherever an external original crosses its
correlation boundary. Compiler-visible exhaustive fields supplement, not replace,
full payload equality controls.
