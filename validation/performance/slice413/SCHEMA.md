# Configured append/metadata delta (413)

## Shape and ownership
Reuse private NextLog::Delta and revision-scoped LogSummary. No new public API,
cache, wire/disk format or durability capability. Exact predecessor metadata and
owner-private validated summary are mandatory. Preserve summary.plain=false for
configured/checkpoint histories. Existing Core pending membership=None reuses the
validated accepted membership; native accepted overlays retain canonical prefix
and new owned tail until the same exact barrier.

## Eligible transitions
Plain histories retain current ballot behavior. Nonplain histories additionally
use Delta only with identical complete HardState, no incoming snapshot/membership
base, and absent suffix or Command/Noop-only append at last_index+1. Revisions,
commit bounds, entries/payload limits and term/index checks are unchanged. The
retained ballot origin is unchanged and cannot consult bootstrap for a new voter.
Configuration, changed ballot, replacement/rollback or snapshot uses Full oracle.

## Failure and restart
Plan validates before canonical installation. Wrong update/predecessor refuses;
no earlier owned batch unit is installed if any later plan fails. Completion,
publication, ticket generation and fencing contracts are unchanged. Recovery
still validates full state and reconstructs its exact private summary. Native
fault tests continue to distinguish written and published accepted state.

## Acceptance
Finite Learners/Joint/Final and compacted stable/joint rows compare complete state,
metadata, term lookup, origin and byte summary with stage_update. Successful
eligible plans must retain prior payload pointers and nonplain classification.
Changed ballots/config/snapshot/replacement remain Full. Core tests compare
pending connection union with old materialized view, exact peer limit refusal,
membership identity reuse, and final retirement only after exact durability.
Native controls include multiple unpublished configured deltas, subset barrier,
full fallback, restart and checkpoint recovery. Existing broad checks required.
No builds/tests/Clippy until root releases CPU qualification after timing.
