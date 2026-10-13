# Slice 418: plain matching-prefix compaction

Purpose: remove retained-history payload cloning from the checkpoint maintenance
transition measured in slice 417. This does not skip maintenance or change storage
formats, public provider contracts, durability or application results.

## Shape and exclusive ownership

A private CompactDelta retains exact predecessor metadata, removed-entry count
and next validated summary. Every original snapshot-only LogUpdate field is
uniquely represented by these two metadata records and the required absent
suffix/membership. Installation destructures every LogUpdate field exhaustively
(without `..`) and compares the complete SnapshotRef, including store, schema and
checksum. Future fields cannot silently evade correlation. Eligible
states are validated plain histories, with clone-stable retained command payload
capacities, unchanged complete HardState, no incoming membership/suffix, and a
matching snapshot prefix at or below the predecessor commit. Other shapes retain
the full validator. A summary flag is established by the existing validated-state
scan and maintained on normalized appends. This prevents a capacity-accounting
change for custom recovered states whose command capacities exceed their lengths.

Preparation preserves apply_update/apply_snapshot validation and error order:
revision/ballot, ballot origin, snapshot scope/boundary, matching membership,
index conversion, generation increment, commit/term bounds, revision increment.
It checks all fallible conditions before any exclusive state mutation. Exact
predecessor and entire original update correlate installation; no caller-supplied
unchecked descriptor substitutes for these checks. Cached retained accounting
subtracts removed inline headers and payload capacities; prior overflow recomputes
the surviving slice. Retained charges continue to use entry length rather than
Vec capacity. Compact installation drains only under the exclusive canonical owner,
sets the exact snapshot/header, and leaves surviving payload allocations intact.
Vec drain still drops removed payloads and moves surviving entry headers.

Native Plain overlays retain a durable-prefix offset plus an owned accepted tail.
Their logical view contains the retained durable slice and retained accepted tail,
with the latest snapshot/header. Accepted compaction changes that offset and may
drain tail entries; it never drains canonical durable state before publication.
Append/compact/append and repeated compactions therefore compose before a barrier.
Promotion drains the canonical prefix, appends the remaining accepted tail and
installs the exact header only after sync/publication. Existing Full overlays can
compact their exclusively owned accepted state. Generation/ticket invalidation
and subset barrier receipts remain unchanged.

Core term lookup for Pending Compact sees the new snapshot boundary, no removed
indices, and the retained terms. A crate-private exact-summary checkpoint accessor
returns None for a validated plain history only after the original index-bound
checks; configured states keep membership replay. The existing pin, load, full
log-state equality, snapshot reference/bootstrap comparison, application restore
proof, durable completion and snapshot reconciliation all remain required.

## Failure and restart

Whole multi-group batches prepare before installing any unit. Invalid later
units do not change earlier groups or issue native I/O. Native uncertain append,
sync or publication fences as before. Durable state remains unchanged until a
successful barrier; recovery replays the existing WAL and validates snapshots.
Power loss follows the actual modeled sync frontier. Old sessions/generations and
altered update completions cannot install a compact plan. No snapshot pin release
moves earlier, and reconciliation failure retains the existing fencing behavior.

## Acceptance

Full-oracle differential states/errors for first/repeated/partial/full checkpoints,
malformed scope/boundaries, commit/term/generation/revision precedence, configured
and nonmatching fallback. Surviving pointers and exact capacity charges including
slack-capacity fallback and overflow recomputation. Stale summary/predecessor and
altered snapshot reference rejection. Atomic invalid later batch. Native accepted
append/compact/append, repeated trim, subset barriers, unaffected groups and old
generation tickets. Actual ModelIO uncertainty plus power-loss recovery states.
Affected Core/log/native/snapshot controls run sequentially in
`target/p9/slice418-checks`; scoped formatter/strict Clippy diagnostics must be zero.
Root performs full hook/service/broad qualification and publishes the checked slice.

Owned normalized incoming slice 419 remains the next candidate; no Arc or public
batch-layout redesign is included here.
