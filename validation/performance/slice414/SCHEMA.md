# Bounded startup batch envelopes

The matched pilots expose a concurrency gap on both hosts. The current Core
admits at most64 originals and replicates at most64 entries per transition,
while the pinned upstream defaults admit4096 and replicate1024. This slice
allows larger bounded batches; measured gains remain unproven until paired runs.

## Data and API

`raft::BatchLimits` has `max_proposal_entries` and `max_replication_entries`.
Defaults remain64/64; validated hard ceilings are4096/1024. Each Core owns its
copy. `configure_batch_limits` accepts a startup configuration only before the
first Core event; `batch_limits` exposes inspection. No live reconfiguration,
quorum-policy change, store-format change or new durability token is introduced.
The ordinary receive ceiling is independently fixed at1024 so replicas with
different outbound batch choices remain compatible. Hosts must choose actual
codec/frame and decoded-memory budgets before traffic. Wire defaults allow1024
entries, but replaceable providers retain exact frame validation authority.

## State, ownership and failure paths

Recovery first validates the original log/membership/snapshot contract. It
initializes default envelopes; the embedding host may then configure them.
First event locks the local envelope, including an unsuccessful event. Restart
requires the host to provide the chosen configuration again; persistent safety
state remains reconstructed through the existing recovery path.

Proposal Vec capacity, entry count, payload capacity, retained-entry limits and
byte ceilings are checked before staging. Every original command keeps its own
ID, ordered position and result; grouping grants no transaction semantics.
Ordinary outbound replication, held-request range inspection and effect
reservations use the local replication count ceiling. Incoming ordinary
appends use the independent1024 ceiling; repair RPCs retain64, and reservations
cover the larger of the local outbound and existing repair ceilings.
Early Written replication remains eligible only if the whole pending suffix
fits that replication ceiling; larger API batches use existing durable chunked
replication. No Written-only acknowledgment or client result is introduced.

Admission refusals create no pending transition. Wrong/stale completions,
elections, configuration changes, suffix replacement and snapshots preserve
their existing fencing and cleanup paths. Native owner reservations continue to
charge all original inputs/results, protect control credits and fit a bounded
prefix. Larger Core capacity does not override scheduler visit/count/byte limits.

## Acceptance

- Defaults retain64-entry behavior; zero/over-ceiling settings are rejected,
  and any post-event reconfiguration is refused without changing the Core.
- A4096-original suffix retains exact order/IDs and exact ticket ownership;
  allocation/byte/retained limits reject the entire batch atomically.
- Three actual Cores replicate1024-entry chunks, require exact durable voter
  receipts and commit/apply every original; no premature result on Written.
- Wrong identities/terms/configurations, restart/stale tickets, replacement,
  snapshots and membership/repair regressions retain meaningful coverage.
- Existing full-validator/ModelIO crash checks and relevant integration tests
  pass, along with formatter and all four strict Clippy profiles.
- Matched baseline/candidate trials use immutable independent artifacts and
  original-result counts on Linux/M2 Max. Native routine-control coalescing is
  separately planned and measured; do not attribute its effect to this change.

This advances P9.1. Full minimum curves and sustainable/disk-backed capacity
remain required by the macro plan.
