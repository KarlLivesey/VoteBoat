# Native admission repeated-work audit

Read-only source audit at `2585cf5d00ef2e2bab900d72e93597b57c4a28a7`.
No implementation, build, recording, benchmark or remote operation was performed
for this audit. This is a possible later native optimization, separate from
slice440's private matched-benchmark pending-key experiment. No improvement is
claimed; the full P9 acceptance remains incomplete.

## Actual repeated work

[`ProposalAdmission`](../../../src/application.rs) defaults
`supports_ready_batch()` to false. Its validation is pure, serialized with
application, and returns a per-original receipt retention bound; it grants no
admission, durability or outcome authority. `Counter` explicitly opts in. Each
Counter validation first checks the requested command has eight bytes, then
rebuilds a `BTreeSet` of pending operation IDs absent from its applied dedup map,
checks the remaining-operation budget, and includes the requested ID. It does
**not** clone or replay Counter. Different bytes for an existing ID remain an
application `OperationConflict` outcome, rather than a new admission rejection.

[`check_ready_execution`](../../../src/runtime/clients/ready.rs) invokes that
validation for each checked candidate. Its input is the unapplied log tail,
all retained router commands for this group, then selected predecessors not
already represented by an exact admission/operation/bytes match. Ordinary
registered predecessors already occur in router pending state. Each candidate
also scans pending state again for the **minimum** reserved receipt bound among
exact group/operation/bytes matches.

For a ready prefix of b originals, L unapplied log commands, P group pending
commands, D applied dedup IDs and U novel reserved IDs, the Counter set work is
approximately b times the same base scan with tree lookups/inserts:
`O(b*(L+P)*(log D + log U))`. Router filtering and receipt lookup additionally
scan all-group pending state per candidate; prefix representation checks grow
with prefix length. Full byte comparisons remain relevant. Historical entries
before the applied offset are skipped, not replayed by Counter admission.
Submission separately performs application validation in
[`clients.rs`](../../../src/runtime/clients.rs); a ready-prefix context would
not remove that ingress cost.

[`BucketCounter`](../../../src/bucket_counter.rs) instead clones its whole
application and replays pending commands for each validation. It has **no**
ready-batch opt-in, so its current execution is serial. Its 8192 pending-item
limit counts raw commands, including duplicates; replay preserves parsing,
scope, semantic-byte capacity and original conflict outcomes. An ID-only
Counter context cannot substitute for this behavior. The service Counter
wrapper forwards capability but returns the outer receipt size;
[`Maintenance`](../../../src/maintenance/state.rs) retains default false.

## Smallest plausible later scope

An optional, provided ProposalAdmission context factory could default to the
existing validator, allowing interchangeable providers to retain behavior.
Counter could own one temporary reserved-ID set and borrow the immutable
application. Initialize only when a second candidate is checked, avoiding new
singleton setup costs. The first normal validation plus one context seed still
costs about two base scans; a two-original prefix need not benefit.

The current public API has no such context. It needs a separate complete schema,
composition/inventory review and acceptance tests before editing. Do not
downcast applications, infer capability from concrete types, enable Bucket or
Maintenance batching, or cache solely by group/application pointer. No session
is implemented or selected here.

## Ordering, ownership and cleanup constraints

- Preserve per-candidate configuration callbacks and deployment checks. Current
  ready order is configuration, leader/application/capability, application
  context, applied-index range, proposal validation, then receipt validation.
  Submission has its own order; do not unify them casually.
- Counter's requested-byte check must precede building or reporting cached
  pending-capacity errors: malformed requested bytes currently win over
  over-capacity pending state.
- Preserve full operation IDs and exact command bytes. Do not turn conflicting
  retries into admission errors or merge per-original admissions/outcomes.
  Keep the exact matching receipt minimum, inline outer receipt size and global
  receipt limit check for every original.
- [`ready_batch::collect`](../../../src/runtime/ready_batch.rs) checks the first
  candidate once, then checks successors against one unchanged Core. A later
  refusal leaves that candidate queued and submits only the selected prefix.
  Incorporate only accepted, otherwise-unrepresented predecessors, never the
  unselected future window. A receipt refusal must not dirty reusable context.
- Existing window/owner gates retain leader, configuration, protected-control,
  visit/output reservations, pending-dependency, term and proposal limits.
  Logical authorization remains singleton. A context grants no bypass.
- Router pending state is unchanged during collection and updated after owner
  advancement. Cancellation of a wait does not release the retained command's
  reservation. Keep canceled/deferred commands in admission accounting until
  existing log/application takeover or failure cleanup releases them.
- Drop/reset temporary context at each empty-prefix/batch/group boundary,
  before Core stepping and after refusal/failure. App/Core mutation, compaction,
  fencing, restore and epoch changes require fresh validation. Do not retain
  log/payload references or introduce restart state. Scratch storage and any
  retained capacity require explicit bounds; no physical-memory claim follows
  from logical command counts.

## Checks required for a future context

Existing ready-batch tests cover duplicates/conflicts, canceled observations,
written-but-unsynchronized failure, output budgets, queued later refusals,
original ordinals, wrapper receipt sizes and default context-dependent serial
execution. Relevant sources are `tests/client_admission/ready_batch/`,
`tests/client_admission/ready_batch.rs` and `src/runtime/ready_batch/tests.rs`.
They do not qualify a new context API.

Future comparison tests need malformed-byte/capacity error priority, full-width
IDs and conflicting bytes across log/pending/selected inputs, exact matching
receipt minima, outer wrapper bounds, default-provider fallback, declined
successors and partial prefixes, canceled reservations, group/context resets
after durability/Core changes, and unchanged application/error/cleanup state.

## Existing native evidence and limits

[Native421 Linux](../native421-quantum/linux/README.md) and
[Mac](../native421-quantum/mac/README.md) exercise actual Counter admission with
three replicas, TCP/TLS or QUIC and physical durability barriers. Each host has
36 rotating short cases: 256 originals plus 64 warmups, windows32/1 and
quantums16/32/64. At window32, quantum16 produces sixteen 16-original batches;
32 and64 produce eight 32-original batches. Leader barriers fall32 to16.
Linux TCP/TLS median completed PUTS/s improves 1,399.046 to 2,684.935 and p99
43.517ms to18.262ms at quantum32. Singleton regressions remain and default16
was retained; window32 cannot demonstrate a 64-wide batch.

This evidence establishes the native path and batching opportunity, not
admission's CPU share, a context speedup, sustained maintenance, fairness,
offered-load headroom or deployment capacity. The minimal matched framework
does not exercise ClientRouter/ProposalAdmission. Native improvements cannot
replace its required one-group floors, and elapsed method intervals are not
additive CPU attribution.
