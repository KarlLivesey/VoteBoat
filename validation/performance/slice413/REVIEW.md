# Bounded configured-history delta extension (read-only proposal)

Slice412 is independently qualified/frozen. This review makes no 413 source edits
or performance claims. The proposed extension avoids retained configured-log
copy/replay without first building a general persistent membership cache.

## Safe minimum eligibility

An exact owner-private LogSummary matches current canonical/accepted metadata,
and that predecessor was structurally/membership validated on construction or a
previous admitted transition. Incoming update has unchanged HardState (including
voted_for), no snapshot/base change, and no suffix or a pure append beginning at
last_index+1 containing only Command/Noop. Generation and all prior entries remain
unchanged. Existing revision, ballot-origin, contiguous term/index, payload/entry,
commit nonregression/frontier and exhaustion checks remain mandatory.

This can use Delta even when summary.is_plain() is false. The current plain path
can retain its broader ballot capability; nonplain histories get only the narrow
unchanged-HardState branch. New vote/term, config entries, rollback/replacement,
snapshot import/compaction all keep the original full validator for this slice.

## Why membership reuse is equivalent

Membership::replay_from checks contiguous indexes, verifies checkpoint/bootstrap
base and folds only Configuration records. A validated command append preserves
old indexes/config content and adds no new config. A metadata update changes no
entry. The only global committed-prefix checks during configuration acceptance
are predecessor/joint indexes <= committed_prefix. Commit may only increase, so
checks already true stay true; stable/joint configuration values, record order,
operation-history set and last-configuration index do not change.

Therefore replaying the entire new log would reconstruct the exact existing
accepted Membership. Core Pending may reuse Core.membership as it does for plain
Delta. Configuration ID/readiness invalidation stays unchanged. Commitment can
still demote/retire a removed leader through the existing complete() conditions;
reusing accepted membership must not skip those commitment-dependent effects.

Retained identical HardState keeps its exact historical BallotOrigin. The shared
ballot_origin_for rule does not invoke its candidate lookup when a voted_for
promise remains identical; voted_for None retains no origin. Thus bootstrap-only
candidate lookup is irrelevant for this narrow nonplain branch. Do not allow a
changed/new ballot through it: accepted voters may differ from bootstrap.

## Critical implementation condition

plan_plain currently writes summary.plain=true. The extension MUST preserve the
predecessor summary's classification instead. Configured/checkpoint histories
remain nonplain after ordinary command/metadata deltas. Accidentally marking them
plain would make rollback_replicas and connection_replicas use bootstrap-only
shortcuts and discard needed configured peers/resource bounds. Rename the planner
to reflect append/metadata deltas rather than assume bootstrap-only history.

## Pending connection and term views

For configured Delta, durable connection inspection still runs its existing
configured-history path. It includes committed membership plus every uncommitted
Learners/Joint assignment, including all stores a higher commit could promote.
The pending append introduces no stores; higher commitment cannot introduce a
store absent from this already retained union. Old durable plus new pending full
views therefore have the same union as durable alone under this narrow extension.
Keep exact NodeId/store conflict and limit checks; prove this comparison in tests,
including a compacted joint base and accepted Final commitment.

NextLog::term_at still resolves unchanged old prefix plus only new commands. Native
accepted overlays already borrow a canonical prefix and admitted command tail;
configuration classification can remain false while using append-only overlays.
No public API, disk/wire format, durability token or provider change is required.

## Mandatory differential/negative acceptance

- Stable Learners, Joint and accepted Final histories: append Command/Noop and
  advance commit; complete state/metadata/summary equals old stage_update oracle.
- Both committed and uncommitted config boundaries; Final commit across retirement
  index retains original announcements/demotion/readiness/read-invalidation rules.
- Matching checkpoint with Membership base and a compacted Joint checkpoint:
  command appends, pending connection union and exact limits match old full view.
- Historical ballot origin for a removed candidate survives unchanged-HardState
  delta; changed term/new vote uses full predecessor accepted voter validation.
- Attempts to append config, truncate/replace, import snapshot, regress/gap commit,
  exceed budgets, mismatch cached metadata or exhaust revision retain refusals or
  exact full fallback. No cache bypass for an unvalidated constructor.
- Old payload pointers preserved; snapshot membership/config records untouched;
  resulting summary.is_plain stays false and retained charge matches fresh scan.
- Multiple native accepted unsynchronized deltas, subset barrier/all-written
  publication and subsequent full fallback preserve exact state, generation and
  session/ticket semantics through native checkpoint/recovery.
- Existing broad membership/learner/retirement/ballot/snapshot/Core/store controls
  and zero diagnostics across all configurations. Benchmark configured/checkpoint
  steady histories separately from already optimized bootstrap-only ones.

## Cost and limitations

This removes O(retained history) copies and membership replay from the eligible
transition/versioning path. Current configured rollback/reservation/connection
inspection still scans history; those need separate measured summaries later.
Recovery, owned exports, snapshot encoding and complex full fallback remain
history-sized. Do not claim configured throughput is entirely independent of
history until those remaining inspections are measured and addressed.
