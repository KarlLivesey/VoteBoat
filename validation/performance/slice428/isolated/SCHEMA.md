# Slice428: exact protected-prefix checkpoint verification (planned schema)

Purpose: remove repeated traversal of already verified retained entries in the
measured Linux426 checkpoint interval, without weakening full equality. User
prioritized this over isolated427's per-entry empty-byte experiment.427 remains
unpromoted and incompletely checked, with351 passed/21 socket-permission failures
and unrun checks preserved. Main production is published426, not427.

## Public data/API and exactness

Keep LogStore::matches_state(group, &GroupLog) unchanged: exact arbitrary-state
comparison, including forged same-revision payload/operation/header values.
Add an object-safe method named matches_checkpoint_state(&mut self,
expected: CheckpointLog<'_>) -> Result<bool, StorageError>. Default calls existing
matches_state(expected.group(), expected.state()), so every interchangeable
provider remains source compatible and fully exact. An optimized provider is
optional. Snapshot compaction alone selects this method; other snapshot/recovery
verification remains full comparison unless separately qualified.

CheckpointLog has private fields, no public constructor, and borrows the immutable
Raft durable GroupLog. Raft::checkpoint_log() alone constructs it. It exposes
readonly state/group and an opaque cloneable equality identity
CheckpointLineage. The identity is privately owned by Raft and changed on every
unclassified or rewriting durable installation. Public callers cannot attach an
identity to altered GroupLog data. A private Arc-backed identity allocates only
at owner creation/invalidation, no global counter/resource. Comparing these
identities certifies a mutation domain, never byte equality, durability or quorum.
Raft has no Clone; recovery creates a fresh domain even for identical headers.

A crate-private bounded CheckpointComparison record belongs to each actual
canonical provider/group owner. It retains one expected lineage and one absolute
verified entry interval, with no payload/log buffers. A new provider/group,
consumed owned state, restore/reopen or binding change starts empty. Provider
rewrites invalidate its own record. Thus both owners' protected histories are
required; a copied header, store session or large revision is insufficient.
Downstream providers can use the exact default or the existing OwnedLogState
owner's public matches_checkpoint_state(view) operation, rather than accepting
an unsafe public cache over arbitrary mutable GroupLog.

Every comparison checks ALL current header fields via exhaustive GroupLog
field destructuring: full Bootstrap/group/configuration/policy/voter stores;
revision/generation/hard state/ballot origin/commit; every SnapshotRef field and
snapshot membership. Existing LogMetadata alone is insufficient. Equal ordered
entry lengths/bounds remain required. Only an interval already compared exactly
and certified preserved by BOTH owners may be skipped; all remaining entries
compare exact LogEntry equality, operation IDs, variants, configuration and bytes.
Malformed public expected values still use raw matches_state full equality.

## Transitions and mutation funnels

Initial or changed core lineage: full comparison. Success records current
absolute retained interval; false/error creates no proof and invalidates stale
comparison evidence. A cache may serve only one core owner at a time; interleaved
owners force full comparison. Successful incremental equality extends the record.

Core durable installation is p.next.install; OwnedLogState privately owns groups
and installs validated plans; NativeLogStore accepted promotion alone changes its
canonical durable map. Classify existing exact planned transition variants:
- Append-only Delta or metadata-only Delta preserves old entry contents.
- Validated matching-prefix Compact removes a prefix and preserves the surviving
  ordered sequence. Rebase the verified interval to its exact surviving overlap.
- Any Full/unclassified transition, replacement, snapshot installation or
  configuration rollback invalidates. Conservative full fallback is acceptable.
Never preserve based only on mutation input revision/generation or on length.

Ordinary compaction increments LogGeneration. Old-generation marks cannot be
silently reused. Successful exact Compact installation at each owner transfers
only the already verified surviving interval to its new generation. All current
generation/header values are still compared exactly next time. For verified
1..10000, trimming through9000 retains only9001..10000, not removed entries.
New entries10001.. are compared exactly. If survivors have no overlap, compare
all current entries. Unknown configured fallback invalidates, preserving features.

Native accepted staging can invalidate future proof for rewriting changes but
cannot advance durable verified progress. Capture classification at accepted
plans/install and apply/reset proof only at successful promotion. Combined
pending transitions and subset barriers must preserve exact durable visibility;
if classification cannot certify a chain, reset and full-compare. Failed write,
sync/manifest publication, uncertain promotion, fenced/unknown group errors retain
existing precedence and establish no new proof. Reclaim changes no logical entries
unless its existing exact codec checks succeed; no raw bytes grant cache validity.
MemoryStore completed-state refusal remains first, before comparison/cache access.

## Lifetime, restart and cleanup

A CheckpointLog cannot outlive its immutable core borrow and exposes no mutation.
Cache identities retain no core/store/log buffers, only bounded opaque identity
and interval. Owner drop releases its marker; another recovered owner gets a
fresh identity. Markers are volatile and never serialized or reconstructed from
revisions. Restart initially full-compares. Overflow/index/bound inconsistencies
must reset/fallback rather than wrap or grant evidence. No new asynchronous work,
barrier token, result receipt, format, quorum release or rollback path is added.
Existing snapshot binding-first short-circuit and pin/load/app validation/persist/
reconcile/fencing order stays unchanged. No checkpoint frequency change.

## Acceptance and observation

Before promotion: independent full-Eq differential histories across empty/nonempty
commands, arbitrary IDs, bootstrap/header/config/snapshot changes, append/metadata,
compaction survivors, suffix replacement, rollback and restore. Same-revision raw
forgeries remain false. Fresh core/provider with identical metadata, alternating
owners, stale identities and session replacement force full comparison. Native
pending/partial barriers/promotion errors/fences and memory gate/error ordering
retain exact results. Lost checkpoint receipt, crash/reopen, mismatched binding,
configuration histories and every original completion remain covered.

Full Eq stays the debug/test oracle for optimized results; release correctness
comes from private ownership and complete transition instrumentation. Record full
versus incremental comparisons and actual compared entry counts in tests/optional
observation, distinguishing them from logical equal-state items. Existing old
clone/exact comparison scopes remain separately named; add a separately named
checkpoint verification phase if its scope changes, update provider inventory,
conformance ledger and observer controls honestly. No inclusive intervals are CPU
or additive attribution. Disabled observation performs only required correctness
work. Bounds include retained marker resources, not only request cardinality.

Run relevant all/core library/provider/log/snapshot/accepted/raft controls and real
TCP/QUIC retry/replacement/recovery, fmt/four strict profiles/standalone strict.
Freeze complete source and new modules, preserve every failure and focused fix.
Publish each meaningful checked slice through enabled hook. Build fresh asserted
published artifacts before three rotating Linux/M2 Max singleton/bulk pairs.
Then sustained20M active-maintenance/full seven-cell acceptance, latency/refusals,
RSS/debt/headroom/fairness and durable/P8 measurements remain full P9 requirements.
No small marker milestone redefines completion. Windows and P10 excluded.

## Cargo profile reference-oracle gate (approved before implementing gate)

The framework release profile deliberately keeps debug_assertions=true. Therefore
cfg(debug_assertions) cannot select the full comparison reference: it would restore
all retained-entry scans in the measured fast path. A minimal dependency-free
root build.rs emits rustc-check-cfg and voteboat_checkpoint_debug_oracle when Cargo
PROFILE is anything except release (missing/unknown conservatively enables it).
The comparator performs unconditional reference assert_eq under cfg(test) or this
actual dev/debug profile marker. Asserted release keeps ordinary Rust assertions,
overflow checks and exact ownership-based algorithm semantics, and omits only the
redundant full reference scan. Raw matches_state stays full Eq in every profile.
This build script is a new exact build input and must be included in inventories,
archives/transfers/artifact provenance. Qualification records actual build-script
outputs and verbose compiler cfg for debug and release; no RUSTFLAGS/wrapper or
benchmark assertion-profile change is authorized by this gate.


Native/harness refinement (before implementation): Native keeps at most one
record per durable group. Its accepted Overlay::Plain can arise only from exact
append/metadata or validated Compact plans; combined pending Plain overlays
retain that provenance. Existing Overlay::Full remains Full through subsequent
safe updates, so promotion resets proof even when its final headers happen to
match an older head. At successful durable promotion Plain clips/preserves the
verified interval against the actual installed state; it never extends it.
Fencing clears retained records. Reclamation's existing full decode equality
certifies no logical change and therefore transfers no new verified progress.

Observer adds only store_checkpoint_state_protected_inclusive_nested (29 phases,
15 methods per scope), preserving426's old raw exact and clone phases. This new
phase includes any private full fallback/oracle work without inventing a raw
provider call. Items remain logical entries in successful equal states, never
actual compared/skipped counts. Private Native/common-owner tests record actual
algorithm counts and distinguish them from test/debug full-Eq oracle work.
state_to_pin starts after this entire protected verification call. Debug-profile
oracle elapsed cannot stand in for asserted-release performance. No additional
public metrics seam is introduced solely for benchmark bookkeeping.
