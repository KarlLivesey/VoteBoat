# P9 data structures and operation order audit

Read-only inspection, 13 October 2026. Source is the current touched-group
candidate, not a measured performance profile. No benchmark, tests or production
changes were performed for this audit. Line numbers identify this candidate and
will move when implementation changes.

## Ordinary command dependency path

Let L be the retained entries/payload bytes in the hot group, C its retained
configuration records, V its current/rollback replica population, D its retained
application dedup outcomes, B the incoming batch, and P the recipient count.
These are separate dimensions; retained history is not useful work per command.

1. Runtime admission computes authoritative reservations before ownership
   transfer (`src/runtime/effects.rs:442-470`). The core's
   `event_effect_reservation` calls `rollback_replicas` and `reserve_effects`
   (`src/raft.rs:629-758`). Both traverse retained entries, even for ordinary
   commands. `rollback_replicas` tracks every possible configuration reached by
   truncating uncommitted history; `reserve_effects` charges entry metadata and
   actual retained payload capacities plus snapshot membership and fanout slack.
   Related reservation APIs can be called again for execution/completions; exact
   call frequency is assembly-dependent, not inferred from source alone.
2. Core checks leader/voter/transfer, payload limit and checked index, creates
   one command entry, and emits a one-entry suffix (`src/raft.rs:1010-1049`).
3. `persist_update` validates/stages one owned next GroupLog, then reconstructs
   next membership (`src/raft.rs:1257-1288`). `stage_update` deep-clones the whole
   touched GroupLog (`src/log.rs:374-387`). `apply_update` checks revision,
   historical ballot origin, boundary/generation rules, commit monotonicity,
   term compatibility and configuration history (`src/log.rs:441-491`).
   `apply_suffix` validates the incoming entries and clones them into the staged
   vector (`src/log.rs:578-635`). Thus one appended entry still costs O(L) copying.
4. While a dependency exists, Core::step returns Busy (`src/raft.rs:890-896`).
   Pending retains an exact update clone plus staged state and membership;
   outgoing Persist owns another update (`src/raft.rs:1280-1288`). These objects
   support exact downstream-effect and ticket checks, not independent authority.
5. Serialized owner transfers bounded Persist leases to a selected worker
   (`src/runtime/replica.rs:881-929`, `src/worker.rs:256-323`). The selected native
   worker batches already-ready independent groups, admits at most one live unit
   per group, and shares their barrier (`src/native/worker.rs:186-195,296-405`).
   It checks the exact original Core effect before the handoff and validates the
   selected provider's returned bindings/groups/tickets.
6. Native store encodes the original mutation, stages the touched group again,
   appends the frame, installs accepted state, and returns written tickets
   (`src/native/log_store.rs:269-348`). It must independently admit its public
   storage requests. On sync and publication success it promotes all dirty groups
   (`src/native/log_store.rs:350-379`). This still deep-clones each dirty GroupLog
   into durable state. Unrequested dirty groups become physically durable, but
   only requested tickets escape as DurableLog evidence.
7. Exact durable completion moves Core's pending state into durable state
   (`src/raft.rs:1111-1128`). Local leader evidence is updated and replication
   broadcast happens only after local append durability (`src/raft.rs:1209-1213`).
   Current Append construction copies a bounded range per recipient, including
   heartbeat retries of an outstanding original request (`src/raft.rs:1411-1514`).
   Responses retain full term/configuration/store/request identity checks.
8. Durable voter frontier permits an additional commit-only persistence
   transition (`src/raft.rs:1518-1529`); that currently repeats the GroupLog clone,
   membership reconstruction, selected-store validation and barrier process even
   though entry contents are unchanged. Do not confuse this second boundary with
   an unnecessary durability guarantee: simplifying it requires an explicit
   recovery/durable-evidence argument, not just deleting the write.
9. The completed commit transition materializes owned Committed entries
   (`src/raft.rs:1152-1160`). Application router reserves receipts, calls the app,
   and verifies exact receipts/applied position before exposing outcomes
   (`src/runtime/applications.rs:389-455`). Counter copies all retained dedup
   history to make the complete batch atomic (`src/application.rs:344-408`).
   Consequently the real application path has O(D) copying in addition to O(L).

## Known information repeatedly recomputed

| Information | Current work | Reuse condition and invalidation |
|---|---|---|
| Retained payload/metadata charge | Whole-entry walk in reserve_effects | Maintain checked aggregate from original owned allocations; append adds, truncate/snapshot subtracts, restore rebuilds. Capacity changes, sharing and transient buffers must remain charged. |
| Whether the log contains configuration entries | Whole-entry `any` in apply_update | Maintain config positions/count as part of the validated owner state. A public GroupLog can be arbitrary: do not trust a claimed caller count. |
| Accepted membership | apply_update replay when configurations/base exist, plus unconditional Core replay | Reuse only a membership proven for the exact retained log version. Pure command/noop append does not alter electorate; metadata-only changes preserve accepted config content. New config, suffix replacement/truncation, snapshot base/import and restore need update/rebuild. |
| Maximum rollback replica envelope | Walk accepted history in rollback_replicas | Store prefix/config-position summaries for all reachable uncommitted rollback points, not only the latest electorate. Commit advance changes rollback range; replacement/snapshot/recovery invalidates affected summaries. |
| Existing entry contents | GroupLog deep clones in Core staging, selected store staging and publication | Retain immutable old contents through accepted/durable versions; mutate only a new tail or materialize on public-owned export. Generation/identity still fences stale suffixes. |
| App dedup outcomes | Counter clones BTreeMap before each apply | Borrow committed map and stage only first-seen operations in this batch; repeated IDs consult staged outcomes first, then retained map. Only install after all entries validate. |
| Exact provider outcomes | Core-effect equality, ticket/receipt matching | Keep these boundary validations. Replace repeated searching with exact keyed matching if measured, not with unchecked provider assertions. |

Accepted membership is not durable voter evidence (`src/raft.rs:606-613`). A cache
must preserve that distinction. A key of configuration ID alone is insufficient:
suffix versions, Joint/Final positions, ballot-origin history, snapshot base and
operation-history content matter even when a resulting electorate looks similar.
Recovery rebuilds/validates caches from the selected authoritative history.

Membership reconstruction currently validates contiguous entry indexes even
for non-config commands (`src/membership.rs:173-209`), reconstructs/validates
bootstrap or checkpoint membership, then checks each configuration's exact
expected predecessor, operation uniqueness and predecessor commitment
(`src/membership.rs:230-279`). Reusing already-validated state is justified only
inside the owner whose mutation path preserved these properties. Public
`apply_batch`, recovery codecs and host-replaceable provider requests still need
structural validation of their inputs. Incremental membership acceptance already
has a useful narrow primitive: `Membership::preview_next` (`src/membership.rs:220`).

## Provably redundant versus correctness-dependent work

Provably redundant within the candidate's existing contracts:

- Copying unrelated groups for a single-group update: removed by touched staging.
- Deep-copying unchanged Counter dedup rows solely to roll back a malformed later
  entry: batch-local staging can give the same atomicity and identical outcomes.
- Recounting unchanged retained allocations inside one owner after no log mutation,
  provided a checked cached aggregate is updated on every mutation/export path.
- Replaying the same accepted config history inside Core for an append containing
  only ordinary commands/noops, provided predecessor state is already validated
  and the append's index/term constraints are independently checked.
- Copying entry payloads to stage only commit/term/vote metadata, provided Pending
  represents that delta and no accessor assumes a fully materialized next log.

Not redundant merely because Core previously checked similar facts:

- Native/host LogStore admission at its public boundary. It is callable outside
  Core, must reject stale revisions, invalid ballots/snapshots and oversized data,
  and cannot trust an untyped `validated = true` from a caller.
- Store identity/session/generation checks and exact durable-ticket correlation.
- Bounded input/output reservations, transient capacity and control reserves.
- Snapshot dependencies, configuration predecessor checks, historical ballot
  origin and committed-prefix protection during replacement/import.
- App malformed-batch checks, dedup conflicts/overflow/capacity outcomes and
  post-call provider receipt checks. Preflight does not prove a generic provider
  respected its contract.
- Per-recipient request context and authenticated identity correlation. Current
  heartbeat reuse is intentional to avoid starving long-RTT acknowledgements
  (`src/raft.rs:1412-1416`). Replacing contexts each tick is a known bad direction.

## Data structure choices

**Keep contiguous log indexes, not a per-entry search tree.** `GroupLog::entry_at`
uses base-index arithmetic and Vec indexing (`src/log.rs:156-172`), giving O(1)
lookup. Ordering is known and suffix/prefix operations dominate. A per-entry
BTreeMap adds nodes, pointer chasing and O(log L) lookup without solving owned
payload duplication. Group maps remain a sensible keyed ownership structure.

**First use precise deltas for metadata-only work.** An internal PendingNext enum
could retain checked metadata (revision/hard state/origin/commit) while reusing
current entries; append/snapshot transitions remain materialized initially. All
admitted-ticket comparisons must read the same proposed index/generation/term;
completion installs the delta only after exact durable evidence. This is a Core
representation change, not a new storage token. Native accepted/durable versions
need their own analogous representation to remove their metadata-only clones.
Avoid an unsafe or partial in-place mutate-and-undo path across fallible checks.

**For general append versions, immutable bounded chunks/pages are a stronger fit
than a general persistent tree.** Share sealed chunks, copy only the changed tail,
carry base index/offset and summaries, and retain original versions until pending
completions release them. Truncation drops later chunks; matching checkpoint
advance drops prefix chunks; nonmatching snapshot discards suffix. This bounds
payload copying by changed-tail size, while index-to-page lookup remains simple.
However a naive Vec of chunk handles still copies O(number of chunks) on every
version; an append-delta/version chain needs a bounded flattening policy, or a
paged directory needs measured costs. Arc per entry may replace payload copies
with expensive refcounts/allocations; prefer chunk granularity if measurements
support sharing. Charge outstanding shared chunks and abandoned old generations;
reference counting does not make retained memory free.

**Ring buffers suit bounded in-place windows only when old versions are no longer
needed.** Pending accepted/durable logs, replacement rollback and async message
lifetimes need original contents. Reusing cells without pins/generations corrupts
those views. A ring also spans wraparound slices and still needs separately
bounded variable payloads. Do not start there merely because indexes are ordered.

**Application state needs different structures from logs.** Counter's BTreeMap
operation ID ledger needs exact lookup, deterministic export and retained retry
outcomes. A tiny batch-local overlay on that existing map avoids O(D) copying
without a new dependency. BucketCounter additionally stages changed buckets/keys;
its proposal admission currently clones the entire app and replays pending work
(`src/bucket_counter.rs:418+`), so optimizing only apply_batch is incomplete.
Broader distributed transaction state has separate prepared-lock invariants and
cannot inherit Counter's simpler scalar plan unchanged.

## Public API materialization limit

Current GroupLog.entries and LogUpdate.suffix.entries are owned Vec<LogEntry>;
LogEntry commands own Vec<u8> (`src/log.rs:30-48,92,177-200`). LogStore::state returns
an owned GroupLog and fetch_range returns an owned Vec (`src/log.rs:285-308`).
Core Effect::Committed and outbound Append similarly own entries. An internal
shared/chunked log can save private versioning copies, but exports still require
owned materialization under these APIs. A future shared immutable range/lease
contract must preserve bounded ownership, cancellation and asynchronous retention,
codec/provider replacements, exact request identity and memory accounting. Do not
hide borrowed lifetimes behind a background worker. Public API changes are allowed
in this unused project, but need one complete usable assembly and conformance,
not a simultaneous generic rewrite of every seam.

## Recommended next complete slices

1. **Counter batch-local transactional overlay**, independent of consensus:
   stage scalar value/applied index, new dedup IDs and receipts, consult retained
   map plus overlay in original order, reserve capacity for unique new IDs, and
   install only after full validation. Preserve repeated-ID conflicts, overflow,
   exact receipt indexes and checkpoint schema. Tests must include malformed
   last entry, repeated/conflicting IDs within a batch, exhausted dedup capacity
   and restart/retry equality. Measure fixed incoming work against growing D.
   This addresses real-service overhead, not the empty-state-machine framework
   comparison.
2. **Core metadata summaries and validated membership reuse**, before replacing
   log storage: cached byte/config-position summaries reconstructed on recovery,
   updated on suffix/snapshot boundaries, and independently compared with old
   computation in tests; reuse accepted membership for proven non-config changes.
   Preserve rollback fanout reservations and every config/ballot/security gate.
   Measure fixed command count against growing L and C and include complex
   uncommitted membership rollback histories. Do not expose caches as new authority.
3. **Metadata-only Pending delta**, then **ready proposal batching**: avoid copying
   log contents for commit/vote/term-only transitions and amortize ordinary append
   work across already-ready commands. Core currently one pending transition and
   worker one live unit/group, so batching belongs before that dependency is
   emitted, not in a loop calling step while Busy. Singleton dispatch stays
   immediate, per-operation IDs/receipts remain original, and hot/cold fairness
   and aggregate byte/entry/receipt budgets remain bounded.
4. **Chunked private versions and persistence/network overlap**, only after these
   smaller slices quantify remaining costs. Sending replication before local sync
   may overlap I/O but requires new explicit pending/term/suffix handling; local
   leader durability and durable voter quorum still gate success. This is not a
   safe performance patch consisting of moving broadcast() a few lines earlier.

Do not choose all four simultaneously. The minimal next production slice is the
Counter overlay; the minimal framework-specific follow-up is cached owner
summaries and conditional membership reuse. Neither requires a dependency,
cryptography removal, weakened limits, a different quorum or disk-format change.
