# Like-for-like source audit

Read-only inspection, 2026-10-13. No implementation, qualification or timing claim.
Main slice421 remains frozen. This note supplements root-owned COMPARISON.md.
Pinned upstream: e88bb40707f2e511d8ecc44d31edcb1d814a11a8, read from Cargo's
local checkout; upstream source was not edited. Source references below are
repository-relative for VoteBoat, and relative to that pinned checkout for OpenRaft.

## Genuinely common workload and environment

- One shared driver supplies both engines: `src/workload.rs:102–125` creates the
  same globally unique original sequence, clients, batch1/4 and closed-loop wait.
  Warmup is singleton on both; rounding covers equal work per client. References
  here and below without upstream prefix are under `benchmarks/matched/`.
- `src/main.rs:131–154` creates separate client1/server16 Tokio runtimes, 3MiB
  stacks, same timeout and cleanup. Three majority voters, empty application bytes,
  no disk and in-process network are common. Both complete every requested original
  after leader application, and verify all three exact applied frontiers outside
  the measured interval (`src/workload.rs:127–152`). Setup and shutdown are excluded.
- `src/openraft_engine.rs:20–37` preserves pinned upstream primary election200/2000,
  purge1024, network payload1024, snapshot5000/keep1000 and API channel65536.
  OpenRaft defaults supply heartbeat50 and API linger0. VoteBoat actor uses heartbeat50
  and no added batching timer (`src/voteboat_engine/actor.rs:64–96,207–226`).
- Release profile declarations are debug2/thin-LTO/codegen1 on both standalone
  manifests. Actual artifacts require compiler JSON guards for opt3/assertions/
  overflow checks; source manifests alone do not prove flags. Existing guarded
  build script explicitly sets those (`validation/performance/slice415-harness/
  build-linux.sh:5–19`). Cargo locks/features and actual compiler paths still belong
  in each frozen artifact record.

## Two source-derived corrections

**A stream does not eliminate OpenRaft's per-original oneshots.** The local adapter
`src/openraft_engine.rs:85–101` awaits singleton client_write or drains every result
from client_write_many. Pinned `openraft/src/raft/api/app.rs:119–145` creates a
ProgressResponder::complete_only per payload, collects its receiver and awaits each
in submission order; `raft/responder/impls/progress_responder.rs:82–90` creates one
C::oneshot. VoteBoat `src/voteboat_engine.rs:209–230` also uses one per original.
Replacing VoteBoat with one channel per request would be a new host design, not a
correction of an established upstream advantage. Neither side may count one batch
receipt as four unchecked original outcomes.

**Current API batch numbers have different units.** Pinned
`openraft/src/core/merged_raft_msg_receiver.rs:29–32,201–225` limits merged ClientWrite
MESSAGES and extends every message's full payload. VoteBoat
`src/voteboat_engine/actor.rs:207–218` and Core limits cap ORIGINAL entries.
Consequently API capacity4096 can combine up to16384 originals at request batch4;
Common64 can combine up to256, while VoteBoat caps4096/64 originals respectively.
These are source-derived ceilings, not measured selected widths. Pinned
`openraft/src/engine/command_scheduler.rs:45–85` uses max_append_entries to limit
merging another append command; it does not split an oversized first command.
Therefore a report called api_entry_batch_capacity=4096 does not prove equal
original-count envelopes. The actual network replication cap1024/64 is an entry cap
and is genuinely common. Preserve untouched upstream primary; label its API units.
A separately labeled equal-original profile can set OpenRaft API request capacity
floor(original_cap/request_batch), preserving storage/replication caps and linger0.
For these fixed batch sizes it yields4096/1 or1024/4 requests for original4096,
and64/1 or16/4 for original64. This profile must not silently replace or lower the
untouched-upstream minimum used by the user's performance requirement.

## Intrinsic differences to retain and disclose

- Snapshot5000/keep1000 is a comparable maintenance intent, not identical snapshot
  semantics. Pinned minimal `src/store.rs:181–217` serializes the latest applied
  state; OpenRaft retains older logs already included in it (Config docs287–291).
  VoteBoat's snapshot is also the concrete log base, so the no-op host checkpoints
  historical applied−1000 and preserves the1000-entry suffix
  (`src/voteboat_engine/actor.rs:384–419`). Its pin/load/full-state/compact proofs
  remain real work. Matching the current-image model requires a distinct production
  snapshot/log-retention design; changing the benchmark image frontier alone is
  invalid. Retain-all is a separate sensitivity mode, not the primary result.
- OpenRaft splits async Raft, replication and state-machine work, and its minimized
  store uses RwLock/BTreeMap + immediate IOFlushed callback
  (pinned minimal `src/store.rs:96–115,270–288,308–327`). VoteBoat actor executes exact
  LogStore append/barrier and follow-on effects synchronously in its owner
  (`actor.rs:247–281`; `src/store.rs:42–86`). Its one-outstanding volatile transition,
  validation and exact tickets are mandatory provider contracts, not adapter work
  that can simply be removed. Snapshot tasks/pipelines and scheduling are engine/host
  differences; label them rather than claiming identical internal concurrency.
- VoteBoat protocol logs OperationId/group/store identities and validates application
  ordering; minimal OpenRaft ClientRequest/Response are empty
  (pinned minimal `src/store.rs:36–46`; `src/application.rs:23–39`). No-op apps on both
  have no business/dedup state. Shared driver verifies counts on both and exact IDs
  on VoteBoat; do not imply upstream empties prove content/ID equality.
- Elections are not exactly common: OpenRaft timeout range200..2000 has its own engine
  tick/jitter; VoteBoat actor campaigns after2s contact gap on50ms ticks
  (`actor.rs:137–148`). Initialization also differs (OpenRaft membership0/blank1;
  VoteBoat bootstrap plus noop1), while both verify original count+1 frontier.
  Stable-leader campaigns do not erase their scheduling differences.
- Both channel capacities65536 are count bounds, but VoteBoat has separate client and
  peer channels plus peer-priority collection; OpenRaft combines consecutive API
  messages until a boundary and has separate notification/state-machine queues.
  Peer-priority is actual host work/fairness, not a diagnostic cheat. Do not bypass
  controls to match a private upstream schedule.
- Pinned bench-minimal enables runtime-stats by default (Cargo.toml features), also
  feature-unified into the shared OpenRaft dependency. VoteBoat always retains its
  own atomic diagnostics, while optional elapsed attribution is off for comparisons.
  Neither manifest nor report implies identical instrumentation overhead.

## Common driver versus untouched upstream binary

Pinned minimal `src/bin/bench.rs:307–414` uses the same closed-loop batching and
start barrier, but Vec join handles rather than JoinSet; its request payload Vec is
built before latency start in batch mode (379–385). The shared OpenRaft adapter
constructs its empty payload entries after shared latency start (workload115–117,
openraft_engine91). The shared driver also constructs original IDs before latency
start; that allocation remains inside global elapsed throughput. Common driver
latencies are directly comparable between its adapters; they are not exactly the
untouched binary's latency boundaries. All-results follower verification is outside
global elapsed on the shared driver and is absent in the untouched throughput loop.
Publish shared head-to-head and untouched oracle separately, with actual resolved
features/flags, rather than presenting either as identical engine-only time.

## Smallest safe follow-on schema

First correct reporting units and independent comparison policy. Root's approved
next primary direction preserves untouched upstream capacity4096 REQUESTS and brings
VoteBoat's opt-in aggregate ORIGINAL ceiling to checked4096*settings.batch (4096 for
batch1,16384 for batch4). This requires a bounded public Core ceiling extension,
currently4096 in production `src/raft/batch_limits.rs:15–27`; do not lower upstream
to establish a primary win. Existing Core/native defaults64, fixed ordinary receive/
replication maximum1024, repair64 and startup ready selection1..64 stay unchanged.
Configure before the first step, reapply after verified recovery, and retain the
existing startup lock. No live resizing or safety state is introduced.

Data/API: `BatchLimits::MAX_PROPOSAL_ENTRIES=16384`, same existing BatchLimits shape.
Harness primary derives its selected bound with checked multiplication and validates
it before actor launch; a Common profile must label units explicitly rather than
silently assert64-original parity. Capacity reservation/input inspection continues
to use each actual Core setting; no raw64 assumption may truncate expanded outcomes.

**Raising count alone is insufficient.** Production `src/log.rs:228–239` defaults
max_batch_bytes to1MiB and max_entries_per_group to16384. Proposal staging charges
full allocated Proposal capacity plus staged LogEntry count and each payload capacity
(`src/raft/proposals.rs:107–149`). The harness's default-retention retained limit is
already65536 (`matched/src/main.rs:123–126`), but its currently default1MiB owned
staging limit can independently refuse a16384-empty-command batch. Before starting
actors, compute the exact empty-payload staging minimum with checked size_of and
selected capacity, include any complete update/provider envelope, and explicitly
select a bounded harness-only LogLimits budget that fits it. Record that choice.
Do not change native defaults or waive raw capacity checks. Nonempty/slack payloads
still obey command, total input, staging, output and retained-history limits. The
actor's4GiB effect reservation guard and clients*4+1 pending-original cap remain;
proof must show both fit or decline rather than silently raising every bound.

Transitions/ownership: a successful larger batch still owns separate originals and
one exact Persist dependency. Nothing completes at Written or accepted state.
Ordinary replication sends bounded1024-entry ranges, partial ACKs advance only their
validated prefix, and application returns every original independently. Log revision,
generation, term/config, fencing, snapshots/retention and replay remain unchanged.
A malformed later command or insufficient staging/retained capacity rejects atomically
before pending state. Changing the startup ceiling never changes on-disk format.

Acceptance: existing default64 behavior; select4096 and16384 before first step;
reject0/16385 and late configuration, count/allocation/slack/byte/retained-budget
boundaries; actual queued4096 batch4 requests select up to16384 distinct originals
without exceeding declared memory/output/receipt bounds; every ID/index and histogram
weight equals actual originals. ModelIO proposal/commit publication cuts and recovery
with independent explicit reconfiguration, delayed/duplicate/lost partial1024 ACKs,
finite owed commit notification, follower snapshot installation, protected controls
and maintenances all remain present. Run both feature profiles and exact artifact
readback/guards, then both-host comparison with unchanged untouched upstream. No new
speed claim precedes qualification; the public capacity extension is not itself a
pipeline or an expectation that every selected batch fills.

A separate COMMON-ORIGINAL diagnostic may instead set OpenRaft API request capacity
floor(target_originals/request_batch) (4096/1 or1024/4;64/1 or16/4), preserving append/
replication/linger settings and validating fixed request shapes. Its soft first-append
bound still needs explicit accounting. This tuned upstream result is a sensitivity
comparison, never an automatic substitute for the untouched minimum/performance floor.

If measured host admission/results remain material, the smallest data-structure
improvement is the comparison host's pending association, not removing channels:
`actor.rs:38,229–245,323–332` inserts/removes each result in BTreeMap<OperationId,Reply>.
Pinned OpenRaft instead assigns monotonically increasing log indices to responders
(`openraft/src/core/raft_core.rs:668–687`) and drains an indexed VecDeque prefix
(`core/client_responder_queue.rs:18–20,43–75`). VoteBoat can keep one response per
original and exact ID checks with an ordered pending queue of
(index,term,operation,reply), bound by the existing clients*4+1 original budget.
Install associations only for a successfully accepted proposal and its exact returned
positions; drain only after application emits that same index/operation. Existing
Noop receipt must include the index or delivery must zip exact applied command entries;
never predict acknowledgement from commit/Written. A bounded outstanding-ID set is
still necessary to preserve duplicate admission rejection: replacing the BTreeMap
with a FIFO without a duplicate gate changes the existing contract. A HashSet makes
expected O(1) gate/queue work, while retaining IDs and exact per-original receivers;
its actual memory/hash costs must be counted and measured, not called free.

Ownership/failure: queued requests own unaccepted replies; accepted queue owns every
original until its exact applied result. Core failure/term change/shutdown closes
remaining waits and fails the campaign, preserving current outcome behavior. Partial
committed prefixes drain only matching prefixes; snapshots/follower applications do
not fabricate leader results. Cancelled receivers keep existing error behavior.
Full request shape/duplicate/budget validation must precede destructive queue install,
including late malformed candidates; no original or maintenance work is dropped.
Meaningful controls: interleaved IDs,4-entry request crossing Core batch boundary,
partial prefix, later malformed/duplicate candidate, cancelled waiter, follower drain,
leadership/worker failure, original-count budget, checkpoint+snapshot installation
and exact every-original receipt/count on1/4 requests. The production ClientRouter's
richer cancellation/admission contracts are separate: this host change must not be
advertised as a general service rewrite. No implementation is authorized by this note.
