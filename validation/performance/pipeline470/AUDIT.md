# Before-design audit: bounded same-group persistence pipeline

Read-only feasibility note, not an implementation schema, authorization to edit,
or performance prediction. Root469 rollback/checks are in progress; the durable
production baseline under consideration is455, not rejected468 or466. Observed
HEAD: `6e91e6ecefda652da42c6130354263484378f6f6`. Source line references below identify the actual files read.
No source edit, build, test, workload or new profile was run for this audit.

## Observed source and contract constraints

| Layer | Actual source | Constraint |
|---|---|---|
| Core | `src/raft.rs:299`, `:349`, `:1060`, `:1083`, `:1108`, `:1279`; `src/raft/proposals.rs:61` | One `Option<Pending>`; exact update then exact ticket; planning starts at durable revision; any pending dependency prevents another ordinary step. Completion installs one plan. `state()` stays durable; pending membership is separately exposed at `raft.rs:639`. |
| Scheduling | `src/runtime.rs:327`, `:336`, `:769`, `:864` | One visit/group; step and finish refuse a pending dependency. Input credits remain tied to that visit. |
| Effect ownership | `src/runtime/effects.rs:99`, `:154`, `:474`, `:1017`, `:1113` | One active/group, retained output reservation and peer source; one persistence unit/group per submission; exact Written then Durable event scope. Visit remains retained until outputs/dependencies finish. |
| Worker | `src/worker.rs:154`, `:225`; `src/native/worker.rs:73`, `:171`, `:257`, `:296`, `:370` | Public worker contract explicitly permits at most one accepted unit/group. Native group set rejects a second. Ready-window gathering precedes appends; ticket lookup by group assumes uniqueness. Already queued independent groups can share a barrier today. |
| Native store | `src/native/log_store.rs:303`, `:386`; `accepted.rs:103` | Separate append calls can advance one group's accepted revisions before sync. Each append batch still permits one atomic unit/group. Barrier verifies requested tickets, syncs WAL, publishes manifest, promotes accepted state, and returns only requested evidence. Physical coverage of other accepted bytes grants no implicit Core completion. |
| Publication | `src/native/log_store/file.rs:138`, `:156`, `:159`, `:162` | Manifest write/file-sync precedes rename and directory-sync. Group commit can amortize this sequence; cannot remove its crash dependencies. |
| Application | `src/application.rs:30`, `:39`, `:48`; `src/runtime/clients/ready.rs:22`, `:33`, `:49`, `:71` | Context validation plus all unapplied/queued/in-flight originals is mandatory. `supports_ready_batch` promises unchanged-context admission only; it is not permission for state-dependent admission to skip earlier commands. |

Existing storage proof: `src/native/log_store/accepted/tests/configured.rs:44`
(`configured_multiple_accepted_deltas_subset_publication_and_full_fallback_recover`)
stages first and second same-group updates at lines56/68, then publishes through
another dependency at73 while preserving explicit ticket identities; suffix
replacement invalidates old tickets at93/94. This supports using the current WAL
format and accepted-overlay machinery, not claiming the new pipeline is tested.

## Proposed smallest complete usable vertical slice

A startup opt-in bounded **ordinary Command append window** across Core, shard,
effect owner and selected worker, retaining serialized mutation by one owner.
Keep existing public singleton/ordinary stepping behavior as the default.

1. Keep durable canonical state, local durable quorum progress, commitment and
   application separate from staged/accepted state. Plan a bounded sequence of
   append-only same-term revisions against accepted metadata plus a staged tail;
   retain immutable exact original updates and predecessor revision/generation.
   Current `OwnedNextLog` append plans can install sequentially without a new
   shared-history container or cloning the complete retained log per append.
2. Assign each transition an exact persistence identity. Correlate by group plus
   transition/visit sequence and full store/session/ticket, never group alone.
   Decouple scheduling permission to stage the next eligible append from retained
   continuation/input/output reservations for earlier units. Maintain one mutable
   owner, and keep source-ingress leases through their actual retirement.
3. Permit bounded multiple outstanding units/group in the worker contract and
   implementation. Preserve FIFO per group and sequential atomic append calls,
   then barrier an explicit bounded union. Retain precise per-unit admission,
   Written and terminal status. Rejecting a later append is not evidence that an
   earlier accepted append disappeared. Host providers need the same documented
   contract/capability and downstream conformance, not native-only bypasses.
4. Stop opening/extending the append window when a control transition is next;
   drain before term/vote, membership, snapshot, transfer, reads, suffix rewrite
   or compaction. Ordinary control reserve remains available; already submitted
   I/O cannot be canceled by a later control. Preserve exact queue order and
   visit/count/byte limits. Do not revive full-tip fusion or ready-reply joining.
5. Keep aggregate originals and physical retained bytes bounded BEFORE staging:
   queued command buffers, every independent pending plan/effect copy, worker
   request/event metadata, accepted tail, all completion outputs and per-original
   receipts. Per-unit ceilings alone do not bound the window. No credit release
   on Written or caller cancellation. Application checks must include every
   retained predecessor original exactly as required by the current contract.
   Existing ready-batch opt-in can authorize only a genuinely unchanged Core/app
   context; otherwise serialize or define a separately reviewed guarantee.

### Barrier closure and progress

A barrier already syncing A has a closed dependency set. Staging B while A is
in flight may queue B/C for the **next** barrier after A returns; B never joins
an already syncing A. This is a possible steady-state amortization mechanism.
The existing worker's ready-window scan can collect those already queued B/C,
provided the new Core/owner/worker contracts permit them to exist concurrently.
An alternative preclosed multi-unit submission must be explicitly bounded too.
No compulsory wait, timer/default change or unbounded queue is implied.
A depth allowing only A plus one queued successor does not itself guarantee
multiple units at the next barrier. Measure actual units/originals per barrier;
do not infer useful grouping solely from pipeline depth or queued admissions.

Do not advance canonical state past a completion hole. Completion of later units
may be recorded but releases effects only through the exact contiguous matching
prefix. `raft.rs:1188` after-durable callbacks can create further effects, and
`raft.rs:1526` maybe_commit creates a new commit-only revision: these must not
plan against an obsolete durable head while later accepted append revisions
exist. A first conservative slice may drain the window before these transitions,
while preserving committed-first output and exact old request contexts. New
entries alone do not authorize success: durable quorum/current-term commitment,
local durable state and ordered application remain required.

## Hard checks before a performance claim

- Two or more genuine same-group outstanding append units, exact revision/byte
  conservation, explicit Written/Durable distinction and actual shared barrier;
  independent groups still progress and protected control budget remains usable.
- Out-of-order/duplicate/foreign tickets, completion holes, old store/worker or
  suffix generations: no false local prefix, quorum vote, commit or effect.
- Failure between any append and sync, during WAL sync, manifest write/sync,
  rename or directory sync; short writes and later safe rejection. Fence uncertain
  work and preserve each original's unknown/known-rejected distinction.
- Higher term, configuration/learner changes, snapshot/compaction/read/transfer
  boundaries and after-durable commit chaining; preserve original input order,
  no double vote, no obsolete revision and no early dependent response.
- Context-sensitive/default-off application, invalid middle proposal, full128 IDs,
  cancellation before/after submission, exact positions and every original result.
  Repeated overload/stop/drain must retain and then release all bounded owners.
- Native modeled crash and actual file recovery plus downstream public worker/store
  substitution and TCP/QUIC service retry/recovery/join controls. Recovered suffix
  bytes are not proof of commitment/application. No WAL-format change appears
  necessary; provider completion semantics and public inventory do change.

Observed native barrier83–88%/publication≈55% are overlapping elapsed scopes,
not additive CPU percentages or proof that this design wins. The first usable
slice must span Core through a real native owner/worker/service and demonstrate
actual barrier amortization; isolated types or only a simulator are insufficient.
All existing failed cases and rejected experiments remain outside this proof.

## Design read receipt

Read before this audit: architecture/safety/durability/lifecycle/composition
chapters and the contract sheet below; Markdown originals were not modified.

- `VoteBoat-Design-Pack/README.md` SHA256 `02e36594f2ad50cd896c70122368f46c24d2da0a62a7727f4f0df90aedeed28d`.
- `VoteBoat-Design-Pack/docs/02-system-architecture.md` SHA256 `44514cc3209d319823cbdd22c4e0f90b9d42f59fa75124cf64952fc7456ce8e6`.
- `VoteBoat-Design-Pack/docs/03-consensus-and-quorum-trees.md` SHA256 `7a89a49c34308da42c6df270d5eeca9cef32aaee0db8246bb042c50cabc1d806`.
- `VoteBoat-Design-Pack/docs/04-wal-and-durability.md` SHA256 `35340072ed656d36d9332f18682244b7a1f0db705e44ec2c3b20267efb7d27a8`.
- `VoteBoat-Design-Pack/docs/07-membership-and-lifecycle.md` SHA256 `1add3138a92db3e12016a3940b2660434ecfda46afb60f995400c96f39cf6f7e`.
- `VoteBoat-Design-Pack/docs/11-testing-and-safety-case.md` SHA256 `19914fbfd927b6cb3b49d00532aea9a6bb1daf057fc3f53de60a450e4853e28c`.
- `VoteBoat-Design-Pack/docs/17-native-first-composition.md` SHA256 `4e66e749565ab58c69ae45813185c0845dea97e2aeebf404552fdd855328fc3c`.
- `VoteBoat-Design-Pack/specs/component-contracts.md` SHA256 `a522f73081d50591d4881c6cbcb4bf137ca648becdd228c977949028387beccb`.
