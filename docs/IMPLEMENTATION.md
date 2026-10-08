# Implementation record

## Scope and roadmap

This repository implements the proposed VoteBoat baseline in bounded vertical
slices. The local architecture pack, version 0.2 of 7 October 2026, remains the
design input and is intentionally excluded from Git. Nothing in this status
record claims that unimplemented phases already work.

| Phase | Intended behavior | Current status |
| --- | --- | --- |
| P0 | Checked identities, validated policies, public seams, deterministic failure harness | Storage/core/application/checkpoint/runtime seams, virtual deadlines and delayed-completion histories implemented; other subsystem contracts and broader simulation remain |
| P1 | Native durable three-node Raft, application retries, recovery, snapshots and reads | Static-config replication, read barriers, pinned compaction and follower snapshot catch-up implemented; production transport/runtime remain |
| P2 | Shared Multi-Raft, bounded scheduling and overload isolation | Bounded single-owner ingress scheduling, shared WAL batches and 100-group overload isolation implemented; transport coalescing, worker assembly and output admission remain |
| P3 | Recursive quorum integration at every consensus quorum site | Elections, durable commitment and read barriers use validated predicates; check-quorum sites and full audit remain |
| P4 | Learners, joint membership/policy transitions and membership recovery | Pending; online configuration changes rejected |
| P5 | Recursive responsibilities, manifests, selective placement and routing | Pending |
| P6 | Durable split/import/fence/publish/activate, compatible merge and retry lineage | Pending |
| P7 | Evidence-backed batching, lanes, reclamation and throughput tuning | Pending; no benchmark claims |
| P8 | Logical voters, striped single-group WAL and broader transactions | Research, deferred behind separate protocol/proof gates |

Initial targets are Linux and macOS. Windows is deferred. CI is intended to run
in the background without gating changes or implementation progress. Local tests
should follow the changed contract, without repeatedly running unrelated checks.

## Safety rules carried forward

Responsibilities, quorum trees, execution lanes and WAL lanes are distinct
structures and types. A concrete Raft group has one ordered log/commit prefix.
Normal child writes never require an ancestor commit. Learners, stale
incarnations and written-but-unsynchronized records never count as durable
voter evidence. Membership, weights and topology are replicated configuration,
not locally hot-reloaded policy.

Source fencing precedes target activation; imported data must be recoverable
before an import receipt can count. Operation IDs, digests, deduplication,
outbox state and ownership lineage survive snapshots and scope movement.
Linearizable reads require protocol evidence, not cached leadership or process
liveness. Queues are bounded and future application traffic must leave a control
and recovery reserve. No homemade cryptography or implicit fallback to insecure
networking. Each replica has exactly one authoritative log-store binding.

## Slice 1: durable voting

`Voter` is a deterministic RequestVote gate with no clocks, files, sockets or
concrete providers. It checks authorized group/incarnation/configuration and
candidate membership, term monotonicity and log freshness. A new vote, or a
higher term even when denying a stale-log candidate, emits `Persist`. The host
attaches the exact admission ticket and supplies a durable completion before a
reply can escape. One outstanding transition bounds retained core work. Storage
failure fences that voter until explicit recovery. Replayed admission sequences
and unrelated/future/old-session completion tickets cannot release a response.

`drive_vote` exercises the public `VoteStore` seam. A downstream integration test
substitutes a host-owned store with the native feature disabled. Native storage
uses the public `VoteIo` and `VoteLogCodec` seams; downstream tests inject both.
No provider creates threads or hidden shared runtimes.

Recovery requires explicit host-authorized static membership and matching
configuration identity. This slice cannot create groups from network messages.
The store rejects a configuration-ID change and any erased/double ballot in one
term. Persisted membership content and protocol-controlled bootstrap will be
added with the replication/configuration slice. The host must not reuse an
identity for different membership or supply fabricated recovery state.

### Durability and recovery

The native provider writes bounded, checksum-protected atomic vote batches to
one append-only segment. An append ticket represents admission/written bytes;
it never represents durability. An explicit barrier synchronizes the WAL,
writes and synchronizes a new manifest, atomically publishes it, synchronizes
the directory, and only then certifies the requested tickets. Other physical
writes may also become durable, but the completion lists only requested known
tickets. No future admission is certified. Duplicate barrier requests after a
ticket has been released are rejected; completion duplicates are safe at the core.

The manifest records store ID/incarnation, monotonically increasing startup
session, and a **contiguous complete-frame byte boundary**. It prevents recovery
from treating a missing/truncated acknowledged frame as an ordinary interrupted
tail. Sequence numbers identify admissions; they are not Raft indexes or durable
prefixes. Complete unacknowledged batches may survive and are valid recovery
inputs; recovery synchronizes them before returning. An incomplete final batch
beyond the acknowledged boundary is discarded. Invalid complete frames or
manifest checksums stop recovery; records are never skipped through corruption.

A reopened store durably increments its session before admitting work, so an
old completion cannot certify a new session. This assumes manifests are not
rolled back externally and store identities are never reused after disk loss.
New-store creation and old-store recovery are separate paths. Missing old WALs,
wrong identities, exhausted sessions, incompatible codecs and concurrent
writers fail closed. An exclusive OS file lock survives for the provider's
lifetime and is released on process exit.

Write, sync and publication errors are uncertain: bytes might have persisted.
They fence the binding. Recovery resolves durable state, rather than retrying
under an assumption that nothing happened. Dropping accepted work is not
rollback; dropping the store emits no fabricated completion.

The native baseline is synchronous, intended for a bounded worker in the later
runtime. Its default budget is 256 records per batch, 64 outstanding batches,
4096 groups and a 16 MiB WAL. It refuses further admissions at capacity; it does
not reclaim or rotate files yet. Batch state is validated completely before
submission. Recovery checks lengths before allocating from storage input.

Durability assumes an OS/filesystem/device honoring successful synchronization
and atomic same-directory rename. File and directory publication use
`File::sync_all`. The pinned Rust implementation uses `fsync` on Linux and
`F_FULLFSYNC` on Apple platforms; see [Rust's implementation](https://github.com/rust-lang/rust/blob/1.98.0/library/std/src/sys/fs/unix.rs)
and [Apple's synchronization documentation](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html).
Unsupported synchronization returns an error; no weaker mode is advertised.
Hardware power-loss durability has not been established by these tests.

### Physical format

This temporary vote-only format is distinct from the architecture pack's
proposed full entry WAL. It is not a stable format promise for a future log
engine. Migration is required rather than interpreting these bytes as a new
format. Integers are explicitly little-endian; CRC32C is for accidental
corruption, not authentication.

| Structure | Layout |
| --- | --- |
| Manifest, 52 bytes | `VBSTORE1`, store ID u128, store incarnation u64, startup session u64, durable byte length u64, CRC32C over preceding 48 bytes |
| Batch header, 32 bytes | `VBVOTE01`, sequence u64, record count u32, payload length u32, CRC32C over preceding 24 bytes, zero flags u32 |
| Vote record, 48 bytes | Group ID u128, group incarnation u64, configuration ID u64, term u64, voted-for u64 (zero means no vote) |
| Batch trailer, 16 bytes | `VBEND001`, CRC32C over header and payload, zero reserved u32 |

The native codec is selected at construction. A replacement advertising format
1 must implement the same physical format, bounds and validation. An unknown
format is rejected before provider operations. Providers are trusted contract
implementations, not a sandbox against malicious code in the host process.

## Slice 1 validation

Linux, Rust 1.98.1, 7 October 2026. The installed toolchain is registered under
the `stable` alias; local verification uses `cargo +stable` after checking that
its compiler version matches the pinned toolchain. No dependencies were fetched.

- Native feature: 20 tests pass, including the child-process fixture.
- Core/contracts-only build: seven tests pass without native providers.
- Clippy with warnings denied, formatting, documentation build and the CLI
  example are checked before the slice is committed.
- Quorum tests exhaustively check intersection among all 256 accepted subsets
  of the nine-voter example and compare frontiers against an independent
  threshold enumeration for 512 deterministic progress maps.
- Storage tests inject failure after every byte of a one-record append;
  sync failure; publication failure before and after replacement; every
  incomplete-tail length; every truncation of an acknowledged frame; and every
  single-bit mutation of a complete frame and manifest.
- Native files are reopened with the prior vote intact, a concurrent writer is
  refused, and an acknowledged ballot survives abrupt child-process exit
  without Rust destructors. The failure model separately discards volatile
  bytes to simulate power loss; this is not a real device power-cut test.
- Host implementations exercise the public storage, platform-I/O and log-codec
  seams. Unsupported formats are refused before initialization.

macOS execution has not yet been observed. The CI template is committed at
`ci/platform-feedback.yml`; activating it in `.github/workflows/` is deferred
because GitHub rejected workflow publication with the current token's scopes.
This does not block code pushes or implementation. No branch protection or
required status checks have been configured.
Those slice-1 checks do not establish replication, networking, snapshots,
joint transitions, split/merge or throughput. Slice-2 evidence follows.

## Slice 2: replicated log and static-configuration Raft

`LogStore` exposes atomic per-group bootstrap and entry/hard-state transitions,
explicit barriers, durable state recovery and bounded range fetch. A bootstrap
persists the validated recursive policy, configuration identity and exact
voter-to-store identity map before elections. Repeated creation, unknown groups,
reused group IDs, ballot regression and local configuration hot reload are
rejected. A node verifies that its authoritative store matches its persisted
voter identity before entering the core.

`LogUpdate` binds an expected revision, hard state, contiguous commit prefix and
optional suffix. Missing indexes, decreasing entry terms, hard state behind the
log, commit regression and replacement of committed entries are refused before
submission. Each complete atomic transition advances the persisted revision.
Replacement of an existing suffix additionally advances its logical generation
and invalidates outstanding tickets for the old suffix. Pure append retains the
generation. Restart reconstructs both numbers by verified physical batch order.
The store does not invent a prefix by selecting the highest term at each index.

The native full-log provider selects `FileLogIo` and `NativeLogCodec` through
public seams. It uses `log.wal`, format `VBLOG002`/`VBLEND02`, and a `VBLSTR02`
manifest. All integers are bounded, explicit little-endian fields. Batch CRC32C,
header count/length checks and trailer validation cover the complete payload.
Policy decoding bounds recursion, node counts and voter counts before creating
a validated policy; command and entry allocations obey declared limits. The
vote-only format remains separate and cannot be reopened as a full-log store.
There is no automatic stacking of the two stores.

The manifest uses the same exact 52-byte layout as the voting manifest, with
the distinct magic above. Bootstrap records include policy trees and voter
store identities; update records include group/revision, term/vote, commit and
optional consecutive entries with operation IDs and owned command bytes. This
is still a development format. New formats require explicit migration.

Defaults bound the WAL to 64 MiB, batches to 1 MiB/256 group units, pending work
to 1024 units, each group to 16,384 entries, and commands to 64 KiB. A 1 MiB WAL
reserve is unavailable to application commands; control/term/commit/no-op
records can use it. There is no reclamation or segment rotation yet. Exhaustion
is explicit rejection, not silent weakening of durability.

`Raft` is deterministic and constructs no concrete provider. Hosts supply
campaign/heartbeat events and authenticated, identity-bound message delivery.
An election persists the new term and self-vote before soliciting ballots.
Follower votes and append acknowledgements wait for exact durable transitions.
Candidates check log freshness; followers check matching prefixes and never
replace committed entries. A leader persists a current-term no-op, counts only
matching durable acknowledgements using the validated policy, and commits only
through a qualifying current-term entry. Its own durable prefix is mandatory.
Commit persistence precedes `Committed` notifications and commit publication.

Requests carry the group/configuration, sender node/store incarnation and an
origin store-session/sequence context. Replies must match the current request;
old-session replies and foreign identities fail closed. Terms, group revisions
and suffix generations are checked independently of request correlation.
Peer authenticity is a host precondition at this stage, not a cryptographic
claim from public message fields. No insecure production transport is supplied.

One in-flight durable transition per core bounds its retained work. The host
must queue or retry `Busy` deliveries within its own budget. Replication batches
contain at most 64 entries and obey byte budgets. No majority counters bypass
the validated policy for the implemented election/commit sites. Read-index,
check-quorum and membership sites are still future work, so P3 is not complete.

The `StateMachine` seam applies contiguous committed entries. `Counter` retains
operation IDs, exact eight-byte request content and original outcomes; retries
return the original result and conflicting content never changes the original
effect. Overflow outcomes are also cached. No-ops advance the applied boundary.
Malformed, gapped or capacity-exceeding batches do not partly advance state.
Deduplication is bounded with no implicit eviction. Service admission must
reserve application capacity before accepting work; the serial demo stays
within its fixed 10,000-operation schema. Diagnostic `read_applied` checks only
the local applied boundary and supplies no distributed read proof.

The three-replica example exercises the real full-log store, public storage
driver, deterministic core and application contract. Message delivery is
explicit in-process host code. Restart reconstructs the counter and dedup map
from the entire retained committed prefix. Checkpoint/replay truncation and
snapshot state are not yet implemented.

### Slice 2 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full native suite: 45 tests pass. Core/contracts/application-only suite:
  14 tests pass, including a three-node host-store history and the recursive
  election/commit history with native providers disabled.
- The shared public-store conformance history runs against native storage and
  a downstream host implementation. It covers bounded fetch, durability before
  visibility, suffix fencing, commit protection, multi-group rejection atomicity,
  unknown groups and stale revisions/tickets.
- Every interrupted-byte boundary of a suffix replacement is recovered as the
  old or complete new state. Every missing byte in the acknowledged prefix
  fails recovery. Sync and publication faults fence without certifying the
  transition. Every single-bit mutation of a complete bootstrap batch is rejected.
- Three-node histories commit a command, isolate its leader, leave another
  command uncommitted, elect a new leader, commit a new command and repair the
  old leader's conflicting suffix. Native and host stores run the same history.
- Thirty-two seeded schedules of 256 events combine elections, proposal attempts,
  delayed/reordered/duplicated messages, partitions, healing, heartbeat events
  and simulated power-loss restart. After every event the harness checks that
  committed histories agree and no two leaders exist in the same term. Every
  restart checks preservation of acknowledged committed state. A stable majority
  subsequently elects a leader and commits a new command on all three replicas.
- Nine-voter recursive scenarios elect/commit with two majorities in two sites,
  refuse a flat five-voter set lacking the tree quorum, and prevent old durable
  progress from certifying a later command while its recursive quorum is absent.
- Actual native files on all three replicas recover the committed history.
  The CLI demo is rerun against the same files: operation 1 adds seven once,
  survives restart/retry at seven, then operation 2 adds three to reach ten on
  every replica. Client results are printed only after commitment and application.
- Formatting, warning-free Clippy, documentation and dependency-free builds
  are checked locally. macOS CI activation remains deferred as recorded above.

These finite histories are regression evidence, not a full protocol proof or a
production release gate. At the end of slice 2 there was no production network service, distributed
linearizable-read API, checkpoint/snapshot, joint reconfiguration, scope transfer
or throughput claim yet. Real hardware power-cut testing remains outstanding.

## Slice 3: quorum-backed reads

`Event::Read` admits one read invocation on the serialized group owner. The
leader first requires a durably committed entry from its current term; a new
leader cannot use an inherited committed prefix before establishing that fence.
Admission captures the contiguous committed index and creates a fresh
`RequestContext` scoped to the persisted store session. Explicit `ReadProbe`
messages gather individual `ReadAck` responses under the same validated policy
used for elections and writes. Append acknowledgements and previously collected
read quorums do not count. Heartbeat events retry the outstanding read context.

A follower persists any higher term before its acknowledgement can escape. A
same-term acknowledgement depends on the already durable hard state; it proves
current-term authority participation, not new log replication. The leader counts
only distinct configured voter identities with matching group/configuration,
request context, origin session and current term. Responses from a higher term
invalidate read state and force term persistence. Leadership loss, campaigns,
storage failure, cancellation and restart discard pending and ready reads.

`ReadReady` carries an opaque, one-use `ReadBarrier`. Its index is the committed
prefix captured at invocation, not the maximum received or applied index. The
host waits for ordered application through that index, then consumes the barrier
on the same core immediately before reading immutable application state. The
public `ReadableStateMachine` extension and `read_at_barrier` helper perform this
path for the native counter and a downstream application with its own query and
result types. Insufficient application progress retains the barrier for retry;
successful consumption prevents reuse for another invocation. The host must bind
the correct application to the group and apply only committed entries.

Read request IDs increase within a store session. One admitted or ready read per
group bounds retained resources; callers receive `ReadInFlight` and must apply
their own bounded admission/retry. Cancellation releases that slot without
undoing any writes. No clock leases, cached leader hints or process-heartbeat
authority are used. A delayed acknowledgement may finish its original read
which overlapped a new leader's write; it cannot authorize a subsequent read.

### Slice 3 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full native suite: 55 tests pass. Core/contracts/application-only suite:
  22 tests pass. Both builds pass Clippy with warnings denied; documentation
  builds without dependencies.
- Read histories run against both native storage and a host-supplied store.
  Local durability alone cannot complete a read, an isolated old leader cannot
  serve a new read, and a replacement leader's barrier covers its committed write.
- The nine-voter recursive policy rejects fresh responses from a flat majority
  lacking the tree quorum. Duplicate voters, previous read contexts, foreign
  groups/configurations/incarnations, and old origin sessions cannot certify it.
- Higher-term read acknowledgements wait for their exact durability ticket;
  insufficient applied progress, cancellation, token reuse, campaign and storage
  fencing are checked. Recovery cannot restore a volatile ready read.
- A delayed old quorum can finish only its original overlapping read. A second
  invocation after the new leader's completed write cannot reuse those responses.
- The real three-WAL counter example obtains its printed linearizable value
  through the same public read path. Native and host applications exercise the
  generic helper, including a host-specific query/result type. Three invocations
  on the same files print 7, 7 after restart/retry, and 10 after the next operation.

These histories are finite regression evidence. A general end-to-end history
checker and virtual-time read scheduling remain to be added with the runtime.
The read mechanism does not enable leases, follower linearizable reads, snapshot
installation or online policy changes. The full P0–P7 goal remains incomplete.

## Slice 4: crash-atomic application checkpoints

`SnapshotStore` is a public logical binding for one local group. Its native
provider uses the same stage/chunk/seal/publish/load/abort operations available
to downstream hosts. `SnapshotIo` and `SnapshotCodec` are separate replaceable
native mechanisms; host implementations exercise both. Provider handles may
share physical resources; this initial native implementation uses an explicit
directory and lock per local group, with no threads or hidden resource owners.

`begin` admits one bounded stage with exact metadata and an application length.
Chunks have checked offsets, lengths and ticket identity. Admission and chunk
writes are not durability receipts. The native provider incrementally computes
CRC32C over submitted metadata and bytes, then compares it with the complete
staged file before sealing, so corruption before seal cannot be silently blessed
by calculating a new checksum from damaged storage. Seal verifies framing and
synchronizes both data and its directory entry before the root can reference it.
Publication writes and synchronizes a temporary manifest, atomically replaces
the root, synchronizes the directory and only then returns `SnapshotReceipt`.
Any uncertain I/O or staged/published corruption fences that snapshot binding.

The root identifies one of two alternating data slots. A stage may overwrite
only the inactive slot, so an interrupted stage cannot damage the latest
published checkpoint. Both slots, the root and its temporary file have fixed
budgets: defaults are 64 MiB application data, 1 MiB metadata and 64 KiB chunks.
Seal re-reads the complete bounded file; this is not a zero-copy or throughput
claim. An aborted stage leaves only the bounded inactive slot, and late tickets
cannot publish it. There is no unbounded archive of orphan snapshot files.

Tickets bind local store identity/incarnation, persisted snapshot-store session,
group incarnation and stage generation. Each new stage in a session advances
the generation, even after abort. Recovery validates the root and its complete
referenced file, reconstructs the published generation, then persists a fresh
session before admitting work. Generations alone are not global monotonic
watermarks across restart; the entire ticket scope is required. Creation and
recovery are distinct. A missing/corrupt old manifest or referenced file cannot
be treated as an empty successful recovery. Store identities must not be reused
after disk loss or reset while stale work may exist.

Native `VBSNAP01` files contain schema/index/term, bounded embedded bootstrap
metadata (native log framing version 2), application bytes and a CRC32C trailer.
`VBSSTR01` roots bind store/group identities, session, selected slot, generation,
length and checksum. Unsupported formats and oversized lengths are rejected
before allocation. Checksums detect accidental corruption; they are not
cryptographic identity or an authenticated import certificate.

`CheckpointStateMachine` supplies explicit schema, serialization and restore
operations. Counter schema 1 encodes its applied index, configured dedup capacity,
value, sorted operation IDs, exact command content and original outcomes,
including cached overflow. Capacity and schema changes require migration rather
than silent restore. Malformed lengths, duplicate/zero IDs and invalid outcomes
are rejected. A host adapter is responsible for serializing all of its own
state, including any outbox or lineage fields; the current counter has neither.

`checkpoint_application` captures only an applied contiguous prefix no greater
than the core's durable commit index, with the matching term and exact persisted
bootstrap. `restore_application` validates the local binding, bootstrap, schema
and included boundary against the retained Raft log, restores a fresh application
and replays only the later committed entries. It builds the restored result
before replacing live application state, so a bad checkpoint or invalid tail
cannot partly advance the application. Replayed results are not new client
acknowledgements. The host serializes its correctly bound group/application
while calling these helpers. Raft term/vote/commit state remains owned by its
one authoritative log store.

### Slice 4 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full native suite: 68 tests pass. Core/contracts/application-only suite:
  27 tests pass. Both builds pass Clippy with warnings denied; documentation
  builds successfully without dependencies.
- Shared snapshot conformance runs against native storage and an independent
  downstream provider with native features disabled. A host application uses
  its own schema while exercising the same capture/restore helpers.
- Interruptions at every prefix byte, chunk byte and trailer byte retain the
  prior publication. Sync failures and root publication failures recover the
  prior or complete new image. An uncertain receipt never certifies publication.
- Every bit mutation and every truncation of a published image, plus every bit
  mutation of its root, is rejected. Corruption during staging is detected before
  seal. Missing roots, wrong group/store incarnations, stale sessions and mutated
  sealed tickets cannot establish recovery or publication.
- Valid-checksum oversized lengths, unsupported versions, bounds, offset gaps,
  duplicate publication, cancellation and incompatible codecs are exercised.
- Native WAL/checkpoint compositions preserve acknowledged counter state across
  simulated power loss during later checkpoint sync/publication. Restore can
  select the old checkpoint and replay the retained tail, or use the complete
  new checkpoint; both preserve exact retry outcomes and Raft hard state.
- Actual files verify exclusive writers, checkpoint/replay recovery and repeated
  publication. Three real-WAL replicas restore checkpoint indices 3 then 6 in
  repeated CLI runs, retain value 7 on retry, and reach 10 after a new operation.

At the end of slice 4, these local checkpoints did not install a remote leader's snapshot, reclaim any
Raft prefix, or provide permanent pin/retention handles. A publication receipt
is superseded by later publication and is deliberately not log-deletion
permission. Local images must not be reused as imported voter state without
the future snapshot-install protocol. Failed/corrupt recovery does not silently
fall back to an empty application. Hardware power-cut and macOS execution remain
outstanding; the finite failure model is regression evidence. P0–P7 remains active.

## Slice 5: pinned compaction and follower snapshot installation

`SnapshotRetention` extends the public snapshot seam with stable `SnapshotRef`
anchors, durable pins, exact pinned loads and reconciliation against the single
authoritative log. A reference binds store/group/configuration, generation,
index/term, schema, file length and checksum. Pins protect the current log anchor
and a replacement during a switch. The two-slot provider refuses an overwrite
while the inactive slot remains pinned. After the new log binding is durable,
reconciliation selects that image and releases obsolete pins atomically. These
are log retention anchors, not general backup or consumer reference counts.
Transfers own their loaded image, so releasing an obsolete log pin cannot
invalidate an in-flight message buffer.

`compact_replica` verifies the actual log/core binding, application boundary and
checkpoint contents, then durably pins the image before persisting the new log
base. The base index is a contiguous covered prefix, never a maximum observed
index. Matching suffix entries remain; fetches below the retained range return
`Compacted`. Compaction advances the logical generation and invalidates old
suffix tickets. The native WAL still retains old physical records; no disk-space
reclamation or segment cleaner is claimed. Snapshot binding is mandatory record
kind 2 inside the existing version-2 WAL framing; old kind-0/1 records remain
readable, while older readers reject the unknown mandatory kind. The snapshot
root is now `VBSSTR02` with at most two pinned descriptors. Recovery validates
every retained image and upgrades a valid version-1 root atomically. Snapshot
data framing remains `VBSNAP01`.

A leader emits `SnapshotRequired` when a follower needs a compacted prefix.
`supply_snapshot` checks the exact live request and loads its pinned image. A
follower emits `StageSnapshot` only for an identity/configuration/term-valid
request ahead of its committed prefix. `stage_snapshot_effect` checks the exact
effect and application image, publishes and pins it, then persists its log
binding and hard state through the authoritative store. This emits only
`SnapshotInstalled`; the group remains busy and no remote acknowledgement can
escape. `finish_snapshot_install` restores the application, reconciles pins and
then permits `SnapshotAck`. A lost receipt is resolved by recovery. Higher terms
still require durable term transitions, and stale contexts cannot establish
replication progress. A compacted-range response lets a leader move forward
when its outstanding append precedes the follower's base.

`recover_replica` verifies the exact log anchor, restores its application and
committed tail, and reconciles interrupted switches before returning a voter.
Basic `Raft::recover` refuses compacted state without this verification. A
published remote image with no durable log binding is discarded and the old log
is replayed. Missing or corrupt anchored data refuses recovery. Term/vote and
configuration remain owned by the log; application retry outcomes survive both
installation and restart.

### Slice 5 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full native suite: 76 tests pass; core/host-only suite: 31 tests pass. Both
  builds pass Clippy with warnings denied; formatting and documentation pass.
- A shared three-node history runs with independent host providers, injected
  native I/O and actual files. A partitioned follower installs a compacted
  snapshot, replays the tail, preserves original retry outcomes, serves a read
  through quorum evidence, recovers and subsequently wins an election.
- A nine-voter recursive history preserves its persisted policy through snapshot
  catch-up and refuses commitment by a flat majority lacking the tree quorum.
- Every interrupted byte of a snapshot-binding WAL record, plus sync and
  publication failures, recovers the old or complete new binding without an
  early acknowledgement. Lost application completion recovers before the voter
  resumes. Pin switches survive restart; blocked slot reuse and version-1 root
  migration are exercised. Missing anchors and swapped stage effects fail closed.
- Shared native/host log conformance rejects committed-prefix conflicts,
  foreign snapshot owners and regressed bases; retained suffix repair and
  generation-guarded range fetch continue after compaction.
- Repeated CLI runs with three real WALs compact through indices 3, 6 and 9,
  restore the previous image, preserve value 7 on retry and reach 10 after a
  new operation.

Transfers currently retain a whole owned image bounded to 64 MiB by default;
disk writes use bounded chunks. Wire fragmentation, shared buffer/admission
budgets, production transport, physical WAL cleaning and online configuration
are still pending. Finite crash histories are regression evidence; real hardware
power-cut and macOS execution remain outstanding. The P0–P7 goal remains active.

## Slice 6: shared ingress scheduling and explicit deadlines

`Shard<ReadyScheduler>` owns registered Raft cores and bounded event queues. The
native `FairScheduler` maintains unique readiness and round-robin service;
continuing work returns to the tail. A registered group has one mutable owner
and at most one active visit. Every visit has item, retained-byte and elapsed
time budgets, checked between core steps; these cannot preempt an individual
step or callback. The public scheduler carries readiness only, never consensus
state. No group scan is needed to select ready work.

Mandatory admission checks group ownership and node/group item and byte ceilings
before retaining an event. User proposals and read admissions cannot consume the
control reserve; replication acknowledgements, term/vote work, read protocol
messages and timer events can. Snapshot inputs also consume an explicit shared
background ceiling. Retained vector capacities are charged rather than only
payload lengths, with bounded policy-tree and membership accounting. These
accounting units bound retained ingress structures, not exact allocator RSS.
Bounded class priority retains a cursor across visits, so repeated control
arrivals cannot starve data merely because the visit limit is one event.
`Overloaded` is a capacity rejection; `EventTooLarge` requires reducing or
renegotiating the event's retained representation. Rejection returns the exact
owned event and implies no successful admission or client result.

`VisitTicket` binds store identity/session, execution lane, runtime generation,
group incarnation and visit sequence. A fresh store session fences restart-era
work; replacing an owner requires a fresh host-supplied runtime generation.
Sequence numbers identify visits, not durable prefixes. One shard and timer
service lifetime share that owner; resetting either sequence under a reused
owner is forbidden. Only an exact live visit permits mutable core access.
Inputs remain charged through a suspended visit. `finish` refuses outstanding
storage or snapshot dependencies, while unrelated groups remain schedulable.
`Raft::admit_effect` correlates the complete submitted update before accepting
its storage ticket, enabling batched workers to deliver later durable evidence.
The existing exact durability tokens still gate all dependent Raft effects.

`TimerService` exposes bounded register/replace/cancel/poll operations and exact
generation-scoped tokens. Native `DeadlineQueue` uses ordered deadlines with
O(log timer count) updates, physically removing replaced/canceled entries rather
than accumulating tombstones. It is a deliberate initial alternative to the
pack's proposed timer wheel; no constant-time or throughput claim is made.
Expiration reports lateness separately. The consumer compares its expected
token, retains/retries expiration on queue overload and explicitly rearms it.
The native monotonic `Clock` uses a shared cloneable epoch; virtual clocks are
injected in tests. `ElectionEntropy` supplies jitter, with an explicitly seeded,
reproducible native generator. Independent node seeds are a host responsibility;
this generator has no cryptographic or identity role. Clock regression and
deadline arithmetic overflow fail explicitly. The core reads no clock.

Closing admission allows bounded draining. Stopping a group fences its core,
invalidates its visit and returns unprocessed inputs. Already submitted external
work has an unknown outcome until recovery; cancellation is not rollback.
Dropping a shard does not close a host-owned WAL, clock or executor. Effects
transfer to host-owned workers; this slice does not budget their outbound or
application buffers. Synchronous native storage remains a mechanism to run on
the later bounded blocking worker, not inside an indefinitely blocking scheduler
callback. Automatic leader-contact/election timer management, background worker
assembly, per-peer/tenant admission, wire framing and secure transport remain.

### Slice 6 validation

Linux, Rust 1.98.1, 8 October 2026:

- Native tests: 88 pass; core/host-only tests: 39 pass. Both builds pass Clippy
  with warnings denied, formatting and documentation checks.
- Independent downstream scheduler, timer, clock and entropy providers exercise
  the same contracts without native features. Readiness deduplication, fair
  requeue, bounded expiration polling, cancellation, replacement, stale tokens,
  timer lateness and clock regression are covered.
- A shared history hosts 100 groups on each of three nodes with one log store
  per node. Elections use virtual deadlines polled in bounded batches. WAL
  transitions coalesce into batches of 100 groups. Delaying one group's durable
  completion retains its charged input while the other 99 commit and complete
  quorum-backed reads. Group overload leaves control credits available.
- The history runs with independent host storage/scheduling/timers, native
  simulated I/O and actual native files. Every group's counter reaches 10,
  duplicate operation IDs retain their original result, and reopening the three
  WALs restores all 100 counters. Old-session visits cannot access recovered
  owners. This is in-process delivery, not a network or throughput benchmark.
- Independent node item and group byte ceilings, oversized retained capacities,
  background quotas, elapsed visit limits and bounded control priority are
  exercised. A suspended storage visit refuses premature finish and unrelated
  completions; another group remains serviceable.
- Failed shared sync/root publication produces no vote messages. The host fences
  all submitted visits and recovery resolves the atomic batch. Stopping returns
  queued inputs, invalidates tickets and makes unresolved outcomes explicit.
  Dropping one shard leaves another owner and their host-supplied store live.

These checks establish the bounded ingress scheduling slice. They do not claim
automatic failure detection, a complete asynchronous runtime or node-wide bounds
on all output/application resources. macOS execution and hardware power-cut
testing remain outstanding. P0–P7 remains active.

## Slice 7: automatic consensus timers and stable replication retries

`TimedShard` composes the existing shared owner with any public `TimerService`
and `ElectionEntropy` providers. Registration arms a randomized election
deadline. Leadership arms periodic heartbeats; returning to follower service
restores election timing. The host supplies monotonic time explicitly when
polling, stepping or delivering storage/application completions. Timer periods
are construction-time local runtime settings, not mutable quorum policy.
Construction checks owner/capacity compatibility, a quiescent shard, positive
intervals and enough control byte reserve for a tagged expiration.

The core now exposes a volatile election-reset sequence for campaigns, valid
leader contact and durable granted votes. Denied votes, stale-term requests,
unrelated acknowledgements and rejected identity/format checks do not reset that
sequence. Append entry structure is validated even when the local prefix does
not match, so malformed suffixes cannot masquerade as leader contact. A pending
vote does not reset on preparation; its durable completion or an already durable
repeat grant does. Recovery starts the volatile sequence at zero under a fresh
runtime owner. It is neither a persisted prefix nor authority to serve a read.

Every managed expiration retains its exact owner/group/kind/deadline token.
Admission tags the queued timer with that token. Leader contact, role changes,
vote completion and shutdown invalidate old queued tokens before dispatch can
campaign. This prevents an expiration queued behind a valid heartbeat or delayed
vote from starting an unnecessary new term. The controller retains at most one
pending expiration per group under overload and retries in bounded fair polls;
late heartbeats coalesce instead of replaying every missed tick. Timer progress
reports maximum lateness, not a quorum watermark. Closing admission cancels
future deadlines and makes queued timers inert while admitted work drains.
Provider errors latch an explicit failure; stopping groups remains available
to return queued work and resolve already submitted work through recovery.

Automatic heartbeats exposed a liveness defect in the earlier replication
driver: each tick replaced an outstanding request context, so a round trip
longer than the heartbeat interval could prevent any acknowledgement from
counting. A failing 12 ms round-trip/4 ms heartbeat regression reproduced it.
Retries now preserve the outstanding context and exact entry range (or pinned
snapshot reference). A response retires the request before another range is
sent; term changes and compaction invalidate it. Retries reconstruct bounded
entries from the authoritative log without retaining an extra payload per peer.
The advertised commit index may advance, but the correlated matching prefix
cannot expand. Read probes still use their independent invocation context.
The shared test transport now explicitly retains rejected messages in a bounded
deferred queue, exercising the ingress backpressure contract under retries.

### Slice 7 validation

Linux, Rust 1.98.1, 8 October 2026:

- Native suite: 98 tests pass; core/host-only suite: 46 tests pass. Both builds
  pass Clippy with warnings denied; formatting and documentation pass.
- Shared virtual-time histories automatically elect leaders, maintain healthy
  leadership, replace a partitioned leader and continue an unrelated group.
  The isolated old leader cannot complete its quorum-backed read. Restart,
  delayed/duplicate old-session traffic and healing preserve committed values
  and exact retry results. These histories run with host replacements, native
  simulated I/O and three actual native WAL files.
- Queued expiration after leader contact, vote preparation versus delayed durable
  completion, stale/denied/malformed/unrelated traffic, saturation, lateness,
  retained expiration retry, shutdown and timer-provider failure are exercised.
- Host and native histories sustain commitments and quorum reads when the
  network round trip exceeds several heartbeat intervals. The same tests failed
  before the stable-context fix. Existing recursive quorum, snapshot catch-up,
  crash-recovery and 100-group overload histories still pass.

Timer automation does not add leases or check-quorum leadership withdrawal.
An isolated leader still needs fresh quorum evidence for reads and writes.
Native blocking worker assembly, output/buffer admission, wire framing and
authenticated transport remain pending. This is finite regression evidence,
not a full liveness proof or production performance claim. macOS and hardware
power-cut execution remain outstanding; P0–P7 remains active.

## Slice 8: bounded asynchronous WAL worker

`PersistenceWorker` is a public host replacement seam. `NativeLogWorker` moves
the selected quiescent `LogStore` into one explicitly created blocking thread;
many groups share it. It creates no second log and no hidden executor. Cores
remain on their shard owner. `submit_for_shard` and `submit_for_timed` validate
the complete pending persistence effect before transferring the owned batch.
Written admission associates the exact log ticket but releases no dependent
effect. Only its durable barrier completion releases votes or acknowledgements.
Each delivery validates its original visit; a stopped group does not discard
other groups in the same completion.

Worker tickets identify admissions, not durable prefixes. A construction-time
worker generation is scoped to the recovered store session and must not be
reused there. Runtime visit and store-session checks reject stale owner delivery;
the existing exact log-ticket check remains the durability authority. Recovery
obtains a fresh store session and reconstructs cores from the authoritative WAL.

Admission bounds outstanding requests, units and retained vector-capacity bytes,
with reserves for control work. Mixed command/control batches consume the bulk
budget. One pending unit per group prevents competing transitions. Credits stay
charged until terminal events are consumed, including when physical sync has
already completed. The completion channel holds at most two stages per admitted
request. `WorkerWake` is an injected nonblocking scheduling hint; `ThreadWake`
unparks a caller-selected thread. Neither supplies quorum evidence. Caller-held
events and output effects need their own budgets; these bounds are not an RSS
limit or an operating-system fsync latency guarantee.

Close rejects new work, drains accepted requests and allows nonblocking
`try_reclaim` to return the store after terminal consumption and thread exit.
Dropping observation cannot cancel accepted writes. Uncertain writes, corrupt
provider completions and worker panics produce failures, never fabricated
durable evidence; the host fences affected cores and recovers the store.
Shared host wake resources remain host-owned.

### Slice 8 validation

Linux, Rust 1.98.1, 8 October 2026:

- Native suite: 104 tests pass; core/host-only suite: 48 tests pass. Both builds
  pass Clippy with warnings denied; formatting and documentation pass.
- A manually progressed host worker uses only public contracts with native
  features disabled. Exact-effect rejection, written-before-durable delivery,
  stale visits and independent delivery after one group stops are exercised.
- Blocked sync leaves another group runnable. Control admission survives bulk
  saturation, oversized retained capacity is rejected, and completed physical
  work retains credits until terminal consumption. Close drains every request.
- Injected barrier uncertainty and worker panic fail every accepted request
  without releasing durable effects. One actual native WAL worker persists 100
  groups in one batch; reopening recovers every exact term/vote with a fresh
  store session.

This worker slice tests durable voting and worker lifecycle, not a new
three-node asynchronous replication assembly. Existing synchronous three-node
replication, timer, snapshot and crash histories still pass. Live asynchronous
snapshot work, outbound admission and authenticated transport remain pending.
Current snapshot helpers require quiescent synchronous store access. macOS and
hardware power-cut execution remain outstanding; P0–P7 remains active.

## Next slice

Add bounded output admission, wire
codec and authenticated-session transport seams. Extend virtual-time histories
to leader loss, overload and message delay through the new assembly. Native
sockets must use established secure
sessions supplied by the host; production assembly cannot silently select an
insecure simulation transport. Preserve downstream substitution and durable
histories as these providers enter the assembly.
