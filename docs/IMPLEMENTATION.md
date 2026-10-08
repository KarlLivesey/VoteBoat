# Implementation record

## Scope and roadmap

This repository implements the proposed VoteBoat baseline in bounded vertical
slices. The local architecture pack, version 0.2 of 7 October 2026, remains the
design input and is intentionally excluded from Git. Nothing in this status
record claims that unimplemented phases already work.

| Phase | Intended behavior | Current status |
| --- | --- | --- |
| P0 | Checked identities, validated policies, public seams, deterministic failure harness | Storage/core/application/checkpoint/runtime/wire/TLS/peer-transport seams, virtual deadlines and delayed-completion histories implemented; other subsystem contracts and broader simulation remain |
| P1 | Native durable three-node Raft, application retries, recovery, snapshots and reads | Static-config replication, reads, snapshot catch-up and asynchronous checkpoint/compaction implemented through native workers, owned node facade and real TCP/TLS histories; broader fault coverage remains |
| P2 | Shared Multi-Raft, bounded scheduling and overload isolation | Bounded ingress/effect/outbound scheduling, listener/dial workers, ingress/client/read admission, replica/peer drivers, owned node assembly/shutdown and native 100-group histories implemented; broader scale/fault coverage remains |
| P3 | Recursive quorum integration at every consensus quorum site | Implemented elections, commitment and reads audited through accepted-log membership; online policy transitions remain gated under P4 |
| P4 | Learners, joint membership/policy transitions and membership recovery | Journal, recovery, snapshot/wire, local proposals, native TCP/QUIC readiness, bounded activation model and owned Node administration, durable status/resumption and native placement authorization implemented; selected codec/transport envelope admission and explicit native member restart implemented; service enrollment/envelope enforcement and faulted online transitions remain gated |
| P5 | Recursive responsibilities, manifests, selective placement and routing | Pending |
| P6 | Durable split/import/fence/publish/activate, compatible merge and retry lineage | Pending |
| P7 | Evidence-backed batching, lanes, reclamation and throughput tuning | Pending; no benchmark claims |
| P8 | Logical voters, striped single-group WAL and broader transactions | Research, deferred behind separate protocol/proof gates |

Initial targets are Linux and macOS. Windows is deferred. CI is intended to run
in the background without gating changes or implementation progress. Local tests
should follow the changed contract, without repeatedly running unrelated checks.

## First usable milestone — priority updated 8 October 2026

The user's immediate priority is both an embeddable Rust library and a runnable
networked service, as quickly as possible. This milestone comes before finishing
the full P0–P7 roadmap. Static membership is sufficient for the first service;
online membership, recursive responsibilities and split/merge follow afterward.
The existing durability, bounded-resource and quorum contracts still apply.

Deliver a small native service over the existing Node facade and public provider
contracts, with explicit local identity, durable data directory, peer addresses
and host-supplied TLS credentials. Provide a three-node Linux/macOS quickstart,
an application write/read entry point, verified restart and retry behavior, and
graceful shutdown that drains and joins the selected workers. Keep setup code
usable from Rust embeddings as well as the executable. Do not build a second
consensus engine or substitute an in-process message demo for a network service.

The existing replicated_counter example is usable today as a local library
composition demo. On Linux, three consecutive invocations on a fresh directory
were checked on 8 October: operation 1 adds 7; restart/retry of operation 1 keeps
7; operation 2 adds 3 and reaches 10 on all three replicas. Each invocation
completed a quorum-backed read and durable checkpoint/compaction. This verifies
that example, not a standalone network service or macOS execution. Actual
TCP/TLS and Node-facade histories already exist in the integration suite; the
first three-process executable now exposes that assembly. Generic startup
the shared typed startup and configured endpoint path are described in Slices
38–39 below. Separate-host deployment and operational packaging remain work.

## Linked macro and mini plan — updated 8 October 2026

The macro plan tracks usable capabilities, not the number of internal slices.
The mini plan covers the current deliverable and the next two, including their
dependencies and acceptance checks. A completed helper advances a milestone;
it does not create a new milestone by itself. Update this section when evidence
changes the next step, and explain any added prerequisite before implementing it.

### Macro plan

| Milestone | User-visible result and completion criteria | Position in the full design |
| --- | --- | --- |
| Usable static service and Rust embedding | Run a durable three-node service, write/read/retry, recover after leader loss and restart, and shut down cleanly; document the same composition for Rust hosts. TCP and optional QUIC are implemented and exercised on Linux. macOS execution and separate-host operational validation remain outstanding. | First usable delivery, built on P0–P3. Keep it usable while later milestones develop. |
| Online membership | Add/catch up a learner, establish readiness, change voters through joint consensus and retire peers; demonstrate recovery, rollback and partial-delivery behavior before exposing online configuration ingress. | Current P4 work. This permits safe replica placement changes needed by later responsibilities and ownership movement. |
| Recursive responsibilities and routing | Resolve responsibility manifests, selectively place groups and route requests; cached child operation must survive parent unavailability without an ancestor commit in the normal write path. | P5, using the existing group/runtime foundation and P4 placement changes where required. |
| Split and merge | Move real application data with source fencing, import readiness and durable activation; preserve retry/deduplication lineage and recover without two active owners. | P6, using P5 manifests/routing and the membership/recovery foundation. |
| Measured tuning and broader validation | Reproduce committed/applied performance results and improve batching, lanes, reclamation and recovery throttling where measurements justify them; broaden failure coverage. | P7 plus remaining cross-cutting P0–P3 validation. Target Linux/macOS; CI stays background feedback. |

These are capability milestones, not a claim that every earlier phase is
finished. The detailed phase table above remains the scope ledger. P8 remains
deferred research. Online membership and split/merge do not block use of the
static service; no calendar estimate or completion percentage is inferred from
the count of remaining milestones.

### Mini plan: current deliverable and next two

Slices 51–59 establish routing/credentials, readiness, local proposals,
partial-final recovery, owned administration, durable resumption, native placement
and selected transport envelope checks. Slice 60 supplies explicit native member
restart. These remain foundations for complete online add/promote/remove.

Slice 60 is verified: eight native TCP/QUIC restart cases cover learner/joint/final
and compacted recovery, rollback provisioning, removed-local refusal and cleanup.
Seeded recovery fixtures do not establish distributed enrollment or commitment.
Slice 61 adds trusted checkpoint enrollment through public storage contracts and
native files, with exact-image retry and TCP/QUIC member startup. Administration
endpoints and enforced bounds remain within the current deliverable.
Slice 62 adds local service configuration observation and the counter's enforced
lifetime envelope. Mutation endpoints and binding their declarations to placement,
readiness and transport admission remain within the same current deliverable.

1. **Enrollment and administrative service integration (current, P4).** Connect
   explicit durable enrollment and administration/status endpoints to placement,
   readiness, capacity and member restart. Bind/enforce declared application
   schema/command/checkpoint bounds as deduplication grows. Depends on slices
   51–62 and authenticated service scope. Check exact committed assignments,
   rejection before mutation, lost-reply resumption and matching TCP/QUIC
   assemblies. Provides the native path for release testing, not an early opening
   of public configuration ingress.
2. **Fault-tested remote membership release (next, P4).** Exercise actual
   add/catch-up/promote/remove with partial joint/final delivery, weighted and
   recursive policies, leader loss, rollback, snapshots and restart over TCP/QUIC.
   Depends on item 1. Resolve activation/catch-up gaps before releasing ingress.
   Completes safe placement for P5 responsibility routing and P6 ownership movement;
   P5–P7 remain the global capability chain above.
3. **Responsibility manifests and routing (following, P5).** Implement the first
   complete manifest-to-group request path with explicit responsibility identities,
   validated placement and bounded routing. Depends on safe P4 placement changes
   for moving replicas. Check stale manifests, wrong ownership and unavailable
   parents; normal child writes must not require ancestor commits. This establishes
   the routing foundation for P6 data movement, fencing and activation.

### How the current work fits globally

For immediate implementation, distinguish the next two concrete changes from
the capability milestones above:

| Immediate change | Why it belongs now | Completion check | Global contribution |
| --- | --- | --- | --- |
| Integrate native enrollment and administration | Placement, durable resumption, selected transport envelope checks and explicit native member restart now exist; the native service must bind them to enrollment and enforced application bounds. | Service/embedding can enroll and recover exact stores, administer and query outcomes; unsupported assemblies and outgrown envelopes reject before mutation. | Makes P4 usable through the native service and supplies the release-test path. |
| Release fault-tested remote membership transitions | Local journal/readiness/administration evidence does not yet establish remote enrollment or configuration delivery. | TCP/QUIC add/promote/remove with partial joint/final delivery, weighted/recursive policies, leader loss, rollback, snapshots and restart; resolve activation/catch-up gaps before opening configuration ingress. | Completes safe placement for P5 routing and P6 ownership movement. |

The partial-final election fix is complete as slice 55, with its bounded model
and actual-core limitations recorded below. The first change is a prerequisite
of the second, not a new global
milestone. Review this immediate pair after each completed slice. Keep the static
service and embedding usable throughout; their remaining macOS and operational
validation does not depend on finishing P4.

The capability chain is **safe replica placement → responsibility routing →
safe data/ownership movement → measured tuning**. Readiness establishes whether
an exact learner/store/session can support the required application and retained
history. Joint consensus uses that evidence to change a group's voter set. P5
uses concrete groups and placement to resolve which group serves a responsibility.
P6 adds movement of application data and ownership, with source fencing before
target activation and preserved retry lineage. P7 measures the resulting paths
before changing batching or resource layout. Readiness alone does not certify
P6 imports or ownership activation; those require their own durable receipts.

TCP and optional QUIC carry the same protocol across this chain. Transport work
should therefore serve the current exchange and later routing/movement without
introducing a separate consensus path. Shared bounded scheduling, authoritative
storage bindings and restart/session checks remain cross-cutting contracts.

There are two delivery horizons: keep improving the already usable static service
and Rust embedding, while completing the broader P4–P7 design. macOS execution
and separate-host operational validation remain first-delivery gaps and can
proceed independently of online membership. P5/P6 are not prerequisites for
using a static group. At each mini-plan review, state both the immediate result
and the capability it unlocks globally; add a prerequisite only when an explicit
acceptance check needs it.

Before editing each item, sketch its data/API shape, transitions, ownership,
failure cleanup and focused checks. If that sketch reveals another dependency,
first decide whether it is essential to the stated completion criteria. Record
essential scope changes here; defer unrelated improvements. A failed check calls
for a cause and a focused fix, not an unchanged test loop or a new redesign.

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

## Slice 9: three-node asynchronous worker integration

The end-to-end public assembly now drives three independent WAL workers from
three shard owners, with 100 groups per node. Once a store moves to its worker,
the driver has no synchronous log access. Each owner batches exact persistence
effects, retries rejected admission with the returned owned units, processes
written and durable stages separately, and applies only committed entries.
The same history runs against a manually progressed host worker with native
features disabled and against three native worker threads with actual WAL files.

The integration driver explicitly bounds its pending units, network retention,
per-poll visits, application receipts and read results. It retains one deferred
durable completion for an intentionally stalled group. That group retains its
active input credits while 99 unrelated groups continue. Rejected network
ingress remains in a bounded deferred queue, and duplicate delivery exercises
request correlation. These are test-driver budgets; a production outbound
admission/transport provider is still required.

### Slice 9 validation

Linux, Rust 1.98.1, 8 October 2026:

- Native suite: 107 tests pass; core/host-only suite: 49 tests pass. Both builds
  pass Clippy with warnings denied; formatting passes. No production API changed.
  The actual-file asynchronous cluster history also passes 20 repeated runs.
- Both provider histories elect 100 leaders through durable votes, replicate
  commands, preserve original retry outcomes, and serve fresh quorum reads.
  Group-local saturation and a delayed durable owner delivery leave another
  group's read and committed writes runnable.
- Partitioning the original leader prevents its fresh read from completing.
  The surviving two workers elect a replacement and commit new operations;
  healing catches the former leader up. Closing one instance leaves the other
  two working, including when all native workers share the same injected wake.
- Closing and reclaiming all three stores, reopening actual files and replaying
  committed entries preserves acknowledged results and fresh store sessions.
  The stopped follower initially lacks the final operation; the durable voters
  retain it and bring the follower up to date after restart. Original retries
  still return 7 and 15, and a new command advances each recovered value once.
- A focused native integration test drives automatic campaign deadlines through
  `submit_for_timed`/`apply_to_timed`, preserving the written/durable distinction
  and owner timer bookkeeping.

This history uses an ordinary three-voter majority and an in-process bounded
test transport. It does not establish native socket security, asynchronous
snapshot installation, full recursive-policy integration, power-cut behavior,
production performance or macOS execution. Existing recursive quorum and crash
histories remain separate evidence. The full P0–P7 goal remains active.

## Slice 10: bounded outbound ownership and dispatch

The version-1 public `OutboundQueue` contract admits owned same-peer batches,
dispatches them to a caller-driven transport, and retains credits until a local
completion consumes the batch after transport buffer release. Rejection returns
all messages. Node and peer budgets bound batches, message counts and retained
capacity bytes across queued and dispatched work. Command and snapshot traffic
cannot consume control reserves; snapshots also have a separate ceiling. Mixed
batches use the most restrictive payload class. Construction rejects reserves
too small for a basic control/no-op batch.

`NativeOutbound` constructs no sockets, threads or executors. It visits peers
round-robin and each peer's control/data/background queues with two control
turns, one data turn and one background turn. FIFO holds within a peer/class;
priority scheduling may reorder different classes. Empty peers are removed
after their final completion. Retained peer/class queue capacities have finite
construction-time bounds; capacity-byte accounting is not an exact RSS promise.
Ingress and outbound now share the same retained-message accounting, including
command vector capacity and bounded snapshot policy/index traversal.

Send tickets are local admissions, never durable prefixes. Their scope is a
node, recovered store session and fresh construction-time outbound generation.
The host must not reuse that generation within the session. Unknown, stale,
wrong-peer or undispatched completions cannot release native queue credits.
Local sent/failed/cancelled completion makes no claim about remote delivery or
durable voter state and has no path into the core's quorum bookkeeping. Only
received protocol acknowledgements count there. The native queue retains no
application delivery log; after process loss, recovery and protocol retries use
the authoritative Raft log.

Close rejects admission while accepted work drains. Abandoning an in-flight
batch leaves credits held; callers must resolve external I/O and return its
terminal completion. A queue cannot shut down host networking resources.
Caller-retained rejected effects, encoded buffers, remote receive queues and
application results require separate budgets. Production effect staging before
an owner step and authenticated transport assembly remain pending.

### Slice 10 validation

Linux, Rust 1.98.1, 8 October 2026:

- Native suite: 111 tests pass; core/host-only suite: 52 tests pass. Both builds
  pass Clippy with warnings denied; formatting and documentation pass.
- Public conformance runs with the native provider and an independent FIFO host
  replacement. It covers peer saturation, control admission, byte reserves,
  snapshot ceilings, retained vector capacities, complete-batch rejection,
  retained in-flight credits, invalid construction and exact terminal scope.
- Native dispatch exercises peer fairness and bulk service under a control
  backlog. Sequential use of more peers than the roster ceiling verifies that
  empty peer metadata is reclaimed. Closing one instance leaves another usable.
- The three-node/100-group worker histories now inject host/native outbound
  providers and deliberately saturate their budgets. Rejected sends remain
  owned for retry. Local sends complete before bounded in-process delivery;
  duplicate traffic, leader replacement and actual-file restart still preserve
  acknowledged state and original retry outcomes.
- An isolated leader locally sends an uncommitted command and read probe;
  neither obtains quorum success. Healing replaces its uncommitted suffix and
  brings it up to date from the surviving durable majority.

This is outbound admission and scheduling, not a production network transport.
Tests retain separately bounded effect/network queues. Socket/session security,
wire decoding, asynchronous snapshot workers, recursive-policy assembly and
macOS execution remain outstanding. Full P0–P7 remains active.

## Slice 11: bounded native peer frames

The public `WireCodec` seam covers fixed-prefix inspection, bounded same-peer
batch encoding and exact-frame decoding. `NativeWireCodec` supplies independent
wire format 1 for every current RPC, including recursive snapshot policies.
The complete layout is committed in [WIRE_FORMAT.md](WIRE_FORMAT.md); persistent
WAL and snapshot-file codecs are not its serialization format dependencies.
Unknown wire versions, flags and mandatory kinds fail explicitly.

Encoding first measures and validates without a frame buffer, then requests
exactly the final capacity. Decoding validates the fixed 24-byte prefix and its
length/count ceilings before frame-buffer allocation by the caller. It then
checks exact length and integrity, validates peer scope, and charges decoded
arrays, payloads, snapshot objects and policy index units before allocating them.
Counts also have minimum remaining-input checks. No partial message batch escapes.
The retained-object budget is not an exact allocator/peak-RSS promise. The host
separately budgets partial stream buffers, encoded output and decoded ingress.

`WireScope` is supplied from an authenticated connection, not derived from
untrusted decoded claims. Codec checks bind messages to that expected sender
node/store session and recipient, but do not perform authentication themselves.
The frame's CRC32C is integrity only. Wire decoding creates no durable prefix,
read authority or new generation. Existing message term/configuration/context,
group/store incarnations and recovered store sessions remain subject to core
checks. Local snapshot installation still needs its original data, pin, log
and application dependencies. Native frames transfer bounded complete snapshots;
wire-level snapshot chunking remains a future extension.

### Slice 11 validation

Linux, Rust 1.98.1, 8 October 2026:

- Native suite: 117 tests pass; core/host-only suite: 53 tests pass. Both builds
  pass Clippy with warnings denied; formatting and documentation pass.
- Native round trips cover every RPC, mixed groups in one peer frame, command
  and no-op entries, original remote request contexts, recursive weighted
  snapshot policies and canonical re-encoding. A fixed 161-byte read-probe
  layout anchors the prefix/envelope/RPC offsets.
- Every single-bit mutation and truncation of that frame is rejected, together
  with trailing input, unknown versions/flags/kinds and malformed booleans.
  Independently recomputed checksums cannot hide oversized counts/lengths,
  zero identities, wrong peer sessions, invalid entry order/terms, duplicate
  voter leaves/store keys, overflowing weights or excessive policy depth.
- Frame/message/entry/command/snapshot and decoded-retention ceilings are
  exercised on encoding and decoding. Tiny valid limits still allow a probe;
  unsupported construction limits fail before allocating a codec resource.
- The real-file three-node/100-group asynchronous WAL/outbound history now
  traverses encoded frames while preserving overload retries, fresh reads,
  leader replacement, recovery and original operation results. Snapshot catch-up
  histories traverse frames too, including the nine-voter recursive policy and
  actual native snapshot/WAL files.
- A host-only single-fixture codec demonstrates public selection with native
  features disabled. It is explicitly not evidence of a second full wire codec.

These remain in-process simulated connections with trusted scopes supplied by
the driver. No secure socket, session authentication, wire negotiation handshake,
partial-stream transport, production effect staging or asynchronous snapshot
worker has been implemented. macOS execution and hardware power cuts remain
unobserved. Full P0–P7 remains active.

## Slice 12: authenticated native TLS sessions

The public `secure::SecureSession` seam now separates nonblocking authenticated
channel I/O from Raft framing and delivery. `NativeTlsSession` uses exactly
pinned Rustls 0.23.45 with its explicit ring provider, TLS 1.3, strict mutual
certificate authentication, DNS verification on clients and exact peer leaf
certificate pins. No global provider, runtime, listener or pool is installed.
The optional default `tls` feature implies `native`; core/host-only and
native-without-TLS builds retain no third-party runtime dependencies. This is a
deliberate dependency exception for established cryptography, not a homemade
security protocol. Cargo.lock pins the selected provider dependency closure.

The host supplies credentials and trusted certificate-to-node/store mappings.
A fixed authenticated hello binds the peer's recovered store session before
any application plaintext escapes. The owner supplies fresh connection
generations within its recovered local store session. These are connection
identities, not durable log evidence. No session completion permits a Raft
acknowledgement, committed operation or read result; those retain their existing
exact log/barrier/quorum dependencies. The full identity, lifecycle and resource
contract is in [SECURE_SESSIONS.md](SECURE_SESSIONS.md).

Poll limits external I/O calls and each direction's byte progress. Handshake
byte/time ceilings, fixed hello buffers and finite credential inputs bound
admission work. Rustls' application write buffer ceiling is not a total TLS
memory cap; its separate parser bounds and plaintext backpressure remain part
of the resource model. Hosts must separately budget concurrent channels, socket
buffers, partial frames, encoded output and ingress retention. Short I/O and
WouldBlock retain ownership. Clean close drains accepted channel output and
preserves decrypted input; truncated EOF, identity mismatch, backward time and
revocation latch failure. Rotation creates another authenticated connection.
`require_authenticated` supports trait objects and rejects simulator-only or
not-ready providers. Host replacements attest their security capability.

### Slice 12 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full local suites pass: 129 tests with default native/TLS features, 119 with
  native storage but no TLS, and 55 core/host-only tests. All three builds pass
  Clippy with warnings denied. Formatting and documentation pass.
- Actual mutual TLS over bounded memory streams exercises seven-byte I/O,
  Interrupted/WouldBlock, per-poll budgets, a 128-byte application send buffer,
  backpressure and exact 8192-byte plaintext transfer without hello leakage.
- A real loopback TCP/TLS connection carries twenty groups in one native wire
  frame. Reconnection uses fresh connection generations and a changed recovered
  peer store session. Closing that connection leaves a second live connection
  on the same configurations/listener usable.
- Negative tests cover missing client certificates, an untrusted root, wrong
  DNS name, a different otherwise trusted certificate pin, wrong node/store
  incarnation claims, premature plaintext access, malformed credentials,
  invalid limits and owned-handle cleanup on failed construction.
- An incomplete fragmented ClientHello consumes exactly its 1024-byte
  configured ciphertext allowance before failure. Virtual handshake timeout,
  backward time, zero poll budget, clean close with pending data, truncated
  disconnect and latched revocation are exercised. Terminal I/O reclamation is
  one-use. The host-only simulator fixture is rejected by production admission.

These tests validate channel authentication and lifecycle, not a complete
networked consensus service. Three-node/100-group durable histories still use
simulated connections. Partial-frame transport, production effect staging,
reconnect policy and asynchronous snapshot workers remain pending. macOS
execution and hardware power cuts remain unobserved. Full P0–P7 remains active.

## Slice 13: bounded authenticated peer transport

The public `transport::PeerTransport` contract now owns one authenticated peer
stream, multiplexing groups in each bounded frame. `NativePeerTransport<S, C>`
uses the public session and codec contracts. Construction copies the selected
outbound queue's exact binding and limits; it does not own or create another
queue. Ready authenticated identity, wire version and resource compatibility
are required before traffic. The full resource and lifecycle contract is in
[TRANSPORT.md](TRANSPORT.md).

One dispatched outbound batch occupies the send slot through its unobserved
terminal completion. It retains original messages and vector capacity, together
with a separately bounded encoded buffer. Short writes advance an exact offset.
Only full acceptance and drained local channel output release that buffer and
produce Sent. Failure first drops the channel and partial buffers, then returns
the original batch as Failed with unknown remote delivery. The owner consumes
that exact batch through the original outbound queue to release its credits.
Neither event is a Raft acknowledgement or durable token. Existing persistence,
quorum, term/configuration/incarnation and read-barrier checks still control
protocol effects and application results.

Receive reads only the fixed bounded codec prefix before reserving its declared
frame. Complete decode validates the authenticated scope and retained-object
ceiling before publishing one batch. A held receive blocks further frame reads
while sends can continue. Complete receive/send slots remain available after a
later failure; partial frames never escape. Session identity/generation changes
fail the handle. Owners allocate fresh connection generations and reject
obsolete bindings before ingress. No new persisted watermark or consensus
generation is introduced by this driver.

Session I/O and plaintext visits have separate call/byte budgets. Alternating
read/write preference supports single-call visits; WouldBlock stops the blocked
direction for that visit. Close rejects sends, finishes the accepted send and
closes the owned channel, discarding partial receive work. Abort releases only
this logical channel and conservatively fails an accepted send. Original
outbound retention, TLS/socket memory, concurrent connections and ingress are
separate budgets. The driver creates no hidden threads, runtime or listeners.

### Slice 13 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full local suites pass: 141 default native/TLS tests, 130 native-without-TLS
  tests and 56 core/host-only tests. All three builds pass Clippy with warnings
  denied; formatting and documentation pass.
- Injected trusted-host channels exercise seven-byte short I/O, separate poll
  budgets, retained queue credits, exact terminal ownership, unobserved-result
  backpressure, delayed channel flush and failed writes. Their authentication
  capability is test attestation, not evidence of encryption.
- Holding a decoded batch stops the next frame while opposite-direction sends
  still complete. Zero-budget polling performs no plaintext I/O. Single-call
  visits with simultaneous sends preserve progress in both directions.
- Invalid prefixes, checksum corruption, partial-frame EOF, changed connection
  generation, stale outbound binding, excessive original vector capacity,
  insecure providers and incompatible buffer limits fail without partial
  message delivery. Failed construction releases its owned logical channel.
- Local close drains an accepted send; abort returns it once as Failed and
  releases frame/channel buffers. Already decoded input survives later abort.
  Queue credits remain charged until the owner consumes the returned batch.
- Actual TCP/TLS exchanges simultaneous 100-group batches plus a bounded
  snapshot carrying a nine-voter recursive policy. Snapshot installation
  durability remains covered by its separate existing history.
- The real-file three-node/100-group WAL-worker history now selects boxed
  `PeerTransport` instances backed by native TLS sockets. It preserves delayed
  durability isolation, overload retries, exact application retries, fresh
  reads, an isolated old leader's uncommitted write/read, replacement, healing
  and recovery with fresh persisted store sessions. A bounded staging queue
  retains transport admission rejections; decoded ingress reserves a full batch
  before transfer. Accepted/received frame accounting prevents the test driver
  from treating locally drained sockets as remote quiescence.
- Native-without-TLS and core-only histories retain their host/simulated paths.
  A core-only fixture demonstrates object-safe transport selection, not a full
  alternative production provider. An injected host queue and single-fixture
  wire version 37 codec compose with the native driver, including frames whose
  entire contents fit in their fixed prefix. No full alternative codec claim.

Partition and duplicate faults in the worker history are injected after decode;
this is not kernel-level network fault simulation. A production node owner,
global peer roster/reconnect policy, shared effect/ingress staging, integrated
automatic timers and asynchronous snapshot workers remain pending. The snapshot
installation history still uses simulated connections. macOS execution and
hardware power cuts remain unobserved. Full P0–P7 remains active.

## Slice 14: reserved effect ownership

`runtime::EffectOwner<Q, T, E>` now owns a quiescent timed shard and coordinates
bounded effect lifetimes through selected public scheduler/timer/entropy and
persistence-worker seams. This fixed coordination layer creates no hidden
runtime or I/O resources. Its worker binding includes the exact store session
and worker generation. See [the contract](EFFECT_OWNER.md).

Each visit reserves a conservative capacity bound before executing one event.
The bound includes retained log payload capacities, bounded replication fan-out,
input and metadata slack; core, application, worker and transport memory remain
separately budgeted. Bulk visits and extensions cannot consume control reserves.
One lease per group preserves ingress and output credits until completion or
accepted transfer. Rejection returns original persistence leases. Worker request
metadata validates the exact visit set and Written-before-Durable order before
core delivery. Only existing validated durable tokens release dependent effects;
no quorum rule, consensus generation or persisted watermark changes.

Committed output requires application catch-up; ReadReady consumes its original
one-use core barrier before the completion callback. Callbacks run serialized and
must be nonblocking. Oversized retained output, provider failures and invalid
completion order fence the owner. External leases remain charged until explicit
failed-owner discard; accepted storage still requires worker drain/recovery.
Closing stops ingress and timers while accepted healthy work drains.

### Slice 14 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full local suites pass: 151 default native/TLS tests, 140 native-without-TLS
  tests and 65 core/host-only tests. All three feature sets pass Clippy with
  warnings denied; formatting and documentation pass.
- Host-provider cases cover automatic election, rejection ownership, exact
  written/durable stages, duplicate and stale completions, delayed-group
  isolation, application catch-up, one-use reads, reserved control visits,
  reservation extension, oversized callback capacity, failure discard and drain.
- The three-node/100-group owner history uses actual native WAL workers and,
  with default features, native framed TCP/TLS connections. It checks exact
  operation retries, fresh reads, an isolated old leader's uncommitted write,
  replacement, healing and actual-file restart with fresh store sessions.
- This history selects injected host scheduler/timer/entropy providers and
  explicit campaigns; automatic election is tested separately. Native runtime
  providers retain their earlier conformance tests. The no-TLS history uses
  bounded simulated delivery. Network isolation is injected after decode.

This is a production coordination component, not a complete node facade.
Snapshot effects can remain leased, but asynchronous native snapshot work and
installation remain pending. Application assertions depend on the selected
state-machine contract; host result admission is separate. macOS execution,
hardware power cuts, physical WAL cleaning and the later P0–P7 protocols remain
unobserved or unimplemented. Full P0–P7 remains active.

## Slice 15: asynchronous snapshot publication and installation

`SnapshotWorker` now exposes owned Publish and Load requests with exact runtime
visits and a distinct snapshot-worker generation scoped to the recovered WAL
session. Native snapshot handles retain their separate persisted sessions.
`NativeSnapshotWorker<S>` owns a bounded selected group map on one explicit
thread. It never owns the log store or application, and uses the same public
snapshot-retention seam as host providers. Construction validates identities,
limits and output allowances before starting the thread. No dependencies or
persisted formats change. See [the contract](SNAPSHOT_WORKER.md).

Request and capacity-byte credits include original payload capacities and a
finite loaded-image allowance; they remain charged through terminal observation.
Bulk send loads cannot consume reserved installation/control capacity. Rejection
returns original work. One accepted request per group includes unpolled terminal
results. Errors fence further work on this snapshot worker; accepted requests
receive explicit failure. Panic/disconnection reports unknown accepted outcomes.
Explicit close/drain/reclaim returns selected handles without closing host resources.

Owner-side helpers validate binding, the original staged effect and application
checkpoint before preparing publication. Publish reconciles the current durable
anchor, chunks/seals/publishes, pins and verifies the image. Its completion
produces only Persist. The selected WAL worker's exact durable completion then
produces SnapshotInstalled. An installation Load verifies and reconciles that
durable reference; owner completion restores a clone, replays the committed tail
and calls the existing core application-install transition before SnapshotAck.
A send Load preserves pins and uses the original leader request context.
`EffectOwner::complete_effect_with` inspects its original owned effect without
cloning a snapshot merely to complete its lease.

No new effect or persisted watermark is introduced. Existing snapshot references,
log admission/durability tickets and core request contexts retain authority.
Worker request sequences identify accepted work only. Host assembly must retain
a bounded one-use admission-to-lease map, reject obsolete/duplicate events and
reserve loaded-image space before polling; helpers do not replace that mapping.
Provider buffer/codec scratch and application clones remain separately budgeted.

### Slice 15 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full local suites pass: 161 default native/TLS tests, 150 native-without-TLS
  tests and 68 core/host-only tests. All three feature sets pass Clippy with
  warnings denied; formatting and documentation pass.
- Downstream selection covers an object-safe host snapshot worker, rejection
  ownership, zero-limit polling, stale generation rejection and failure fencing.
  Native worker tests use host snapshot stores, shared group requests, exact
  terminal ordering, unpolled credit retention, control reserves, excess retained
  capacity, invalid construction and close/reclaim. A failed request returns
  failure for every already accepted group without publishing later work.
- A pinned asynchronous load supplies an actual current leader snapshot request
  through its original context. Missing retention reconciliation and invalid
  application bytes fail before application replacement or acknowledgement.
- The real-file installation history composes native ready/timer/entropy
  providers, EffectOwner, snapshot thread and WAL thread. Snapshot publication
  produces only Persist; Written releases no effect; Durable produces only
  SnapshotInstalled; verified application completion finally produces SnapshotAck.
  Restart covers loss before WAL submission, loss after WAL durability but before
  application completion, and completed installation. Recovery preserves value
  and exact operation deduplication, discarding publications without a WAL anchor.
- A thread-safe native-storage crash model injects short prefix/chunk writes,
  failed and lost syncs, failed and lost publication/pin receipts, and provider
  panic. No failure advances core commitment or application state. Recovery uses
  the authoritative WAL anchor and discards orphan publications/pins. Earlier
  synchronous snapshot tests retain exhaustive corruption/interruption coverage.

The new real-file history directly injects a protocol message; it is not a
network/automatic-election history. Broader TCP/TLS and 100-group histories still
pass, but snapshot catch-up through this worker has not yet been integrated into
them. Checkpoint creation and compaction still use quiescent synchronous handles.
Full node admission, reconnect/result routing, physical WAL cleaning, membership
and later lifecycle protocols remain pending. macOS execution and hardware power
cuts remain unobserved. Full P0–P7 remains active.

## Slice 16: bounded snapshot lease routing and native network histories

`runtime::SnapshotRouter` now owns a bounded one-use map between accepted
snapshot requests and original effect leases. It selects one exact runtime owner
and snapshot-worker binding at construction and creates no I/O resources.
`SnapshotWorker::load_reservation(group)` exposes a stable selected-group output
allowance. The router validates live leases, worker identity and image ceilings,
then reserves queued/original effects plus image/envelope space before preparing
or submitting work. Retries reserve a minimum rather than repeatedly charging
the same rejected lease. Permanently incompatible reservations return a size
error; temporary shared saturation returns overload. See [the contract](SNAPSHOT_ROUTER.md).

Worker rejection returns the original lease. Accepted tickets must use the exact
binding and strictly increasing nonzero sequence, allowing gaps for shared owners.
Unknown, repeated, old-generation or wrong-visit completions are rejected before
core access. The saved allowance bounds returned image capacities. Delivery uses
the existing snapshot helper and serialized effect-owner completion; publication,
WAL durability and application restore remain separate dependencies. No persisted
format, consensus generation, quorum rule, effect or watermark changes.

Invalid accepted tickets fence service but return the original effect lease for
explicit discard; accepted prepared work can still have an unknown outcome.
Storage/install failure drops and discards the current router-owned lease after
fencing the owner. Other accepted leases remain charged until explicit failed
owner discard. Closing ingress still permits accepted publication's subsequent
WAL and application-installation work; providers close only after this chain drains.

### Slice 16 validation

Linux, Rust 1.98.1, 8 October 2026:

- Full local suites pass: 169 default native/TLS tests, 158 native-without-TLS
  tests and 74 core/host-only tests. All three feature sets pass Clippy with
  warnings denied; formatting and documentation pass.
- Core-only routing cases cover original rejection ownership, unchanged reservation
  on retry, bounded request maps, selected image limits, permanently excessive
  effect-owner reservations, wrong worker/runtime owner, stale generation/visit,
  duplicate terminal delivery, invalid accepted tickets, oversized provider output,
  failure discard and healthy shutdown through publication, WAL and application.
- The three-node/100-group snapshot history now uses native ready/timer/entropy
  providers, the effect owner/router, actual shared WAL and snapshot threads,
  bounded outbound queues and actual TCP/TLS transports with default features.
  Two replicas start from explicitly pre-seeded committed counter checkpoints
  and pinned logical compaction; a fresh third installs all 100 snapshots through
  the worker/router/network before ordinary replication. It checks retries,
  fresh reads, native timer-triggered heartbeat traffic, actual-file restart with
  fresh store sessions, restored deduplication and further replicated writes.
  Its elections remain explicit campaigns. Pre-seeding is fixture setup, not
  evidence that those initial commands were committed over the network.
- This history initially exposed outbound snapshot overload. The driver now
  retains original rejected Send messages under their original effect tickets,
  preserving the configured limits. It also bounds rejected snapshot staging by
  live leases, reserves decoded ingress and accounts for every external lease's
  bounded holder. Accepted/received frame counts prevent false network quiescence.
- A separate native three-node/100-group history uses timer-driven initial
  elections, isolates the node with the most leaders, replaces those leaders
  through the surviving quorum, then heals. Isolated uncommitted writes never
  enter the final applied history. It verifies exact retries and fresh reads
  after healing. Default features use actual TCP/TLS; partition injection happens
  after receive decoding. This is one bounded seeded schedule, not the full
  network/storage fault matrix or a formal proof.
- Final full-suite runs exposed a filesystem-fixture race in the existing
  abrupt-exit ballot test: process creation can temporarily inherit another
  test thread's locked descriptor before exec. The three filesystem ballot
  fixtures now coordinate around process creation/reopen. Native nonblocking
  exclusive locking is unchanged; this does not serialize unrelated tests.
- The earlier 100-group partition/replacement/file-recovery history now also
  selects native runtime providers. Host provider conformance remains separate.
  Native-without-TLS uses bounded simulated message delivery in these histories;
  core-only builds exercise injected providers without native threads or sockets.

The assembly drivers above are integration harnesses, not a shipped full node
facade. Peer roster/reconnect policy, application/result admission and broader
fault schedules remain pending. Checkpoint creation and compaction still require
quiescent synchronous store access; long-lived asynchronous maintenance remains
necessary. Physical WAL cleaning, membership, recursive responsibilities and
split/merge protocols remain unfinished. macOS execution and hardware power cuts
remain unobserved. Full P0–P7 remains active.

## Slice 17: asynchronous local checkpoint maintenance

`Event::Checkpoint` now enters bounded background ingress and emits a local
CheckpointRequired effect with an existing checked RequestContext. The context
is scoped to group and recovered WAL session, not a new durable generation.
`SnapshotWorker::checkpoint_bytes(group)` declares the selected store's stable
serialization ceiling. The router reserves image capacity before preparing work;
the owner serializes and validates restore on an application clone. Only a
contiguous applied prefix strictly beyond the old base and at most the committed
prefix is eligible. All file I/O remains on the selected worker.

The same native Publish path reconciles the old WAL anchor, stages/seals/publishes,
pins and verifies the image. Its exact Published completion permits only Persist.
The WAL worker's Written event releases nothing. Exact durable anchoring advances
the logical base and emits CheckpointCompacted, retaining the suspended visit.
The new Reconcile job loads/verifies the pinned image, then durably reconciles
retention against that authoritative anchor. Only its exact Reconciled completion
finishes maintenance and refreshes leader replication requests. Other groups
remain schedulable. Maintenance produces no client result or quorum evidence.

No native file format, quorum rule, durable watermark or persisted generation
changes. Restart uses the authoritative WAL snapshot reference and existing pin
manifest. Losing observation before WAL anchoring retains the old image and
required log tail; losing it after anchoring restores the new image. An uncertain
retention error fences the live owner and requires recovery. Synchronous
checkpoint/compaction helpers remain available for quiescent callers.

### Slice 17 validation

- Full local suites passed with 173 tests for default native/TLS, 162 for
  native-only and 76 for core/host-only. Clippy passes all three feature sets
  with warnings denied; formatting and documentation builds pass.
- Public-interface tests reject stale contexts/bindings, insufficient applied
  progress, serialization ceilings, foreign publication references and incorrect
  retention completion kinds before releasing their dependent stage.
- Actual-file recovery covers five receipt-loss boundaries: publication before
  owner delivery, before WAL submission, after durable WAL compaction, after
  retention work without delivery, and normal completion. It starts with an
  older pinned checkpoint and committed replay tail, verifies the correct old/new
  base, checks old pin retention/release, restores exact state and preserves retries.
  Another group completes a durable election while one checkpoint receipt is held.
- Native snapshot storage injects failed and lost reconciliation-manifest
  receipts. Both failures fence the core; recovery restores the durable WAL anchor.
- The native three-node/100-group history now repeats checkpoints across all
  replicas while both workers retain their storage handles, continues replicated
  writes/retries/reads, recovers actual files, and repeats maintenance. Defaults
  use actual loopback TCP/TLS; native-only uses bounded simulated delivery.
  Admissions occur in bounded waves within background ingress ceilings.

Physical WAL reclamation, full node facade, peer roster/reconnect management,
application result admission, membership, recursive responsibilities and
split/merge remain unfinished. macOS execution and hardware power cuts remain
unobserved. The full P0–P7 goal remains active.

## Slice 18: bounded peer roster and reconnect coordination

`transport::PeerRoster<P: PeerTransport>` now owns the construction-authorized
remote-node/store map and selected peer connection handles. It reserves declared
transport frame/decoded ceilings before issuing ConnectTickets, caps concurrent
connection attempts, checks exact ready-result identities and security capability,
and fairly visits connections with the existing bounded session/plaintext budgets.
`PeerTransport::security` exposes production/simulator capability; boxed providers
forward the same public interface. The native network histories use this fixed
coordinator over NativePeerTransport rather than bypassing it with a raw map.

The host supplies a disjoint inclusive SecureSessionGeneration range per roster
within the recovered local WAL session. Checked ticket allocation stays within
that range. Drained replacement can reclaim the next unused generation; restart
changes the persisted WAL session before service. No new persistent generation,
durability token, quorum rule or remote progress watermark is introduced. Local
transport status never establishes replication or client success.

Failed/expired attempts use monotonic exponential retry backoff. `next_deadline`
exposes local scheduling hints without requiring a busy retry loop while capacity
is full. Remote authenticated sessions cannot regress within the roster. Failure
retires the connection, discards obsolete input and preserves its accepted send
and slot reservation until exact terminal consumption. The host completes that
returned batch through the original outbound queue to release separate credits.
Malformed completions fence the coordinator and return the offending payload;
explicit failed discard never allows that roster to resume.

Admission sequence is deliberately **not** used as dispatch order. Native
integration exposed that reserved priority scheduling can dispatch newer control
tickets ahead of older bulk work. Exact in-flight correlation is sufficient;
imposing an unrelated sequence watermark would incorrectly reject valid batches.
See [the peer-roster contract](PEER_ROSTER.md) for budgets, ownership, shutdown,
host generation allocation and connection-establishment obligations.

### Slice 18 validation

- Full local suites pass with 186 tests for default native/TLS, 175 for
  native-only and 89 for core/host-only. Clippy passes all three feature sets
  with warnings denied; formatting, documentation and contract JSON checks pass.
- Public host-provider tests cover authentication/identity/wire/limit rejection,
  fair visits, aggregate reservation, timeout/backoff, stale attempts, remote
  session rollback, exact failure ownership, invalid terminal completions,
  mis-scoped/excess-capacity input, priority reordering, close/drain, generation
  handoff and range exhaustion. They exercise the same public PeerTransport and
  OutboundQueue surfaces as the native assembly; their security attestation is
  a test assumption, not cryptographic evidence.
- All three native three-node/100-group effect-owner histories now use the
  roster with actual TCP/TLS under default features, alongside real WAL/snapshot
  workers. The snapshot/checkpoint history replaces one quiescent peer pair with
  fresh generations after the retry deadline, then continues automatic heartbeat
  traffic, replicated writes/retries/reads, repeated checkpoints, actual-file
  recovery and more replicated work. Native-only uses bounded simulated delivery
  and tests the roster separately. This does not cover every kernel fault or
  replacement with partially delivered frames.
- Native histories explicitly close every roster and poll TLS shutdown, checking
  that all connection reservations are released. Host replacement tests also
  accept bounded preallocated frame buffers rather than requiring native layout.

Socket/listener/dial execution, complete node ingress/result admission, physical
WAL cleaning, membership/policy changes, recursive responsibilities and split/merge
remain unfinished. Host-owned external handshakes must be canceled on expiry or
shutdown and remain under separate budgets. No production release, performance
claim, macOS execution or power-cut evidence is implied. Full P0–P7 remains active.

## Slice 19: bounded asynchronous native TCP dialing

`PeerDialer` is the public address-execution boundary, with host-selected endpoint
and channel types. `NativeTcpDialer` implements it using a fixed authorized
node/store map, the recovered local identity, existing `ConnectTicket` scopes,
request limits, per-call timeouts and a shared wake handle. It explicitly starts
one blocking connect worker and returns nonblocking TCP streams. No dependencies,
listener, TLS authority or consensus state are added to that worker. Dial results
are untrusted address outcomes; TLS authentication and exact roster attachment
remain required. No durability token or Raft acknowledgement follows from them.

Accepted requests retain credits across queueing, connecting and unpolled
completion. Exact-ticket cancellation skips queued network work, holds active
credits until the connect returns, and closes late/unobserved successful sockets
before releasing their slots. Generation floors increase per peer and are bounded
by the fixed peer map. Hosts reserve nonoverlapping ranges within a persisted
store session; restart requires a fresh session. Close stops admission and
cancels all accepted work; terminal polling and nonblocking join finish shutdown.
Worker panic/disconnection reports every outstanding ticket and stops admission.
See [the complete dialing contract](DIALING.md) for queue-delay, socket-budget and
drop limitations.

The shared TCP/TLS fixtures used by the native worker and three-node/100-group
effect-owner histories now dial through this public provider, then authenticate
and frame traffic as before. They drain/join the dial worker explicitly. Listener
acceptance and TLS driving remain fixture-owned; this is concrete dialing
integration, not a finished production connection owner or node service.

Validation on Linux:

- Full local suites pass: 195 default native/TLS tests, 184 native-only tests,
  and 91 core/host-only tests. The core-only public host provider uses a local
  channel type without native implementation dependencies.
- Clippy passes all three feature configurations with warnings denied;
  formatting, documentation, contract JSON and new RPL header checks pass.
- New tests cover real TCP success/nonblocking I/O, exact scope and rejection
  ownership, refusal, timeout/generation validation, completed-but-unpolled
  socket cancellation, and independent shutdown while sharing a host wake.
- Deterministic held-worker tests verify active/queued cancellation credit
  lifetime, skipping queued connects, drop cleanup, and exact terminal accounting
  after a worker panic. They do not rely on a nondeterministic unreachable address
  to simulate a slow kernel connect.

The timeout bounds one connect call and excludes queue delay; the future
connection owner must cancel on the roster's end-to-end deadline. Production
listener/routing/TLS-handshake ownership, decoded ingress/result admission,
physical WAL cleaning, membership/policy changes, recursive responsibilities
and split/merge remain unfinished. macOS execution and power-cut evidence remain
outstanding. Full P0–P7 stays active; CI remains background feedback.

## Slice 20: bounded native listener/routing/TLS ownership

`PeerConnector` now owns authenticated connection establishment over exact
roster tickets, caller-supplied monotonic deadlines and separately bounded
anonymous sockets. Its public associated endpoint/session types support host
replacement. `NativePeerConnector<D>` consumes a selected public `PeerDialer`,
optional caller-owned nonblocking listener, pinned TLS configuration and fixed
authorized peer map. It creates no implicit thread, listener or shared runtime.
Failed construction returns the live dialer/listener for explicit cleanup.

The native connector writes a fixed 16-byte `VBCONN01`/node-ID routing preface.
This untrusted hint can only select an existing authorized accept ticket;
Rustls must still validate the exact peer certificate and authenticated
node/store/session hello. No authority or durability token follows from the
preface or TCP connect. Only Ready authenticated sessions with exact
local/peer/store/generation/wire scopes can escape. The existing roster validates
attachment and remote store-session floors independently.

Attempt and anonymous expiry occur before I/O. Fair bounded polls separate
socket/preface, TLS and completion budgets. Exact-ticket cancellation closes
prefaces/handshakes and suppresses unpolled Ready outputs; an active dial retains
its slot until the selected provider's actual receipt. Deadline cancellation
includes dial queue time. The owner checks the injected provider's immutable
binding/limits, outstanding count and exact completions. Alien receipts stop
admission without releasing the real accepted ticket. Close drains outcomes and
returns the selected dialer for explicit native worker join. See
[connection establishment](CONNECTIONS.md) for caps, scheduling, compatibility
and broken-provider limitations.

The three-node/100-group effect-owner histories now keep one long-lived connector
per node. They reserve roster attempts, pass exact `attempt_deadline(ticket)`
values, choose a consistent dial/accept direction, poll native establishment and
attach returned sessions through public transports. Reconnection uses these same
live listeners/dialers with fresh generations, then continues actual
WAL/snapshot/checkpoint/write/read/recovery histories. Shutdown closes/drains every
connector and joins its dial thread before draining peer transports/workers.
Blocking fixture-owned TCP/TLS setup is no longer used for those histories.

New conformance tests cover an independent host connector/session type, native
three-peer shared listeners with tiny fair budgets, fragmented/invalid routing,
anonymous caps/expiry, a forged authorized hint with another trusted certificate,
scope/time/generation rejection, cancellation of stalled and unpolled Ready
sessions, retained active host-dial credits, returned failed-construction
resources, and mis-scoped host receipts. These are finite Linux tests, not a
complete kernel fault matrix or liveness proof.

Local validation passes 207 default native/TLS tests, 186 native-only tests and
93 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON and new RPL header
checks pass.

Complete decoded ingress/result admission, production reactor/facade, physical
WAL cleaning, membership/policy changes, recursive responsibilities and safe
split/merge remain unfinished. macOS execution and hardware power-cut evidence
remain outstanding. Full P0–P7 stays active; CI remains background feedback.

## Slice 21: bounded decoded-frame ingress ownership

Transport contract 2 adds immutable exact `received_info` metadata while retaining
wire format 1. `ReceivedBatch::info` validates identity/class/count/capacity;
`PeerRoster` validates inspection and compares eventual owned frames against it.
Host providers implement this same required method, including boxed forwarding.
The native provider exposes its one retained decoded frame without transferring it.

`runtime::IngressRouter` is a fixed ownership/identity guard over these public
peer providers and the existing serialized effect owner. It reserves
frame/message/byte/class credits before extraction. Overload leaves the frame in
the transport and prevents further decoding there. Control has explicit reserves;
background/snapshot traffic has a separate ceiling. Accepted frames retain their
full original charge across partial admission, including original spare vector
capacity and nested payloads. Fair frame/message scans retry overloaded groups
while allowing other groups to enter the owner.

Dispatch checks the runtime lifetime and current connection before each transfer.
Retired connections discard only still-held input; previously admitted events
remain runtime-owned. Unknown groups and other terminal admission rejections are
explicitly reported, not used to create replicas. Scoped checked ingress tickets
are allocation IDs, never durable maxima/prefixes. Close drains, and abort reports
remaining cancellations without rolling back runtime work. No new effect,
durability token, membership authority or client success follows from ingress.
All existing Raft persistence/application dependencies remain mandatory. See
[decoded ingress](INGRESS.md) for accounting, ordering, physical TCP head-of-line
limits, provider errors and shutdown semantics.

The default native three-node/100-group histories now route decoded TLS frames
through a per-node ingress router into the same effect owner that drives WAL,
snapshot and application work. Fixture-owned partition injection still discards
authenticated traffic explicitly; it is absent from the production router.
Quiescence and shutdown assert that all ingress charges drain. Native-only
histories retain their bounded simulated network, while the public ingress tests
also execute in core/host-only builds.

Seven new host-provider tests check retained full charges through blocked/partial
admission and retry, separate control/background budgets, no extraction on
overload or excessive spare capacity, old connection replacement with partial
admission, false metadata returning the original batch and fencing, wrong runtime
generation, explicit unknown-group rejection, abort counts, and independent
router shutdown over a shared owner/roster. These finite Linux checks do not prove
complete liveness, macOS behavior or power-cut durability.

Local validation passes 214 default native/TLS tests, 193 native-only tests and
100 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON and new RPL header
checks pass. Native socket tests ran with loopback access enabled after the
sandbox rejected listener creation in the first native-only run.

Complete client/dedup/result admission, production reactor/facade, physical WAL
cleanup, membership/policy changes, recursive responsibilities and safe split/merge
remain unfinished. Full P0–P7 stays active; CI remains background feedback.

## Slice 22: bounded committed application results

Optional application receipt contract 1 introduces `BoundedStateMachine` and
`ApplicationReceipt` over the existing application seam. Native Counter and host
applications declare output bounds and receipt identity/nested capacity through
the same public contract. Counter preallocates its exact command receipt vector;
its application/dedup/checkpoint semantics and persistent formats are unchanged.

`runtime::ApplicationRouter` binds one runtime and a fresh host-reserved router
generation. It validates live Committed leases, exact durable log entries, their
contiguous interval and the application's previous applied boundary. Count and
capacity credits are checked before calling apply on the serialized owner. Bulk
cannot consume command-free control reserves. Overload and other preflight
rejections return the original lease without invoking application. The native
three-node/100-group histories now route every live committed application batch
through this guard; startup checkpoint/replay uses its existing recovery contract.

Successful output must preserve command index/operation order, exact applied
boundary, declared vector capacity and nested capacity. Only then does the owner
release its Committed lease and the router publish an opaque result envelope.
Polling retains credits until the original envelope is consumed once. Wrong
router/runtime envelopes return intact. Application error, malformed output or
post-application release failure fences both intake and owner; partial state may
exist, so checkpoint/replay recovery resolves it. Invalid results never escape.
This creates no new effect/durability token, no proposal success, no client-delivery
evidence and no node-wide watermark. Tickets are allocation sequences scoped to
fresh router/runtime/store lifetimes. See [application results](APPLICATION_RESULTS.md).

Six host-only tests cover Written vs Durable, runtime/effect/log mismatch,
consumer-held charges, overload before apply, control reserve/unrelated-group
progress, exact cross-router ownership, retries/content conflicts, custom nested
receipts, malformed results, partial apply failure, byte ceilings and close/drain
over a shared application. A separate Counter test applies a full default maximum
log-sized commit and verifies its output fits the router defaults without growth.
Finite Linux tests do not establish macOS execution,
power-cut durability, liveness or performance.

Local validation passes 221 default native/TLS tests, 200 native-only tests and
107 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON, diff and new RPL
header checks pass. Socket histories ran with local loopback access enabled.

Client proposal correlation and dedup capacity reservation before commitment,
unknown outcomes, read-result admission, production reactor/facade, physical WAL
cleanup, membership/policy changes, recursive responsibilities and safe split/merge
remain unfinished. Full P0–P7 stays active; CI remains background feedback.

## Slice 23: client proposal admission and exact outcomes

Application admission contract 1 adds `ProposalAdmission`. Native Counter and
host applications validate commands and reserve dedup capacity against applied
state, unapplied durable log entries and all retained queued/in-flight commands.
Repeated operation IDs share capacity while content conflicts still produce the
existing deterministic application outcome. No format or checkpoint schema changes.

`admit_tracked` on the three runtime layers allocates exact scoped volatile input
tickets, carried through scheduling to Stepped/OwnerStep alongside the actual
proposed index/term from the Persist effect. Runtime priority and repeated
operation IDs cannot confuse invocations. These IDs/positions establish no new
durability, prefix, quorum or application evidence.

`ClientRouter` reserves bounded command/result/metadata space before ownership
transfer and preserves the original rejected request. A mandatory internal check
immediately before core proposal repeats current application/log/capacity and
receipt-bound validation. This closes the queued-admission gap across snapshot
installation, application catch-up and leadership changes. Admission errors
produce NotProposed for that invocation without generating Persist; direct low-level
embedding APIs remain explicitly host-budgeted alternatives, not the service path.

Applied replies require exact application-result ownership plus the tracked
group/index/term/operation match and retained-log content cross-check. Opaque
application outputs now retain pre-budgeted original verified positions, so
logical compaction cannot erase this evidence. Polling keeps charges through consumer
completion. Cancellation emits Unknown and retains queued capacity; fair
reconciliation transfers abandoned reservations only with exact execution/log
evidence or a failed owner. Abort fences the owner and does not cancel accepted
external WAL. Fresh router/runtime/store identities reject old observations.
See [client contracts](CLIENTS.md) for budgets, all-service-proposals requirements,
unknown outcomes, compacted correlation and shutdown.

Nine host-only tests cover queued retries/conflicts, dedup exhaustion,
cancellation reservation lifetime, exact positions and Written vs Applied,
runtime rejection ownership, per-group isolation, foreign completions,
close/abort, recovered unapplied capacity, host validation, wrong runtime,
execution-time rejection, growing output bounds and delayed replies after actual
pinned logical compaction. A separate host test checks
exact tracked-input stop results. The native 100-group histories
now submit client writes and consume their replies through these same contracts.
They assert applied replies for healthy writes, no applied replies under isolated
leader quorum loss, Unknown after replacement, and further writes/retries after
healing, snapshots/checkpoints and actual-file recovery.

Local validation passes 231 default native/TLS tests, 210 native-only tests and
117 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON, diff and new RPL header
checks pass. Native socket histories ran with loopback access enabled.

Read-result admission, production reactor/facade, physical WAL cleanup,
membership/policy changes, recursive responsibilities and safe split/merge remain
unfinished. Full P0–P7 stays active; CI remains background feedback. Finite Linux
checks do not establish macOS behavior, a full proof or performance.

## Slice 24: bounded read execution and result ownership

Optional application contract 1, `BoundedReadableStateMachine`, declares
query allocation capacity, result retention bounds and actual nested output
capacity. Counter and an independent downstream application implement the same
public contract. No wire, checkpoint, WAL or consensus protocol changes.

`ReadRouter` reserves node/per-group count and byte capacity before immutable
read execution, validates the exact live ReadReady lease, and consumes its
original one-use quorum barrier through the serialized EffectOwner immediately
before the application callback. Wrong scopes/effects, overload and application
catch-up return the original untouched query and lease. Invalid output bounds
fence the owner without publishing success; an application query error consumes
authority once and produces an owned error result. Defensive failures after
callback execution fence and discard the failed lease rather than pretending
the consumed query can be retried.

Opaque results retain the original ticket, effect and consumed barrier. Polling
does not release credits; only completion of the original envelope does.
Foreign completions return the envelope intact. Close drains accepted results,
and a valid read completed before a later owner failure remains consumable.
Fresh host-reserved router/runtime/store lifetimes reject obsolete observations.
Result sequences are volatile allocation identities, never protocol watermarks.
The barrier remains the existing contiguous committed read boundary, not a
clock lease or global timestamp. See [read result contracts](READ_RESULTS.md).

Six downstream conformance tests exercise original allocation ownership,
catch-up retry, node byte and per-group overload, unrelated group progress,
query errors, provider fencing, wrong runtime/effects, one-use authority,
foreign output ownership, consumer credit lifetime, close/drain and valid output
after a later failure. The three-node/100-group native WAL/TLS and native-only
histories now execute and consume their reads through this guard, including
quorum loss, replacement, reconnection, snapshot/checkpoint and file recovery.

Local validation passes 237 default/native/TLS tests, 216 native-only tests and
123 core/host-only tests. The expanded valid-output-after-failure regression also
passes. Clippy passes all three feature configurations with warnings denied;
formatting, documentation, contract JSON, diff and new RPL header checks pass.
Native socket histories ran with loopback access enabled.

The host still owns, budgets and correlates original read invocations before
ReadReady, handles read-step errors and cancels abandoned pending reads. A
production invocation facade/reactor must own those lifetimes next; this result
owner does not claim that missing integration. Physical WAL cleaning,
membership/policy transitions, recursive responsibilities, split/merge and P7
throughput evidence also remain unfinished. Full P0–P7 stays active. CI stays
background feedback. Finite Linux tests do not prove macOS behavior or the full
protocol across arbitrary schedules.

## Slice 25: original pending-read admission and cancellation

`ReadRequests<Q, R>` now owns the original query, tracked Read admission,
future result reservation and exact completion before quorum establishment.
Construction composes an explicitly selected empty/open ReadRouter, validates
owner and execution capacities, and returns that original execution owner on
failure. Native and downstream applications use BoundedReadableStateMachine;
there is no additional backend, thread, runtime, clock or application store.

Bounded node/per-group request and byte ceilings include pending queries and
consumer-held replies. The separate ReadInvocationUsage request gauge does not
pretend pending work is a completed result. One unresolved protocol read per
group is admitted, while completed outputs may remain held under the group's
retention budget. Admission rejection preserves the original query allocation
without spending a ticket. The checked request allocator uses the live core's
maximum accepted read-ID floor and service sequence; neither is a contiguous
protocol prefix or durability watermark. Restart and replacement use fresh
invocation/runtime/store lifetimes; no query or authority is replayed as a write.

Shared owner steps, including ClientRouter-driven steps, feed exact tracked
invocation/cancellation correlation before effect dispatch. ReadReady execution
selects only the original owned query, revalidates current bounds against the
original reservation, and passes the exact live lease through ReadRouter's
existing one-use quorum/application checks. A successful step, local send,
Written receipt or cached leader cannot produce Read. Query errors consume
authority once; provider capacity violations fence the owner. Output envelopes
remain opaque and charged through consumer completion, with foreign envelopes
returned intact and earlier valid results preserved across later failure.

The checked EffectOwner.cancel_read path now consumes an exact ready lease
without a query callback or application catch-up requirement. Otherwise fair
reconciliation queues tracked control cancellation only after the original data
Read step, preventing priority inversion from orphaning a read. Queued
cancellation remains charged through its exact step even when its ready lease
was already canceled; an obsolete request ID cannot clear a subsequent read.
Cancel/leadership/failure outcomes never roll back accepted WAL. Close drains
accepted work; explicit abort fences the runtime and preserves already completed
outcomes. No new durability effect, quorum predicate, WAL/wire format or checkpoint
schema is introduced. See [read invocation contracts](READ_REQUESTS.md).

Ten downstream tests cover original query/result identity, a different live
invocation's barrier, retained consumer credits, foreign completions, capacity
isolation, runtime rejection ownership, pre-step and lagging/ready cancellation,
queued cancellation against newer reads, bound growth, query/provider failures,
core read-ID floor/exhaustion, stale step rejection, explicit construction-owner
return, close/drain and abort retaining an earlier valid result. The actual
three-node/100-group native WAL/TLS history verifies 100 isolated reads return
no values under quorum loss, resolve unavailable after replacement, stay canceled
after healing, and permit fresh replacement-leader reads. Snapshot/checkpoint,
reconnect, automatic-election, file-recovery and native-only histories also use
the same original invocation/result owners.

Local validation passes 247 default/native/TLS tests, 226 native-only tests and
133 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON, diff and new RPL header
checks pass. Native socket histories ran with loopback access enabled.

Production reactor/facade assembly, physical WAL cleaning, membership/policy
transitions, recursive responsibilities, safe split/merge and P7 throughput
evidence remain unfinished. Full P0–P7 stays active; CI is background feedback.
Finite Linux histories do not establish macOS execution, arbitrary schedules or
production protocol/performance claims.

## Slice 26: bounded local replica driver

`ReplicaDriver` moves the native fixture's local drive loop into the library.
`ReplicaParts` borrows explicitly selected EffectOwner, PersistenceWorker,
OutboundQueue, application map, application/client/read owners and optional
SnapshotWorker/SnapshotRouter. Native and independent host providers use the
same path. Construction checks exact owner/provider/router lifetimes, quiescence,
local node, fixed group coverage and applications replayed to the recovered
commit boundary. No hidden runtime, clock, thread or provider is constructed.

Bounded polling delivers snapshot and Written/Durable completions, correlates
tracked input steps before effects, executes committed applications and original
reads, submits outgoing messages and snapshot work, and reconciles cancellation.
Separate bounded control/data persistence queues preserve worker control reserves.
Batch formation shares the worker's capacity-byte accounting and count/byte
ceilings. Rejections retain original leases and their EffectOwner reservation;
preallocated driver metadata and work budgets have separate limits. Consumer-held
client/read outputs retain their existing credits. Wrong bindings, regressed time
and invalid budgets reject before provider polling; fatal failures fence service.
Explicit failed cleanup releases only driver/router-owned work, with external
provider drain and authoritative recovery still the host's responsibility.

No new durability token, watermark, generation allocator, consensus rule, WAL
format or checkpoint schema is introduced. Exact durable barriers still permit
dependent protocol effects; local submission, Written and successful owner steps
cannot produce committed/application/read success. The three-node/100-group
native histories now use the library local driver through elections, quorum
loss, replacement connections, snapshot/checkpoint catch-up and file recovery.
Six independent host-component tests exercise exact client/read outcomes and
held credits, WAL overload and multi-group batching, Written-before-Durable,
binding/time/budget rejection, startup replay checks and failure/retained-lease
cleanup, including an invalid accepted outbound ticket whose transferred payload
remains provider-owned through explicit drain. See [local replica assembly](REPLICA_DRIVER.md).

Local validation passes the full matrix of 252 default/native/TLS tests, 231
native-only tests and 138 core/host-only tests. A sixth host-driver test added
after that matrix also passes in the focused six-test core run; the migrated
native histories passed in the full matrix. Clippy passes all three feature
configurations with warnings denied; formatting, documentation, contract JSON,
diff and new RPL header checks pass. Native socket tests ran with loopback access.

Full P0–P7 remains active. Peer network reactor/facade assembly, physical WAL
cleaning, membership/policy transitions, recursive responsibilities, safe
split/merge and P7 performance evidence remain unfinished. Linux execution does
not establish macOS execution or arbitrary-schedule/protocol/performance claims.

## Slice 27: bounded public peer reactor

`PeerDriver` now owns explicitly selected connector, transport factory, roster,
ingress and route instances. Construction checks exact local/runtime/outbound
bindings, authorized route coverage, compatible attempt timeouts and quiescent
components; rejection returns all original parts. `PeerTransportFactory` supplies
the session-to-transport assembly seam. NativeTransportFactory clones an
explicitly selected codec and uses NativePeerTransport, while independent host
factories use the same public contract. No hidden runtime, socket, clock, worker
or fallback provider is created by the driver.

The reactor fairly polls roster/transport progress and connector outcomes,
cancels exact expired attempts, validates live authenticated session scopes before
attachment, starts bounded attempts, completes local sends, preserves rejected
batches and reserves ingress before transfer. Canceled provider work retains its
driver slot until the actual terminal receipt, preventing replacement attempts
and generation consumption for that peer in the meantime. Capacity-only roster
filters preserve all existing authorization/deadline checks and omit blocked
waiting peers from retry deadline hints. Late returned sessions close without
attaching. Fresh persisted store sessions and existing disjoint roster generation
ranges remain authoritative; no new watermark, durability token or ID allocator
is introduced.

Preallocated staging covers the selected outbound node batch ceiling, preventing
a smaller local retry queue from allowing one stalled peer to block extraction of
other peer/control work. Original outbound payload/class/count/capacity-byte
charges survive rejection and retire only through exact local completion.
Budgets bound scans, I/O, connection receipts, sends/retries and ingress dispatch.
Provider/factory violations fence the owner and retain observed exceptional
payloads for explicit recovery; oversized provider returns are quarantined whole
after failure, not admitted as normally bounded work. Close drains accepted
network work without closing the host queue. A separate drain path cancels held
ingress and returns network buffers even after the local core has failed, without
accessing or admitting to that core. Full node shutdown still checks external
queues, local/service owners and workers, and joins owned native threads.

The native three-node/100-group histories now select both library drivers for
local and peer work, including automatic elections, partition/replacement/healing,
fresh connections, snapshots/checkpoints and actual-file recovery. Partition
injection lives solely in a test transport wrapper; the production reactor has
no test fault policy. Sixteen independent host tests cover attachment, stalled
peer isolation, original send credits, exact receipts, ingress admission,
canceled/late connections, generation retention, wrong bindings/time/budgets,
constructor resource return, provider/factory faults, quarantine and explicit
payload recovery, outstanding-work shutdown and failed-core drain. See
[peer reactor contracts](PEER_DRIVER.md).

Local validation passes 269 default/native/TLS tests, 248 native-only tests and
155 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON, diff and new RPL
header checks pass. Native socket histories ran with loopback access enabled.
No consensus/storage/wire/checkpoint format changes or performance claims are
introduced. These finite Linux runs do not establish macOS execution or arbitrary
schedules.

Full P0–P7 stays active. The native node facade and coordinated lifecycle,
physical WAL cleaning, membership/policy transitions, recursive responsibilities,
safe split/merge and P7 throughput evidence remain unfinished.

## Slice 28: owning node facade and coordinated lifecycle

`runtime::Node` now owns the selected local and peer provider assemblies and
composes ReplicaDriver with PeerDriver. NativeNode/NativeNodeParts/NativeLocalParts
aliases select the same public native implementations. Construction validates
quiescence, local/service/provider scopes, required snapshot support and configured
remote voter store coverage, returning all original parts on rejection. Hosts
still explicitly create/recover files, restore/replay applications, assemble
security/routing/resource configuration and start selected native workers.
Construction creates no hidden I/O, runtime, clock or fallback log.

The facade exposes original proposal/read admission, opaque reply polling and
completion, cancellation, bounded polling and explicit campaign/heartbeat/checkpoint
control. Both budgets and monotonic time prevalidate before either driver works.
Existing tracked operation/read identities, committed/application validation and
original capacity charges remain authoritative; no new durability token,
watermark, protocol generation or consensus/storage/wire format is introduced.

Healthy shutdown closes service intake first, waits for original accepted service
work and consumer-held replies, then closes owner admission/timers and peers.
Local/network/provider drain precedes closed handle reclamation. Explicit abort or
driver failure fences the owner, preserves earlier replies and outstanding
provider/driver ownership, and reports uncertain writes as Unknown. Failed nodes
cannot resume polling or masquerade as healthy drained nodes; into_recovery
returns original components for existing explicit drain/recovery contracts.
Handle drain does not imply native thread join. See [node lifecycle](NODE.md).

Nine independent host tests exercise exact held write/read outputs, constructor
component return, missing peers/snapshots and wrong voter stores, time/budget
prevalidation, unknown-group original input ownership, independent instances,
Written-before-Durable abort, rejected accepted-work retention, provider failure,
earlier outputs across abort and failed-core cleanup. A native three-node/100-group
history selects NativeNode with real WAL files, native workers and actual loopback
TCP/TLS. It exercises snapshot catch-up, writes, quorum reads, checkpoints, healthy
shutdown and explicit worker joins, actual-file recovery with fresh store sessions,
original dedup retries and further replication. Existing lower-driver histories
continue to cover partition/election/reconnect/failure schedules.

Local validation passes 279 default/native/TLS tests, 257 native-only tests and
164 core/host-only tests. Clippy passes all three feature configurations with
warnings denied; formatting, documentation, contract JSON, diff and new RPL header
checks pass. Socket histories ran with loopback access. These finite Linux runs
do not establish macOS execution, arbitrary schedules or performance claims.

Full P0–P7 stays active. Native filesystem/provider convenience assembly, physical
WAL cleaning, membership/policy transitions, recursive responsibilities, safe
split/merge and P7 throughput evidence remain unfinished.

## Slice 29: physical live-state WAL reclamation

The optional public `LogStore::reclaim(max_bytes)` now rewrites native WAL
history to a bounded, self-contained live-state checkpoint. It requires drained
transition tickets and identical accepted/durable state, preserves every current
group, voter binding, hard-state vote, exact revision/generation, snapshot
reference, committed prefix and surviving committed/uncommitted suffix, and
continues the original batch sequence. It advances no logical retention floor
and emits no new durability ticket. A bounded codec round-trip must reproduce the
entire state and sequence before I/O; non-shrinking images leave files untouched.

JournalIo is now a dedicated public native platform seam with optional pair
replacement. FileLogIo retains an exclusive lock across two bounded slots, syncs
replacement WAL/manifest and directory, writes/syncs/renames CURRENT and syncs the
directory again, then removes old files. Every uncertain I/O failure fences the
store until recovery. Recovery follows only the selected pair; corrupt or missing
selections/files cannot fall back to stale votes/history. Legacy journals still
open unchanged, and new ballot/log creation rejects existing replacement stores.
A failed first cleanup may leave legacy files; subsequent cleanup retires them.

The explicit version-1 VBLCPT01 live-state image precedes unchanged format-2
batches and preserves the exact high batch sequence. Canonical mandatory
bootstrap/snapshot/suffix checks validate recovery before exact stored counters
are restored. The full image may exceed one batch within the WAL budget. Old
binaries/codecs without image support cannot read cleaned files; unsupported
codecs/providers reject maintenance before work. See [physical cleanup](WAL_RECLAMATION.md).

Implementation decision: this first cleaner rewrites the complete bounded live
image and temporarily retains both old and replacement files. It does not yet
implement segmented incremental/throttled cleaning. Maintenance is synchronous on
an explicitly reclaimed store handle (or an externally selected storage worker),
with live NativeLogWorker scheduling still pending. Logical compaction continues
to require durable snapshot pins/application checkpoint/replay; no implicit backup
retention or future tombstone/lifecycle policy is invented.

Eight downstream tests cover exact votes/state/suffix/cold-group preservation,
sequence/session continuation and stale tickets, pending-work/budget/capability
rejection, non-shrinking images, image truncation/every-byte corruption and invalid
semantic state, mandatory validation of host-decoder results, images exceeding one
batch, repeated native slot reuse, exclusive
locks, missing/corrupt selected files and cross-store creation rejection. A native
file test injects failure after each of 13 publication/deletion boundaries for
both initial replacement and slot reuse (26 cuts), recovers exact state and cleans
again. The host journal models power loss before/after atomic publication; native
file interruption tests are process-level evidence, not hardware power-loss tests.

The actual three-node/100-group native WAL/TLS facade history now physically
reclaims its stores after coordinated shutdown and explicit worker joins. It
checks every exact group state before/after cleaning, reopens files, restores
application snapshots/deduplication, retries operations and replicates new writes.
Local validation passes 288 default/native/TLS tests, 266 native-only tests and
164 core/host-only tests. Clippy passes all three configurations with warnings
denied; formatting, documentation, contract JSON, diff and new RPL header checks
pass. These finite Linux histories do not establish macOS execution, arbitrary
schedules, live maintenance fairness or throughput claims.

Full P0–P7 stays active. Native provider convenience assembly, live WAL-worker
maintenance, incremental cleaning/general retention, membership/policy transitions,
recursive responsibilities, safe split/merge and P7 evidence remain unfinished.

## Slice 30: bounded maintenance inside selected WAL workers

The public PersistenceWorker contract now offers optional submit_reclaim and
poll_reclaims operations, with explicit unsupported defaults. LogStore exposes
its optional reclaim capability. NativeLogWorker captures the selected capability
and WAL ceiling, admits at most one cleanup request through its existing bounded
queue and checked worker sequence, and charges fixed request/result metadata to
data credits while preserving control reserves. ReclaimTicket is distinct from
persistence tickets and carries the exact store session/worker generation.

The existing WAL thread executes cleanup between complete append/barrier units.
Older Written/Durable receipts may remain owner-held because reclamation preserves
their state/IDs; later bounded accepted writes execute against the same binding
and revisions. No new runtime, store or worker is created. Pending/unpolled work
retains original credits, and scoped close drains both event classes before
explicit thread join/store return. Cleanup does not certify Raft persistence or
application success. No persistent format or logical watermark changes here.

Safe refusal preserves worker usability. Uncertain, corrupt and fenced results,
invalid reports or changed store bindings fence the worker and fail queued writes
without fabricated Written/Durable events. Panic/disconnection reports each
retained request once in its own event class. ReplicaDriver tracks one original
maintenance ticket/budget and one matched result, validates exact completion
scope/bounds, preserves an observed bad receipt for recovery and includes the
fixed metadata in construction/drain accounting. Fatal results fence the owner.
Node now exposes reclaim while Running and poll_reclaim through shutdown/recovery;
unconsumed maintenance results hold healthy drain open. Earlier service replies
and uncertain accepted writes retain their existing semantics. See
[live worker maintenance](WORKER_MAINTENANCE.md).

Four new host-facade tests cover held-result shutdown, unsupported/safe rejection
with continued proposals, wrong-receipt fencing and original ticket retention,
and uncertain cleanup preserving earlier replies and unknown pending writes.
Five native-worker tests inject an independent gated host LogStore and exercise
complete-barrier ordering, retained credits/close/join, control reserves,
capability/byte rejection without sequence consumption, safe refusal,
uncertain/invalid/binding-changing providers and panic recovery of both classes.
The actual three-node/100-group native WAL/TLS facade history now cleans inside
live selected workers and continues new writes before shutdown, actual-file
recovery, snapshot/dedup restoration, retries and further writes.

Local validation passes 297 default/native/TLS tests, 275 native-only tests and
168 core/host-only tests. Clippy passes all three configurations with warnings
denied; formatting, documentation, contract JSON, diff and new RPL header checks
pass. Native socket histories ran with loopback access. These finite Linux runs
do not establish macOS execution, arbitrary schedules or throughput claims.
Cleanup still pauses I/O on its selected WAL lane for a bounded full-image rewrite;
incremental cleaning, automatic scheduling/throttling and general backup retention
remain unfinished.

Full P0–P7 stays active. Native provider convenience assembly, incremental cleanup,
membership/policy transitions, recursive responsibilities, safe split/merge and
P7 evidence remain unfinished.

## Slice 31: durable learner/joint/final configuration journal

The quorum-use audit found static bootstrap references in elections, commitment,
read barriers, message membership/context checks, recovery and snapshots. Online
changes cannot safely be enabled by replacing only the election predicate.
This slice implements the durable journal those paths will consume, through the
existing public LogStore seam and mandatory shared transition validator. It does
not enable an incomplete live reconfiguration protocol.

`membership::Configuration` validates exact voter/store bindings and separate
learners with bounded policy and replica sets. Replicated configuration records
carry operation IDs, expected configuration IDs and learner/joint/final phases.
Learner edits preserve the voter policy. Joint promotion requires an exact-store
learner in the preceding committed configuration; retained voters cannot change
stores. Joint records immediately select old AND new predicates. Final records
match the joint operation and reserved target ID, require joint commitment and
immediately select new-only rules. Another transition waits for the previous
configuration record's commitment. Strictly increasing configuration IDs fence
distinct phases; operation IDs identify lifecycle work separately.

`GroupLog::membership` reconstructs accepted-log state from the surviving journal.
Uncommitted final rollback restores joint rules; uncommitted joint rollback
restores the preceding learner/stable state. Existing commit/suffix-generation
guards remain authoritative. Configuration data now counts toward retained
payload, range, effect, ingress and outbound budgets; metadata-bearing Append
input uses data capacity, preserving control reserves. The native format-2 WAL
adds configuration entry tag 2 with subformat version 1; old logs remain readable,
older binaries reject new records. Physical full-image reclamation preserves the
whole journal and its operation-reuse checks.

There are no new effects or durability domains. Existing exact LogTicket/barrier
dependencies guard exposure; a derived predicate is not a receipt. Snapshot
compaction refuses to discard configuration records pending configuration-base
format integration. The bootstrap ballot membership guard remains static.
Raft recovery/Append and native wire encoding explicitly reject configuration
entries while live quorum/snapshot integration remains incomplete. Host-injected
RPCs cannot bypass that refusal. See [configuration journal](CONFIGURATION_JOURNAL.md).

Seventeen downstream journal tests cover host/native logical transitions,
assignment and operation identity, overlap/early-final rejection, same-voter
recursive weighted changes, all 3,125 five-replica prefix assignments, Written
versus Durable, multi-group rejection, suffix rollback, snapshot refusal, bounded
range/ingress accounting and live-core/wire refusal. Native crash tests cut every
byte of learner, joint, final and rollback WAL frames, inject sync and manifest
publication failures, and reclaim/reopen joint/final histories. Two native codec
tests cover every record truncation, unknown tags, zero IDs, invalid counts,
duplicate identities and overlapping maps.

Full local suites pass 315 default/native/TLS tests, 294 native-only tests and
179 core/host-only tests. The subsequently added ingress-budget regression also
passes in focused runs of all 17 default and 11 core-only journal tests (316
distinct default tests in total). Clippy passes all three configurations with
warnings denied; formatting, documentation, contract JSON, diff and new RPL
header checks pass. Native socket histories ran with loopback access. These
finite Linux checks do not establish macOS execution, full joint Raft correctness,
real power-loss behavior or throughput.

Full P0–P7 remains active. Online learner catch-up/promotion, all-quorum activation,
configuration-aware snapshots, removed-replica service fencing and the formal
membership model/faulted network histories remain unfinished. Native provider
convenience assembly, incremental cleanup, recursive responsibilities, split/merge
and P7 evidence also remain outstanding.

## Slice 32: configuration-aware snapshot bases and recovery

Snapshots now retain the validated stable/joint membership at their included
boundary, alongside immutable bootstrap and application state. GroupLog and
LogUpdate carry the same optional boxed membership base; None denotes original
bootstrap. Snapshot references now name the included effective configuration,
which can differ from the current accepted suffix. Accepted-log replay starts
from this base, preserving joint rules after an uncommitted final is rolled back.
Final checkpoints retain new membership and lifecycle operation identities even
when all configuration entries have been compacted.

Shared logical validation requires exact base equality for matching prefixes.
Nonmatching snapshot installation cannot regress committed configuration identity
or index, or discard committed operation identities. A membership base without
its snapshot is rejected. Publication separately rejects configuration regression
and changed content under one ID; new configuration records must be beyond the
previous published boundary. Configuration operation retention is bounded to
16,384 identities; exhaustion refuses future transitions rather than evicting
retry evidence. Metadata byte limits can bind sooner.

Native VBSNAP02 preserves original bootstrap plus membership-checkpoint subformat
1 and application data under the existing checked root. Static VBSNAP01 remains
readable and is still emitted for static metadata. Snapshot codec capability 2
preserves membership and reads legacy data; capability 1 is restricted to static
metadata, rejecting unsupported publication/recovery before mutation. Native
format-2 WAL snapshot tag 3 carries the membership base in the atomic snapshot
update. Full-image reclaim restores and revalidates base, suffix and exact
counters. Old binaries fail closed on new mandatory data; downgrade needs migration.

Existing publish, durable pin, exact WAL barrier and reconciliation dependencies
remain authoritative. No new effects or durability domains are introduced.
Snapshot and log generations retain their existing reconstruction/stale-completion
rules. Membership allocations now count toward snapshot, ingress, effect and
persistence-worker budgets. The worker audit also fixed configuration-entry
payload accounting; configuration-bearing persistence uses data credits and
preserves control reserves. Static core recovery, Append and Snapshot paths and
native wire continue to refuse configuration-bearing state until live protocol
integration is complete. See [configuration snapshots](CONFIGURATION_SNAPSHOTS.md).

Ten new downstream snapshot tests cover host/native compaction through joint
rollback and later finalization, original application deduplication outcomes,
operation reuse after complete prefix compaction, forged/missing metadata,
publication regression, bounded operation retention, worker/ingress accounting,
legacy codec capability rejection, actual-file restart and full-image reclaim.
Restore, compaction, installation and send helpers validate the full membership
base, with a regression rejecting altered metadata under the same configuration ID
without advancing application state.
Native fault histories cut every snapshot-prefix and application/footer write,
fail sync/publication before/after durable selection, and cut every byte of a WAL
snapshot/base switch while both roots are pinned. A codec unit test checks every
membership-checkpoint truncation, versions/flags, count limits, changed operation
identity, boundary and trailing bytes.

Local suites pass 327 default/native/TLS tests, 305 native-only tests and 184
core/host-only tests. Clippy passes all three feature configurations with warnings
denied; formatting, docs, contract JSON and diff checks pass. Native loopback
histories ran locally. These finite Linux checks do not establish macOS execution,
full joint Raft correctness, power-loss certification or throughput.

Full P0–P7 remains active. All-quorum live activation, learner catch-up/admission,
request/context handling, removed-replica service fencing, the formal membership
model and faulted network histories remain unfinished. Native convenience
assembly, incremental cleanup, recursive responsibilities, safe split/merge and
P7 evidence remain outstanding.

## Slice 33 — explicit membership-aware wire format

The native codec now carries learner, joint and final configuration records and
configuration-aware snapshots through explicitly selected wire format 2.
The default constructor retains static-configuration format 1. Each selected
codec accepts only its own frame version, and native transport rejects a
codec/session-version mismatch before accepting work. There is no silent
compatibility fallback or persistent-format dependency. The existing host codec
contract is unchanged; its public selector/capabilities expose the new choice.

Configuration payloads carry versioned records, validated recursive policies,
exact voter/store maps and disjoint learner maps. Snapshot metadata preserves
original bootstrap separately from the accepted stable/joint membership base,
including all configuration operation identities. Format 2 can also send a
checkpoint predating the first change: its original bootstrap configuration is
explicit even when the sender's accepted head has advanced. The envelope's
configuration and checkpoint base remain distinct; the core must establish
provenance rather than treating decoded metadata as authority.

Lengths, tree depth/node counts, combined replica counts, map/set allocations,
record retention and snapshot data remain bounded. The membership operation set
retains its 16,384 hard cap. Encoding measures before frame allocation; decoding
charges bounded objects before their allocations. Semantic validation may use
bounded temporary clones; these retained-object ceilings are not exact peak-RSS
claims. See [wire formats](WIRE_FORMAT.md) for the complete independent layout.

No new consensus effect, durability token, watermark or generation is introduced.
A decoded record cannot authorize a ballot, quorum, application result or snapshot
installation. Existing snapshot publication/pinning and authoritative WAL
completions remain mandatory dependencies. The live Raft core still rejects
configuration-bearing ingress and recovery before state/term mutation. A
successful format-2 decode is tested against that explicit core refusal.

Six new wire tests cover all transition records, recursive weighted targets,
joint/final snapshots, original bootstrap checkpoints behind a newer sender,
strict versions/mandatory tags, every frame truncation and bit flip, valid-CRC
hostile fields, operation-set bounds and exact decoded-memory thresholds. The
full 16,384-operation checkpoint roundtrips without identity loss. A new native
transport test carries configuration Append and membership Snapshot through
short host-channel I/O, retaining outbound credits until exact terminal release
and rejecting mismatched session versions. These channels use host test
attestations; they do not establish new authenticated dynamic-Raft histories.

Local validation passes 334 default/native/TLS tests, 312 native-only tests and
184 core/host-only tests. Clippy passes all three feature configurations with
warnings denied. Formatting, generated API docs, contract JSON, unchanged RPL
source headers and diff checks pass. Existing actual TCP/TLS regression histories
ran on Linux. No macOS execution, power-loss certification, throughput claim or
joint-consensus proof follows from these finite codec/transport checks.

Full P0–P7 remains active. Accepted-log activation at every live quorum site,
learner readiness/admission, configuration-scoped requests, removed-replica
service fencing and the formal activation/ballot model remain unfinished.
Recursive responsibilities, safe split/merge and P7 evidence remain outstanding.

## Slice 34 — accepted-log membership at core quorum sites

`Raft` now keeps one derived membership view for its durable state and one for
an accepted pending transition. The public borrowed view exposes the latest
accepted predicate without certifying persistence. The authoritative durable log
advances only on the exact admitted ticket's completion; other events stay busy
while that dependency is unresolved. Storage failure discards the pending view
and fences the replica. No new effect, receipt, watermark or generation is added.

Election admission, self-vote completion, received ballots, commit frontier,
read admission/probes/readiness/consumption and outgoing configuration IDs now
use the accepted-log predicate. Joint state requires both validated policies.
Current-term commitment, local durability, exact sender/term/request checks and
one-use applied read barriers remain mandatory. Changes invalidate old read
barriers immediately and conservatively recollect ballots/replication progress
under fresh contexts after durability, including same-voter weighted changes.
Rollback and compaction derive the view from the surviving journal and base.

Voting store identity and replication store identity are separate queries.
The bounded allocation-free replica iterator includes learners and both sides of
a joint configuration exactly once. Elections and read probes target voters;
replication includes learners, whose progress never satisfies a policy.
Learners cannot originate election, leader or read-authority messages. Nonvoters
cannot campaign or admit new client work. A locally retiring leader can finish
final commitment, then relinquishes leadership and volatile quorum state.
Node construction now checks roster compatibility for every effective replication
assignment. Output reservation counts all current replicas, including learners.
See [the core membership audit](MEMBERSHIP_CORE.md) for each implemented site.

Six internal tests drive the real core helpers with explicit journal fixtures
and exact host-asserted completion tokens. They check accepted/durable separation,
storage failure, learner identity/exclusion, promoted-voter joint commits,
same-voter weighted elections and reads, invalidated barriers, stale responses,
final rollback into a compacted joint base and local demotion fencing. The
fixtures deliberately prepare committed boundaries and do not prove their tokens
represent physical storage. A downstream public-interface test checks replica
and voter identities across activation, rollback and snapshot replay. Public
online ingress/recovery gates remain in place; these checks do not validate the
complete transition protocol or replace its formal activation/ballot model.

Local validation passes 341 default/native/TLS tests, 319 native-only tests and
191 core/host-only tests. Clippy passes all three configurations with warnings
denied. Formatting, API docs, inventory JSON, RPL headers and diff checks pass.
Existing real TCP/TLS histories remain static-configuration histories and ran on
Linux; no macOS, power-failure, online-membership proof or performance claim is
made. CI remains background feedback.

The audit exposes remaining bootstrap ballot checks in both log validation and
core recovery. Simply validating a recovered ballot against the latest electorate
would lose valid earlier promises after removal/rollback/compaction. A ballot
provenance/recovery design and model are required before promoting new candidates.
Strict ingress configuration equality also needs lagging-follower protocol rules,
without allowing stale acknowledgements to authorize a newer context. Explicit
learner recovery/readiness, prospective fanout reservation, retiring-leader final
propagation, route/roster admission and faulted native/host transition histories
remain unfinished. Full P0–P7 stays active; recursive responsibilities, safe
split/merge and P7 evidence remain outstanding.

## Slice 35 — durable historical ballot origin

GroupLog now retains the configuration and exact candidate store identity of a
hard-state vote. New promises validate against predecessor accepted membership;
a suffix or snapshot cannot authorize its own candidate in the same update.
Unchanged promises survive rollback, removal and compaction without requiring
current electorate membership. Single-vote/term guards remain mandatory. Raft
local service/campaign/grant checks use exact store assignment, and same-term
repeat grants cannot transfer a ballot to a replacement store with the same NodeId.
Exact admitted durability completion still gates replies; Written remains insufficient.

Normal WAL bytes remain unchanged and derive origin during replay. Reclaimed
images use VBLCPT02 when bootstrap scope cannot represent the historical promise;
VBLCPT01 remains available for representable state. Recovery validates canonical
log structure separately from restoring historical promises and rejects mismatched
snapshot/suffix hard states. Host decoder output receives mandatory structural
validation before publishing a recovery session. Selected stores remain trusted
for history provenance; metadata/CRC is not authentication. No ballot is imported
from remote application snapshot metadata. See [ballot recovery](BALLOT_RECOVERY.md).

Ten downstream ballot tests cover shared host/native eligibility and rollback,
removal/compaction, torn vote frames, failed barriers and replacement, actual-file
restart, every extended-image truncation and bit corruption, resealed semantic
corruption and invalid host-decoder recovery. A seventh actual-core membership
test retains an old-store vote through removal, learner-store replacement and
promotion, rejects a same-term replacement-store vote, then permits a new term
only after exact completion. Prepared fixtures do not prove online administration.

The independent bounded local model checks 237,556 reachable states and 665,097
edges without invariant violation. Three deliberately broken variants produce
counterexamples for erased rollback promises, early replies and ignored store
identity. Bounds, oracle assumptions, actions and exclusions are recorded in
[validation/REPORT.md](../validation/REPORT.md). This is not the distributed joint
Raft model or a full proof and does not justify enabling online reconfiguration.

Local validation passes 353 default/native/TLS tests and 331 native-only tests
before the final host-decoder test was added; the final focused ballot suites
pass all 10 tests in both configurations (354 and 332 total distinct tests).
The core/host-only full suite passes 197 tests. Clippy passes all three feature
configurations with warnings denied; formatting, API docs, inventory JSON, RPL
headers and diff checks pass. Linux only; macOS, hardware power-failure behavior
and performance are not established. CI stays background feedback.

## Slice 36 — replication request scopes across accepted heads

Append/Snapshot requests from an exact locally authorized voter can now bridge
differing accepted configuration IDs. Responses echo the requester's accepted
head rather than the responder's membership. Outstanding leader replication pins
scope with its original context/range; retries and deferred snapshot sends retain
that scope. Response processing requires current configuration plus exact admitted
request scope/context, term, store and origin session. Changing configuration
clears old requests/progress, so relabeling a delayed reply cannot restore authority.
Elections and reads still require equal configurations; learners cannot originate
replication. Incoming records/bases cannot activate beyond the declared sender head.

Partial chunks prove only their exact matching end, even if the follower has not
reached the requester's configuration. An older joint leader can replace an
uncommitted final suffix using higher-term log rules. Snapshot-base configuration
remains distinct from request head; a deferred acknowledgement retains its original
scope through snapshot persistence, log durability and application installation.
No new effect/token/generation/watermark or wire/persistent format is introduced.
Per-peer pinned scope is bounded inline state; output fanout does not increase.
See [replication scopes](REPLICATION_SCOPES.md) for rules and limitations.

Seven actual-core tests cover lagging probes/joint receipt, final rollback,
partial chunks, snapshot/application dependencies, compacted hints, scope/context
invalidation and learner/read/election exclusion. They use prepared committed
fixtures and host-asserted completions behind the retained public configuration-data
gate. Four downstream public tests use host/native storage for static-log scope
bridges, exact completion refusal, failed sync/publication followed by recovery,
and native wire-format-2 request/response/snapshot scope round trips. A runtime
test verifies valid cross-scope retained-voter contact resets an election deadline
without activating its scope or changing the durable vote; foreign-scope reads
remain rejected. These are not online administrator/network membership histories.

Local validation passes 366 default/native/TLS tests, 344 native-only tests and
206 core/host-only tests. Clippy passes all three configurations with warnings
denied. Formatting, API docs, inventory JSON, RPL headers and diff checks pass.
The bounded local ballot model remains unchanged. Linux evidence does not
establish macOS execution, physical power-failure certification, unbounded proof,
liveness or performance. CI remains background feedback.

## Slice 37 — explicit learner enrollment and verified restart

The promoted-sender audit exposed a prior missing capability: the core could not
start any learner, because all recovery required an initial voter store. This
slice supplies that prerequisite without treating transport authentication or a
claimed newer head as group authority. The full online authorization objective
remains unchanged and unfinished.

Raft::recover_learner explicitly enrolls a host-authorized non-voting local replica
from selected durable state. Both committed and accepted membership must contain
the exact local learner store. An initial uncommitted assignment, absent/replaced
store, initial voter, recovered vote or retained joint/final transition is refused.
The supported electorate stays the bootstrap policy/store map; unrelated pending
learner changes may survive if both views retain the committed local assignment.
The original voter recovery and public configuration ingress gates remain closed.
Snapshot bases must retain that same electorate and fit the selected log budget.

snapshot::recover_learner_replica verifies the exact pinned data, membership,
index/term/schema and application replay before exposing a compacted learner core.
Checkpoint, verified pin, logical compaction and physical reclaim reuse the existing
contracts. No new record/format, token, effect, generation or watermark is introduced.
Learners replicate ordinary commands durably but cannot campaign, propose, read,
grant ballots or return read-probe authority. A higher-term denied vote persists
its term before replying. Public local_voter exposes the exact accepted assignment
for runtime coordination, not durability evidence. TimedShard has no election
timer for a non-voting follower; retiring leaders retain heartbeat scheduling
until their existing final-commitment step-down dependency is satisfied.

Eight downstream learner tests cover host/native recovery and replication,
exact completion ordering, committed/accepted identity checks, removal/replacement,
joint/policy refusal, missing pinned data and checkpoint/application recovery.
Every native assignment-frame byte cut and failed synchronization/manifest barrier
recover old or complete new assignment state. An actual native log/snapshot-file
history compacts and reclaims a learner, closes/reopens both stores and verifies
its exact assignment/application. A timed-runtime test processes durable learner
replication with no election timer, even after a large virtual-time advance.
The imported assignment is an explicitly authorized host action, not evidence
that an online administrator committed it on remote voters. See
[learner recovery](LEARNER_RECOVERY.md) for the complete evidence boundary.

Local validation passes 375 default/native/TLS tests, 353 native-only tests and
211 core/host-only tests. Clippy passes all three configurations with warnings
denied. Formatting, API docs, inventory JSON, RPL headers and diff checks pass.
The local ballot model is unchanged. Linux evidence does not establish macOS,
hardware power-failure behavior, a distributed membership proof or performance.
CI remains background feedback and did not gate this slice.

## Slice 38 — runnable native three-process counter service

The usable-service priority now has an executable, `voteboat-counter`, with
explicit create/recover startup, three independent processes over native mutual
TLS, automatic elections, ticket-correlated write/read outputs, checkpoint
admission and draining shutdown with explicit worker joins. Its setup reference
uses the existing public NativeNode/NodeParts and selected providers. No new
consensus effect, durability token, watermark, protocol generation or public
provider seam is introduced. Each recovered store session reserves a disjoint
10,000-generation range for its peer roster; exhausted ranges fail closed.

The local command protocol has one active connection, a 256-byte command limit,
a five-second observation deadline and nonblocking partial I/O. Timeout cancels
observation and exact tickets keep late outputs separate from later connections.
Peer authentication remains independent of group membership. Credentials are
explicit host-supplied files, bounded before allocation; the loopback quickstart
selects the deliberately public test fixtures. This sample is fixed to one group,
three voters, fixed identities and loopback addresses. It is not yet a generic
configuration loader or a remote deployment package. See
[service and embedding](COUNTER_SERVICE.md) for commands and current limits.

Three executable-process tests exercise native files and TCP/TLS: operation retry,
quorum-backed reads, follower rejection, abrupt leader death and replacement,
checkpoint/restart/retry, quorum loss with uncertain writes followed by healing,
bounded commands, oversized-credential refusal, missing-store/invalid-ID refusal,
refusal to overwrite existing data and drained worker joins. Recovery separately
confirms that the checkpoint advanced the durable log base before process restart.
These exercise the shipped binary, not an in-process delivery substitute.

Local validation passes all three service-process tests on Linux, default-feature
Clippy across all targets with warnings denied, and all-target compilation for
native-only and core/host-only builds. Targeted Clippy passes again after the final
test additions. Formatting, document links, new Rust RPL headers and diff checks
pass. The unchanged core/storage suites were not repeated for this executable-only
slice. No macOS run, production certification or performance claim is established.
CI remains background feedback.

## Slice 39 — shared typed native startup and Rust embedding

Mini schema plan: one explicit config holds bootstrap/node/store identities,
mode, files, routing and TLS; a fresh host application is restored/replayed before
exposure. Validate → bind → create/recover → verify → spawn → assemble. Failure
returns the application and owns idle-worker cleanup; initialized files are not
rolled back. Verify generic embedding, real configured peer traffic, restart,
resource cleanup and independent host instances before extending the service.

NativeStartup now implements that single-group convenience path over the existing
public providers. The application is generic; the host supplies WorkerWake and
initial monotonic time. Node-driver limits are explicit, with other native provider
limits selected at their existing defaults. Exact bootstrap/local/voter/peer
identities, endpoint shapes, TLS names and retained peer metadata validate before
file modification. Open creates one authoritative native WAL path and uses verified
checkpoint/replay recovery. Create and Recover stay separate; learner/dynamic-voter
recovery gates remain unchanged. Node::from_parts remains available for shared
multi-group stores, alternative providers and other provider limits.

NativeStartupRejected returns the host application and explicit nonblocking
try_cleanup, which closes/joins any started idle workers and releases listener/WAL
ownership. There has been no accepted runtime work before successful construction.
Initialization or recovery may already have changed files, so cleanup is not a
rollback. No new consensus effect, watermark, durability token or generation type
is introduced; startup reserves the existing per-store-session connection range.

The counter executable now selects this public API and accepts an optional bounded
peer-address/TLS-name file. Commands remain a trusted loopback-only endpoint; peer
listeners/addresses can target configured interfaces. Routing cannot grant voter
membership. The process acceptance history now exercises non-default configured
TCP/TLS ports through the same startup path. Separate-host networking is configurable
but has not been exercised here. The new embedded_counter example also uses the
public API, submits and consumes original client/read tickets, verifies checkpoint
recovery and explicitly joins workers. It waits for the new durable leader state
after recovery, rather than interpreting restored applied state as leadership.

Local evidence: all three executable-process tests and five downstream startup
tests pass on Linux. The startup tests use a host-defined application and non-demo
identities, late constructor failure after worker creation, returned application
and explicit joins, two independent instances sharing a host wake, invalid identity
refusal and propagation of the host's initial clock domain. The embedding example
was run for create/write 7, checkpoint/restart/retry (duplicate=true, value 7), then
restart/new operation (value 10); each run reported joined workers. Default-feature
Clippy across all targets, native-only/core-only all-target compilation, API docs,
formatting, inventory JSON, new Rust RPL headers and diff checks pass. Core/storage
protocol suites were unchanged and were not repeated. macOS, separate-host deployment,
production certification and performance remain unestablished; CI is background.

## Slice 40 — bounded automatic local leader selection

Mini schema plan: keep one original add/read command, one socket and one bounded
reply. Try the three local command endpoints; move on only after a connection
failure before sending or an explicit not-leader rejection. After connection,
uncertain writes stop routing and retain their operation identity for manual retry.
Use an absolute deadline, not an I/O-progress timeout. Verify actual leader loss
and native restart, then fault peers proving no resend after receipt loss.

The executable now accepts client BASE auto for add/read. It tries local nodes
without caching leadership or interpreting status as authority. Exact
ERR NOT_LEADER replies come from typed admission failures or exact not-proposed /
not-read outputs. Other errors and Unknown outputs terminate routing. Connected
I/O failure or a missing complete framed reply marks a write Unknown; no new
operation ID is generated and another node is not contacted. Requests are bounded
before concatenation, replies use a fixed 4096-byte buffer, and nonblocking partial
I/O observes one ten-second deadline. Automatic mode has at most 100 rounds with
bounded backoff. Administrative controls still require an explicit node. This is
local routing; configured remote peer addresses do not expose remote client ports.

The real three-process history now uses auto for writes, retries and quorum reads,
including immediately after abrupt leader loss and after full native-file restart.
Fault socket tests verify exact command bytes after proven non-acceptance, no third
node contact after receipt loss, truncated success, explicit Unknown or another
error, and rejection of automatic administrative commands. A slow peer trickles
an incomplete reply; the client still exits at its absolute deadline and does not
reroute. These are finite executable/client checks, not new consensus proofs.
The expanded parallel fixtures now hold distinct port-block assignments for their
lifetimes instead of choosing bases from released ephemeral listeners.

Local validation passes all six service/routing tests together in the final
combined run; targeted Clippy, formatting, new Rust RPL headers,
document links and diff checks pass. No consensus/storage protocol, native startup
contract, peer wire format or persistent format changed. The prior five startup
conformance tests were not repeated. Linux only; no new macOS, separate-host or
performance evidence is claimed. CI remains background feedback.

## Slice 41 — prospective effect reservation through membership rollback

Mini schema plan: derive an allocation-free replica bound from accepted state,
rollback-reachable uncommitted prefixes and incoming configuration records.
Inspect the exact next priority/byte/deadline choice without consuming it; reserve
before executing. Reject oversized ingress with its original event before queue
or ticket allocation. Retain existing protocol/durability gates and verify the
usable static service alongside the core/runtime checks.

Raft::effect_reservation now covers local rollback as well as the accepted head.
A committed shrink releases obsolete fanout allowance; an uncommitted shrink or
final record still budgets the larger predecessor. Snapshot membership is the
initial replay view. Raft::event_effect_reservation additionally bounds incoming
append configurations, including intermediate joint unions before final shrink,
and incoming snapshot membership. Incoming joint counts are conservative upper
bounds capped by the mandatory validated membership limit. This calculation
allocates no replay state and grants no configuration authority.

Shard and TimedShard expose next_effect_reservation. Selection, next_class and
execution share one priority/byte/deadline helper, preserving the cursor across
visits. EffectOwner checks prospective class capacity at ingress and rechecks the
exact selected event before execution. Initial rejection returns EventTooLarge
with the original input and no ticket, queue, core or timer mutation; it does not
fence the owner. Admission uses total class capacity rather than free capacity,
so other held effects do not incorrectly reject otherwise admissible input.
Temporary reservation saturation still requeues an untouched event. If earlier
work makes a queued event exceed its entire class capacity, execution is refused
and the owner fences rather than leaving an impossible bulk visit queued forever.

Four new core fixtures cover learner/final shrink rollback, committed shrink,
compacted bases, append/snapshot growth, intermediate joint fanout, arithmetic
overflow, pure inspection and the unchanged public configuration ingress gate.
Downstream tests check exact priority selection, repeated non-consuming inspection,
visit exhaustion/deadline/stale tickets, original-input rejection with no spent
admission identity, continued control service and admission while a lease is held.

Linux validation: final default-feature library/runtime/effect-owner run passes
26 + 25 + 94 tests, including native TLS/worker histories. The first sandboxed run
could not open loopback sockets; rerunning those checks with loopback permission
passed. All six process-service/routing tests and five startup tests also pass.
The core-only library/runtime/effect-owner run passes 18 + 18 + 90 tests; its run
preceded the final additional held-lease assertion, which passes in the default
run. Default all-target Clippy, native-only all-target compilation, API docs,
formatting and diff checks pass. Persistent/wire formats and configuration gates
are unchanged. No new storage optimization, formal model, macOS execution,
separate-host deployment or performance claim is made; CI remains background.

## Slice 42 — explicit dynamic member recovery

Mini schema plan: use one checked recovery implementation with explicit static,
bootstrap-learner and dynamic-member modes. Dynamic recovery requires committed
and accepted exact local store assignment; reconstruct the accepted predicate
without restoring volatile leadership. Verify pinned application data before
exposing compacted cores. Preserve historical ballots and existing ingress gates;
exercise host/native elections, rollback and crash/restart boundaries.

Raft::recover_member now explicitly restores authorized durable dynamic voter or
learner state. The new snapshot::recover_member_replica helper validates the same
assignment and completes pinned checkpoint verification/application restoration
and committed-tail replay before returning a core. Accepted joint records use
both predicates; an accepted final uses new-only rules only after its predecessor
joint commitment. Uncommitted final rollback restores joint rules. Historical
ballot promises survive finalization/restart/rollback, and learners remain unable
to campaign or vote. Original static and bootstrap-learner recovery contracts and
NativeStartup remain unchanged. There is no new escaping effect, durability token,
generation, persistent or wire format. The host remains responsible for authorized
restart/import and trusted committed log provenance; this is no transferable
network election certificate.

Eleven new downstream host/native tests check joint versus final elections,
weighted final policy, exact committed/accepted assignment, replacement stores,
learner exclusion, historical ballots, rollback, missing checkpoint data and
verified application restoration. Native model histories cut every byte of a
joint frame and fail sync/manifest publication. A native file history compacts,
reclaims, closes/reopens both providers and checks exact dynamic membership,
application value and duplicate operation behavior.

The service regression run exposed a probe/rebind race in fake-peer fixture ports.
Fixtures now retain bound listeners and hand those listeners directly to fake
peers. All placeholder child-process listeners are released before launching the first
child, preventing early peers from connecting to them; the validation test also
releases its directly launched child's ports. Used blocks are not
recycled within the finite test process. Production networking is unchanged.

Local Linux evidence: default library/ballot/learner/member/membership/Raft/snapshot
suites pass 26/10/8/11/27/22/19 tests. Core-only versions pass
18/3/4/7/16/10/8 tests. Default all-target Clippy, native-only all-target
compilation, API docs, formatting, inventory JSON and diff checks pass. The
existing six service/routing and five startup tests pass after the final fixture
repair, including parallel fake peers, actual processes and native TLS restart.
No new macOS, remote deployment, formal protocol or performance claim is made.

## Slice 43 — optional caller-polled QUIC sessions

Mini schema plan: put Quinn's protocol engine behind the existing SecureSession
contract, supplied with a dedicated UDP socket and exact authorized peer. Reuse
the authenticated identity hello and frame codec. Bound packets, handshakes and
stream ownership; drive all progress and deadlines from host polls. Test partial
progress, loss, authentication, close/failure and actual replicated application
before exposing service startup selection.

The optional quic feature adds NativeQuicSession and QuicSessionOptions, using
pinned quinn-proto/bytes and existing Rustls/ring. TCP/TLS remains the default.
The native framed transport already accepts the new provider. No hidden runtime,
reactor, worker or socket binding is added. Client/server constructors consume
explicit dedicated sockets; only polling performs I/O. Mutual certificate
authentication, exact pins, distinct ALPN and the existing node/store/session
hello precede Ready. Message format, store generations and durability tokens are
unchanged. Reliable stream chunks are ordered into the existing byte channel;
QUIC ACKs permit local buffer release, never a Raft durable acknowledgement.

Polls bound calls/bytes and protocol visits. One retained encrypted datagram
survives send backpressure; separate receive scratch prevents overwrite. Packet
MTU is fixed at 1200 bytes, with bounded stream/connection windows and one active
incoming/outgoing chunk. Foreign sources are discarded before admission. Migration,
early data, bidirectional streams and unreliable application datagrams are disabled.
Handshake byte/time limits, monotonic deadlines, revocation and truncated close
fail closed. Clean close permits draining retained input. A one-call budget
alternates read/write across polls. Completing the identity hello also consumes
its stream FIN so flow-control credit can reach the first application write.
Accepted output lost during close fails on timeout rather than hanging Closing.

Nine QUIC tests pass, including UDP loss/retransmission, exact pin/name/store
rejection, partial ordered plaintext, budget fairness, handshake limits, foreign
datagrams, clocks, revocation, clean close, unacknowledged close failure, framed
outbound ownership and a three-replica election/commit/apply history. The latter
uses host log tickets with actual encrypted UDP, not native file durability.
The combined final Linux QUIC/secure/transport/startup/service run passes
9/12/13/5/6 tests. All-feature all-target Clippy with warnings denied passes.
Core-only and native-only all-target compilation and all-feature API docs pass.
See QUIC_TRANSPORT.md for construction and integration scope.

## Slice 44 — shared QUIC establishment and native service selection

Mini schema plan: one explicit UDP socket per node, bounded queues routed by
configured peer source addresses, and generation-scoped session leases. A live
lease prevents reuse; cancellation/drop clears its queued packets. Preserve
terminal request slots and transferred sessions during connector close. Reuse
the native storage/recovery builder with explicit TCP/QUIC selection. Test shared
peers, budget charging, cancellation, failed startup and native process recovery.

NativeQuicConnector implements the public PeerConnector contract. It owns no
thread or runtime, and consumes the host's bound UDP socket. Eight queued
1200-byte datagrams per live peer lease bound socket sharing; unknown sources
allocate no mailbox and full queues drop packets for QUIC retransmission. All
consumed packets are charged to the visiting session, including foreign/routed
packets; queued reads are charged conservatively again. At most one OS read
occurs per read visit. Established QUIC sessions now have a five-second idle
timeout so dead peer routes can be released for reconnect. Hosts still drive
all polling and wakeups.

Exact local/peer/store/generation/address/deadline checks precede admission.
Owned handshakes release leases on cancellation/expiry, while accepted tickets
retain slots through exactly one terminal poll. Transferred sessions retain
their leases until dropped and survive connector close/drop. A new connection
cannot overwrite a live lease. Generation-checked cleanup discards old queues
without removing replacements. Protocol connection IDs and crypto isolate old
packets; recovered store sessions and host generation ranges remain authoritative.
No new wire format, escaping consensus effect or durability evidence is added.

NativeStartup::open_with_protocol selects NativePeerProtocol::TcpTls or optional
Quic, returning NativeNode with NativeServiceConnector. Box<S> now forwards the
public SecureSession contract. Original open/default native aliases retain TCP;
the aliases also accept an explicit connector type. The common builder performs
the same WAL/snapshot verification and application restore/replay for both.
QUIC starts two storage workers and no dial worker. Connector construction is
the last provider step before node assembly so any subsequent rejection returns
started TCP worker ownership or drops idle QUIC resources for explicit cleanup.
No failed initialization is treated as rollback.

The counter accepts trailing --transport tcp|quic after any peer file; QUIC
requires --features quic. Each node uses one configured UDP peer port, with
unchanged TCP local commands. The process fixture also reserves UDP ports before
launch and releases all placeholders before any child starts.

Four downstream connector tests check multi-peer one-socket establishment,
one-call/one-visit fairness, independent sessions surviving connector close/drop,
lease release and fresh generation reconnect, exact cancellation/terminal slots,
expiry, rejected budgets/time/identities/routes and returned construction socket.
One internal UDP test checks queue/drop ceilings, routed/unknown byte accounting
and retired queue disposal. A sixth startup test rejects late node assembly,
joins both storage workers, rebinds UDP and reopens the WAL. A seventh process
test runs the durable counter history through QUIC, including abrupt leader loss,
replacement writes, recovered former leader catch-up, checkpoint, shutdown/join,
restart and dedup. The final Linux library/connect/service/QUIC/QUIC-connector/
secure/startup/transport run passes 27/12/7/9/4/12/6/13 tests (90 total).
Core-only/native-only all-target builds, all-feature API docs and Clippy pass.
Default TCP service/startup regressions pass 7/5 tests, including unavailable QUIC
selection before store creation. The QUIC process history passes again after
selecting non-default ports through a peer file alongside --transport quic.
Formatting, inventory JSON, local documentation links and diff checks pass.
No macOS, remote deployment, production readiness or performance claim is made.

## Slice 45 — retiring leader final commitment

Mini schema plan: produce commit-only output from the exact durable final
completion before clearing a removed/demoted leader's volatile state. Accept
only the receiver's already stored final boundary, from an exact predecessor
joint voter in the same current/final term. Preserve role, term, vote and timers;
persist changed commitment before reply. Check duplicates, malformed/obsolete
messages, compaction, failed barriers and restart without opening online ingress.

The audit found that final durability cleared a retiring leader's role before
After::Commit could broadcast. The core now constructs bounded final notices
before that cleanup and emits them after its exact DurableLog completion. The
existing empty Append encoding carries only the final index/term and matching
leader_commit. A separate restricted receive path checks locally surviving final
and committed joint history, exact group/configuration/node/store/context, same
current/final term and empty data. It cannot append, raise a term, campaign,
establish reads or reset election timers. Changed receipt commitment and reply
wait for the receiver's exact storage dependency; duplicates are idempotent.
Existing request generations, formats and durability tokens are unchanged.

Three internal tests cover removed/demoted leaders, missing exact completion,
bounded current-peer fanout, prepared receiver commitment, duplicate receipt,
eleven invalid/obsolete messages, verified joint snapshot bases and refusal to
recreate old authority after final compaction. The compacted fixture initially
used the leader's physical-store pin for the receiver and correctly failed
recovery; its explicit host-verified local pin now binds the receiver store.
Three public host/native tests check dynamic recovery receipt, unchanged voting
state/timers, failed sync/manifest publication, power-loss recovery and retry.

Default library/member/membership/Raft/replication-scope/runtime/snapshot suites
pass 29/14/27/22/4/25/19 tests (140). Core-only versions pass 21/8/16/10/1/8,
omitting runtime. The final added newer-term negative assertion passes its focused
check. TCP/QUIC service, QUIC session/connector and startup regressions pass
7/9/4/6 tests. All-feature all-target Clippy, native-only all-target compilation
and all-feature API docs pass. See RETIRING_LEADERS.md for authority and evidence.

This is one-shot final commitment notification, not retired leadership, a
transferable election certificate or a persistent retry outbox. Receivers missing
the final entry still need catch-up. Full faulted activation/retirement and roster
histories remain required. Online configuration ingress remains closed; full
P0–P7 remains active. No macOS, formal completeness or performance claim is made.

## Slice 46 — direct promoted-replica witness authorization

Mini schema plan: retain one pending direct query and one volatile replication
permit, bound to exact witness/candidate stores, local session, base head and fresh
request context. Witness only durable committed membership, including its reserved
final target; grant no vote/read/term/commit/timer authority. Preserve ordinary
matching-prefix and storage dependencies; clear on cancellation, configuration
change, fencing and recovery. Test partial progress and stale/faulted boundaries
before extending public activation.

Public authorization/cancellation events and AuthorityRequest/AuthorityReply now
implement this exchange. The source reconstructs the requester's retained committed
base, checks both historical identities and grants only a committed voter. The
receiver admits exact-head candidate Append/Snapshot traffic through a volatile
permit. Controls never update leadership or hard state. Queries use existing
request/session generations; source durability is its earlier exact completion or
verified recovery. A permit is neither receiver progress nor a quorum certificate.
Explicit native wire format 3 adds tags 10/11 and retains format 2 membership
payloads. Existing native sessions still negotiate format 1.

Eight internal actual-core tests and three downstream tests cover promoted joint
receipt behind the public gate, exact completion, committed versus accepted
promotion, final target, stale/canceled/forged replies, session restart, partial
progress, higher-term persistence, vote/read exclusion, fencing, retired witnesses
and compacted history. A codec test checks variants, versions, invalid shapes,
truncations, bounds and membership snapshots. Two fixture corrections preserved
the intended contracts: final entries reuse their joint operation ID, and runtime
byte-reserve tests now derive exact event size after enum growth instead of using
an obsolete fixed allowance. Production budgets were not increased.

Default library/member/membership/Raft/scope/runtime/snapshot/wire suites pass
37/17/27/22/4/25/19/13 tests (164 total). Core-only equivalents excluding runtime
pass 29/10/16/10/1/8/1 tests (75 total). TCP/QUIC service/session/connector/startup
regressions pass 7/9/4/6 tests. All-feature all-target Clippy and API docs pass.
See REPLICATION_AUTHORITY.md for protocol and evidence limits.

Public configuration ingress remains closed. This exchange cannot bootstrap
trust when all old witnesses are unavailable or have compacted the required view.
Native session negotiation, roster admission, readiness, online administrative
integration and complete faulted activation histories remain required. Full P0–P7
remains active; no macOS execution, formal proof or performance claim is made.

Additional verification: connect/effect-owner/outbound/peers/secure/
transport/worker suites pass 12/94/4/13/12/13/9 tests (157 total). Native-only
all-target compilation, formatting, inventory conformance paths, changed local
links and diff checks pass. Public witness histories also traverse format-3
query/reply frames with native support. Final focused library/member checks
include a too-old claimed commit boundary and a high-term control query.

## Slice 47 — authenticated native wire selection

Mini schema plan: select one supported message version in NativeTlsConfig,
defaulting to 1. Bind it into the encrypted fixed-size identity hello, session
binding and connector completion checks. Derive startup codec/roster from the
same value. Preserve identities, ownership, handshake budgets and persistent
formats. Reject mismatch before Ready without downgrade. Check actual witness
traffic and complete native create/write/drain/recover composition.

NativeTlsConfig now exposes with_wire_version and wire_version for exact versions
1–3. TCP/TLS and QUIC use that choice in the existing 52-byte encrypted hello;
ALPN/session framing families, pins and TLS security remain unchanged. Unsupported
selections reject before session construction. The choice is copied per config
and session; cloned credentials may select independently. Both connectors verify
the transferred version. NativeStartup selects its message codec and PeerRoster
from the same config for create/recover, retaining version 1 defaults.

Two TLS tests and one QUIC test cover versions 2/3, invalid selections, config
clone isolation, fragmented streams, real TCP and mismatches before Ready. One
TCP connector and one shared QUIC connector test verify selected-version handoff
and mismatch cleanup. Two native-storage witness histories send actual core
queries/replies through format-3 authenticated TCP/TLS and QUIC framed transports,
with exact send completion/credit release. Two startup tests exercise both
versions and transports in three-node native-file clusters: election, committed
write, full drain/join, restart and applied recovery. The cluster fixture was
corrected to create its parent directory; it reserves TCP/UDP endpoints and
releases all placeholders before any real peer starts.

The final QUIC-enabled library/connect/member/QUIC/QUIC-connector/secure/startup
run passes 38/13/19/10/5/14/8 tests (107 total). Default service/effect-owner/
peers/transport regressions with QUIC enabled pass 7/94/13/13 tests (127 total).
Core-only and native-only all-target builds, all-feature Clippy and API docs pass.
The inventory's prior authorization entry was misplaced under not_yet_implemented;
it is now in contracts. validation/check-inventory.mjs checks all entry shapes,
uniqueness and conformance paths, and rejects the prior committed metadata as a
negative control. Its pass validates metadata, not component behavior.

Online membership remains gated. This is exact compatibility selection, not
multi-version fallback or a readiness protocol. The counter executable remains
on its default version 1; embedding hosts select a version in their startup TLS
config. Historical evidence retention, membership-aware roster/route admission,
readiness and complete faulted activation/retirement remain required. Full P0–P7
remains active; no macOS, formal proof or performance claim is made.

## Slice 48 — membership-derived shared peer assignments

Mini schema plan: derive a bounded exact peer/store union from all hosted cores,
including committed, accepted rollback predecessors and pending storage states.
Preflight routes, provisioned credentials and capacity before mutation. Withdraw
receive authority immediately; retain canceled attempts and accepted sends until
exact terminal receipts. Keep any peer still required by another hosted group.

Raft::connection_replicas and PeerAssignments expose the local connection view.
PeerRoster reconciles assignments; PeerDriver and Node derive all hosted groups
at their serialized owner and return owned route hints on rejection. Native TCP
and QUIC connectors implement the same public supports_peer contract over their
construction pin maps. Default host connectors safely decline new reconciliation
until they implement that check. Staged sends to removed peers fail locally while
accepted sends retain their original tickets and queue credits. Held input is
revalidated against the current roster and discarded after revocation.

Two actual-core histories verify pending/accepted rollback retention and exact
final-commit durability. Four downstream roster tests cover shared-group unions,
stale Ready results, queue-credit retention, same-store session floors, capacity
and identity rejection. Three driver/Node tests cover canceled provider receipts,
held input, accepted/staged sends and owned route rejection. Existing native
startup histories now reconcile the live roster before committed writes and
recovery under both wire versions 2/3 and TCP/QUIC.

A material edge-case check tightened the plan: different stores under the same
node ID require a drained roster/generation handoff. Live identity replacement
could erase session history across an ABA switch. Inactive same-store records
remain under the peer ceiling; distinct-node churn can exhaust it and require a
handoff. Rejection preserves existing connections and accepted work. No unbounded
history or online credential rotation is introduced. See [peer membership
contracts](PEER_MEMBERSHIP.md).

QUIC-enabled library/effect-owner/peers/startup tests pass 40/97/17/8 (162 total).
Core-only all-target build passes. Additional checks are recorded in the validation
report. Public online configuration ingress remains gated: this slice provides
current-view connection reconciliation, not prospective resource admission,
readiness, distributed activation or complete fault coverage. Full P0–P7 remains
active; Linux evidence does not establish macOS execution coverage.

## Slice 49 — prospective connection preview and recovery admission

Mini schema plan: inspect an incoming event's exact peer union before it executes,
including intermediate learner/joint configurations and membership snapshots.
Retain existing rollback/pending views and every other group's requirements.
Check routes, provisioned credentials and fixed ceilings without mutation or
clock advancement. Construction must also provision rollback-reachable peers.

Raft::event_connection_replicas and PeerAssignments::for_event compute this
bounded conservative union. PeerDriver::preflight_event and Node::preflight_peer_event
borrow route hints and share the existing reconciliation preflight for exact
stores, pins, retained history, metadata and lifecycle checks. Unknown groups and
wrong message group/destination reject. A malformed protocol history may pass
resource inspection and still fail protocol validation: previews cannot grant
membership or voting authority. No new effect, durability token, persistent
watermark, generation or network ownership is created.

Node::from_parts now checks connection_replicas rather than only the latest
accepted membership. A recovered uncommitted learner removal cannot omit a
connection still needed by the committed predecessor. Rejection returns parts
before external provider polling. The regression constructs two actual recovered
cores with committed learner assignments and accepted removals; their accepted
heads omit that learner but both rollback views retain it.

Actual-core tests inspect learner appends, intermediate joint/final sequences,
compacted joint snapshots, capacity, conflicting stores, wrong destination and
fencing without mutation. A shared-group downstream test retains another group's
peer while previewing an addition. Driver/Node tests verify missing routes/pins,
capacity, unknown groups, shutdown and pure future-time preview. A fixture initially
used Shard's default capacity with a smaller test scheduler; correcting that
fixture to its supported 100-group capacity made the recovery check exercise the
intended constructor path.

QUIC-enabled library/effect-owner/peers/startup suites pass 41/99/18/8 (166 total).
Core-only all-target build, all-feature/all-target Clippy, all-feature API docs and
inventory checks pass. Native TCP/QUIC unchanged-roster commit/drain/recover
histories remain green. No macOS execution or complete protocol claim is made.

This is preview plus recovery admission, not a retained online event reservation.
Success does not reserve capacity for several queued events. Serialized execution
must recheck and retain resources through accepted/pending transitions; readiness
and distributed activation/retirement histories remain required. Public online
configuration ingress remains gated. The full P0–P7 goal stays active.

## Slice 50 — retained owner connection capacity

Mini schema plan: use the existing owned event queue as the reservation ledger,
not a second asynchronous ticket table. Admission unions queued prospective peers
with current/pending core views and exact retained identity history. Execution
rechecks, then transfers capacity into core state or releases queue-only peers on
rejection. Close retains queued work until drain; stop returns events/tickets.
Seed a networked Node's budget from all tracked roster records only after assembly.

ConnectionBudget is a fixed local identity, peer ceiling (1–65536) and bounded
exact store history. Shard, TimedShard and EffectOwner expose installation and
inspection. Queue/core union calculations are bounded by that peer ceiling and
existing event/group/item/byte limits. Temporary maps and retained history are
separate from output effect reservations; no throughput/allocation claim is made.
A shared peer is charged once; conflicting physical stores cannot share a node ID.
Budget tightening/replacement preserves history and validates the entire owned
union before mutation. New group registration also checks capacity before insertion.

Configuration append, snapshot and granted witness-reply admission checks run
before queue ownership transfer or admission-ticket consumption. Rejection returns
original input. Execution rechecks, while accepted/pending peers enter retained
identity history; rollback cannot erase it. A serialized with_core mutation that
exceeds its reservation stops/fences the group before returning its result/effects.
Node construction prepares the budget before provider assembly and installs it only
on success; rejected parts do not acquire this new owner state. Standalone hosts
opt in and must seed all shared roster history and the actual provider limit.

Handoff inspection found two volatile states beyond a pending log write:
Snapshot staging and a verified promoted-peer replication permit. connection_replicas
now includes both. Event preview includes granted witness candidates and snapshot
bootstrap peers even when dynamic metadata is absent. The conservative output
reservation also covers those snapshot bootstrap counts. Neither the preview nor
this capacity guard grants protocol authority, credentials or quorum evidence.
No new durable effect, watermark or generation is introduced; restart reconstructs
from verified core state and explicitly supplied roster history under StoreSession.

Five downstream owner tests cover competing queued changes, shared peers/store
conflicts, untouched rejected tickets/payloads, protocol rejection, close/stop,
budget tightening/history preservation, witness-reply reservations and registration
with occupied capacity. Two actual-core tests exercise pending/durable/rollback
handoffs, fail-closed unreserved callbacks, staged snapshots before log persistence
and a verified permit for a candidate absent from the receiver's membership.
Node construction tests verify automatic budget installation and rejection purity.
QUIC-enabled library/effect-owner/peers/runtime/startup/service suites pass
43/104/18/25/8/7 (205 tests). Core-only all-target build, all-feature/all-target
Clippy, API docs and inventory validation pass. No macOS execution or performance
claim is made; whole P0–P7 remains active.

This completes capacity retention, not online configuration activation. Pins and
owned route plans still need admission-time integration, followed by readiness
and faulted distributed histories. Public configuration ingress stays gated.

## Slice 51 — retained credential and route admission

Mini schema plan: retain owned exact-store hints beside selected peer providers,
check every hint against preprovisioned credentials, and bind the owner queue
reservation ledger to that closed set. Replacement validates all current and
queued requirements before mutation and returns the whole proposed map on
failure. Reconcile actual core requirements around Node replica polling, while
preserving a retiring peer until its owner-held sends and original queue credits
finish. This advances P4; learner readiness and online lifecycle validation are
the next two deliverables in the linked plan above.

PeerRoute and PeerRoutesRejected expose owned plans and rejection. PeerParts
retains admission_routes through drain, failed construction and recovery. Node
construction derives a default from current routes, validates exact connector
pins and metadata, and installs the closed owner policy only after successful
assembly. Standalone PeerDriver hosts explicitly install the owner policy.
ConnectionBudget::provisioned_peers exposes it; public capacity changes preserve
the current policy so old cloned budgets cannot restore withdrawn peers.

Queued prospective peers reserve admission without authorizing connects. Actual
core requirements, including verified witness permits, drive planned roster
reconciliation. Node checks before network polling and after replica polling to
make new connection deadlines visible immediately. Unchanged assignments retain
fairness, attempts and bindings. Pending sends are tracked by destination in the
existing lease kind, without duplicating payloads. Per-peer retention avoids
dropping retirement notices or blocking idle-peer removal behind unrelated work.
Plans charge bounded hint/identity metadata; generic endpoint heap and cloning
remain a host contract, while native socket addresses are fixed-size.

New downstream histories cover rejected credentials, queued route withdrawal,
stale budget restoration, witness-driven connections, original outbound credits,
live Node plan replacement and failed construction ownership. Native TCP/QUIC
wire-2/3 startup histories replace valid plans, reject changed stores and preserve
live bindings before committed writes, drain and recovery. Library/effect-owner/
peers/runtime/startup/service suites pass 43/109/18/25/8/7 (210 tests). Core-only
all-target check, all-feature/all-target Clippy, API docs and inventory validation
pass. An earlier process test hit AddrInUse; reservation release before child
binding leaves a fixture race, and the later full run passed without code changes.

No new durable effect, watermark or generation is introduced. Restart uses the
existing fresh StoreSession and recovered core plus explicitly supplied hints and
credentials. Live credential rotation remains unsupported. Readiness and complete
faulted distributed configuration histories remain required before public online
configuration ingress opens. No macOS execution or performance claim is made.

## Next slice

Implement exact learner readiness evidence, then the faulted online membership
path described in the linked mini plan. Preserve the usable static TCP/QUIC
service; recursive responsibilities, split/merge and measured tuning remain in
the full P0–P7 goal.

## Slice 52 — fresh host-driven learner readiness

Mini schema plan: keep one fixed-size pending request on the core; bind it to
leader context, stable committed configuration, learner store/session and a
current-term committed prefix. Verify through selected log/snapshot/application
providers, then match the exact authenticated reply before issuing a checked
token. Recheck at promotion; changes to commit, term, configuration or peer
session require another round. Failure leaves no new persistent state and
invalid responses do not consume the pending request. This advances P4 and feeds
the next online membership integration, followed by P5 responsibilities.

ReadinessRequirements carries checkpoint schema and minimum command/snapshot
capacities. The learner helper rejects pending dependencies and applied lag,
compares selected durable log state, checks provider limits, verifies any exact
pinned compacted image and exercises bounded checkpoint restoration on a clone.
A fresh request context uses the existing shared allocation floor and leader
StoreSession. Receipt assertions are constructible for authenticated host wire
adapters under the same non-Byzantine trust model as storage completions; they
are not cryptographic certificates. ReadyLearner itself is core-issued.

No new durability token permits an effect to escape: there are no new protocol
Send/Persist effects in this host-driven seam. The readiness prefix is the
existing contiguous committed boundary supported by exact prior DurableLog
completions (or verified recovery and pinned snapshot data), not an index maximum.
No new generation is persisted. Restart loses pending requests, uses fresh
StoreSessions and requires another round. The host serializes maintenance against
the core/application and handles provider faults with existing fencing/recovery.

Five downstream histories use actual election/replication and selected providers
to test rejection and freshness, including pending/Written work, changed schema
or limits, wrong bindings, complete request matching, duplicate/old replies,
advancing commitment/term/configuration, missing compacted data and native file
checkpoint/reopen. Assignments are explicitly host-imported. A fixture failure
identified a missing minimum snapshot envelope in HostSnapshots; its file-size
accounting now includes 48 bytes, and snapshot conformance is rerun. Another
fixture correction preserves the durable ballot and completes an existing
heartbeat before acknowledging a newly appended index.

The host-driven contract is implemented; native readiness RPC encoding and
asynchronous Node maintenance, placement authorization and consumption by online
promotion remain next work. Configuration-bearing public ingress stays gated.
No production online membership, macOS execution, benchmark or proof claim.

Validation: QUIC-enabled library/effect-owner/learners/peers/runtime/snapshot/
snapshot-worker/startup/service suites pass 43/109/13/18/25/19/14/8/7 tests (256).
Core-only all-target check, all-feature/all-target Clippy with warnings denied,
API docs, formatting/diff and inventory shape/path validation pass (50 records).

Next deliverable: integrate this readiness exchange into native wire/maintenance
and joint/final promotion admission; complete the faulted online membership
release checks before moving to recursive responsibility manifests and routing.

## Slice 53 — local configuration proposals consume readiness

Mini schema plan: introduce an explicit owned administrative event containing
one journal record, bounded promotion proofs and host-authorized capability
requirements. Validate the shared transition grammar and each promoted learner
before persistence. Reserve prospective peers/fanout through the same queued
owner ledger. Recheck live authenticated sessions at execution; default runtime
stepping rejects proofs without that check. This advances P4 toward native
readiness exchange, then faulted online activation; P5–P7 remain the macro scope.

Event::Configure uses ConfigurationProposal, PromotionReadiness and typed
ConfigurationProposalError. A voting leader needs a committed current-term
prefix. Every newly promoted exact-store voter needs fresh core-checked readiness
covering that prefix and the exact required schema/capacities. Missing, duplicate,
extraneous or mismatched proofs reject before mutation. Existing voters need no
new readiness; same-voter policy changes still enter joint consensus. The shared
journal validates expected configuration, operation reuse, assignment and final
ordering. Final requires the committed joint and its exact operation/target.

Successful admission emits only the existing Persist transition. No new durable
effect, token, watermark or generation is introduced. Exact LogTicket/DurableLog
completion permits dependent sends; Written cannot release them. Restart uses
the existing journal/snapshot replay and fresh store session; readiness remains
volatile. Configuration replay advances application progress without ordinary
command receipts, client operation tracking or a client proposal position.

Runtime input accounting includes record metadata and proof vector capacity.
Prospective connection/output reservations include learner and joint targets
before queue ownership transfer. Protocol rejection releases queue-only peers;
retained owned route plans cannot withdraw queued requirements. Default Shard,
TimedShard and EffectOwner stepping (including ClientRouter/Node advancement)
refuses promotion proofs without authentication. Low-level hosts explicitly use
advance_with_configuration_bindings to compare current sessions at execution;
direct core hosts authenticate adjacent to Raft::step. Native Node integration
with its actual roster is still next work, not implied by this callback seam.

Eight new downstream histories cover proof admission, joint-before-final,
native file reopen, old/new recursive weighted commitment, failed native sync
and modeled loss of unsynchronized bytes, execution-time session checks, retained
proof-capacity rejection, shared queued peer budgets and pin/route withdrawal.
Assignments and peer-2 acknowledgement messages in local proposal fixtures are
host assertions; no remote configuration-delivery claim is made. A test fixture
was corrected to move its original oversized vector rather than cloning it and
losing spare capacity before admission. No production accounting was weakened.

QUIC-enabled library/effect-owner/learners/membership/peers/runtime/snapshot/
snapshot-worker/startup/service suites pass 43/111/19/27/18/25/19/14/8/7 tests
(291 total). Updated learner assertions additionally verify no command receipts
or client proposal position. Core-only all-target check, all-feature/all-target
Clippy, API docs and inventory validation pass. Public configuration-bearing
Append and membership Snapshot ingress remain gated pending native readiness
exchange, placement authorization, activation modeling and faulted network
histories. No macOS, hardware power-loss, performance or full proof claim.

## Slice 54 — native learner readiness exchange

Mini schema plan: one pending leader request/result and one suspended learner
verification per group. Carry the full fresh context in explicitly selected wire
4 over authenticated TCP/TLS or QUIC. Retain the original owner effect/visit while
selected snapshot work verifies its existing pin and limits; finish application
checkpoint/restore checks on that owner. Capability mismatch denies without a
fence; storage/output failure fences. Completion checks cover exact sessions,
stale tickets, other-group progress, retained budgets and accepted-work shutdown.
This advances P4 toward the online administrator and fault-tested activation;
P5 routing, P6 split/merge and P7 tuning remain the global capability chain.

Event::CheckLearnerReadiness emits Rpc::LearnerReadinessRequest. The learner
validates exact scope without term/timer/vote/read authority, then suspends on
Effect::VerifyLearnerReadiness. SnapshotJob::Readiness loads the existing pin and
returns validated snapshot limits; it does not publish/reconcile retention or
change WAL state. Native work uses bulk credits, preserving control/recovery
reserves. SnapshotRouter keeps the exact effect lease, worker ticket and owner
visit, reserves loaded-image plus live-checkpoint capacity, and rejects oversized
limits/images before application work. Other groups remain schedulable.

Exact completion uses shared scope/application/snapshot checks. Application or
capability mismatch returns ready=false; storage failure fences without a reply.
A positive authenticated reply matching the full pending request yields one
volatile ReadyLearner exposed through Raft::ready_learner. Commit/term/config
advancement, cancellation or a new round clears the retained result. Promotion
still rechecks scope and the current authenticated peer session at execution.
Node::request_learner_readiness captures the current roster binding and requires
format 4. Lost replies need explicit host cancellation/retry; no automatic retry
loop or service CLI administrator is introduced.

No new persistent record, watermark, generation or durability certificate.
Existing exact WAL completions establish the core's contiguous durable prefix;
read-only snapshot work verifies its existing anchor. Native recovery takes core
limits from the authoritative log provider. Custom hosts must honor that binding
and limits contract; the synchronous helper also compares live LogStore state.
Readiness covers application/log/snapshot capabilities; wire/transport capacity
and placement authorization remain administrator obligations before promotion.

Wire 4 uses tags 12/13 with checked capability-size conversions and exact
reconstructed envelope scope. The request is boxed to bound Event size, with
symmetric decoded/outbound heap accounting and exact-budget tests. TLS/QUIC,
connector handoff and startup preserve exact format selection with no downgrade;
formats 1–3 retain their layouts and refuse readiness tags.

Four new learner histories and one wire history cover suspended verification,
future/stale scope, wrong sessions, duplicate replies, capability denial, storage
failure fencing and recovery, every wire truncation, invalid fields/Boolean,
older-version refusal and exact memory limits. Real TCP/QUIC compacted-learner
histories use native framing/worker and original EffectOwner/SnapshotRouter with
a host-selected snapshot provider; initial enrollment and peer-2 quorum replies
are host assertions. Separate existing tests verify actual native-file readiness
and compacted reopen. This is not yet remote online enrollment/promotion.

Affected library, effect-owner, learners, wire, outbound, runtime, Raft, snapshot,
snapshot-worker, secure sessions, QUIC, QUIC connector, startup and service suites
pass. Native histories additionally verify delayed/stale completion, other-group
progress, original send credits and closing with accepted work pending. Static
three-node TCP/QUIC startup commits/drains/recovers under formats 2/3/4. A fixture
initially canceled a nonexistent read in the unrelated group; replacing that
with an inert authority cancellation corrected the test. No production failure
semantics were weakened. Core-only checks, Clippy/API docs and inventory checks
are recorded in validation/REPORT.md. Public configuration-bearing Append and
membership Snapshot remain gated pending online administration, placement and
distributed activation/fault checks. No macOS, physical power-loss, benchmark or
full proof claim; the complete P0–P7 goal remains active.

## Slice 55 — elections across partially delivered final membership

Mini schema plan: retain existing message fields, local membership authority,
ballot origin and exact LogTicket/DurableLog dependencies. Treat a Vote request's
configuration as reply-correlation scope while validating the exact candidate
store against the receiver's accepted electorate and log freshness. Keep Voted
responses bound to the candidate's current configuration/context. Learners gain
no election authority. No new format, token, watermark or generation is needed.
This is a P4 activation prerequisite: without it, partial final delivery followed
by old-leader loss strands an otherwise sufficient surviving quorum.

The reproduced actual-core history starts with committed joint membership,
delivers the final record only to one survivor, loses the retiring leader and
previously failed with WrongIdentity on the survivor's Vote request. It now
elects using joint-view ballots, repairs a follower, commits a current-term
entry and recovers the follower while preserving its historical ballot origin.
Prepared journal states and host-asserted durability tokens isolate the core;
this history is not remote online enrollment or physical storage evidence.

The independent activation model enumerates 3,007 durable prefix placements,
20,029 election certificates and 98,488 certificate pairs across overlapping
and disjoint sets with majority, weighted and nested policies. Mutants reproduce
new-only joint-quorum and premature-final safety failures, the equal-scope
election stall and written-before-synchronized ballot failures. This factored
model checks fixed-term linear-prefix placements and receipt/restart behavior;
it is not a proof of arbitrary forks, term traces or network liveness.

Public host/native conformance checks exact completion, local ballot origin,
repeat requests with changed scope, foreign-store and second-candidate rejection,
and stale-log denial after durable higher-term observation. Native sync/manifest
faults with power-loss recovery release no uncertain vote reply. Actual native
files preserve the promise on reopen. Existing negative configuration tests now
exercise ReadProbe; positive Vote bridging has dedicated assertions.

Affected core, ballot, learner, member-recovery, Raft and runtime tests pass,
including existing native TCP/QUIC histories. Those network histories do not
establish the new mixed-head election path over sockets. Focused core-only checks
also pass. See validation/REPORT.md for final checks. Online administrator,
placement/capacity admission and broader activation failure histories remain
next; public configuration-bearing Append and membership Snapshot stay gated.
P5 routing, P6 split/merge and P7 tuning remain outstanding under the full goal.

## Slice 56 — owned Node administration and committed outcomes

Mini schema plan: reuse exact owner AdmissionTicket and ProposalPosition for
bounded configuration observation. Retain one record/result per group with
global count/byte limits; keep the original proposal/proof allocation in the
existing charged owner queue. Extend normal ClientRouter/ReplicaDriver advancement
with execution authorization while preserving application admission. Node checks
the selected wire version and exact live promotion sessions; host authorization
covers service scope, failure domains and provider capacity. No direct membership
mutation, new durable record/token, generation or watermark is introduced.

The public ConfigurationRequests observer and Node configure/poll/cancel path
now report exact-record committed receipts, pre-proposal errors or unknown
outcomes. Configuration steps carry operation and proposed index/term. Only the
exact record in the selected core's durable committed prefix yields success;
Written, queue admission and prospective accepted membership do not. Queued
requests use their actual execution term rather than admission-time leadership.
Cancelled waits preserve queued work. Shutdown retains committed results and
reports unresolved work unknown; recovery transfers results with original
providers. Ordinary Node polling denies configuration execution, and explicit
host-authorized polling retains existing application validation.

Host Node histories cover joint/final sequencing, no receipt before persistence,
authorization denial at execution, static-wire rejection, byte/count retention,
cancellation followed by actual commitment, queued campaign, Written failure and
shutdown/recovery. Native startup selects format 4, commits a same-electorate
single-voter joint/final transition through its WAL worker, joins the workers,
reopens actual files and reconstructs exact membership with explicit member
recovery. Static NativeStartup recovery is not extended to dynamic journals.
See [administration contract](CONFIGURATION_ADMINISTRATION.md).

The existing queued-promotion test now expects configuration operation/position
metadata instead of None. One fixture initially used an invalid zero-effect
budget; a valid one-effect budget establishes the intended pre-durability check.
An existing QUIC service run encountered AddrInUse; its isolated rerun passed.
Validation and remaining evidence are recorded in validation/REPORT.md.

Native placement/failure-domain and complete codec/transport-capacity admission,
durable operation status/resumption, service enrollment and faulted remote
add/promote/remove remain required. Public configuration Append and membership
Snapshot stay gated. In particular, broader partial-joint activation/catch-up
histories still need validation; slice 55's partial-final history is not enough.
This advances the current P4 milestone without changing its exit or the full
P0–P7 goal; P5 routing, P6 split/merge and P7 measured tuning remain pending.

## Slice 57 — durable operation status and safe resumption

Mini schema plan: derive separate committed and durably accepted operation phases
from the selected journal and existing checkpoint membership base. Preserve exact
retained positions; report compacted completion without inventing phase/payload,
and active compacted joint metadata without inventing its discarded term. Reuse
existing operation IDs, commit/log boundaries and normal owned admission; add no
format, generation, durability token or watermark. Pending/Written state cannot
advance status, local absence is inconclusive, and only a committed joint with
matching accepted joint produces a final planning hint. Queued work and failure
remain subject to normal authorization, journal and persistence checks.

GroupLog/Raft expose ConfigurationOperationStatus; Node exposes status and
resume_configuration. A resumed final uses the existing joint operation/target
with a fresh ticket and execution authorization. Accepted uncommitted final waits;
completed operations return Completed; absent local identity does not reconstruct
learner intent. RecoveryRequired/fenced owners refuse status. Existing retained
results still occupy their group slot until consumed. See the
[administration contract](CONFIGURATION_ADMINISTRATION.md).

Shared host/native histories check durable versus committed phases, rollback,
exact group scope, checkpoint compaction and discarded joint term. Native faults
cover Written commit and uncertain sync/publication followed by modeled power
loss. Actual files reopen a resumable joint and committed final, and existing
checkpoint/reclaim reopen histories now inspect compacted completion. Node tests
lose the original wait, deny resumption through default polling, then authorize
a fresh final. Native startup uses resumption for its same-electorate one-voter
final and inspects completion after actual WAL reopen. This is local evidence,
not multi-node enrollment, distributed activation or dynamic NativeStartup recovery.

Native policy/capacity admission is next, then enrollment/service endpoints and
faulted remote transitions. Public configuration Append/membership Snapshot stay
gated. Partial-joint catch-up/election histories remain a release requirement;
the slice 55 partial-final check does not establish them. This advances P4's
restart behavior without shrinking P0–P7 or claiming P5–P7 implementation.
Validation is recorded in validation/REPORT.md.

## Slice 58 — native placement authorization

Mini schema plan: add a synchronous public PlacementAuthorizer contract and
native bounded group-scoped assignment plan. Separate FailureDomainId from
quorum and responsibility identities. Exact store assignments cover voters and
learners; minimum voting domains and optional any-single-domain-loss tolerance
apply to each old/new predicate using validated recursive quorum evaluation.
The caller owns/reconstructs immutable policy; Node borrows it at execution.
Construction failure returns original assignments, denial accepts no persistence
work, and no receipt/generation/watermark or asynchronous cleanup is introduced.

NativePlacementAuthorizer implements that contract; Node's selected-authorizer
polling path composes it through existing execution authorization and all normal
core/binding checks. Bounded plans retain at most 4,096 replicas and 64 domains.
The policy checks accepted stable/joint and proposed configurations, including
finalization. Learners supply no voting domains. Store identity uniqueness is
checked in the plan; deployment labels remain trusted assertions, not physical
failure-independence evidence. See the [administration contract](CONFIGURATION_ADMINISTRATION.md).

Six downstream native-policy tests cover group incarnation, store incarnation,
unknown learners, count versus actual weighted/recursive survival, valid weighted
and nested policies, accepted joint target during finalization, journal rejection
of unprepared voters and owned constructor rejection. Host Node injection checks
policy revocation after queue admission and fresh retry commitment; native startup
uses the selected native placement policy for local joint/final resumption and
actual WAL reopen. This does not claim remote membership delivery or full native
capacity admission. A test initially called a private journal helper; it now
checks the same unprepared-voter rejection through public Membership replay.

Complete selected codec/transport admission is next, then service enrollment and
faulted remote transitions. This division follows the distinct existing placement
and transport contracts; both are required by the same P4 milestone. Public
configuration ingress remains gated. P5–P7 and remaining Linux/macOS operational
validation stay active; no new global milestone or reduced exit criterion.

## Slice 59 — selected codec/transport configuration envelope admission

Mini schema plan: extend public codec/factory contracts with a synchronous borrowed
ConfigurationWireRequirements query and versioned ConfigurationWireCapacity
footprints for one proposed append, declared command and membership checkpoint.
Reuse native encoder counting/validation and existing journal grammar preview;
count opaque application bytes virtually. Node rechecks the actual selected
factory and roster format/frame/decoded limits at execution before persistence.
Default providers deny the optional operation. No async work, application-sized
counting buffers, new durability effect/token, generation, watermark or format.

Native formats 2–4 count prospective stable/joint metadata, immutable bootstrap
and retained operation IDs, including decoding retention. Native factory rejects
malformed/over-budget reports; roster independently checks its own upper limits
and selected format even without an active connection. Core grammar remains
mandatory. Local-only host administration does not require an invented transport.
Static traffic and explicitly selected native TCP/QUIC use their existing path.

Four downstream tests compare counts with actual frames/decode, exact and
one-byte-short budgets, operation-history growth, unsupported format, policy
limits and invalid envelopes. Host Node injection exercises unsupported capacity,
wrong version, restrictive roster budget, malformed footprint and explicit success.
Native startup rejects an oversized envelope without changing its log, then
commits/resumes/reopens a valid same-electorate one-voter joint/final operation.
A host fixture initially replaced connector-compatible roster limits with generic
defaults; it now retains the original limits. Test requests explicitly clone owned
proposal data instead of requiring ConfigurationRequest to implement Clone.

This establishes declared envelope capacity, not arbitrary multi-message batch or
future application/history size support. Native service integration must enforce
actual schema/command/checkpoint bounds as deduplication grows, enroll exact peers,
and support dynamic recovery/admin endpoints. Faulted partial-joint and remote
activation histories remain release gates. Configuration ingress stays closed;
full P0–P7 remains active and no macOS/performance/formal completeness claim is made.
See validation/REPORT.md for actual checks.

## Slice 60 — explicit native member restart

Mini schema plan: introduce NativeMemberStartup as explicit trusted deployment
input containing original startup/bootstrap and a bounded exact provisioned-store
map. Require Recover and membership wire selection; never create/enroll from
incoming messages. Reuse verified recover_member_replica, existing store sessions
and native assembly/cleanup. Derive active peers from recovered committed/accepted
and rollback-reachable views; require exact provisioning for all of them. Retain
extra provisioned hints without granting votes or active roster membership. Return
the application and normal cleanup handle on failure; files/sessions are not rolled
back. No new effects, durability token, generation, watermark or file/wire format.

NativeMemberStartup::open_with_protocol reopens a locally committed and accepted
exact member, including a node absent from the original bootstrap voter set.
The common build path selects static or member verification explicitly. Native
TCP dial authorizations and TCP/QUIC pins use the selected provisioned map. The
roster uses the recovered required peers, retaining old peers behind uncommitted
finals and allowing obsolete provisioning to disappear after final commitment.
Existing static startup entry points continue to refuse dynamic journals.

Eight downstream tests seed native durable journal fixtures and reopen learner,
joint, accepted-final, committed-final and compacted state over TCP/QUIC. They
check non-voting campaign rejection without term advance, restored application
retry state, retained/future hints without active voting authority, exact rollback
peer provisioning, uncommitted local assignment, removed-local refusal, missing
checkpoint rejection, pre-I/O mode/version validation and late worker/socket
cleanup. Existing startup histories still pass, including real three-node static
wire/session selection. Seeded fixture recovery is not proof of distributed
configuration commitment or native network enrollment. See the
[service/embedding guide](COUNTER_SERVICE.md).

Next is native enrollment/admin service integration and enforced application
bounds, then full faulted remote transitions. Public configuration ingress stays
gated, including partial-joint catch-up/election release checks. This advances
P4 restart usability without changing its exit; P5–P7 remain active. Linux results
are not macOS, physical failure, performance or full proof evidence.

## Slice 61 — explicit checkpoint enrollment for native learners

Mini schema plan: complete the trusted handoff portion of the current enrollment
and administration deliverable, rather than seeding native files in test-only
code. Accept an original-bootstrap-bound committed Snapshot, exact learner/store,
selected log/snapshot providers and a fresh application. Validate stable learner
assignment and restore on a clone before import. Publish, pin, persist the log
boundary and verify recovery in that order. Reuse exact publications/completed
imports on retry; reject different images and progressed stores. Caller establishes
source commitment. No source ballot is copied, no network message grants assignment,
and no new durability token, generation or watermark is introduced. Existing
snapshot publication, pin and log barrier establish their respective dependencies;
the commit index remains a contiguous imported prefix.

The public snapshot::enroll_learner_snapshot uses existing public storage seams;
NativeMemberStartup::enroll_snapshot creates or reopens selected native files with
bounded explicit provisioning. No workers/listeners start during import. The
result can reopen through the existing member startup over TCP/QUIC. Failed initial
creation may leave incomplete bootstrap/snapshot-store setup; Recover refuses it
rather than creating missing files or replacing uncertain state. This limitation
is separate from publication/pin/log transition recovery once both stores exist.

Host-provider conformance preserves application retry state, non-voting status
and exact completed retry. Snapshot boundary faults and native WAL short-write,
sync and manifest faults check non-exposure of partial assignment and same-image
retry after recovery. Native filesystem imports reopen through TCP and QUIC.
These trusted fixture sources do not establish a remote commitment certificate,
network enrollment, fault-tested joint activation or online service release.
Current mini item remains enrollment/admin integration and enforced application
bounds; next is fault-tested remote membership, then P5 routing. Full P0–P7 stays
active; P8 remains deferred. See validation/REPORT.md for executed checks.

## Slice 62 — service configuration observation and enforced counter envelope

Mini schema plan: add a bounded local configuration-status command using the
existing Node durable observer, with explicit evidence scope, committed/accepted
phases and non-authoritative resumption hints. Parse one nonzero operation ID;
retain no operation request or mutation ticket. Reply contains fixed-size scalar
phase data, never configuration policy payload. Missing local history is
inconclusive; follower observation grants no read or mutation authority.

Bind the counter service's declaration to its actual configured capacity using
Counter::readiness_requirements/validate_readiness_requirements. Schema is fixed,
commands are eight bytes and the maximum checkpoint includes all retained retry
outcomes, independent of current occupancy. Existing proposal admission accounts
for pending identities, application checks capacity and restore rejects a capacity
change. Compare declared bounds with selected native payload limits before open.
No new durability effect, generation, watermark, protocol or format is introduced.

Real TCP/QUIC process histories query local absence on all nodes and after restart,
reject operation zero and retain application write/read/retry/leader-loss behavior.
Application tests fill retry capacity, preserve overflow/duplicate state through
restore, reject undersized/wrong-schema declarations and forbid capacity-changing
restore. A full 10,000-operation service checkpoint round-trips through native
wire formats 1–4 with its exact 330,032-byte payload.

This completes service observation and the counter-specific application bound,
not a generic host envelope capability or service mutation API. Dynamic phase
semantics remain supplied by the existing Node observer tests; the CLI process
fixtures have static membership and observe local absence only. Growing membership
metadata still uses selected configuration-capacity admission. Current mini item
remains enrollment/admin integration: connect mutations and readiness to these
bounds, placement and provisioning, then exercise faulted remote transitions before
release. P5 routing and P6 split/merge remain subsequent global milestones; P0–P7
is active. See validation/REPORT.md for executed checks and limits.
