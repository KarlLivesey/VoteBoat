# Implementation record

## Scope and roadmap

This repository implements the proposed VoteBoat baseline in bounded vertical
slices. The local architecture pack, version 0.2 of 7 October 2026, remains the
design input and is intentionally excluded from Git. Nothing in this status
record claims that unimplemented phases already work.

| Phase | Intended behavior | Current status |
| --- | --- | --- |
| P0 | Checked identities, validated policies, public seams, deterministic failure harness | Storage/core/application seams and reproducible fault schedules implemented; virtual-time runtime and other subsystem contracts remain |
| P1 | Native durable three-node Raft, application retries, recovery, snapshots and reads | Static-config elections, replication, conflict repair, recovery and deduplicated counter demo implemented; transport, reads and snapshots remain |
| P2 | Shared Multi-Raft, bounded scheduling and overload isolation | Pending |
| P3 | Recursive quorum integration at every consensus quorum site | Elections and durable commitment use validated predicates; read/check-quorum sites and full audit remain |
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
production release gate. There is no production network service, distributed
linearizable-read API, checkpoint/snapshot, joint reconfiguration, scope transfer
or throughput claim yet. Real hardware power-cut testing remains outstanding.

## Next slice

Add read barriers and application checkpoint/snapshot publication, then introduce
the bounded shared scheduler, timers, wire codec and authenticated-session
transport seams. Extend the simulator to explicit virtual time and independently
delayed storage completion events. Native sockets must use established secure
sessions supplied by the host; production assembly cannot silently select an
insecure simulation transport. Preserve downstream substitution and durable
histories as these providers enter the assembly.
