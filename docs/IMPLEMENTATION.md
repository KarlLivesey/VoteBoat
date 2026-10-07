# Implementation record

## Scope and roadmap

This repository implements the proposed VoteBoat baseline in bounded vertical
slices. The local architecture pack, version 0.2 of 7 October 2026, remains the
design input and is intentionally excluded from Git. Nothing in this status
record claims that unimplemented phases already work.

| Phase | Intended behavior | Current status |
| --- | --- | --- |
| P0 | Checked identities, validated policies, public seams, deterministic failure harness | First slice implemented; complete catalogue and virtual-time runtime remain |
| P1 | Native durable three-node Raft, application retries, recovery, snapshots and reads | Durable term/vote slice implemented; replication and the remaining features are next |
| P2 | Shared Multi-Raft, bounded scheduling and overload isolation | Pending |
| P3 | Recursive quorum integration at every consensus quorum site | Predicates validated; protocol integration pending |
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

## Validation actually run

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
No full Raft history, networking, linearizable application operation, snapshot,
joint transition, split/merge or throughput result is claimed by these checks.

## Next slice

Extend storage to recoverable entry/hard-state transition batches with bounded
range fetch and generation-fenced suffix replacement. Bind group creation and
static voter policy to durable metadata. Build a deterministic three-node
replication/election simulator over that contract, then add ordered application,
operation-ID retries, reads and snapshots. Preserve a simple native baseline
and test downstream substitution at each newly introduced seam.
