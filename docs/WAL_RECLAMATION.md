# Physical WAL reclamation

`LogStore::reclaim(max_bytes)` is optional, synchronous physical maintenance.
The default rejects unsupported providers. NativeLogStore implements it when the
selected JournalIo supports atomic replacement and the selected LogCodec supports
live-state checkpoints. Run it on a storage worker or an explicitly drained,
reclaimed store handle, outside the consensus owner's hot loop. NativeLogWorker
now executes explicit bounded requests on its existing thread; Node exposes
reclaim/poll_reclaim. See [live worker maintenance](WORKER_MAINTENANCE.md).

Reclamation requires no outstanding store-side transition tickets and identical accepted and
durable state. It retains every current group, bootstrap/configuration/voter-store
binding, hard state including votes, exact revision and suffix generation,
committed boundary, snapshot reference and complete surviving suffix. It drops
only superseded physical transitions. It never advances the logical retention
floor or makes a new snapshot. Existing snapshot pins, verified application
checkpoint/replay and durable logical compaction must precede any loss of logical
history. General backup/lifecycle retention policies remain future work.

Encoding is bounded by the supplied byte ceiling, itself bounded by max_wal_bytes.
The store round-trips the encoded image and compares the entire recovered map and
batch sequence before I/O. A non-shrinking image leaves the journal untouched.
LogReclaimed reports active journal bytes before and after, not a new consensus
receipt or total temporary disk usage. Revisions, suffix generations, store
binding and batch IDs do not change. Later appends continue the original checked
batch sequence, so cleaning cannot revive stale tickets. Any uncertain I/O error
fences the store until explicit recovery; a lost cleanup receipt is resolved by
recovery, never by assuming the rewrite did not happen.

## Native files and publication

JournalIo is a dedicated public platform contract, with the existing six journal
operations and optional supports_replacement/replace_log. VoteIo remains separate.
A replacement must recover either the complete old journal/manifest pair or the
complete new pair. A provider must synchronize new data and metadata and their
selection before deleting old data. Unsupported providers decline before work;
no weaker in-place rewrite is substituted.

FileLogIo opens existing legacy log.wal/MANIFEST stores unchanged. Its first
cleanup writes log.1.wal and MANIFEST.1; subsequent cleanups alternate slots 0 and
1. CURRENT contains a positive checked selection generation with a version and
CRC32C. That generation selects one pair by parity; it is a physical file-choice
identity, not a logical prefix or a replacement for store sessions. Recovery reads
the selected pair exclusively. Missing/corrupt selections or selected files fail;
an older pair cannot silently restore an older vote or acknowledged history.

Publication order is:

1. Write and sync the inactive WAL and its manifest.
2. Sync the directory containing both new files.
3. Write and sync CURRENT.tmp, rename it to CURRENT, then sync the directory.
4. Select the new open handle, delete the old pair, and sync the directory again.

Before selection publication, abandoned inactive files are ignored and may be
overwritten by a later cleanup. After durable selection, old files are unnecessary
for recovery. A crash can leave the old pair; later cleanup also removes leftover
legacy files. File counts are bounded by two slots plus a possible legacy pair and
fixed staging names. The exclusive LOCK spans all slots and errors. New log and
ballot-store creation both reject these existing store names.

Replacement temporarily requires space for old and new journals plus metadata;
max_wal_bytes bounds the active journal, not total staging space. Disk-full, sync,
publication and deletion failures are uncertain and fence the store. There is no
claim that this first cleaner is incremental or throttled against live traffic.

## Checkpoint encoding and compatibility

The rewritten journal begins with a version-1 VBLCPT01 live-state image, followed
by unchanged format-2 WAL batches. It uses bounded counts/lengths, header and whole
image CRC32C and a complete trailer. Canonical bootstrap/snapshot/suffix transitions
pass the same mandatory validation as ordinary replay; recovery independently
validates host codec output as well. Exact stored revision and
generation values are then restored. An image may exceed one ordinary batch but
must fit the selected journal budget. It must lie entirely inside the manifest's
durable byte boundary. Corruption is rejected, never skipped.

This is an explicit persistent-format extension. Existing uncleaned stores remain
readable; old binaries/codecs that do not implement checkpoint images cannot read
cleaned journals. Unsupported codecs reject maintenance before publication and
reject checkpoint recovery. Do not use binary rollback as a format migration.
The baseline rewrites the full bounded live image; segmented incremental cleaning
and automatic scheduling/throttling remain future work.

## Evidence

Eight downstream tests cover exact state/vote/committed and uncommitted suffix
preservation, ticket continuation and fresh recovery sessions, pre-I/O budget and
pending-work rejection, non-shrinking images, unsupported providers/codecs,
image truncation/bit corruption and invalid semantic state, images larger than one
batch, cold groups, repeated real-file slot reuse, exclusive locks and missing or
corrupt selected files. A native unit test injects errors after each of 13 file
publication/deletion boundaries for both first cleanup and slot reuse (26 cuts).
Each reopens the selected pair, verifies exact state and cleans again.

The crashable host journal models power loss before/after atomic pair publication;
the native file tests cover interrupted process-level operation boundaries, not
physical power-failure hardware. The actual three-node/100-group WAL/TLS facade
history now cleans reclaimed stores before restart, preserves every exact group
state, restores application checkpoints/deduplication and replicates new commands.
These finite Linux histories do not establish macOS execution, arbitrary schedules,
live maintenance fairness or performance.
