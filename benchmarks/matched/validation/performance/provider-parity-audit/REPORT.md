# Pinned minimal-provider and timing audit

Read-only audit of OpenRaft e88bb40707f2e511d8ecc44d31edcb1d814a11a8 and the
current matched host after reviewed 422/423 integration. No source, configuration,
dependency or timed workload was changed. Cargo's pinned checkout identifies that
exact commit; the separately frozen upstream store file has the same SHA256.

## What the upstream provider actually retains

[Minimal store](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/src/store.rs)
lines 96–99 hold vote, actual `BTreeMap<u64, Entry>` and purge boundary under
Tokio RwLocks. Append 270–275 moves every original entry into that map; reads 156–169
clone the stored entries in the requested range. Truncate 244–251 and purge 256–264
remove actual entries. get_log_state 222–233 reports the retained map's last entry
or purge boundary. This is real volatile log retention, not fabricated reads or
lastLogId-only storage.

The application state 90–94 is deliberately minimal: last applied LogId and last
membership. Apply 308–325 advances those and sends an individual original's empty
response. It does not store user rows or deduplication data. Snapshot 181–216
serializes that state as JSON, clones the bytes/metadata for current publication
and returns the image. Snapshot install/get-current 332–365 deserialize or clone
that real image. There is no physical disk durability in this provider.

[Minimal network](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/src/network.rs)
lines 104–110 resolve and retain the target Raft handle. append 121–134 passes an
owned RPC to that handle and awaits its response. Default simulated delays are
zero; serialization/socket costs are absent. Our host also transports owned
messages in process, through bounded peer queues. Those are different scheduling
models, not evidence that upstream skips peer consensus or log writes.

## Additional VoteBoat obligations and removable costs

| Surface | Preserved VoteBoat obligation | Potential implementation cost; not a feature to remove |
| --- | --- | --- |
| Log provider | Authoritative store binding/session, group incarnation, exact revision/generation/ticket, separate admitted/completed visibility, atomic checked owned updates | Per-update one-group planning map and group/ticket Vec allocation in MemoryStore/OwnedLogState; singleton-owned containers/reuse could avoid allocation while retaining atomicity and every field check |
| Core state | Full canonical ordered original entries plus provider's ordered state | Separate copies/entry Vec allocations; exact borrowing or immutable ownership sharing could reduce copies without replacing content checks with metadata |
| Checkpoint log proof | snapshot::verify_log checks binding and full GroupLog equality | state() materializes a complete clone before equality; a default-full-equality matches_state seam with exact borrowed overrides could remove the clone, retaining content comparison and visible/durable errors |
| Snapshot provider | Exact metadata/binding/generation, staged write/seal/publish, checksum, pins and anchored reconciliation | Repeated small metadata validation/image clones;422 will measure them rather than infer they dominate. An 8-byte Noop image's CRC is not an explanation for 400ms by itself |
| Compaction | Exact snapshot boundary/term/configuration and retained byte accounting for Core and provider | Removed-prefix payload-capacity scans and ordered Vec drains occur twice; prefix accounting/canonical layout may reduce work while retaining every budget and removal/drop obligation |
| Client results | Every nonzero 128-bit OperationId is retained/checked, duplicate admission rejected, each applied receipt correlated and awaited | BTreeMap pending lookup and singleton proposal/reply/wait Vecs. Upstream has empty application request/result types; its InlineBatch uses SmallVec<[T;1]>, avoiding singleton container allocation. Preserve our ID checks and per-original completion rather than replacing them with an opaque count |
| Host network | Owned bounded message delivery plus identity/context/currentness validation | Queue scheduling and full entry ownership may differ. No queued-control bypass or weaker protection is justified;420's lookahead regressed and remains rejected |

The pinned engine uses InlineBatch backed by SmallVec, not a shared Arc payload
batch (openraft/src/batch/inline_batch.rs1–14). Its per-original oneshots remain;
there is no batch-four single-future response aggregation advantage to mimic.
Current Noop application checkpoints carry only the known group/applied prefix,
not general row/dedup state. Do not extrapolate either minimal workload to a
real application or remove the production contracts to make this table match.

## Timing boundaries and the guard defect

[Untouched driver](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/src/bin/bench.rs)
uses WAIT_TIMEOUT=400s at 28 and wraps do_bench 273–276, after cluster creation.
Our main.rs23/137 also wraps the whole workload in 400s, after creation. Both guards
include warmup; our guard additionally includes exact follower verification and
latency sorting. Shutdown is outside our campaign guard. Current shell curve
scripts impose no separate external timeout. Extending an outer watchdog cannot
bypass the untouched driver's internal 400s limit.

Both reported throughput clocks begin at the ready-client barrier after warmup and
end after all client completions. The implementation currently differs in sample
aggregation: our workload.rs143–149 flattens all client latency vectors before
stopping that clock; upstream bench.rs 408–414 retains each vector, stops elapsed,
then flattens/sorts 431–435. Moving only our flattening and global sample validation
after elapsed is an avoidable host mismatch. Every client still waits/checks each
original before completion; all counts, samples and follower proofs must remain.

A 20M singleton campaign needs more than 400s at any rate below 50,000/s, even before
warmup, sorting or drain. At 11,000/s it needs 1818.18s. The archived untouched Mac414
short singleton trials are 12,287.61/10,233.22/10,004.55 originals/s: median implies
1954.42s, and even the best implies 1627.65s. These are projections, not sustained
measurements. Even the 49,500/s published-singleton margin minimum takes 404.04s.
Consequently the current guard can censor a reference cell and a passing-floor
candidate. A timeout is an invalid incomplete campaign, never a zero throughput
sample, a speed cap, or permission to count partial writes as success.

## Small next steps

1. Fix the guard and flattening boundary as measurement correctness before a full
   comparison; separate schema covers finite common budgets and reference status.
2. Use 422's actual state/snapshot scoped methods to attribute the persistent
   maintenance interval. If the clone is material, the exact borrowed equality
   seam is the smallest reusable next slice; if residual dominates, inspect its
   equality/compaction ownership work rather than deleting proofs.
3. After maintenance, measure singleton request and one-unit provider allocation
   costs. A one-or-owned-batch representation can transfer to native hosting;
   FIFO keyed only by this benchmark's monotonic IDs cannot be generalized to
   arbitrary multi-group/replay/native routing without preserving those contracts.

Source hashes and numerical projection inputs are in SOURCE.json. This audit makes
no performance claim, suggests no rival slowdown and authorizes no implementation.
