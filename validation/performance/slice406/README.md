# P9 first slice: touched-state staging

Base337c235. No disk format, synchronization, RPC, quorum, authentication or client
outcome change. Public transition validation still accepts or rejects the whole
batch before installing any state. Crate-private staging creates only changed
GroupLog replacements. Core singleton transitions avoid the duplicate map clone.
Native barriers synchronize/publish as before, then promote only dirty groups;
all physically covered accepted groups advance, but only exact requested tickets
are returned. Recovery/fencing/generation/retention contracts stay unchanged.

## Checked implementation

Linux all-feature library+Raft/reference+storage/reclaim/striped/provider/shared
barrier+new benchmark controls:431/0. Core-only counterpart250/0. Formatting and
all four strict all-target Clippy configurations: zero. Real three-process TCP and QUIC leader-replacement/retry/native-recovery
service checks pass2/0 (16.02s). Matching Mac controls are running from the
frozen1317-input source; no Mac pass is inferred from compilation.

## Paired diagnostic results

Five interleaved reference/candidate trials per cell: one changed group,256
retained commands per group,16 warmups,512 measured suffix replacements,8-byte
payload, one real native codec/validator append and simulated barrier each.
Equal encoded bytes and original successful states/recovery are checked. No
state reads/model verification/CSV I/O occur inside the measured stages.

| Unrelated retained groups | Median old ns/transition | Median new ns/transition | Median paired duration speedup |
| --- | ---: | ---: | ---: |
| 1 | 24,867 | 9,590 | 2.59x |
| 8 | 108,288 | 9,620 | 11.17x |
| 32 | 378,841 | 9,475 | 40.36x |

This is **volatile native-store CPU attribution**, not device durability, useful
application throughput or a framework floor comparison. The new fixed-work path
no longer pays for every unrelated retained history. The touched history still
copies and scans. Higher-rate inputs and long-run maintenance remain unmeasured.

Three interleaved paired public-Core scaffold trials:3 real Raft cores,512
original empty commands after64 warmups, exact modeled tickets, in-process
messages and no-op ordered application. Single-client median23,260 ->31,811
operations/s;64-client median59,573 ->68,436. This is a short diagnostic with a
single cooperative driver and no purge/checkpoint. **It is unmatched to OpenRaft;
no minimum floor is claimed passed.** Batch4 is explicitly unsupported in this
first slice, not multiplied from one result.

Linux AMD Ryzen7 9800X3D,16 logical CPUs, kernel7.2.9-2-cachyos, rustc1.98.1.
Release compiler artifacts report opt3 with assertions/overflow checks enabled
on both sides. No VoteBoat builds/tests overlapped timed trials; desktop CPU/cache
conditions remain uncontrolled. Exact original binaries/libraries and candidate
source hashes match before/after. Raw CSV/JSON, command lines, five/three paired
trials, state/recovery guards and analysis are retained. Old source is frozen in
ignored target/p9/reference; diagnostic harnesses are identical on both sides.

## Next

Reduce waiting and contention by real ready-command batches and bounded
persistence/replication overlap. The dependency audit identifies exact Written
admission, held send leases and simultaneous Durable callbacks as required
ownership work. Metadata-only transitions, cached derived summaries and bounded
application overlays remove additional O(history) costs. Keep security/features
and useful completion semantics; no new batch or overlap capability is claimed
by this first measured change. See docs/P9.md and the linked macro/mini plan.
