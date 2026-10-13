# Validated log deltas

Core and native storage stage plain appends and metadata changes without copying
unchanged history. Cached allocation/membership inspection avoids old scans.
Configuration history, snapshots and replacement keep the full validator path.
Disk/wire/public APIs and exact durability/quorum boundaries are unchanged.

Linux: all-feature affected470/0, core-only296/0; real TCP/QUIC service retry,
leader replacement and native-file recovery2/0. Formatting/four strict Clippy
configurations: zero. Shared oracle includes504 finite differential cases;
allocation controls preserve old payload pointers and normalize new capacity.
Native controls cover multiple unsynchronized appends, metadata, subset/old
tickets, all-written promotion, rejected batches, and I/O/publication fencing.
Mac1328-input qualification passes all470/core296, fmt/four strict0 and three
complete source guards. [Mac records](mac/README.md).

## Three interleaved paired trials per cell

Reference production6c404559 versus candidate, identical harness/profile:
Linux AMD9800X3D,16 logical CPUs; opt3, assertions/overflow checks enabled.
All source and copied executable/library hashes match before/after.96 cases;
no local Cargo builds/tests overlapped successful timing. Desktop/cache conditions
are uncontrolled. One sandbox child-observation EPERM attempt is excluded.

Native real codec, simulated volatile barrier,512 measured transitions after16
warmups; exact complete state and native recovery checked after timing. Pure
append history grows identically in each pair; unrelated histories stay fixed.

| Unrelated groups | Initial history | Reference mean ns, median trial | Candidate | Median paired reduction |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 256 | 22,979 | 1,763 | 11.79x |
| 0 | 4096 | 169,516 | 1,523 | 109.18x |
| 8 | 256 | 18,911 | 1,532 | 12.33x |
| 8 | 4096 | 138,181 | 1,507 | 90.65x |

Replacement fallback controls improve1.56–1.84x by moving the staged full state
after publication instead of cloning it again. They retain history-sized work.

Unchanged three-Core framework:4096 empty commands +1024 singleton warmups,
clients1/64/256/1024 and batches1/4. Median paired rate improves1.73–2.03x.
Candidate64-client batch4 median83,673 entries/s; singleton7,690/s.
All original outcomes and all-replica applied prefix5121 are checked.

**Unmatched volatile CPU diagnostics; no OpenRaft floor or durable application
win.** The framework's modeled MemoryStore still uses the full materialized
validator, leaving history-sized provider cost. Its single cooperative driver
and no purge also differ from the pinned upstream benchmark. Native ready
batching, replication overlap and optimized configured-history paths remain.
