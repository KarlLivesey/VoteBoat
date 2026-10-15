# Slice466 Linux: rejected

All 24 cases and validators completed with exit 0. Every candidate OFF throughput and p99 pair regressed on the required Linux host. Each case completed 1,048,576 originals plus 1,024 warmups; all three frontiers reached 1,049,601. These short volatile, in-process framework runs are not sustained P9 acceptance.

| Cell |455 OFF originals/s |466 OFF originals/s |466 ON originals/s |OpenRaft originals/s |455 OFF request p99 ms |466 OFF request p99 ms |466 ON request p99 ms |OR request p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
|bulk|7,444,889|6,055,125|5,868,409|3,327,889|3.668682|3.807844|4.039379|7.009550|
|singleton|108,097|77,659|63,826|20,105|0.018955|0.024576|0.030036|0.127469|

Bulk OFF rate ratios: 0.802230 / 0.854225 / 0.831950. Request p99 ratios: 1.037932 / 1.193558 / 1.187429.
Bulk ON rate ratios: 0.867392 / 0.760396 / 0.806296. Request p99 ratios: 1.162000 / 1.310084 / 1.080353.
Largest named-point charge across the ON trials: Core 2,256,000 bytes; store 2,256,000 bytes. Each owner is charged in full. These values are not summed and are not RSS or continuous peaks.

Singleton OFF rate ratios: 0.711896 / 0.711152 / 0.733919. Request p99 ratios: 1.313873 / 1.325897 / 1.269058.
Singleton ON rate ratios: 0.626518 / 0.603649 / 0.588744. Request p99 ratios: 1.400107 / 1.558207 / 1.599420.
Largest named-point charge across the ON trials: Core 1,030,528 bytes; store 1,030,528 bytes. Each owner is charged in full. These values are not summed and are not RSS or continuous peaks.

Actual per-case rates, latency, intervals, maintenance/checkpoint/snapshot/queue/retention/proposal counters and ON owner observation counts/last/max are in READOUT.json and the raw reports. Original stderr/exit/time/commands are retained. There is no separate framework refusal counter; fixed requested completion does not establish offered-load headroom.

ON owner charges include full backing, payloads, hidden entries and descriptor capacity at named points over the actor/store lifetime. They exclude pending/effect/application/snapshot/queue/transient coalescing allocations and other process memory. Baseline observation metrics are unavailable: N/A, never zero. Nested elapsed intervals overlap and are not CPU time.

Optional GNU time/RSS wrapper preparation failed because /usr/bin/time was absent. The changed-driver guard refused before workloads. Original drivers and commands were restored byte-exact; resource-extension preserves the failure and restored guard. No wrapper, installation, RSS claim or rerun was used.

Fresh CPU data is archived separately by its owner. These paired results establish rejection, not its cause. No allocator inference or retuning follows from them. P9 remains active with sustained, seven-cell, both-host, durable and composition requirements open.
