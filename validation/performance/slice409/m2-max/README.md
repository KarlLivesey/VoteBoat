# Apple M2 Max measurements

12 cores,96GiB,macOS27.0/Darwin arm64,Rust1.98. Reference production6c404559;
candidate qualified409 source. Same96 paired workload cases/order as Linux,
three trials per cell. Opt3 with assertions and overflow checks enabled.
Each version uses a separate build directory; genuine crate/example compilation,
source-path/profile checks and distinct original artifact hashes precede timing.
All96 cases and six source guards pass; immutable artifact hashes remain exact.

| Framework clients/batch | Reference entries/s | Candidate | Median throughput ratio |
| --- | ---: | ---: | ---: |
| 1/1 | 4,474 | 8,904 | 1.99x |
| 1/4 | 17,811 | 35,550 | 2.00x |
| 64/4 | 67,977 | 131,319 | 1.93x |
| 1024/4 | 66,110 | 121,142 | 1.83x |

Native pure append: initial history256 improves8.11–8.16x; history4096
improves59.85–65.07x, reaching2.66–2.93µs per staged append/simulated barrier.
Replacement fallback improves1.40–1.80x. SUMMARY.json contains all cells,
latencies, hardware and aggregation; raw measurements/build commands remain.

**Volatile CPU diagnostics, single cooperative framework driver, no OpenRaft-floor
or real-device durability claim.** Framework rates count all original applied
commands; native state and modeled power-loss/recovery checks run outside timing.
Linux/Mac numbers are separate hardware results, not a platform speedup claim.

The initial shared-cache campaign reused reference binaries for the candidate.
It is invalid and excluded; its raw records are retained separately. Corrected
builds enforce isolated targets plus nonfresh/source-path/distinct-artifact gates.
Compiled artifact copies remain under local target/p9/mac-perf409/artifacts;
their verified hashes and original compiler paths are archived here.
