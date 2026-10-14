# Raspberry Pi hardware/ISA comparison

The user requested this additional host to test different CPU capacity and
instruction sets and avoid optimizing for one use case. Authorized SSH is
karllivesey@ups.local. Read-only preflight succeeds: Pi4ModelB Rev1.2, four
Cortex-A72 cores,64-bit Debian13.7/kernel6.18.50,3.7GiB RAM,ext4 SD storage,
ondemand600–1500MHz,52.1C/no reported throttling. M2 and Pi are different ARM64
implementations; Linux desktop is x86-64. This adds hardware/ISA evidence, with
the original Linux/M2 full P9 floors and all remaining acceptance unchanged.

Before setup: the initially observed Rustup process is now absent; its cause is
unknown. Stable1.99.0/LLVM23 is installed and default. To keep compiler drift out
of the CPU comparison, install an additional user-local minimal1.98.1 toolchain,
matching Linux1.98.1/LLVM22; leave the user's default1.99 untouched. Record paths,
versions/hashes. Mac1.98.0 patch difference remains disclosed. No CPU-native flags
or platform-specific algorithm changes. One build job and fresh targets on
disk-backed HOME; /tmp is1.9GiB tmpfs and unsuitable for build trees. Do not change
swap, governors, cooling, policy, UPS, Docker or other services.

Existing cached Debian arm64 Protobuf3.21.12-11+deb13u1 packages permit a user-local
protoc without sudo/system installation. Download only protobuf-compiler and
libprotoc32t64, verify cached repository package hashes, extract into the fresh
job tools directory; installed libprotobuf32t64 already matches. Verify actual
ELF/loader/version and record any wrapper/source hashes. No apt update, dependency
update or incompatible system compiler substitution. No installation if the
required tools are already available when checked.

Source/API/ownership: after443 is checked/published, freeze committed source,
all modules, fixtures, locks/config and comparison contract in a fresh Pi job.
Preserve source bytes/permissions including private fixtures0600 and prove exact
before/after inventories. Transfer no binaries/build trees/WAL states. Existing
original channels/results, quorum/ordering/auth/durability/retry/fencing and
provider composition are unchanged. This is a measurement host, no new seam.

First CPU comparison: existing minimal framework on one group/three voters,
empty payload/volatile stores/in-process RPC,1024 singleton warmups,clientruntime1/
serverruntime16. Both VoteBoat and pinnedOpenRaft e88bb40707f2e511d8ecc44d31edcb1d814a11a8
use the same Pi/compiler/profile. Fresh asserted opt3/debug2/thinLTO/codegen1 build;
unset inherited flags/wrappers, capture CargoJSON and actual verbose rustc flags,
root/executable/upstream library/minimal fresh:false plus source/lock/tool/symbol
identities. Native disk results/profile are separate, including the SD device.
Cover all seven framework cells (batch1 clients1/64/256/512/1024/4096 and batch4 clients4096), three rotating trials,1,048,576 originals each,
VoteBoat OFF/ON and OpenRaft. These are diagnostic baselines, not20M acceptance or
hardware-normalized speedups. Batch4 equivalent envelopes and upstream soft merge
threshold, snapshot representations and timer-after-client-joins-before-sorting/
verification/shutdown stay disclosed. Keep per-original outcomes.

Finish all setup/builds/checks/copies before any Pi timing; no competing benchmark
or maintenance changes on that host. Keep existing UPS/background services running
and disclose their load. Record temperature/frequency/throttling and available
memory/storage before/after, plus raw latency/refusals/maintenance/retention where
exposed. No browser/SVG, profiler installation or sudo/sysctl. Preserve original
handles, raw failures/slow tails; no timeout-only restarts. If limited RAM or source/
compiler/contract checks fail, make a focused correction before new timing and
retain original errors. Collect only after actual termination.

Acceptance for this slice: proved Pi source/artifacts/tool/profile identities,
matching original counts/frontiers/case settings, actual completed originals/s
and p99 for every completed trial; no fabricated zero refusal/RSS/debt claim.
Use the Pi as additional evidence for subsequent optimizations, not an excuse to
remove real payload/application/provider/fairness cases or shrink the full P9 goal.
