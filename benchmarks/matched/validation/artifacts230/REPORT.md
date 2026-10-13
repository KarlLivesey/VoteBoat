# Independent asserted release qualification

The unified harness and untouched pinned upstream benchmark were built separately
on Linux and macOS. Compiler artifact records show non-fresh release executables
with optimization level 3, debug assertions enabled and overflow checks enabled.
Original verbose compiler output, compiler identities, independent locks and
source/artifact manifests are preserved under each host directory. No executable
or source checkout is included in this evidence archive.

The matched workspace has an independent 149-package lock; untouched upstream
has a 195-package lock including optional profiling dependencies. Shared runtime
dependencies resolve to the same versions. The Linux dependency comparison
records extra lock-graph versions from VoteBoat's ring dependency and upstream's
optional profiling graph; production dependencies were not altered.

Mac qualification passed six meaningful tests, standalone formatting, strict
all-target Clippy and separate CLI checks for both engines. Each CLI check
completed 16,384 originals in 4,096 requests and all three applied frontiers were
17,409. Linux standalone qualification is also preserved in the existing
`validation/linux` directory. These are scoped harness checks, not throughput
qualification or a claim that the minimum has been met.

Linux used rustc 1.98.1 and macOS used rustc 1.98.0. Both comparison engines use
the same compiler within each host; compiler versions are not identical across
hosts. macOS used the recorded reviewed protoc 3.21.12 path and checksum.

Before subsequent root edits, exact commit
`8d59724cb29769b4edba8a2babee8d33ec784561` was exported to the ignored baseline
directory recorded in `baseline8d59724-provenance.json`. All selected source files
are byte-identical to the original compiled 230e037 source export. Immutable
source hashes and original compiler paths remain recorded; future baseline
guards must use that preserved source rather than the changing working tree.

The Linux short primary pilot is archived separately in
`validation/pilot-linux131072`. Mac timing and the full comparison curve are
pending coordination.
