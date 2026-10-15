# 484 Linux fresh native artifact qualification

Published 483 source `efd383dc36b4b9edf8f66021b90115824440b468` was exported once
(original 76794, actual 0), built fresh once (85533, actual 0), then functionally
controlled once (41354, actual 0). ORIGINALS preserves actual terminal chunks and
commands. Export includes 1503 exact files/modes: the 1502 checked source inputs
plus the committed 483 schema. All seven private-key fixtures retain 0600 modes.

CargoJSON and verbose rustc prove the root library, executable and buildscript
compiled fresh:false. The actual native profile is opt3/debug0 with assertions
and overflow checks, native defaultLTO/codegen; inherited flags/wrappers are
unset. Compiler/protoc bytes match immutable 480. Original and copied executable
SHA is 0ff042c1ade83771217ed96af4a0f24912d79917eb4f95cb0d8230808780963d;
root rlib SHA 8643d5a310e63ff5a1de9d311054e005d4b5841237e8b647cd66710f2a86366b.
ELF buildID ee9d5a035f2a39596b453aca1993d1f2223abdcf and debug0 symbol evidence
are retained. The unchanged 480 executable 8d3530a6… remains the future baseline.

Three original functional controls each apply 512 originals, recover 576,
verify individual outcomes/retries, join workers, and finish one checkpoint
and three reclaims with zero refusals/unknown/pending. Explicit Witness TCP/TLS
and QUIC select ready64, two groups, 31:1 offers, 512@128/s/window4096; each has six
capacity and six provider Create/Recover readbacks. Both publication validators
and all three full native/storage validators exit 0. The omitted-publication/
omitted-ready default TCP control uses FileLogIo/ready16/one group: six capacity
readbacks, no opt-in publication fields/readbacks/CSV. Its real CURRENT 20B and
MANIFEST 52B CRC/acknowledged WAL lengths verify; no nonexistent diagnostics file
is required. These are functional controls, not capacity or speedup measurements.

Independent root raw review passed without repeating builds/workloads/checkers.
READY and every bound proof remain unchanged. PREPARATION and the expected
UNSET guard refusal preserve the earlier unpublished state; placeholder hashes
in that historical preparation refer to the prepublication placeholders, while
the archived PUBLICATION/FUNCTIONAL_QUALIFICATION are the actual later bindings.
No failed workload was hidden or replaced. Copied normalized main 483 proofs,
actual exit receipts, enabled-hook publication receipt and 1502 before/after
source guard inventories retain original references; SELECTION maps them to
lossless archive copies.

Full CargoJSON/vv output, compiler/tool identities, source/archive/hash/mode
inventories, guards, commands/exits/stdout/stderr, raw outcome/storage/publication/
maintenance CSVs and selected physical proofs are preserved. Text and large or
whitespace-sensitive files are gzip-packed upfront; original lengths/hashes/modes
and exact roundtrips are recorded. Full source, build trees, binaries and generated
WAL/snapshot states are excluded. SHA256SUMS seals every payload except itself.
No paired campaign has started. Full P9 and real ext4/XFS acceptance remain open.
