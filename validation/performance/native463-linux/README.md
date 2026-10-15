# Native463 Linux artifact and zero-join investigation

Published source is `85eaf3e41ffe9488f78b75b02521ea2bbbc80c3a`.
Original fresh build26038 ended0/DONE; root and native example Cargo records are
fresh:false, opt3/debug0/assertions+overflow, native default LTO/codegen units.
Actual compiler1.98.1, Cargo/protoc paths and tool hashes match native458.
Original/copied executable SHA256 is
`9a49d4a320f1ad140663301fb886e4d9ba556a018f292e0f5235f5477a6b7752`;
root library identity, verbose commands, release build-script/oracle exclusion,
ELF notes/sections/symbols and before/after source guards are preserved.
The selected1558-file source export has exact committed bytes and physical modes,
including seven0600 private test fixtures. `BUILD_PROJECTION.json` discloses that
committed rust-toolchain.toml was omitted from that export and separately retained;
its1.98.1 pin matches the explicitly selected actual compiler. This is a selected
export, not a complete Git tree. [Checked source qualification](../slice463/main/README.md)
remains separate from this fresh native artifact proof.

Original functional-pilot20105 ended0: all four native processes and their
validators pass. Each uses128 offers at500/s, window64, one group/three voters,
8-byte commands, warmup64, ready16, no maintenance/pause; TCP and QUIC each select
ready-replies0 and2 on the same executable. All original ledgers, recovery/retry,
worker join, storage observations and whole-process resource outputs are retained.
TCP0/2 applied65/105 and window-refused63/23; QUIC0/2 applied84/77 and refused44/51.
Each has unknown0/pending0. Both enabled cases report ZERO joined transitions,
reply inputs and originals in BOTH warmup and measurement. The counts verify
functional closure, not a successful demonstration of joined persistence.
These short controls are not paired capacity measurements, sustained headroom,
healthy latency, or a performance-gain/retention claim. Refusals remain visible.
`RESOURCES.json` reports whole-process peak RSS in KiB and normalized bytes,
not continuous memory, a plateau, or measurement-only CPU; block counts are not bytes.

Headless GDB diagnostics use that same immutable executable and one enabled TCP
128-offer case per attempt. All raw commands/scripts/outputs/disassembly/counts
are preserved under `diagnostics/`. The initial `stages` attempt exits1 before
main/WAL creation: its load-bias calculation incorrectly required equal offsets
across ELF segments. That actual error is retained; it is not a workload result.
The focused `stages2` address calculation succeeds, followed by separate
`branches` and `primary` observations. Each successful diagnostic process and
validator exits0, with Core producer entries62/56/11 respectively. GDB perturbs
scheduling; these are whole-launch observations, not uninstrumented hit rates,
CPU samples, throughput acceptance or repeated paired performance trials.

`branches` records56 RPC equality failures and no successes. `primary` records11
failures and no successes; saved fieldwise context/configuration/request-range
observations distinguish stale contexts from exact primary ranges. Examples
include matching17/request-end17 below local durable-tip49, and matching66/end66
below tip82. Some such exact primaries advance beyond the current durable commit.
This establishes a refusal mechanism in those debugger executions: the full-tip
RPC equality rejects the older acknowledged prefix while newer local durable
entries exist. It does not prove later gates would accept, estimate the mechanism's
frequency/cost in the four uninstrumented pilots, authorize weaker protocol gates,
or establish a material performance benefit. See `diagnostics/READOUT.json` and
raw disassembly/COUNTS records for the exact observed scope. No browser, source
change, extra build or workload was performed while assembling this archive.

All files are regular and self-contained evidence payloads; raw records retain
original absolute execution paths and artifact identities. Executable/library
binaries, full source/source.tar, build trees, generated WAL/manifests/locks and
replica storage trees are excluded. Only top-level benchmark CSV/run/summary
files are selected from case storage roots. `SELECTION.json` maps every copied
original evidence file. Text records larger than1MiB are losslessly gzip-compressed;
`COMPRESSION.json` records original/archive paths, sizes and both SHA256 values.
Decompression was checked byte-for-byte. No raw output has been reconstructed.

`SHA256SUMS` covers every regular file recursively except itself. Copied source,
validator and tool inventories/manifests remain ordinary payloads under that
outer manifest; their historical paths/scopes are not rewritten. Re-read all
manifest payloads and lossless decompressions to verify the sealed set. FullP9
framework floors, native sustained headroom/memory, weighted fairness/placement
and remaining acceptance obligations are still open; no P10 work is claimed.
