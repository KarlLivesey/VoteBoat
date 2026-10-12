# Frozen selected qualification 319–321: Linux and macOS passed

Both platforms completed all 29 declared stages with terminal `run=0` and
`source_guard=0`. Independent terminal audits have no missing/duplicate stages,
failed tests, missing mandatory controls or warnings. Formatting and strict
all-target Clippy passed for default, all features, core-only and native-only.
Metadata records 123 inventory entries, 23 partial provider reviews covering
218 operation names, and 33 passing negative controls. Partial review metadata
does not certify every inventory entry as an interchangeable provider.

All actual counts below match on Linux/macOS; all failure counts are zero.

| Profile | Public logical/format/model | Public striped provider | Real file cuts | Striped Node | Owning facade | Original owner | Logical network | Ordinary channels |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| All |15|9|2|2|4|17|6|91|
| Default/TLS |15|9|not repeated|1|2|11|5|70|
| Core |12|native unavailable|native unavailable|TLS unavailable|TLS unavailable|TLS unavailable|TLS unavailable|8|
| Native only |15|9|2|TLS unavailable|TLS unavailable|TLS unavailable|TLS unavailable|57|

Shared functional targets are `logical_voter_application`, `logical_formats`
and `striped_wal_model`. Native-capable profiles run full `striped_log_store`.
All/native profiles explicitly execute both private
`native::log_store::striped::file::tests::` controls: actual publication primitive
cuts and reclaim/parity-reuse cuts. Each control checks 30 original cuts through
real filesystem operations, exact acknowledged prefix, fencing, cold recovery
and safe next publication generation. A zero-test filter would fail the audit.

TLS-capable profiles run actual `striped_node`, the `facade::` subset of
`logical_voter_native`, the rest of that target with `--skip facade::`, and full
`logical_voter_network` plus `logical_network`. The four all-feature/two TLS-only
facade controls cover original provider ownership/results and parent checkpoint
plus suffix/cold child backing recovery. They use one logical parent voter backed
by three physical child voters. They do not qualify a multi-level topology.
Ordinary `wire`, `secure`, `transport` and `quic` regressions run separately.

The source closure is 1026 paths copied after root's stable-cut signal. Root
before/after, local/remote copied path/hash guards and terminal guards passed.
Manifest digest:
`628f99722ca290366e5c5c50fbf7fe87bf90d334935642ce6e0315033da757b4`;
file-list digest:
`ba3304e4b197e7dabb7f6e1a3bbd8ae6488da89295230b9b18b9cc015ae7f366`.
Sources/caches/results were isolated from the working tree, failed 315b and
original 302. Cache donors were checked idle before independent btrfs/APFS COW
copies; no active target hardlinks or symlinks were used. Executable profiles ran
sequentially. Process-local FD 8192, hard/system limits unchanged. No archives were
downloaded or unzipped; the existing reviewed Mac protoc checksum passed.

Saved exact input manifests: [source.sha256](source.sha256) and
[source-files.txt](source-files.txt). Terminal audits:
[Linux](linux-audit.json), [macOS](mac-audit.json).
The runner, complete input selector and final audit tool are saved alongside
them; [evidence.sha256](evidence.sha256) pins every saved artifact.

Linux driver 3564814 / session 41226 exited 0; Mac driver 44838 terminated 0.
Linux uses Rust 1.98.1/x86_64, Mac Rust 1.98.0/arm64 Darwin 27. Actual terminal
inspection retains original 302 shell 20050, Cargo 21495 and routed 83714 separate
and untouched. Raw ignored evidence remains under
`target/divide-membership/qualification321/{results,mac-results}`; remote root
`/tmp/voteboat-macos-CzK1if/slice321-membership`.

One tooling-only first Linux audit error is preserved: clean formatting emits
an empty log, which the first audit treated as missing. The final audit checks
file existence separately from required test summaries. Empty-format-positive
and missing-format-negative controls passed. No feature source/test rerun followed
that tooling correction. Premature markers, zero filtered controls, source
mutation and duplicate stages are rejected by the audit controls.

This proves only the declared frozen 319–321 scope. Newer 322 lifecycle work and
the subsequent timer fix are excluded. The still-failing `logical_nested_native`
and reproduced native-only Mac315b `effect_owner` catch-up remain outside this
matrix; their separate failures are retained. No full P8 completion, performance
tuning or security qualification is claimed here.
