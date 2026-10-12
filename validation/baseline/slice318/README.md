# Frozen qualification 318 — Linux and macOS passed

Both platforms completed all 24 declared stages, with terminal `run=0` and
`source_guard=0`. Independent audits find no missing stages, failed tests or
warnings. Formatting, all four strict full-target Clippy profiles, inventory and
provider-metadata controls pass. Inventory records 121 contracts; metadata retains
23 partial reviews / 218 operation names rather than claiming every inventory
entry is a replaceable provider certification.

The following actual counts match on Linux and macOS; every failure count is zero.

| Profile | Full library | Selected functional | Program driver + striped Node | Physical backing | Wire/channel regression |
|---|---:|---:|---:|---:|---:|
| all |150|29|16|2|91|
| default |149|29|8|1|70|
| core |120|17|TLS unavailable|TLS unavailable|8|
| native |131|29|TLS unavailable|TLS unavailable|57|

Selected functional targets are logical_voter_application, logical_formats,
program_transaction_application, program_transaction_model, striped_log_store and
striped_wal_model. Real TLS profiles execute program_transaction_driver and
striped_node; consumed physical backing histories are the `backing::` subset in
logical_voter_native. Ordinary wire/secure/transport/QUIC regression is recorded
separately. This incremental matrix does not repeat the full old component sweep
or unchanged independent 314 graphs.

Exact source closure is 992 paths, copied after the root's stable-cut signal.
Root before/after and both copied path/hash guards pass. Manifest SHA256 is
`327309f79c2b1b1e740b3f1d9f933a05eaf37fc33f4cd3fd249d9ced797d223d`;
file-list SHA256 is
`ba0db507e4c31f5af93f544138df6f824f8ec9f3e84833868034a8a4387bac4a`.
Source, build and results are separate from 315b and original Mac302. Linux uses
an inspected idle independent btrfs COW cache; Mac uses an inspected idle APFS
cache clone. Profiles run sequentially within each target. Mac process-local
descriptor limit is 8192, hard limit unchanged. No archives were downloaded or
unzipped; the existing reviewed protoc checksum was verified.

Saved terminal audits: [Linux](linux-audit.json) and [macOS](mac-audit.json).
The exact inputs are recorded in [source.sha256](source.sha256) and
[source-files.txt](source-files.txt). Linux actual driver
3028262/session82696 exited zero. Mac actual driver17418 terminated; final handle
inspection at 17:52:13 UTC retained original Mac302 shell20050/cargo21495/routed83714
live and untouched. Linux Rust1.98.1/x86_64; Mac Rust1.98.0/arm64 Darwin27.

This proves the declared frozen 318 scope. Newer endpoint, file-interruption, facade and nested-topology
319/320 edits are excluded, and Mac315b's reproduced native-only catch-up failure
remains incomplete in its separate evidence. No complete P8 claim is made here.
