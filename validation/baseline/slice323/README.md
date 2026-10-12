# Frozen selected qualification 322–323: Linux and macOS passed

Both platforms completed all 16 declared stages with `run=0` and `source_guard=0`.
The independent audit found no missing or duplicate stages, failed tests, missing
mandatory controls or warnings. Formatting and strict all-target Clippy passed
for default, all features, core-only and native-only. Metadata recorded 123
inventory entries, 24 partial provider reviews covering 242 named operations,
and 33 passing negative controls. These partial reviews do not certify all
inventory entries as interchangeable providers.

| Profile | Pure send-window | Full logical native | Controlled sessions | Nine-owner network | Strict Clippy |
| --- | ---: | ---: | ---: | ---: | --- |
| All features | 3 | 28 | 4 | 2 | zero |
| Default/TLS | 3 | 18 | 4 | 1 | zero |
| Core only | 3 | not applicable | not applicable | not applicable | zero |
| Native only | 3 | TLS target excluded | TLS target excluded | TLS target excluded | zero |

All 69 selected Rust tests passed on each platform, with zero failures.
The executed counts in the table match exactly on Linux and macOS. All three pure
`raft::send_window::tests::` controls executed in every profile. All three
`authority::vote_window` controls executed in both TLS-capable profiles,
including original granted-reply follower-window preservation. Full native
histories include actual accepted-abort and cancelled write/read ownership,
original child completions, checkpoint/suffix recovery, and TCP/QUIC facade
controls. Controlled network and nine-owner native histories ran through
actual authenticated sessions and child quorum results. This is selected
functional acceptance, not an exhaustive Raft proof.

The frozen source closure is 1,031 paths. Exact manifests are
[source.sha256](source.sha256) and [source-files.txt](source-files.txt).
Manifest SHA256:
`7d882601c4345f52bcfd30ea64852c18bf22cdb87137483bf1d830245ea2032b`;
path-list SHA256:
`c9e38ad45282b8b4317546ede5c15d98c4c0793e082df9b945c0ca67cd0bdd8f`.
Root before/after, local copied, remote copied and both platforms'
before-run/terminal guards passed. Linux manifests and runner bytes match the
exact Mac cut; selected target summaries and mandatory test names align.
[guard-summary.json](guard-summary.json) records the exact observed
path/hash checks. Saved runner, complete input selector and audit tool match
the independently pinned remote copies. [mac-audit.json](mac-audit.json) and
[linux-audit.json](linux-audit.json) record all stage exits and test counts; raw
stage logs are saved under [mac](mac) and [linux](linux).
[evidence.sha256](evidence.sha256) pins every saved artifact.

Mac driver 65791 exited successfully. The run used Rust/Cargo 1.98.0 on arm64
Darwin 27, an isolated source/cache, and sequential executable profiles.
The completed 321 cache was inspected idle before an independent APFS copy;
no active target hardlinks/symlinks were used. Initial soft FD 256 was changed
to process-only 8192, hard unlimited, without system changes. Existing reviewed
protoc checksum passed; no archive download or unzip occurred. Original 302
shell 20050, Cargo 21495 and routed 83714 were inspected live and left untouched.
Ignored full raw guards/process evidence remain under
`target/divide-membership/qualification323/{results,mac-results,linux-results}`; remote root
`/tmp/voteboat-macos-CzK1if/slice323-membership`.

Linux frozen runner session 21499 also terminated zero, using the same
1,031-input source cut with before/terminal guards. Linux Rust/Cargo 1.98.1
x86_64 and cache `target/root315/build` are recorded in its saved environment;
executable profiles ran sequentially. Initial soft FD 1,048,576 was set to
process-only 8,192; hard 1,048,576 and global settings stayed unchanged. Original working-tree checks remain
separate; current-root or newer activation behavior is not qualified by this
frozen bundle.

The failing `logical_nested_native` and native-only Mac315b `effect_owner`
catch-up remain outside this selected matrix. Their original failures are
retained separately; later nested fixture changes are excluded from this cut.
No full P8 completion, performance tuning or security qualification is claimed.
