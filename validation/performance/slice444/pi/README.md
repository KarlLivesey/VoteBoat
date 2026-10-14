# Pi444 initial diagnosis

Frozen published443 source is `6be136ae6c33e07e8358b7a6141030a573efd920`:
1,434 selected inputs with physical permissions, 1,428 compiled inputs. The Pi4
uses Rust/Cargo1.98.1/LLVM22.1.8 and user-local protoc3.21.12. Stable1.99 remains
unchanged. UPS/Docker/services/governor/swap were not changed. HOME is disk-backed;
no builds used `/tmp` tmpfs. No performance measurements or release builds started.

Original36127 naturally terminated101: standalone64 had63passes and one failure,
`compacted_prefix_reaches_a_lagging_follower_through_actual_snapshot_install`,
client write `RecvError(())`. Original commands/full raw logs and STATUS101 remain.
The initial runner selected1.98.1 Cargo/RUSTC but external fmt/Clippy version checks
found default1.99 shims; those qualification stages never ran. The diagnostic pins
RUSTUP_TOOLCHAIN1.98.1 and its binary PATH explicitly, leaving default untouched.

Approved e890d968 patch adds only test actor-error output and failure cleanup.
Setup68120 stopped before any test because git apply recreated an edited file's
permissions. Its actual metadata exit1 was incorrectly reported by wrapper0:
a global command rc overwrote the trap rc. Both receipts/raw errors are preserved.
Baseline restoration succeeded. The focused runner repair preserves baseline
permissions and scopes command rc locally; no source semantics were changed.

Diagnostic18543 compiled the changed harness in1m07s and ran only that exact test.
It stalled without any actor error: at manual interruption the test PID3596 had
elapsed3m24s, CPU6s and stateSl. Root explicitly authorized SIGTERM after verifying
the exact executable path. Cargo/wrapper returned101 due signal15. This is an
interrupted diagnostic, not a naturally failed test or a passing control. No cause
is established; the paused follower election/queue hypotheses remain unproved.
No automatic rerun, timer change, protocol change, or limit weakening occurred.

The wrapper restored both files and verified all1,434 baseline hashes/permissions
(exit0); the original suite STATUS101/63passes is intact. Raw saved attempt source
files are limited to the two changed modules, not a full source/build tree.
`SOURCE.json`, command records and tool inventories define scopes. An initial
local command-only SSH consumed runner stdin; corresponding local terminal files
are absent where not actually written. Actual tool terminal101 and remoteSTATUS
are recorded honestly; no substitute zero receipts were invented.

`SHA256SUMS` covers every regular file recursively except itself, including copied
nested manifests as ordinary historical payloads. No symlinks, source tar, binaries,
build trees, Cargo cache, generated WAL, or perf data are included.

Subsequent trace receipts are separate in `../pi-trace/`.
