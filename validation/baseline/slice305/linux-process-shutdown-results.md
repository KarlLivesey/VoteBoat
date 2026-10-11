# Schema305 counter process fixture: selected evidence

Owned source is frozen: `tests/counter_service.rs` shutdown registration and `Cluster::stop`; new `tests/counter_service/shutdown.rs`. No production/dependency/shared-doc edits, commits or pushes. Exact owned hashes: `process-shutdown-source.sha256`.

Stop retains discovery-aware quit command ordering and starts the same shared fifteen-second ordinary wait after commands finish. It now visits every live child once per sweep, observes actual `try_wait`, requires successful status and clears only exited successful handles. Each pending child owns a distinct Window and reads only its own service log. First complete exact positive decimal report pins checked observed time + reported interval + the existing five-second exit/join margin; effective cap cannot shorten the original ordinary deadline. Already pinned windows ignore repeated/later fresh reports. Invalid/partial/unrepresentable reports grant no extra time. Pending children still fail at strict deadline equality after their due exit visit. Failed status and timeout retain original logs; Cluster Drop kill/wait cleanup is unchanged. No report or elapsed time can supply a successful process result.

Final targeted command, with isolated `CARGO_TARGET_DIR=target/divide-membership/build`:

`cargo +stable test --locked --offline --all-features --test counter_service shutdown::`

Result: **6 passed /0 failed**,197 other tests filtered; no counter-service process history or full target was run. Controls cover complete-positive/partial parsing; ten malformed/zero/decimal overflow variants; ordinary/extended deadline equality; short/equal signal preserving ordinary cap; repeated/fresh signal refusing renewal; checked Duration and Instant overflow; and independent sibling caps with malformed/partial logs, first valid complete report among multiple lines and later completion of a partial report. Evidence: `process-shutdown-helper-final.log`.

Final whole-repository `cargo +stable fmt --all -- --check` exits0, zero diagnostics (`process-shutdown-fmt-final.log`). Final whole-repository strict `cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings`, same isolated target, exits0 with zero diagnostics (`process-shutdown-clippy-all-final.log`). Parent owns remaining profiles and actual native Linux/macOS/executable qualification against integrated frozen source.

Initial failures retained: `process-shutdown-helper-initial.log` could not compile the concurrent root production edit because `MonoTime` was imported from identity; root corrected it to the runtime contract before the final rerun. `process-shutdown-fmt-initial.log` records indentation and line wrapping corrections in owned files; formatted only owned files with `rustfmt --config skip_children=true`. No lint level, deadline policy or process success assertion was weakened.

Limit: the interval is anchored when this waiter first observes its own child's completed progress line, with the existing explicit join margin. It does not infer the executable's exact emitted wall-clock instant or retroactively diagnose302's original unobserved Mac failure. The production helper owns the actual pinned provider predicate and deadline; this fixture owns only bounded process observation and actual exit.
