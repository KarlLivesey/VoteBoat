# Slice180: voter replacement and removal planning

Source base: `ced34e88caa6a943ca79fed43178175b0abc7058` plus the source
hashes recorded here. Linux local execution, stable Rust; commands use the
locked offline dependency set. No new consensus or persistence format.

Checks:

- `cargo +stable test --locked --offline --all-features --test placement_planning --test placement --test membership`: 13 planning, 8 placement and 32 membership tests passed.
- `cargo +stable test --locked --offline --no-default-features --test placement_planning --test membership`: 11 planning and 18 membership tests passed.
- `cargo +stable test --locked --offline --all-features --test counter_service`: all54 passed in42.39s, including native-authorized removal plans in TCP/QUIC leader-loss histories with both WAL and checkpoint recovery.
- `cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings`: passed, zero diagnostics.
- The same strict Clippy command with `--no-default-features`: passed, zero diagnostics.
- `cargo +stable fmt --all -- --check`: passed.
- `RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps`: passed.
- `node validation/check-inventory.mjs`: 95 contracts, metadata/path check passed.

The planning checks cover recursive weighted leaf replacement, exact stores,
joint/final replay timing, checkpoint reconstruction, explicit retirement or
learner retention, stale/disabled/unprepared targets, authorization refusal,
history exhaustion and both configuration-ID overflow boundaries. The first
premature-finalization assertion used an impossible checkpoint beyond the
committed prefix; corrected the fixture to replay all three original records.
No production relaxation was made to satisfy that check.

`ci-status.json` is an observation of the previous source base: lint passed,
Linux/macOS still running at observation. It does not validate this slice.
The older full-suite run from before slice175 also remains separate evidence;
do not count it as a current-source full-suite result.

These are bounded planning, membership and service histories. They do not
establish automatic relocation, live capacity sampling/reservations, physical
domain independence, remote readiness from a plan, all-platform completion or
the outstanding P7 throughput/p99 acceptance gate.
