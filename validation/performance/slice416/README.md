# Slice416 qualification

The executable Application wrapper now forwards its active application's existing
ready-batch capability. Plain Counter can share a bounded proposal; Maintenance
retains its actual singleton fallback. Receipt types and per-original bounds,
admission, duplicate/conflict outcomes, durability and maintenance schemas retain
their existing contracts. [SCHEMA.md](SCHEMA.md) records the ownership plan.

The actual executable source is imported into the existing owner fixtures using
only a setup shim. Fixture receipt plumbing is generic; production traits were
not widened and no lint suppressions or limits were changed.

## Actual acceptance

| Scope | Result | Raw evidence |
| --- | --- | --- |
| All-feature effect-owner integration | 202 passed | `effect-owner-all-sockets.log` |
| Counter executable tests | 52 passed | `counter-executable.log` |
| No-default-feature wrapper controls | 4 passed | `wrapper-core.log` |
| Scoped all-feature library/executable/owner strict Clippy | Zero diagnostics | `scoped-strict-initial.log` |
| Root TCP/QUIC retry, replacement and native recovery | 2 passed | `voteboat-416-native-services-qualified.log` |
| Root maintenance, checkpoint, reclaim and invalid-schedule controls | 6 passed | `voteboat-416-maintenance-services.log` |
| Root tracked formatting, four strict feature profiles and standalone formatting/strict | Zero diagnostics | `voteboat-416-pre-push-checks.log` |

The four wrapper controls verify a single bounded update for four originals with
exact individual positions and full wrapper-sized receipts; duplicate/conflicting
IDs and pending/retained capacity; immutable original retry outcomes; no receipt
after Written and every result Unknown after injected worker failure; and the
Maintenance variant's actual capability, singleton order and schema.

Scoped commands used `CARGO_TARGET_DIR=target/p9/slice416-checks`, sequentially:

- `cargo +stable test --locked --offline --all-features --test effect_owner`
- `cargo +stable test --locked --offline --all-features --bin voteboat-counter`
- `cargo +stable test --locked --offline --no-default-features --test effect_owner client_admission::ready_batch::wrapper`
- `cargo +stable clippy --locked --offline --all-features --lib --bin voteboat-counter --test effect_owner -- -D warnings`

Initial failures remain inspectable. `wrapper-initial.log` records the missing
Debug bound required only by the newly generic test driver's unwrap; the focused
fix adds that test bound. `effect-owner-all.log` records 194 passes and eight
localhost socket EPERM failures in the sandbox, followed by the authorized socket
run with all 202 passing. Root's original service sandbox failure is preserved in
`voteboat-416-native-services.log`, with its qualified rerun recorded separately.

This slice introduces no startup quantum/limit tuning or multi-update pipeline.
No measured throughput improvement is claimed without a frozen-source comparison.
