# Slice 418: compaction without retained payload cloning

Implements the reviewed [schema](SCHEMA.md) across Core, OwnedLogState and native
accepted overlays. Plain validated matching-prefix snapshots with an unchanged
ballot can prepare a metadata-only Compact delta and drain at exact installation.
Native admission defers durable prefix removal until successful publication, and
composes trimming with an accepted tail. A validated crate-private plain membership
accessor replaces replay in the existing checkpoint proof workflows. Public provider
contracts, WAL format, snapshot proofs, fencing, original results and configured
fallback remain unchanged.

Parent acceptance on the frozen final source also passes: all-feature affected
integrations 416/0, core-only integrations 277/0, actual TCP/QUIC original retry,
leader replacement and native-file recovery 2/0, and actual service maintenance,
checkpoint/reclaim/restart and invalid schedules 6/0. The complete pre-push hook
passes repository/standalone formatting and default/all/core/native strict
all-target Clippy with zero diagnostics. [Exact parent commands](root/COMMANDS.md)
and original outputs are retained under root/. Both-host throughput measurement
and current-source Mac qualification are still pending.

A private clone-stable flag preserves legacy retained-capacity accounting for
custom recovered slack-capacity command payloads by retaining Full fallback.
Surviving payload allocations remain intact on eligible compaction. This still
moves surviving entry headers and destroys removed payloads; it does not eliminate
all work proportional to history. No throughput claim is made by these controls.

## Actual local qualification

Linux commands used isolated `CARGO_TARGET_DIR=target/p9/slice418-checks`; feature
configurations and executable-using suites ran sequentially. Frozen owned source
hashes are recorded in [SOURCE.sha256](SOURCE.sha256). Repository-wide acceptance
and both-host measurement are owned by the parent implementation task.

| Actual command scope | Outcome |
| --- | --- |
| `test --all-features --lib` with actual sockets authorized | 356 passed, 0 failed |
| `test --no-default-features --lib` | 284 passed, 0 failed |
| `test --all-features --test log_store --test snapshot` | 15 + 25 passed, 0 failed |
| Final `test --all-features --lib compact -- --nocapture` | 19 passed, 0 failed |
| `clippy --all-features --lib --test log_store --test snapshot -- -D warnings` | 0 diagnostics |
| `clippy --all-features --lib --tests -- -D warnings` | 0 diagnostics, including library unit-test targets |
| `clippy --no-default-features --lib -- -D warnings` | 0 diagnostics |
| Final `cargo +stable fmt --all -- --check` | exit 0 |

Every Cargo check used `+stable --locked --offline`; strict commands also used
`--keep-going`. Broad library qualification preceded the final exhaustive update
field-correlation form; the final compact suite and all-test-target strict command
ran after that form and expanded mismatch controls. The last source change only
corrected OwnedLogState documentation. Parent full qualification checks the frozen
complete source again before publication.

Controls cover full-oracle states/errors across first/repeated/boundary snapshots,
commit/ballot and generation/revision precedence, configured checkpoint fallback,
surviving payload pointers and exact retained charges, slack-capacity fallback,
cached-overflow recomputation, eleven predecessor/original-field mismatches,
atomic invalid later groups, pending/durable Core terms and membership, native
append/compact/append/repeated trims into accepted tail, subset publication and
stale generation tickets. Trimmed views also retain exact Full fallback for
configuration and suffix replacement.

The new ModelIO fault control performs compaction with short append, failed sync,
publication-before and publication-after failures, fences without a receipt,
calls actual Device.power_loss(), and asserts the exact recovered state: old for
unsynced writes, complete compacted state for synced publication uncertainty.
Old session tickets are rejected after reopening. Raw LogStore snapshot references
in this test exercise storage transitions; the unchanged snapshot suite separately
executes actual pins, loads, full-state equality, application restore and reconciliation.

## Preserved initial failures

- `logs/transition-initial.log`: 17 passed / 1 failed. An old shape assertion
  expected Full for every snapshot; its eligible matching plain case now explicitly
  requires Compact. Full configuration/replacement assertions and differential
  oracle equality remain, with added configured checkpoint controls.
- `logs/all-lib-sandbox.log`: 335 passed / 21 failures, each actual localhost bind
  returned EPERM. The real-socket qualified rerun passed 356/0.
- `logs/strict-initial.log`: one large-enum lint from redundantly retaining an entire
  LogUpdate in Compact. Removing redundant fields fixed the cause without boxing,
  thresholds or suppressions. Exhaustive LogUpdate destructuring correlates all
  fields, including complete snapshot reference equality; future fields cannot
  silently bypass correlation.

Initial and final outputs remain under [logs](logs/). No outstanding failure is
claimed resolved by a lint suppression or an assertion removal.
