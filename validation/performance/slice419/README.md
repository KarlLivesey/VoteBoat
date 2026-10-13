# Slice 419: move admitted incoming payloads

Implements the reviewed [schema](SCHEMA.md) across Core, OwnedLogState and native
accepted storage. Ordinary append validation now returns metadata before copying or
normalization. A private sealed transition owns one admitted update; there is no
second planned suffix. Core retains one normalized Pending copy while moving the
original raw update into the public Persist effect. Storage consumes its input,
normalizes only admitted new allocations, and moves them into accepted/canonical state.

The provider boundary still sees original command/container capacities. Full update
correlation destructures every field exhaustively and compares complete suffix
content, operation IDs and snapshot/membership values. The sealed update has no
mutable accessor. Full fallback and slice 418 compaction remain; existing snapshot
proofs, admission limits, results, Written/durable distinction, tickets, control WAL
reserve and original ticket order remain. No Arc, public provider API, on-disk format,
queue policy or performance threshold changes are included.

Parent acceptance on the reviewed final source passes all-feature affected
integrations416/0, core-only277/0, actual TCP/QUIC original retry/replacement/native
file recovery2/0 and service maintenance/checkpoint/reclaim/restart controls6/0.
The full pre-push formatter/four strict root configurations and standalone checks
are zero. All20 owned inputs match before/after. [Exact commands](root/COMMANDS.md)
and original outputs are preserved under root/. Current-source Mac qualification
and paired performance measurement remain separate; no throughput claim follows
from these correctness checks.

Implementation shape: `OwnedNextLog` is private Append(OwnedAppend) or Other(existing
NextLog + owned input). Both adapters share the borrowed scalar append validator.
`plan_copy` validates the raw update and makes its one immutable copy;
`plan_owned` validates that exact consumed input before normalization. A prepared
append drops its already-checked predecessor metadata and retains only owned update
and next summary. This avoids another allocation or oversized-enum suppression.
The constructor does not compare a fresh clone to its own source again; exact
full-content correlation remains at every external provider completion boundary.

## Actual local qualification

All Cargo commands used `+stable --locked --offline`, with isolated
`CARGO_TARGET_DIR=target/p9/slice419-checks`. Actual socket controls ran with the
already authorized localhost permission. Feature configurations and executable-using
suites ran sequentially. Owned source hashes are in [SOURCE.sha256](SOURCE.sha256).

| Actual scope | Result |
| --- | --- |
| Broad `test --all-features --lib`, before constructor review | 367 passed, 0 failed |
| Broad `test --no-default-features --lib`, before constructor review | 292 passed, 0 failed |
| `test --all-features --test log_owned --test log_store --test snapshot` | 5 + 15 + 25 passed, 0 failed |
| First `test --all-features --lib incoming -- --nocapture` | 10 passed, 0 failed; includes one pre-existing incoming snapshot control |
| Reviewed `test --all-features --lib incoming -- --nocapture` | 12 passed, 0 failed |
| Reviewed `test --no-default-features --lib incoming -- --nocapture` | 9 passed, 0 failed |
| `clippy --all-features --lib --tests -- -D warnings` | 0 diagnostics |
| `clippy --no-default-features --lib --tests -- -D warnings` | 0 diagnostics |
| Final repository `cargo +stable fmt --all -- --check` | exit 0 |

Both strict commands also used `--keep-going`; `--tests` includes library unit-test
and integration target compilation. The final all/core library suites include all
11 new slice 419 controls. Storage qualification ran before the later additional
codec/atomic controls and fixture correction; the final library suites and strict
checks ran after them. The final formatter fix only sorted test module declarations.
Review subsequently removed an avoidable constructor comparison of an immutable
clone against its own source. The reviewed incoming controls and both strict
commands passed after that change; broad library results above preceded it.
Parent full hook, affected integrations and real service checks qualify the frozen
whole source before publication; no throughput claim is made by these local tests.

Controls prove canonical prefix and normalized incoming pointer preservation,
raw Core effect capacity/pointer identity, one Pending allocation shared by entry
access, old normalized charges for slack payloads, full-oracle state/error and term
lookup for valid/invalid/compacted paths, identical-header different bytes/operation
refusal, stale predecessor refusal, invalid later owned unit atomicity, codec-first
refusal observing original capacity with no I/O, original mutation ticket order,
and subset publication of moved payloads. Existing native uncertainty/power-loss,
trimmed accepted compaction and complete snapshot proof controls ran unchanged.

## Preserved failures and focused fixes

- `logs/strict-initial.log`: large-enum lint because PreparedOwnedLog retained an
  already-checked predecessor. Removing redundant prepared metadata fixed the cause;
  no boxing, thresholds or lint allowances were added.
- `logs/changed-ballot-initial.log`: 364 passed / 1 failed. The old overlap fixture
  directly mutated Pending.update. With immutable sealing, it instead prepares a
  valid changed-ballot transition; initially its modeled ticket retained the old
  term and correctly failed exact completion. Its ticket now matches the new pending
  term. Original assertions still require serialized fallback, no Written replication,
  unchanged durable log and no admitted ranges.
- `logs/fmt-initial.log`: test module declarations required sorting. Final formatting
  passes; no functional source rewrite was used.

Original and final outputs remain under [logs](logs/). Full/configured fallback still
clones through the original validator, and network/committed/application copies are
outside this slice's ordinary-incoming ownership change.
