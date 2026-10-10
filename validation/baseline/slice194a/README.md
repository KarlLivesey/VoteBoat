# Slice194a — public transfer decisions

Base revision: ea882b166a9487c94b0ad72adbac4c57bf9fce3c. Source hashes identify
the changed production and test files. RPL-1.5; no dependency, persistence or
wire-format changes. See [the API contract](../../../docs/SPLIT_RECOVERY.md).

`TransferOperation` derives one next action from completed quorum reads of the
original intent, source fences, target imports and publication. It owns no I/O
or durable phase counter. The original split test host now executes this public
component; its separate ownership/data/retry assertions remain unchanged.

## Validation

- `core.log`:29 tests pass without default features:6 operator,7 publication,
  7 source and9 target tests. The new finite adversarial tests obtain opaque
  barriers from a real single-voter core but supply synthetic application
  values; they are not evidence of native read completion.
- `native-split.log`:all4 real three-voter TCP/QUIC split histories pass in97.32s.
  Each uses completed Node reads, discards client receipts, and reopens after
  each phase using WAL/checkpoints. Data, retry/outbox and exclusive ownership
  assertions still run with the metadata/source stopped for child writes.
- `native-membership.log`:all4 existing joint-membership/unread-result abort
  histories pass through the public decisions in40.03s, across TCP/QUIC and
  WAL/checkpoint recovery. Imported configuration2 provenance survives source
  finalization/recovery as configuration3; independent child retry/outbox checks
  remain unchanged.
- `fmt.log`, `clippy-{all,default,minimal}.log`:format and strict all-target
  checks pass with zero diagnostics.
- `rustdoc.log`:warning-denied all-feature documentation passes.
- `inventory.log`:97 contract records pass shape/path checks only.

Initial test compilation found an incorrect private-field access and a refutable
pattern missing its `else`; both test-only mistakes were corrected. The initial
Clippy redundant-closure failure is preserved in `clippy-initial.log` and fixed
without changing lint policy. No production safety guard was relaxed.

## Limits

This is the public decision part of194, not completed authenticated executable
start/status/resume. Hosts must authenticate reads, refresh after every action
or unknown result, encode against actual guards and use ordinary authorized
Node admission. Before publication, changed source configuration after import
requires explicit resolution; it is not silently written into old provenance.
Historical Complete does not grant current ownership. Graceful phase-boundary
restarts are not power cuts during persistence or a complete fault matrix.
The full P0–P7 objective, remaining operator work, platform evidence and original
P7 performance gate remain open.

`background-processes.txt` captures older baseline175 cargo1526695 and
routed1580417 still live. `ci-status.json` captures base-revision CI38026128600
pending and older runs cancelled. Neither proves this source's full suite or
macOS validation. The original processes were not restarted.
