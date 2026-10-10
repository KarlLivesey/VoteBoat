# Slice201b — shared snapshot provider obligations

Starting revisionccc5bb7. Five new tests exercise the same publication and
retention cases through the host and native-file providers. No production code
or format changed. The reusable functions in tests/provider_conformance/snapshot.rs
accept SnapshotStore/Retention providers; fixtures select group1, valid bootstrap
metadata, four-byte chunks and at least seven application bytes. They use actual
returned checksums/lengths and do not impose a native encoding on replacements.

## Executed checks

- `native.log`:8 provider tests plus22 existing snapshot tests pass. These
  include five new tests, existing three LogStore cases and the existing native
  modeled partial-write/sync/publication/corruption/log-install histories.
- `core.log`:3 provider tests plus10 snapshot tests pass without native features.
- `native-only.log`:8 provider tests pass with only native enabled.
- `inventory.log`:105 inventory entries have valid shape and referenced files.
- `metadata-tests.log`:13 checks pass, including missing snapshot-operation and
  broken retention-assertion rejection. `metadata.log` records3 reviewed
  contracts/21 operations and102 unreviewed contracts.
- `pre-push.log`:formatting and all four strict Clippy profiles pass.
- `source.sha256` / `source-check.log`:bind the relevant source, tests and ledger.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test provider_conformance --test snapshot
cargo +stable test --locked --offline --no-default-features --test provider_conformance --test snapshot
cargo +stable test --locked --offline --no-default-features --features native --test provider_conformance
node validation/check-inventory.mjs
node validation/check-provider-conformance.mjs
node validation/check-provider-conformance.test.mjs
sh .githooks/pre-push
```

Initial `clippy-*-before.log` files preserve one new helper's33/25 complexity
failure. The fix separates staging and publication checks, without suppressions
or weakening limits. The final pre-push log covers all profiles.

## Evidence scope

All six ticket fields and eleven retained-reference fields are altered one at
a time. Invalid operations must preserve legitimate pending work/pins; simply
returning an error is insufficient. Both foreign sealed stores have real pending
stages when cross-publication is rejected. Actual files retain two pinned images
after reopen and reject old admission sessions while a fresh stage is pending.
Unpublished begin/chunk/seal states recover the old acknowledged image. Explicit
reconciliation selects a supplied pinned reference or releases all roots.

The test supplies the authoritative-log caller role; it does not certify a WAL
commit from a snapshot receipt. File close/reopen does not simulate hardware
power loss. Existing modeled failure tests are listed separately. Broader
membership/retention/lifecycle schedules, alternative backend failures, worker
cancellation and current platform acceptance remain open. Ledger checks inspect
metadata references, not the semantic truth of test assertions.
