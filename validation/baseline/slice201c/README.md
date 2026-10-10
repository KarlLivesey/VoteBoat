# Slice201c — snapshot worker lifetime and completion ownership

Starting revision54a73ad. Two new shared worker runners and one new core-only
router test; no production code, storage format or API changed.

## Executed checks

- `native.log`:171 owner,10 provider and14 worker tests pass (195 total).
  This includes existing native hundred-group, crash/publication, credit and
  receipt-loss histories, two new shared host/file worker runners and the new
  exhaustive altered-envelope test.
- `provider-final.log`:all10 provider tests pass after the shared drain case
  was made independent of cross-group completion order and split into admission
  and drain phases to satisfy the existing complexity limit.
- `core.log`:165 owner,3 provider and5 worker checks pass (173 total).
- `native-only.log`:10 provider and14 worker checks pass (24 total).
  `native-only-provider-final.log` reruns the10 provider cases after the final
  shared-case refinement.
- `metadata-tests.log`:13 positive/negative validator checks pass.
- `metadata.log`:4 contracts/30 operations reviewed;101 unreviewed contracts.
- `inventory.log`:105 contract metadata records and file references validate.
- `pre-push.log`:formatting and all four strict Clippy profiles pass.
- `source.sha256` / `source-check.log`:relevant source/test/ledger hashes verify.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test provider_conformance --test snapshot_worker --test effect_owner
cargo +stable test --locked --offline --no-default-features --test provider_conformance --test snapshot_worker --test effect_owner
cargo +stable test --locked --offline --no-default-features --features native --test provider_conformance --test snapshot_worker
node validation/check-inventory.mjs
node validation/check-provider-conformance.mjs
node validation/check-provider-conformance.test.mjs
sh .githooks/pre-push
```

`compile-before.log` retains an initial missing test-support import, corrected
before execution. `restricted-network.log` retains165 passing owner cases and
six socket PermissionDenied failures; the final native suite runs with socket
permission and passes all171 owner cases. These are not suppressed failures.
`pre-push-before.log` records the shared drain case's26/25 complexity diagnostic;
separating admission from drain fixed it without changing lint thresholds.

## Scope

The shared functions accept SnapshotWorker through its public contract. Both new
runners use the native executor: one with downstream host snapshot stores, one
with native files that close/reopen before recovered queries. The separate
downstream host worker and forged-envelope tests execute without native features.
This does not certify every possible executor or replace its device fault tests.

Rejection returns original image ownership; accepted requests retain credits
until terminal poll. Close rejects submissions and drains accepted work. Losing
the owner's observation does not undo publication. Recovered readiness, send and
install loads and retention reconciliation preserve exact references/data.
All13 altered work/visit fields reject without consuming the pending lease or
fencing the current owner; the legitimate result still requires WAL durability
before installation. Process reopen is distinct from hardware power loss.

## Older platform observation

`prior-6f26-platform-linux.log` and `prior-6f26-platform-macos.log` belong to
completed run38035161148, revision6f26bc1. Linux job114164326589 passes121 counter
tests and fails one when cancel-leadership returns UNKNOWN LeadershipChanged.
macOS job114164326525 passes114 counter tests and fails eight QUIC histories:
assignment retry, replacement readiness, grouped membership, grouped drain,
grouped leadership, partial runner recovery, group read and distinct-target
runner. Logs retain exact errors, including authentication/reply deadlines and
unknown leadership outcomes. This is historical failure evidence, not a current
platform result and not explained away by worker conformance tests.
