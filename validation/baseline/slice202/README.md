# Slice202a — old checkpoint and later member authority

Starting revision91353b4. Four new tests; production code and formats unchanged.

## Executed checks

- `native.log`:42 member-recovery and32 membership tests pass (74 total),
  including existing real TCP/QUIC witness exchange cases.
- `core.log`:12 member-recovery and18 membership tests pass (30 total).
- `focused-final.log`:all4 new tests pass after adding exact operation-status
  checks. It records three native WAL frames:106 bytes/110 schedules,
  377 bytes/381 schedules and337 bytes/341 schedules,832 failures total.
- `native-only.log`:the4 new tests pass with only native enabled.
- `core-final.log`:the2 new host tests pass with native disabled.
- `pre-push.log`:formatting and all four strict Clippy profiles pass.
- `inventory.log`, `metadata.log`, `metadata-tests.log`:inventory/ledger paths
  and13 metadata checks pass. No new contract is claimed.
- `source.sha256` / `source-check.log`:bind relevant tests and production sources.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test member_recovery --test membership
cargo +stable test --locked --offline --no-default-features --test member_recovery --test membership
cargo +stable test --locked --offline --all-features --test member_recovery older_checkpoint -- --nocapture
cargo +stable test --locked --offline --no-default-features --features native --test member_recovery older_checkpoint
cargo +stable test --locked --offline --no-default-features --test member_recovery older_checkpoint
node validation/check-inventory.mjs
node validation/check-provider-conformance.mjs
node validation/check-provider-conformance.test.mjs
sh .githooks/pre-push
```

Initial `module-before.log`, `import-before.log` and `directory-before.log`
retain fixture setup errors: wrong nested module path, missing BTreeMap import
and missing parent directory for FileLogIo::create. They were corrected before
the successful runs. No lint levels or error expectations were weakened.

## Scope and acceptance

The old checkpoint contains a committed learner assignment and operation900.
The retained WAL contains operation901 and the later configuration journal.
Eleven states cover learner, accepted/committed joint, accepted/committed final,
rolled-back joint/final, demoting joint, demoted member, and accepted/committed
removal. A newer pinned snapshot has no matching WAL switch and cannot become
the recovery root. Original operation statuses, voting eligibility and quorum
predicates are checked, along with original Counter retry outcomes7/10.

Native files undergo physical log reclamation, close and reopen before the
same recovery checks. Two member stages each reject five inconsistent/missing
old-pin cases without modifying a fresh application or using the newer snapshot.

The WAL model injects every partial/full append failure plus failed sync and
manifest publication before/after success at final commitment, demotion admission
and removal. It restores the simulated durable WAL and checks complete old/new
state. Snapshot storage in these cases is the existing cloned host provider;
simultaneous snapshot-device faults are not implied. Explicit configuration
commits are fixture premises, not network quorum evidence. Broader combined
lifecycle/revocation histories and current Linux/macOS acceptance remain open.
