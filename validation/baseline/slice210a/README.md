# Slice210a — first observed group receipts after uncertainty

The old initial-data fixture required duplicate=false even though its explicit
caller retry could follow an uncertain original attempt. The macOS run at942fd93
recorded Value(9), duplicate=true at that assertion. No production behavior,
protocol, deadline, resource bound or public seam changes in this slice.

Two new histories send real authenticated original writes without reading their
application replies. Independent quorum reads prove3/5/9 became visible in the
three assigned groups before those channels disconnect. Each uses operation42,
so exact different payloads also check group-scoped identity. The existing data
fixture fails on the first retained receipt before the change. Afterward it
accepts either exact initial success, immediately requires a duplicate receipt,
then retains the stricter exact duplicate expectation after cold recovery.
Changed payloads return OperationConflict and leave independent reads unchanged.
Both TCP and QUIC stop every process and reopen durable state. The QUIC history
also checks actual installed checkpoint boundaries in the native log stores.

## Executed checks

- `before.log`: initial new-test compile failure from a u64/u128 group identity
  mismatch. The checkpoint tuple now preserves GroupId's u128 domain.
- `before-receipt.log`: both new histories fail at the old duplicate assertion
  after independently observed real writes, reproducing the acceptance error.
- `groups-all.log`: all7 group tests pass with all features after the correction.
- `counter-all.log`: all154 all-feature counter operator tests pass concurrently
  on this Linux host in57.97s, including both new histories.
- `groups-default.log`: all5 default TCP group tests pass in5.06s.
- `pre-push.log`: formatting and default/all/core/native strict Clippy pass with
  zero diagnostics. The tracked push hook remains enabled.

Reproduce in sequence (feature builds share executable paths):

```sh
cargo +stable test --locked --offline --all-features --test counter_service groups:: -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --test counter_service groups::
sh .githooks/pre-push
```

`ci-prior-observed.json` and `prior-macos-failures.log` retain the newly completed
Operator recovery run38042490990 atcd54071: Ubuntu passes, macOS counter records
146 passes/6 failures. Its remaining failures cover assignment original retry,
replacement readiness, group leadership and peer rotation/discovery. That run
predates this change and does not prove macOS acceptance. The still earlier
11-failure run remains in slice201e. No platform improvement is attributed to
this local fix until a matching current-source job finishes. Broader platform,
provider, lifecycle/fault and P7 acceptance remain open.
