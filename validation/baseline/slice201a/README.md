# Slice201a — reusable LogStore obligations

Starting revision446c807. This slice changes tests and the conformance ledger;
it does not change production storage behavior or add an adapter. The reviewed
ledger covers all eight LogStore operations with selected assertions and explicit
limitations. The other104 inventory contracts remain unreviewed by this ledger.
Existing implementation/test evidence for them is retained separately.

## Executed checks

- `native.log`:20 all-feature tests pass: eight reclamation, nine log-store and
  three new provider-conformance cases. The same reusable cases run against the
  host provider, native ModelIo and native FileLogIo. They reject all ten altered
  ticket fields, cross-store barriers and prior-session tickets; preserve real
  pending work; check stale suffix/range refusal and byte/count bounds; verify
  supported/unsupported reclamation and exact recovery state after reopen.
- `core.log`: the host case passes without default features or native code.
- `native-only.log`: all three cases pass with only the native feature enabled.
- `ledger-tests.log`:11 checks pass, including ten negative fixtures rejecting
  missing/duplicate operations, invented contracts, broken assertion references,
  escaping paths, unsupported completion claims and missing limitations.
- `pre-push.log`: formatting and all four strict Clippy profiles pass.
- `source.sha256` / `source-check.log`: the source/test/ledger bindings verify.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test provider_conformance --test log_store --test log_reclaim
cargo +stable test --locked --offline --no-default-features --test provider_conformance
cargo +stable test --locked --offline --no-default-features --features native --test provider_conformance
node validation/check-inventory.mjs
node validation/check-provider-conformance.mjs
node validation/check-provider-conformance.test.mjs
sh .githooks/pre-push
```

An initial unused import warning was removed before the recorded clean checks.
The initial `node --test` invocation reported one file-level test; direct execution
of the node:test module reports all11 named checks, so the retained result uses
the direct command above rather than inferring coverage from the file count.

## Reusing the cases

The functions in `tests/provider_conformance/log_store.rs` take caller-owned
providers through LogStore only. A new provider fixture can invoke cross_store,
exercise and (for persistent providers) reopened. Supply its declared one-entry
range byte charge; the fixture must not impose native on-disk encoding. Native
and current host providers use38 bytes for the selected one-byte command.

The ledger validator checks metadata references, not semantics of test bodies.
Reviewers must inspect assertions, execute their supported feature profiles and
record the results. These selected histories do not certify arbitrary providers,
power-loss schedules, device behavior or every operation of all105 contracts.

## Prior platform observation

Run38034168031 at7fca6a4 is now terminal. Ubuntu job114161770063 has121 counter
passes and one TCP replacement-drain failure (`UNKNOWN configuration: reply
deadline expired`). macOS job114161769901 has111 counter passes and11 failures,
including replacement drain, grouped operations, deadline and authentication
cases. Neither platform reaches the directory/transfer targets in that run.
The correctly named `prior-7fca6a4-*` files retain raw logs. They do not validate
this slice, and the newer446c807 run is not inferred successful from local tests.
