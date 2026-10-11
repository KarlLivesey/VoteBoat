# Shared connector assertions: terminal Linux evidence

Source is frozen after test-only registration in `tests/connect.rs` and new `tests/connect/conformance.rs`. No production, dependency or shared documentation edits. Exact owned source hashes are in `connect-source.sha256`. Parent owns integration/ledger/macOS and final complete platform qualification.

The same generic public-contract runner executes against the existing independent HostConnector and NativePeerConnector with an actual native listener/dialer. The native Accept request has no connecting client; no readiness race or wall-clock timeout is used to produce its cancellation. Supplied time10/deadline90 advances to11 only through zero budgets. Wrong-ticket cancellation, oversized budget at12 and backward-time poll/submission refuse while pending usage/deadline remain unchanged; a subsequent poll at11 confirms failed budget did not ratchet the clock. Owned Dial request ticket/direction/endpoint/deadline survive TimeWentBack, Overloaded and Closed. Close retains the accepted ticket/credit; zero terminal budget still cannot consume it. Exactly one original Cancelled terminal completion releases credit and reaches drained; stale cancellation/repeated poll cannot recreate completion. Existing native finish releases listener and joins its dialer.

Final commands all exited0. `CARGO_TARGET_DIR=target/divide-membership/build` is isolated from other agents; feature configurations ran sequentially.

| Command family | Result | Evidence |
|---|---|---|
| `cargo +stable test --locked --offline --all-features --test connect` |21 passed /0failed | `connect-tests-all-final.log` |
| `cargo +stable test --locked --offline --test connect` |21 passed /0failed | `connect-tests-default-final.log` |
| `cargo +stable test --locked --offline --no-default-features --test connect` |3 passed /0failed | `connect-tests-core-final.log` |
| `cargo +stable test --locked --offline --no-default-features --features native --test connect` |3 passed /0failed | `connect-tests-native-final.log` |
| `cargo +stable fmt --all -- --check` |0 diagnostics | `connect-fmt-final.log` |
| `cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings` |0 diagnostics | `connect-clippy-all-final.log` |
| same strict command, default features |0 diagnostics | `connect-clippy-default-final.log` |
| same strict command, `--no-default-features` |0 diagnostics | `connect-clippy-core-final.log` |
| same strict command, `--no-default-features --features native` |0 diagnostics | `connect-clippy-native-final.log` |

Total48 connector target executions, including two new tests on TLS profiles and one new host-only test on core/native-only profiles. This is selected connector acceptance, not arbitrary replacement-provider or full-platform certification. Shared-reactor/pool/wake lifetimes already have separate reviewed tests; this slice does not invent a new reactor or claim universal backend support.

Initial evidence remains: `connect-conformance-initial.log` host passed/native setup PermissionDenied at real loopback listener creation. Authorized loopback execution (`connect-conformance-local.log`) passed both. `connect-clippy-all-initial.log` found runner cognitive complexity27/25; split pre-close refusal from terminal-close assertions rather than weakening lints or expectations. The initial fmt check requested module ordering; registration was reordered. These failures were causal setup/test structure issues, not hidden behind a clean subset.
