# TimerService parallel workstream — implemented evidence

Implemented new shared primitive contract assertions without production changes. Independent downstream Vec-based HostTimers and native BTreeMap-based DeadlineQueue run identical exact-owner and polling assertions. Five owner fields are varied independently (store ID/incarnation/session, lane ID, runtime generation); matching sequence/group/kind/deadline does not allow foreign cancellation. Original expirations remain owned and appear exactly once.

Poll assertions retain due deadlines under zero budget and regressed-time refusal, transfer earliest deadlines within budget with exact lateness, and cover zero/maximum MonoTime boundaries at capacities1/3/17. Equal-deadline order remains unspecified. Five deliberately defective HostTimers wrappers fail the same shared assertions (store-blind cancellation, budget bypass, destructive regression refusal, false lateness, reversed deadline order).

Local Linux actual checks (CARGO_TARGET_DIR=target/divide-conformance/build):
- all-feature focused runtime timer_contract:12 passed /0 failed (includes3 existing tests).
- no-default-feature focused runtime timer_contract:9 passed /0 failed.
- complete all-feature runtime target:53 passed /0 failed in0.40s.
- strict Clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings:exit0, zero diagnostics.
- cargo +stable fmt --all -- --check final:exit0. Initial concurrent check saw only shutdown worker's then-unformatted QUIC test; retained initial log, final succeeds.
- provider conformance checker:33 passed /0 failed;15 partial reviews /98 reviewed operations unchanged.
- git diff --check:exit0.

Owned changes:
- tests/runtime/timer_contract.rs module declarations.
- new tests/runtime/timer_contract/isolation.rs, polling.rs, mutants.rs.
- docs/provider-conformance.json TimerService operation/assertion/limitation anchors.
- docs/component-contracts.json C11 evidence scope.
- validation/check-provider-conformance.test.mjs three metadata negatives.

No Cargo, CI, protocol, production ownership, timeout, lint threshold, design pack or shared progress-ledger edits. No commit/push. Parent performs frozen four-feature-profile and macOS qualification and updates authoritative ledger. Sequence exhaustion, arbitrary host responsiveness, full TimedShard/worker recovery combinations and every supported capacity remain unqualified; this remains a partial provider review, not a certificate or shutdown diagnosis.
