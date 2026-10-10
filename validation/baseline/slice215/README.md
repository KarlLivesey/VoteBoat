# Slice215 — replaceable timer contract review

Production changes are documentation of the existing TimerService behavior only.
No timer implementation, default interval, protocol or storage boundary changes.
The schema and linked scope are in docs/IMPLEMENTATION.md. Broad security review
remains assigned to the user's Daybreak run.

Shared downstream HostTimers/native DeadlineQueue assertions exercise all six
inventory operations at capacity3. Each provider runs eight seeds of128 sequential
actions against an independently maintained live-token model. Fixed histories
cover every selected stale field, replacement at capacity, group incarnation and
kind separation, zero/single/unbounded caller poll budgets, equal-deadline order
independence, past deadlines and owner recreation with reused local sequence.
The intentionally wrong host cancellation ignores owner identity, removes a live
token and fails the same scoped checker. This finite evidence does not certify
arbitrary providers, sequence exhaustion, every resource capacity or host timing.

- `timer-initial.log`: retained test compilation failures (incorrect incarnation
  type and u64 group ID where the public constructor accepts u128).
- `timer-all.log`: initial3 shared checks pass before owner-recreation assertions.
- `runtime-all.log`: intermediate31 pass before those added assertions.
- `runtime-final.log`: final31 pass, all-feature runtime target, including existing
  timer/backpressure, delayed completion, leader recovery and100-group histories.
- `runtime-core.log`: final23 pass with no default features.
- `clippy-initial.log`: strict all-target/all-feature Clippy passes.
- `conformance-tests.log`: retained initial metadata suite failure after adding
  the review; old hardcoded counts and positional contract references required
  updating. The final suite selects contracts by name and retains negative checks.
- `conformance.json`, `conformance-tests-final.log`: partial ledger7 contracts/45
  operations,101 unreviewed; all18 metadata checks pass, including three timer
  omission checks. Metadata validation only, not test execution certification.
- `checks.log`: formatting and all four strict Clippy profiles pass with zero
  diagnostics through the enabled tracked pre-push hook.
- `ci-observation.json`: same prior-source diagnostic run38048873478/a25e5f7;
  both platform operator jobs live at observation. No matching-platform pass
  or root-cause diagnosis is claimed.

Commands use `cargo +stable test --locked --offline` with the indicated feature
profile and `--test runtime`; the initial shared selection uses `timer_contract`.
Metadata checks use `node validation/check-provider-conformance.mjs` and
`node validation/check-provider-conformance.test.mjs`. Source and artifact hashes
record the reviewed tree, not an entire release gate.
