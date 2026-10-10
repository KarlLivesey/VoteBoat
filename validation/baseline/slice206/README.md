# Slice206 — initialization receipt loss and concurrent fixture startup

Starting revision: `cf9b7b2d4470c00a3a9e95410508b4a873eaa65a`.
Only tests and documentation change. Production commands retain explicit unknown
mutation results. The setup fixture repeats only exact leadership-change unknowns,
at most four attempts, preserving the original command and profile-bound ID.

## Evidence

- `before.log`: two finite checks fail against one-shot fixture behavior. This
  tests the setup expectation; it is not a reproduced defect in production Raft.
  The original Ubuntu initialization failure is retained under slice205.
- `transfer-initial.log`: nine pass/five fail. Three existing histories fail to
  bind listeners; two new interruption histories incorrectly assume an elected
  leader immediately after fresh service startup.
- `transfer-before-spawn-gate.log`: ten pass/four fail. The initial-election
  precondition is fixed, but parallel startup still hits listener bind failures.
- `transfer.log`: all14 tests pass in69.51s in the concurrent full suite, including both
  new TCP/WAL and QUIC/checkpoint initialization histories and the existing split,
  merge, retirement and refusal checks.
- `pre-push.log`: formatting and all four strict Clippy profiles pass.
- `source.sha256` / `source-check.log`: test source bindings for these results.

The new native histories each remove quorum, wait for accepted metadata bootstrap,
grant and source bootstrap proposals, lose their waits and restart all processes.
Original operations resume; later bootstrap replay preserves existing7/11 data,
then a real split retains the original data-operation retry results at the children.

The shared test gate coordinates only parent listener reservations and process
creation, following the Counter fixture's established pattern. Child waits and
service work remain concurrent. The offline profile-generation command finishes
inside its own reservation lifetime. This addresses inherited-descriptor windows
without retrying arbitrary startup errors or serializing the entire suite.

```sh
cargo +stable test --locked --offline --all-features --test transfer_service initialization::setup_ -- --nocapture
cargo +stable test --locked --offline --all-features --test transfer_service
.githooks/pre-push
```

These are selected Linux results. Current macOS acceptance, broader combined
failures, provider conformance and P7 gates remain open.
