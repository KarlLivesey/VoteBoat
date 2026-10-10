# Slice197b4a — mixed voter and learner source drain

Base revision: `06fbcab`. Linux local execution. Full P0–P7 work remains active.

The public membership drain plan now supports unchanged learner-source
assignments alongside voter evacuations. Inputs are sorted and disjoint, with
one complete bounded inventory for the journal, dispatcher and source stop
check. Learner assignments produce no transfer or configuration operation.
They require exact stable committed membership; local source readiness also
requires application catch-up, campaign suppression and drained owned work.

The original voter-only constructor and digest remain supported. Combined
plans use a distinct fingerprint domain including every retained configuration.
The existing journal stores that fingerprint and the complete inventory; there
is no journal format change. Recovery requires the same original plan.

## Executed evidence

- `core-minimal.log`: nine membership-plan tests and29 Raft tests pass without
  default features. The three new tests cover roles, disjoint sorting, complete
  inventory, changed/omitted fingerprint refusal, all-learner construction,
  byte/group limits, accepted-but-uncommitted matching state, changed membership
  and the absence of spurious learner actions.
- `journal.log`:11 native drain-journal tests and all nine plan tests pass.
- `focused.log`: both new TCP/QUIC mixed-role native histories pass. Four
  independent groups share one WAL per node. Before drain, source1 becomes a
  committed learner in group4. Groups1–3 still need voter evacuation. The test
  checks that no action is dispatched for4, retains the mixed plan through WAL
  or checkpoint recovery, completes original voter operations, drains/joins the
  source and checks original receipt values and new writes in all four groups.
- `native-regression.log`: all32 native assembly tests pass, including previous
  voter-only drain, static shared-WAL, lane and recovery histories.
- `service-regression.log`: after the Busy-response repair, all96 service tests
  and nine command unit tests pass. `retirement.log` also records both focused
  TCP/QUIC learner-retirement histories passing.

Formatting and all/default/no-default-feature strict Clippy pass with zero
diagnostics. The inventory check covers104 metadata records and their referenced
files; it does not establish behavioral conformance by itself.

`initial-failure.log` preserves the first mixed-role run: TCP passed, while QUIC
hit an empty-batch assertion because the test used a fixed three-group scan
after adding a fourth group. The harness now uses the complete assignment
count. The production dispatcher retains its explicit caller-supplied budget.

`service-before.log` records the broader regression run:95 service tests passed,
but the QUIC learner-retirement history exposed a drain-runner failure on the
exact `ERR not_proposed=Busy` response. The runner now re-observes source status
and retries the original operation under its unchanged128-request/45-second
limits. `busy-before.log` deterministically reproduces that response-handling
failure; `busy-after.log` verifies the fix and refusal of unrelated, malformed,
unauthorized, unknown and wrong-operation replies. This is an operator liveness
repair, not a weaker source stop or configuration commitment check.

These are finite local histories using joined native worker shutdown and
durable recovery. They do not prove arbitrary power-loss behavior, separate-host
deployment or macOS compatibility. Multi-group executable controls remain open.
No automatic learner removal, file deletion or current remote availability is
implied by source readiness.

The background CI snapshot for parent revision `06fbcab` was still pending
([run38030481281](https://github.com/KarlLivesey/VoteBoat/actions/runs/38030481281));
the two earlier runs had been cancelled. This slice adds no macOS execution
evidence and does not wait for CI to continue implementation.
