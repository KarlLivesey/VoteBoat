# Slice210b — bound handoff replay and retained QUIC liveness failures

The historical handoff fixture rejected the exact not_proposed=Busy response
that the production bound drain runner already treats as non-admission. Two new
real TCP/QUIC histories commit a handoff to an unavailable exact voter, observe
its Pending record by quorum read and require an actual Busy refusal. The old
helper then fails on replay. The corrected helper repeats only the original
command under its unchanged deadline. It also accepts the exact observed
administrative authenticated-read interruption; neither outcome proves success.

Passing histories compare every original record field, cancel durably, reject
a changed target store, retain independent group8 data/absence, and recover
Cancelled records and original counter receipts after cold restart. QUIC checks
an actual installed checkpoint using the existing native recovery helper.
No production consensus, automatic retry, deadline or resource policy changes.

## Executed evidence

- `before.log`: both new transport histories reproduce the old Busy assertion.
- `leadership-all.log`: six passes and one QUIC failure on the explicit unknown
  administrative read result; the service cancels that original waiter.
- `leadership-all-final.log`: all7 focused histories pass after recognizing that
  exact original-command interruption.
- `counter-maintenance-all.log`: counter156 and maintenance6 pass locally.
- `raft-leadership.log`: all11 deterministic core handoff histories pass.
- `counter-maintenance-diagnostics.log`: counter156 and maintenance6 pass again
  after adding bounded per-node last-status failure diagnostics.
- `leadership-default-final.log`: all4 default TCP handoff histories pass.
- `pre-push-final.log`: formatting and all four strict Clippy profiles pass with
  zero diagnostics, including the final diagnostic source.

The additional `runner-temp.log` run does **not** pass:5 focused histories pass,
2 QUIC histories fail to settle their original commands within the existing
15-second outer deadline. This reproduces an intermittent liveness problem
locally. It is retained without another unchanged rerun or a raised deadline.
`log-paths.log` verifies42 service text logs match the CI artifact path under
an explicitly selected temporary directory. CI now preserves those text logs,
not data stores or credentials. Hosted artifact publication remains unverified.

`service-logs/` retains both failed fixtures' text logs. `inspect.rs` recovers
copies of their stopped journals through public native log/snapshot/application
contracts; original failed stores were not reopened in place. The first
inspection used the wrong default-group snapshot path and failed; that log is
retained. Corrected `journal-inspection.log` finds intact original Pending
records and data5/8 on both surviving replicas, with divergent persisted terms:
source node1 term6 while node3 reaches51–74 across the three groups. This is
evidence of a liveness/communication failure, not lost acknowledged data or a
completed handoff. It selects peer/session recovery as the next investigation;
it does not prove the exact transport cause.

The temporary inspection package used this crate with only the native feature.
Its copied fixture roots remain under `/tmp/voteboat210b-inspect/`; commands and
paths are recorded in the inspection log. To rerun the operator checks:

```sh
cargo +stable test --locked --offline --all-features --test counter_service group_leadership:: -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service --test leadership_maintenance
cargo +stable test --locked --offline --all-features --test raft leadership::
cargo +stable test --locked --offline --test counter_service group_leadership::
sh .githooks/pre-push
```

`ci-prior-completed.json` and `prior-macos-failures.log` retain the completed
run38042654374 at36526bc: Ubuntu passes; macOS counter has145 passes/7 failures.
That revision predates this change. Matching macOS acceptance, the reproduced
local QUIC liveness failure and the full P0–P7 baseline remain open.
