# Slice230 — original transfer startup across leadership loss

This changes private service-test callers, not production behavior. Source setup
repeats the same group20 add, operation ID, key and delta only after the exact
native `UNKNOWN Unknown(LeadershipChanged)` reply and its matching CLI error.
The existing four-attempt initialization policy is reused. Other errors and
positive conflicting/malformed results remain terminal. Typed receipt comparison
requires the exact operation, expected value, positive matching indices and the
selected ordinary or retirement wrapper. It allows either original or duplicate
applied receipts; deduplication decides the result.

The original bounded15s discovery loop is shared by both interruption callers.
It repeats an empty leader scan, preserves probe failures and never renews the
deadline. Individual status requests retain their existing12s limits and may
delay observing the outer deadline; this is not a hard15s child-call timeout.
A fresh pinned quorum read prepares the metadata interruption before followers
are removed. Role discovery alone does not establish safe read readiness.

The new native histories stop the source followers, retain an original client
until actual local proposal admission, pause only the fixture-owned old leader,
recover the other two voters and observe their election. After resuming the old
leader, both TCP and QUIC produce the exact original UNKNOWN. Explicit original
retry settles the operation without duplicating its effect. Changed content is
rejected before append with `ERR Application(InvalidCommand)` in one attempt,
and a quorum read retains value7. Source setup also writes original2/key200/11.

Only one metadata voter is then recovered: a real scan returns no leader, while
the established child still serves its original duplicate without a metadata
quorum. Recovering the remaining voters permits discovery. Fresh admitted-read
cancellation leaves pending_reads=0. WAL/TCP or installed-checkpoint/QUIC reopen
preserves the immutable profile, exact original duplicate receipts and values;
complete split, target retries, independent child reads and worker joins pass.
Paused-process and pending-client guards own resume/kill/join cleanup. No source
or provider contract, storage format, durability, deadline or security change.

Validation:

- `startup-all.log`:5 scripted checks and both native histories pass (7 total).
  Scripted checks cover unchanged records, four-attempt exhaustion, unrelated
  error refusal, exact applied receipt shape and bounded delayed discovery.
- `transfer-all.log`: all28 concurrent transfer tests pass in72.24s, including
  the previously failing original phase-cut and interruption histories, actual
  ordinary/retirement receipts, merge, split and checkpoints. This broad run
  precedes a final pause-guard cleanup correction: retain its PID until resume
  succeeds, so failed resume still attempts cleanup in Drop. `suite-source.json`
  records the before/after source hashes. The final native checks below exercise
  both transports after that correction; the full suite was not repeated.
- `startup-default.log`: all6 default-build checks pass, including actual TCP
  leadership loss, metadata recovery, WAL reopen and completed split.
- `startup-quic-final.log`: the final QUIC native history and checkpoint/reopen
  check passes1. `strict-final.log`: formatting and all four strict all-target
  Clippy profiles finish with zero diagnostics. `source.sha256` validates final
  changed source identities; `git diff --check` passes.
- Initial failures remain: `startup-assembly-failed.log` is a module-declaration
  sequencing error, not behavioral evidence. `startup-initial-failed.log`:0/4
  exposes the strict original caller and single-scan assumptions; both native
  tests stop at initial discovery. `data-native-initial-failed.log`:0/2 after
  bounded discovery, both real exact UNKNOWN replies are refused by the old
  source caller. `startup-conflict-assumption-failed.log`:5/2 after retry correction;
  native cases reject a wrong test expectation of a positive OperationConflict
  receipt. The corrected check requires the actual ingress refusal above.

Previous-source CI is separate: completed operator run38062453286 at full source
60d37d7b2fc812dfa6ffbc106623e0f1e36efa9b passes macOS181 counter/22 directory/21
transfer tests. Ubuntu counter180/1 fails drain19701 confirmation with exact
ERR NOT_LEADER; later directory/transfer targets are unrun. `ci229.json` and both
raw job logs retain those results. The first raw-log download refused terminal
escape sequences; the successful file-only download explicitly preserves them.
This is not a current-source platform pass or a fix for earlier independent
configuration/drain failures.

The full P0–P7 goal stays active. Provider review, combined/generated fault
coverage, deployment/platform acceptance and the original250ms P7 gate remain
open. Static service use does not wait for wider online membership acceptance.
Security review remains the user's Daybreak work.
