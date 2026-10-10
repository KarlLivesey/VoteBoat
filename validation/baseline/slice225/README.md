# Slice225 — attribute the runner's unchanged budget refusal

Original19701 QUIC runner failures on macOS previously reported the same UNKNOWN
text for either the absolute deadline or exhausted exchange count. At the existing
refusal boundary, the executable now emits one failure-only stderr diagnostic:
local volatile evidence, original source/sequence/operation, actual target node,
command class, remaining count, elapsed milliseconds and independent request/
deadline exhaustion flags. It then returns the original error unchanged.

Single-group128/45s, multi-group4096/120s and per-exchange deadlines remain.
No new exchange, proposal, success, source stop, read retry, owner, provider seam
or durable record follows from this output. One Instant supplies both flags and
elapsed time. Both flags may be true; the diagnostic does not infer a unique
cause. Command classification shows only the first command word; a multi-group
`group` command does not identify its individual row. The output is bounded
context for the existing internally constructed commands, not a trace history.

Validation:

- `runner-unit-all.log`:19 existing checks pass with uncaptured stderr. Pre-I/O
  refusal distinguishes source3/target1 and the two independent bounds. Actual
  authenticated observation expiry records deadline=true, requests=false,
  remaining127 and301ms elapsed. Accepted lost-observation and configuration
  reply cuts record requests=true, deadline=false, remaining0 at5025/5120ms.
  Exact returned errors, fixed deadlines and counts remain asserted, and the
  exhausted configuration cannot observe readiness or send stop. These local
  tests identify their own bounds, not the macOS run's bound. No new formatting-
  mirror tests or repeated unrelated suite are introduced for this diagnostic.
- `runner-native-all.log`:13 selected checks pass, including actual single/shared-
  group TCP/QUIC runner/source-loss and checkpoint/reopen histories, immutable
  original plan/assignment, readiness before stop and native worker joins.
- `clippy-all.log`:strict all-target/all-feature Clippy passes with zero diagnostics.
- `runner-unit-default.log`:19 pass. `runner-native-default.log`:9 selected TCP
  checks pass. Feature service builds and live targets are sequential.
- `strict-final.log`:formatting and strict all-target default/all-feature/core-only/
  native-only Clippy pass with zero diagnostics through the unchanged enabled hook.

Preceding223 [run38056508918](https://github.com/KarlLivesey/VoteBoat/actions/runs/38056508918)
is now fully terminal. `ci223-terminal.json`/`ubuntu223-job.log` retain Ubuntu171
counter/22 directory/21 transfer passes. The macOS counter170/1 original runner
budget failure and raw job log were retained separately in
[slice224](../slice224/README.md); later macOS targets are unrun. That source
contains the shutdown correction, not224's follower caller or225's diagnostic.
Current224 platform feedback remains background work, without a restart or gate.
Both static TCP/QUIC three-process histories pass on the retained macOS223 source;
their actual assertions include writes/reads/original retries, leader loss,
drained checkpoint, cold recovery and workers_joined=true. The online runner
failure does not prevent use of the static service; this selected evidence is
not broader platform/release or separate-host acceptance.

`ci224-status-last-observed.json` retains the current-source predecessor's live
feedback state. `source.sha256` identifies final source and artifacts;
`source-check.log` verifies them. Unchanged unrelated test targets are not rerun
locally for this failure-only diagnostic, and no full-suite pass is claimed.

Matching225 macOS diagnostic evidence is still needed before changing scheduling
or caller replay. The original runner failure is not fixed by observing it.
Full P0–P7, platform/deployment/provider/fault and original P7 acceptance remain
open; broad security stays with the user's Daybreak run.
