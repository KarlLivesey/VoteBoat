# Slice221 — explicit original-write continuation

The matching219 macOS assignment retry rejected a reply-deadline UNKNOWN after
restart; failed drain-publication recovery rejected explicit not_proposed=Busy.
Neither observation establishes a successful operation. The two test callers now
share one explicit continuation with immutable original arguments. Exact Busy
or LeadershipChanged replies require the CLI's unsuccessful-request error;
recognized reply-deadline/read interruptions require its interrupted-request
error. Modified/unrelated pairs remain terminal. The production CLI still stops
on uncertainty and performs no automatic replay of these writes.

Validation:

- `caller-before.log`:1 pass/3 fail against the extracted old narrow policy. The
  recorded Busy and reply-deadline responses are rejected before any positive
  receipt. The deadline trace cannot advance through that unrecognized response.
  `before.sha256` identifies the historical caller/test state.
- `caller-after.log`:5 pass. Every attempt gets the same original group, operation
  and delta; only an actual positive invocation finishes. Virtual time verifies
  no invocation at/after the fixed caller deadline, no budget renewal and no
  manufactured success on exhaustion. Ten malformed/unrelated pairs stop after
  one attempt with no pause. These are caller-policy checks, not durability proof.
- `counter-all.log`:171 pass. Actual TCP/QUIC assignment histories require exact
  original Value(5)/duplicate=true after checkpoint/restart, OperationConflict
  for the changed delta and a fresh quorum read retaining5. The failed journal
  publication history still requires non-success, uncertain service exit,
  unchanged empty journal, restart refusal of a nonexistent drain and the exact
  original write's duplicate receipt. Existing actual unread-write TCP/QUIC
  histories preserve retained receipts/conflicts through checkpoint and reopen.
- `assignments-default.log`:2 pass; `drain-default.log`:1 pass. Feature builds
  are sequential after the all-feature service processes exit.
- The full171 sweep precedes a final strengthening of the failed-publication
  test: exact Value(7)/duplicate=true and a fresh read of7 are now required.
  `drain-exact-all.log`/`drain-exact-default.log` each pass1 afterward.
  `full-counter-source.sha256` retains the pre-strengthening test source;
  `source.sha256` identifies the final source and artifacts. The two results
  are separate evidence, not a repeated unchanged full sweep.
- `clippy-all.log` and `strict-final.log`:zero diagnostics. Formatting and strict
  all-target default/all-feature/core-only/native-only checks run through the
  unchanged enabled hook. No allowance or threshold change.
  `strict-before-exact.log` retains the earlier clean check; final checks also
  cover the strengthened drain assertions.

The existing10s write and15s assignment caller budgets govern whether another
invocation may start. Each accepted CLI subprocess retains its own existing10s
deadline; this is not a new total wall-clock guarantee or timeout extension.
The helper owns no Node, read/write ticket, journal or runtime. Tests must still
validate real returned application receipts; UNKNOWN/Busy cannot satisfy them.

Preceding220 [run38054167141](https://github.com/KarlLivesey/VoteBoat/actions/runs/38054167141)
has a terminal macOS counter result162 pass/4 fail before directory/transfer run.
Raw logs identify original21101 preparing expiry before configuration_queued,
QUIC19701 resumed runner budget expiry, a node1 native Closed exit in membership
drain, and configure-record17012 authenticated-read interruption. The older
assignment/drain cases pass on this source, but it does not contain221's new
helper. Last retained Ubuntu state is live. Do not infer current-source platform
acceptance or close the new failures from local passes.

Macro progress: usable-service/P4 original-operation recovery has one checked
caller policy instead of duplicated assumptions. Next measure the unchanged
original P7 fixed250ms serial TCP p99 profile while CI runs, then isolate these
exact remaining platform boundaries and review bounded provider obligations.
Full P0–P7, deployment/fault/platform and performance acceptance remain open;
broad security stays with the user's Daybreak run.
