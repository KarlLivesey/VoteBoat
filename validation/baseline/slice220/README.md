# Slice220 — bounded single-authority lookup source selection

`voteboat-directory lookup BASE auto ... [--command-peers FILE]` now uses the
existing route Discovery for one fixed authority/query. Only provisioned endpoints
are considered; status selects an attempt and the existing remote protocol must
produce a validated quorum-backed manifest. Numeric node lookup stays pinned.
The absolute ten-second deadline, connection ceiling, original output and typed
interruption policy remain unchanged. No new retry engine, write repetition,
public trait, Raft timer or read authority is introduced.

Selected evidence:

- `lookup-before-native.log`: both TCP/QUIC histories force the sampled source
  to be a follower; its explicit lookup expires with no output. The new auto mode
  is initially unimplemented, so both checks fail with InvalidDigit. This confirms
  the pinned caller limitation, not the cause of219's unlabelled fixture failure.
  `before.sha256` identifies the historical source/test state.
- `lookup-after.log`: the initial two auto histories pass with byte-identical
  original manifests. Subsequent coverage includes explicit command maps and a
  no-live-authority deadline producing no hint.
- `directory-all.log`: first full sweep21 pass/1 fail. The new QUIC history fails
  its initial explicit sampled-leader baseline before forcing loss. A sampled
  status is not a fresh-read promise. Only that baseline is changed to use auto;
  the later pinned-follower refusal and exact output checks remain required.
- `directory-all-final.log`:22 pass. `directory-default.log`:17 pass, run after
  all-feature service processes exited. Four peer/access-reload read phases now
  use auto; original publication IDs, checkpoint/reopen, denial and retry checks
  remain. Failure-only diagnostics retain phase, sequential local status and up
  to4KiB/node logs before teardown, with a shared3s subprocess/gate deadline and
  kill/join cleanup. Successful histories add no diagnostic subprocesses.
- `discovery-bounds.log`:2 existing checks pass for authentication timeout,
  fixed lookup budget and closed attempt. `lookup-before.log` retains an initial
  test module-path compile error; it is not a protocol failure.
- `clippy-all.log` and `strict-final.log`: zero diagnostics; the unchanged push
  hook runs formatting and strict all-target default/all/core/native profiles.
- `inventory.log`:108 contract paths pass. `ledger-tests-direct.log`:21 metadata
  cases pass. `ledger-tests.log` retains the Node `--test` file-level1 result;
  the direct invocation exposes all21 cases. The ledger remains8 partial reviews/
  57 operations with100 unreviewed contracts, not provider certification.

Matching219 [run38053081242](https://github.com/KarlLivesey/VoteBoat/actions/runs/38053081242)
is terminal: Ubuntu counter166/directory19/transfer21 pass; macOS counter164/2
fails before directory/transfer run. Raw logs and exact-head terminal JSON remain.
The assignment original retry reports UNKNOWN deadline/interruption; drain journal
failure setup reports exact not_proposed=Busy. These CI results include219's
transport correction and predate220's new CLI mode. Fresh220 platform results
remain required; no complete macOS acceptance or original219 timeout diagnosis.

Macro progress: the usable-service/P5 read client can choose an eligible source
without a new consensus path. Next is the retained exact counter caller failures,
then the unchanged original P7 fixed250ms serial TCP p99 profile and bounded
replacement-provider obligations. Broad security stays with the user's Daybreak
run. Full P0–P7, deployment/fault and performance acceptance remain open.
