# Slice224 — current leader, original maintenance intent

The preceding macOS run222 resumed operation96100 at its original source after
that node had become a follower. The service's explicit NOT_LEADER refusal is
correct. The test now reuses its existing current-leader caller for resume and
cancellation while keeping the original operation and durable intent. A newly
started intent uses its newly observed source. Retrying the old cancellation
must leave the newer Pending intent intact.

This is a test-caller correction, not a new production auto-administration mode.
The existing10s caller eligibility deadline,20s leader observation and per-CLI
request deadlines remain. Those nested existing deadlines are not a new single
wall-clock guarantee. Resume recognizes only exact NOT_LEADER, NotRead(ReadNotReady)
and Unavailable(LeadershipChanged) replies. Modified replies, unrelated failures
and arbitrary UNKNOWN remain terminal; they cannot count as successful execution.

New TCP/QUIC histories complete a real handoff, require the original source's
exact non-success/follower refusal, then resume/cancel through a current leader.
Every original source/target/store/configuration/operation/index/term is retained.
Completed remains historical. QUIC waits for the durable checkpoint base to
reach the selected committed prefix before shutdown. All processes reopen; exact
completed observations survive, the original Value(7)/duplicate receipt remains,
changed payload conflicts, the fresh quorum read stays7 and a new write reaches10.
Actual shutdown must drain and join workers. This Completed schedule does not
establish every Pending source-loss schedule; existing Pending/deadline/reopen
and cancellation histories remain separate evidence.

Validation:

- `follower-before.log`:1 pass/3 fail. Both real transports fail at the old
  pinned-source resume with exact NOT_LEADER. The old helper also rejects the
  recorded follower-response class. Unrelated/modified failures already remain
  terminal. `before.sha256` identifies that historical test source; it is not a
  digest of the final corrected files.
- `leadership-all.log`:19 pass, including both new actual histories, exact response
  recognition/refusal, original96100 Pending/deadline/cancellation, admitted
  cancellation/source-loss recovery and independent-group leadership histories.
- `clippy-all.log`:strict all-target/all-feature Clippy passes with zero diagnostics.
- `counter-all.log`:all175 actual counter histories pass. `leadership-default.log`:
  all13 default TCP leadership selections pass. Feature service builds/targets
  are sequential so no executable is overwritten while an older profile uses it.
- `strict-final.log`:formatting and all four strict all-target Clippy profiles
  pass with zero diagnostics through the unchanged enabled hook.

Preceding223 [run38056508918](https://github.com/KarlLivesey/VoteBoat/actions/runs/38056508918)
at `0564df8ea19640a2fa7cffacf101b5cfcfb89ef8` has a terminal macOS counter result:
170 pass/1 fail, only original19701 QUIC runner budget expiry. Directory/transfer
are not executed after that failure. The prior shutdown and follower-resume tests
pass in this run; that does not prove224's caller change, which it does not contain.
`macos223-job.log` is the completed job's raw API log. `gh run view --log` refused
while Ubuntu was still live, and the first direct API read refused terminal escape
formatting (`macos223-log-download.log`). The raw-file-only permitted download
then succeeded; no log text is executed. `ci223-status-last-observed.json` identifies
the actual observed source/job state, with Ubuntu still in progress. The result
is not a whole-platform pass or a newly restarted run.

`source.sha256` identifies final source and artifacts; `source-check.log` verifies
those hashes. Historical before-source hashes remain separate.

No production source, public seam, journal, synchronization, Raft timer or package
license changes. Broader security stays with the user's Daybreak run. The original
19701 runner budget, matching macOS/separate-host acceptance, broader faults/
provider obligations and fixed P7 gate remain open; full P0–P7 stays active.
