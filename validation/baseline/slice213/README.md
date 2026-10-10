# Slice213 — resume original merge retirement

The previous-source Ubuntu operator run38046741849 fails merge retirement on
explicit UNKNOWN Unknown(LeadershipChanged), followed by the CLI instruction to
repeat the same profile/source/release. The production CLI and documented
contract already resolve an existing retirement before collecting new proof.
The merge fixture incorrectly required its first invocation to succeed.

One test-only helper permits at most four attempts for exactly that observed
stdout/stderr pair. Every injected attempt receives the original source/release.
Unrelated unknown outcomes and other failures remain terminal. Successful exit
requires a confirmed retirement record with exact source/incarnation/operation/
release, positive fence, later retirement index and quorum prefix covering it.
Only read_index is normalized across cold recovery; record changes still compare
unequal. Production uncertainty policy, request deadlines and APIs are unchanged.

The actual merge histories now collect a real completed-read retirement proof,
remove source20's quorum, observe proposal admission, kill the waiting caller
and reopen all groups through the existing interruption fixture. They resume
release700; source21 stays live until its separate release701. The original
profile bytes and exact retirement records survive later cold recovery with
metadata closed. Both original data retries and new target writes are checked.
Admission is not a claim that an uncommitted proposal was durably acknowledged.

Executed evidence:

- `before.log`: the scripted original leadership-loss continuation fails under
  the prior single-attempt confirmation behavior.
- `confirmation.log`: all3 scripted cases pass, covering unchanged arguments,
  exhaustion/terminal refusals and exact quorum status/record validation.
- `merge-extra-reopen.log`: all3 initial native merge profiles pass with an
  unnecessary second reopen after the existing fixture had already reopened.
  Those two lines were removed before the final full run; this trial is not
  final-source evidence.
- `transfer-all.log`: all21 final-source all-feature transfer-service tests pass,
  including the3 native merge profiles and3 new confirmation checks.
- `clippy-all.log`: all-feature strict Clippy passes with zero diagnostics.
- `merge-default.log`: both default TCP merge recovery profiles pass on final
  source after the all-feature executable run ended.
- `confirmation-default.log`: all3 scripted confirmation checks pass with
  default features.
- `checks.log`: formatting and all four strict Clippy profiles pass with zero
  diagnostics through the enabled pre-push script.

`ci-observed.json` records existing run38047529334/629db60 live at observation;
it is neither terminal success nor validation of this test-fixture change.
`ci-post-local.json` subsequently records that run's Ubuntu job as terminal
success, with macOS still live. `ci-linux-629db60.log` retains the actual passing
counter/directory/transfer outcomes on that previous source, not the new helper.
`macos-node-context` contains the retained failed-run38046741849 macOS clusters
48 (configuration21101 preparation deadline) and89 (QUIC interrupted multi-group
drain runner). The runner's recovered group7 records show terms123 and181 and
its source stops, but the later group1 read cannot route; this is evidence for
the next cause-specific liveness investigation, not a proven transport cause.
Original job outcomes are in slice212. Downloaded logs are diagnostic data.

This is selected process-abort/admission/receipt-loss evidence, not power-loss,
arbitrary faults, macOS acceptance or a broad security audit. The user will run
Daybreak for broad security work. Full P0–P7 stays active; P8/Windows deferred.

Reproduce feature builds sequentially in a socket-enabled environment:

```sh
cargo +stable test --locked --offline --all-features --test transfer_service retirement_confirmation
cargo +stable test --locked --offline --all-features --test transfer_service
cargo +stable test --locked --offline --test transfer_service merge::
cargo +stable test --locked --offline --test transfer_service retirement_confirmation
sh .githooks/pre-push
```
