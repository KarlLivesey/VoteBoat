# Slice210f — original receipts after sampled leader loss

The completed fe6d5ff macOS counter run passes143/fails13. Its TCP quorum-loss
history obtains a positive original write receipt, then assumes the same sampled
leader owns later observations. The duplicate request instead returns the exact
Unknown(LeadershipChanged) response. A receipt grants no future leadership lease.

The fixture now forces that acknowledged leader offline before observing the
final value and original duplicate. Existing leader_request caller retries use
unchanged operation2/delta3 and only exact documented read/write outcomes within
the existing10s bound. The CLI still stops on an uncertain write. After return of
the stopped node, a full cold restart verifies both original receipts (operation1
retains Value(7), operation2 retains Value(10)) and a fresh quorum read of10.
The initial positive write also uses the existing caller-owned retry path.
No production behavior, timer, format, quorum or lint threshold changes.

Executed evidence:

- `tcp-before.log`: forced old-leader observation fails before correction.
- `tcp-after.log`: corrected native TCP history passes with cold recovery.
- `tcp-default.log`: the affected default TCP history passes.
- `groups-before.log`: unchanged current-source CI-temp group selection passes33
  locally; this does not reproduce the twelve preceding macOS QUIC failures.
- `checks.log`: formatting and all four strict Clippy profiles pass.
- `counter-all.log`: sequential final full all-feature counter suite passes158.
- `counter-overlapped.log`/`tcp-default-overlapped.log`: excluded from acceptance. The default executable
  build was started while the all-feature suite was still using the shared
  target/debug binary. It finishes157 pass/1 new-voter QUIC failure; this cannot
  establish clean feature isolation or prove that failure's cause. Both handles
  became terminal before the sequential full repeat in `counter-all.log`.

`ci-prior-completed.json`/`ci-prior-macos.log` retain completed prior-platform
evidence. `ci-current-ubuntu.log` records56245bb Ubuntu156 pass/2 fail: exact
authenticated configuration read interruption and unresolved initial drain
admission. The runner's generic identity error alone does not identify the source
reply or establish a changed drain identity. `ci-current-latest.json` records that
the matching macOS job was still live. Broader operator/platform, provider, fault
and performance acceptance remains open; the full P0–P7 goal stays active.

Reproduce with socket access, running executable configurations sequentially:

```sh
cargo +stable test --locked --offline --all-features --test counter_service bounded_commands_and_quorum_loss_preserve_retry_identity
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --test counter_service bounded_commands_and_quorum_loss_preserve_retry_identity
sh .githooks/pre-push
```
