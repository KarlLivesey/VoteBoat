# Slice199h — historical handoff after later target loss

Starting revisiona840ed1. The public Completed contract records historical
success, not current leadership authority. An older54a73ad macOS result retained
in [slice199g](../slice199g/prior-54a73ad-macos.log) fails the contrary assertion.
Single/grouped handoff tests now retain original target and record checks without
assuming that no subsequent election occurred. Original grouped retries select
a current leader using the existing bounded helper.

Two new real-service histories complete a group7 handoff, kill the target, and
require another live leader to return the identical completed record. Original
begin/resume/cancel replies cannot rerun or erase the historical handoff. Group8's
same operation ID remains absent. Data deduplication and a new write are checked
before and after restart. QUIC verifies the stopped node's recovered WAL/snapshot
base covers the requested committed prefix. No production code is changed.

## Executed checks

- `historical-handoff-initial.log`: TCP passes; QUIC exposes an unsupported group
  maintenance-status query in the new fixture. The fixture is corrected to
  inspect actual file recovery rather than a nonexistent command.
- `group-leadership.log`: all5 grouped leadership tests pass, including both new
  target-loss histories.
- `counter-service.log`: all131 all-feature counter-service tests pass.
- `leadership-default.log`: all9 default-feature leadership tests pass, executed
  after all-feature processes finish.
- `pre-push.log`: formatting and strict default/all/core/native-only lint checks
  pass with zero diagnostics.
- `source.sha256` / `source-check.log`: final source and test hashes.

```sh
cargo +stable test --locked --offline --all-features --test counter_service group_leadership::
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --test counter_service leadership::
sh .githooks/pre-push
```

## Separate platform evidence

`prior-a41a6ec-linux.log` and `prior-a41a6ec-macos.log` belong to completed
run38036099346: jobs114166904571 and114166904463 respectively. Linux counter
passes122; directory passes8/fails5 on explicit UNKNOWN initialization outcomes.
macOS counter passes114/fails8 across assignment, replacement, grouped command
and runner cases. These are older-source observations, not current-source passes
or failures.199i addresses the directory setup assumption; broader platform,
combined lifecycle/revocation and P7 acceptance remain open.
