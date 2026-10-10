# Slice210g — preserve the actual drain admission refusal

The preceding56245bb Ubuntu operator run fails one drain history with UNKNOWN
initial drain admission: source replied with a different drain identity. The
runner's identity parser uses that message for every non-OK observation, obscuring
the actual refusal. The CI message therefore cannot establish its exact cause.

The existing failure path now returns the escaped original source response.
Non-OK observations remain terminal. Foreign successful identities, duplicate
fields, malformed data, inactive plans and exhausted budgets still refuse;
no automatic uncertain-write retry, configuration or shutdown is authorized.
There is no new provider contract, state, durable token or timer change.

Executed evidence:

- `before.log`: new authenticated-channel admission script fails on the old
  generic diagnostic. An uncertain original drain-node is followed by exactly
  the original drain-status and a real production-format missing-record refusal.
- `runner-all.log`: all19 selected runner tests pass. The new script returns the
  actual escaped refusal, consumes exactly two requests, preserves the absolute
  deadline and allows no resume/configuration/stop request after refusal.
- `runner-service.log`: all9 selected native single/shared-group runner histories
  pass, including actual TCP/QUIC membership/partial-recovery/shutdown paths.
- `runner-default.log`: all19 selected runner checks pass in the default TCP
  configuration, after the all-feature executable histories have ended.
- `checks.log`: formatting and all four strict Clippy profiles pass.

`ci-current.json`/`ci-current-macos.log` retain terminal56245bb platform state:
Ubuntu156 pass/2 fail (raw Ubuntu log is in slice210f); macOS147 pass/11 fail.
Ten macOS failures are QUIC histories; the TCP failure is the exact authenticated
configuration read interruption also recorded on Ubuntu. This diagnostic change
does not fix those failures or establish the missing drain record as the actual
CI cause. Matching operator acceptance and the full P0–P7 goal remain open.

Reproduce sequentially with local socket permission:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter drain_runner
cargo +stable test --locked --offline --all-features --test counter_service drain_runner
cargo +stable test --locked --offline --bin voteboat-counter drain_runner
sh .githooks/pre-push
```
