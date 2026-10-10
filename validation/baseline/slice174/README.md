# Slice174 validation

Linux, RPL-1.5. Final source hashes are in `source-sha256.txt`.

- `cargo +stable test --locked --offline --all-features --test counter_service`: 50 passed in42.38s (`service-final.log`). Includes both live reload histories and existing configuration, enrollment, retirement, maintenance and client-history checks.
- `cargo +stable test --locked --offline --all-features --test credential_journal --test credential_refresh --test authorization --test secure`: 31 passed (`credential-tests.log`).
- `cargo +stable test --locked --offline --all-features --bin voteboat-counter`: 3 passed (`worker-tests.log`), including two new controlled worker lifecycle checks.
- Core-only selected authorization/session tests: 9 passed (`core-tests.log`); the native-gated journal runs zero core-only tests.
- `cargo +stable test --locked --offline --no-default-features --features native --test credential_journal`: 4 passed (`native-journal.log`).
- Formatting and both all-target strict Clippy configurations: zero diagnostics (`fmt.log`, `clippy-all.log`, `clippy-core.log`).
- Warnings-denied all-feature docs and94-record inventory validation pass (`docs.log`, `inventory.log`). Inventory checks metadata/paths, not behavioral conformance.

Initial compile/lint logs retain dispatch-size and borrow failures. The final
implementation separates startup preparation and explicitly splits immutable
access from mutable command control; no lint threshold was relaxed.

`tests-all.log` retains a49/50 service result: an old configuration helper rejected
the documented LeadershipChanged uncertainty. It now retries only that exact
response with the identical operation ID when success is expected. Both focused
administration cases then pass (`admin-regression.log`).

`service-concurrent.log` retains another49/50 result: enrollment startup failed
with AddrInUse. `startup-bind-failure.log` is the actual failed recovery log;
the test previously printed an empty create log. Separate service invocations
were overlapping and share a process-local port pool. The final full invocation
runs alone and passes; this is not evidence of cross-process port coordination.
`reload-first.log` and `reload-final.log` use a name filter: their other selected
test targets run zero tests. The unfiltered credential log above supplies those
31 checks rather than treating filtered targets as tested.

Coverage: exact latest journal retry, owner/sequence/generation refusal,
before/after atomic replacement uncertainty, corruption at every byte, partial
staging recovery, one held worker, shutdown joining, uncertainty fencing,
unobserved reload replies, policy revocation, preserved data/operation retries,
invalid preparation and restart generation/digest checks. No physical power-loss,
macOS/separate-host, full peer key rotation, complete durable audit history or
full P0–P7 completion claim follows.
