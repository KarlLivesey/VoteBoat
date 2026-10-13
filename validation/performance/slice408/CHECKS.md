# Counter408 targeted checks

Source scope: src/application.rs Counter StateMachine::apply_batch only, plus
two tests/application.rs controls. Source stable after rustfmt on those files.

Executed with CARGO_TARGET_DIR=target/p9/counter-checks:

1. cargo +stable test --locked --offline --no-default-features --test application
   Terminal exit0: 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out.
2. cargo +stable clippy --locked --offline --no-default-features --test application -- -D warnings
   Terminal exit0: zero diagnostics.
3. cargo +stable test --locked --offline --no-default-features --test provider_conformance application_cases::
   Terminal exit0: 3 passed; 0 failed; 0 ignored; 0 measured; 11 filtered out.
   Includes the Counter shared contract, independent host application, and
   checker rejection of bad receipts/partial failure/future reads/budget excess.

An initial provider filter `application::` matched zero tests and is not evidence.

Stable source SHA256:
- src/application.rs: 132f2abb5df76c56fec3a8cd6edf4165f9f0baca3e6acaacb47846347d830aea
- tests/application.rs: fab0f71249b5d16351ea04d3f91c322b6c0e31f863580a5abc309f86510d5f79

No performance workloads were run by this agent. No global checks or full
feature/configuration validation are claimed by these targeted results.
