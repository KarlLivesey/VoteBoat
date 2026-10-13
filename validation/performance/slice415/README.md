# Slice415 qualification

This production slice separates ordinary progress sends from heartbeat retries.
The bounded request scalar tracks the original commit notification obligation;
it does not represent peer application or an acknowledged retry frontier.

Implementation and failure/ownership contracts are in [SCHEMA.md](SCHEMA.md).
Snapshot, joint/uncommitted membership and leadership-transfer broadcasts retain
the original fallback. Original outcomes, host scheduling, maintenance, selected
durability and immutable slice414 measurement sources are unchanged.

Scoped final qualification: the Core-only library passed 276 tests; the
Native-feature Raft library filter passed 109 tests, including ten new controls;
Core-only library/test-target strict Clippy completed with zero diagnostics.
Commands and actual terminal results are retained in
`library-core-post-extraction.log`, `raft-native-final.log` and
`clippy-core-final.log`.

Root final Linux qualification also passes: all-feature library345/0, affected
integrations246/0, actual TCP/QUIC three-process original-write/leader-replacement/
native-file-recovery2/0, and standalone matched eight original/maintenance/follower
controls. Formatter/all four strict root profiles plus standalone formatting/
strict all-target checks exit0. [Root raw records](linux).

The affected packet-loss simulations previously relied on opportunistic progress
retries. Their focused histories now retain the original request or explicitly
drive the existing heartbeat after deliberate queue loss/Busy-consumed ACKs.
Counter9, exact one nonduplicate application, process-versus-power-loss outcome
counts, fresh-session recovery, original retries, readiness expiry and failed
election-quorum assertions remain. Shared pump/finish helpers and production
behavior were not weakened to make those histories pass. Original failed logs
are preserved next to the final results.

Earlier raw logs remain: missing scalar initializers in two nested fixtures,
an incorrect fixture assumption that a bootstrap checkpoint is nonplain,
the legitimate new learner commit-notification output, and the 101-line existing
append function after adding metadata. Corrections supplied the initializers,
asserted the exact snapshot reference, validated the notification and its finite
ACK, and separated unchanged retry construction without weakening lints.

Tests exercise real Core transitions with explicitly host-asserted exact durable
tokens; they are not device-power-loss or complete protocol proofs. No performance
claim is made for this slice without a frozen-source comparison.
Current Mac qualification and paired capacity measurements remain pending.

Commands used `CARGO_TARGET_DIR=target/p9/slice415/checks` with:

- `cargo +stable test --locked --offline --no-default-features --lib`
- `cargo +stable test --locked --offline --no-default-features --features native --lib raft::`
- `cargo +stable clippy --locked --offline --no-default-features --lib --tests -- -D warnings`
