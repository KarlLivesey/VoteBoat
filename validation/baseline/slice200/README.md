# Slice200 — explicit source retirement

Base: 78382c0. Native Linux execution; RPL-1.5 and the design pack are unchanged.

Implemented `TransferOperation::retirement_proof`, a fresh v2 source profile,
immutable profile/node/store/group binding, authenticated retirement command and
quorum status, and client resumption using the original lifecycle/release IDs.
This reuses RetirementGuard and the existing log/checkpoint contracts.

`final.log` records the final all-feature run: one executable binding test,
seven existing retirement contract/file-recovery tests, ten transfer-operation
tests and all seven executable transfer histories pass. The new TCP/WAL and
QUIC/checkpoint histories interrupt an admitted retirement without quorum,
restart, complete the original release, refuse conflicting releases and source
service, and retain child retries/writes with metadata/source stopped. Profile
tests reject a legacy profile, a valid but changed profile, missing and corrupt
bindings before recovery. The same release can be confirmed with metadata off.

`services-before.log` retains a preceding run: all122 counter tests passed;
five transfer tests passed and the two expanded retirement histories failed
because their extra blank profile row was rejected by the parser before the
expected binding check. The fixture now changes valid header whitespace.
`retirement-initial.log` and `unit-initial.log` retain earlier, narrower passes.

Formatting, all four strict Clippy profiles and the105-contract inventory pass.
This is not a current full P0–P7 suite, macOS/separate-host or power-loss claim.
The source must select v2 before first creation; no migration is supplied.
Retirement does not remove membership or authorize deleting storage/backups.
General retention, broader operator profiles and failure coverage remain open.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test counter_service --test transfer_service
cargo +stable test --locked --offline --all-features --bin voteboat-transfer --test retirement --test transfer_operation --test transfer_service -- --nocapture
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
node validation/check-inventory.mjs
```
