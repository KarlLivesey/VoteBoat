# Executable metadata authority

Source base:4221875. Final production/test hashes are in source.sha256.

Commands and observed results:

- `cargo +stable test --locked --offline --all-features --test directory_service -- --nocapture`:4 passed.
- `cargo +stable test --locked --offline --all-features --test counter_service`:65 passed after extracting shared native startup/cleanup and command transport.
- `cargo +stable fmt --all -- --check`:clean.
- Strict all-target Clippy for default, all-feature and no-default-feature builds:zero diagnostics.
- `node validation/check-inventory.mjs`:95 inventory entries; metadata validation only.

The original capacity refusal is retained. The general Directory readiness
contract reserves lifecycle control history beyond this executable's fixed
initial-publication whitelist. The binary validates all admitted records and
their complete schema1 checkpoint/history ceiling without altering Directory,
its readiness contract, durability or native transport limits.

Tests exercise actual processes and TCP/QUIC peer traffic, unpublished absence,
writer refusal, exact publication receipts, remote quorum reads, checkpoint
advancement, duplicate retries, changed-plan recovery refusal, oversized input,
and disconnect after observed read admission during quorum loss. The initial
plan generator is checked against independent canonical encoding.

This is a static metadata service and one remote lookup, not recursive CLI
routing, automatic placement, lifecycle administration, separate-host/macOS
certification, or complete P0–P7 acceptance.
