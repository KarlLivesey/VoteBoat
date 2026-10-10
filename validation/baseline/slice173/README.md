# Slice173 validation

Linux. Final commands exited0:

- `cargo +stable test --locked --offline --all-features --test credential_refresh --test quic_connect --test authorization --test secure`:39 passed.
- `cargo +stable test --locked --offline --no-default-features --test credential_refresh --test authorization --test secure`:9 passed.
- Final focused TLS key-rotation test:passed; leases are captured before handshake, and new trust rejects retired credentials.
- `cargo +stable fmt --all -- --check`:clean.
- Both all-target strict Clippy profiles with `-D warnings` (`--all-features`, `--no-default-features`):zero diagnostics.
- Warnings-denied all-feature documentation:passed.
- Inventory:93 contract records; metadata/path checking only.

`compile-first.log` retains the initial test-only Debug-bound failure. The fix
formats only the typed error, preserving owned credential material. Source hashes
identify the final code. This is selected host/native/Linux evidence, not complete
rotation deployment, macOS execution or a P0–P7 completion certificate.
