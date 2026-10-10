# Slice172 validation

Linux, rustc1.98.1. Final commands exited0:

- `cargo +stable test --locked --offline --all-features --test remote_discovery --test connect --test quic_connect --test discovery`:42 passed.
- `cargo +stable test --locked --offline --no-default-features --features native --test remote_discovery`:13 passed.
- `cargo +stable fmt --all -- --check`:clean.
- Both `cargo +stable clippy --locked --offline --keep-going --all-targets` profiles (`--all-features` and `--no-default-features`) with `-- -D warnings`:zero diagnostics.
- `RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps`:passed.
- `node validation/check-inventory.mjs`:92 contract records, metadata/path validation only.

`source-sha256.txt` identifies tested code. Earlier failing logs are retained:
`remote-native-first.log` used a nonexistent fourth test TLS credential;
`quic-idle-first.log` advanced past the five-second QUIC idle timeout;
`clippy-first.log` identifies a range-loop and test-function complexity issue.
The fixes preserve the checks and distinguish endpoint expiry from transport idle.
No macOS, separate-host, full-baseline or exhaustive fault claim follows.
