# Executable command endpoint discovery

Source base:9502cf4. Final Rust source hashes are in source.sha256.

- `cargo +stable test --locked --offline --all-features --test counter_service`
  passed64 tests, including the first four discovery tests. The fifth test was
  added afterward; production source did not change.
- `cargo +stable test --locked --offline --all-features --test counter_service command_discovery -- --nocapture`
  passed all5 discovery tests, including the added scope/startup refusal case.
- Formatting and all-target strict Clippy pass for default, all features and no
  default features. Inventory validates95 metadata entries, not runtime safety.
- Initial check failure is retained. Session binding now uses the actual public
  require_authenticated contract. A service-loop size warning was resolved by
  extracting unchanged administration progression, without a lint exemption.

Native tests require socket access. TCP/QUIC refer to the Raft peer transport;
command/discovery sessions use authenticated TCP. These are local multiprocess
histories, not separate-host/macOS verification or complete P0–P7 acceptance.
