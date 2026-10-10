# Slice187: authenticated remote command endpoints

The executable's command path now supports an explicit authenticated listener
and a bounded, versioned client endpoint list. The default trusted loopback
commands and the existing Rust composition contracts are retained.

## Evidence

- `remote-endpoints.log`: six new tests pass. Real three-process TCP/TLS and QUIC
  clusters use wildcard command listeners and non-default command ports. Tests
  reject wrong TLS names and unauthorized writes, checkpoint, lose a leader,
  retry the same operation without double application, restart every store,
  read the recovered value and gracefully join the selected workers.
- The same target checks reject an unauthenticated listener before creating its
  directory; reject malformed, duplicate, oversized or missing-target endpoint
  declarations before connecting; bound aggregate certificate retention; and
  stop an unknown write after a TLS interruption without trying another target.
- `counter-service.log`: all60 service tests pass in43.39s, including default
  routing, credential changes, membership, retirement, maintenance and recovery.
- Formatting and all three strict Clippy profiles pass; logs are retained.
- Inventory reports95 entries with valid metadata/conformance paths. This is a
  metadata check, not independent proof of implementation correctness.

The first interruption fixture incorrectly reused a plaintext newline reader:
coalesced ClientHello bytes could follow the public principal selector. The
fixture now reads exactly the selector before closing. The production protocol
and conservative Unknown outcome were not weakened.

## Commands

```sh
cargo +stable test --locked --offline --all-features --test counter_service command_endpoints:: -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service -- --nocapture
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
node validation/check-inventory.mjs
```

Native tests ran on Linux over loopback with actual executable processes and
file-backed stores. They do not establish separate-machine networking or macOS
support. TLS command transport remains TCP even when Raft peers select QUIC.
Explicit address files do not discover authorities or mutate Raft membership;
automatic manifest/endpoint integration remains open. No consensus/storage
format, durability gate or operation identity rule changes in this slice.
