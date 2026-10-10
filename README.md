# VoteBoat

A composable Rust consensus runtime with durable replication, TCP/TLS and optional
QUIC, and public interfaces for host-supplied storage, transport and applications.

**Under development.** The static-membership counter service and Rust embedding
are usable; the full roadmap is still in progress. Targets Linux and macOS.
Licensed under [RPL 1.5](LICENSE).

## Try it

Install the Rust toolchain pinned in `rust-toolchain.toml`, then run the local
three-replica counter demo:

```sh
cargo fetch --locked
cargo run --locked --offline --example replicated_counter -- /tmp/voteboat-counter 1 7
```

Run it again with the same arguments to recover the stored state and retry the
operation without adding twice.

For separate networked processes or Rust embedding, follow the
[service quickstart](docs/COUNTER_SERVICE.md).

## Learn more

- [TCP and QUIC](docs/QUIC_TRANSPORT.md)
- [Rust embedding and node API](docs/NODE.md)
- [Roadmap and implementation status](docs/IMPLEMENTATION.md)
- [Validation evidence](validation/REPORT.md)
