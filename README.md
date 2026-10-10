# VoteBoat

Composable Rust consensus with durable replication, TCP/TLS and optional QUIC.

**Under development · Linux & macOS · [RPL 1.5](LICENSE)**

## Try it

With the pinned Rust toolchain, run the local three-replica counter demo:

```sh
cargo fetch --locked
cargo run --locked --offline --example replicated_counter -- /tmp/voteboat-counter 1 7
```

Run it again to recover state and retry without adding twice.

- [Networked service quickstart](docs/COUNTER_SERVICE.md)
- [TCP and QUIC](docs/QUIC_TRANSPORT.md)
- [Rust embedding and node API](docs/NODE.md)
- [Roadmap and implementation status](docs/IMPLEMENTATION.md)
- [Validation evidence](validation/REPORT.md)
