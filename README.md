# VoteBoat

Rust consensus with durable replication, TCP/TLS and optional QUIC.

**Under development · Linux & macOS · [RPL 1.5](LICENSE)**

Run the three-replica counter demo:

```sh
cargo run --locked --example replicated_counter -- /tmp/voteboat-counter 1 7
```

- [Networked service quickstart](docs/COUNTER_SERVICE.md)
- [Metadata authority service](docs/DIRECTORY_SERVICE.md)
- [Authenticated split and recovery commands](docs/TRANSFER_SERVICE.md)
- [TCP and QUIC](docs/QUIC_TRANSPORT.md)
- [Rust embedding and node API](docs/NODE.md)
- [Roadmap and implementation status](docs/IMPLEMENTATION.md)
- [Validation evidence](validation/REPORT.md)
- [Local formatting, lint and test commands](docs/LOCAL_CHECKS.md)

Enable local checks before every push:

```sh
git config --local core.hooksPath .githooks
```
