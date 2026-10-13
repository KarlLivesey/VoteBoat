# Final affected Linux commands

Every command below exited0 on final413 source. Feature configurations using
target/debug executables ran sequentially; native tests had loopback sockets.

```sh
cargo +stable fmt --all -- --check
.githooks/pre-push
cargo +stable test --locked --offline --all-features --lib --test admission --test application --test effect_owner --test provider_conformance --test raft --test runtime --test worker --test log_owned --test log_store --test snapshot --test membership --test member_recovery --example framework_benchmark
cargo +stable test --locked --offline --no-default-features --lib --test admission --test application --test effect_owner --test provider_conformance --test raft --test runtime --test worker --test log_owned --test log_store --test snapshot --test membership --test member_recovery --example framework_benchmark
cargo +stable test --locked --offline --all-features --test counter_service retry_replace_leader_and_recover_native_files
```

The tracked hook enforces formatting and strict default/all/no-default/native-only
Clippy, all targets, locked/offline/keep-going with `-D warnings`. Broad scope
contains840 all-feature and595 core tests; native-only log_store contributes zero
core tests rather than an inferred core result. Counter-service controls are2/0.
