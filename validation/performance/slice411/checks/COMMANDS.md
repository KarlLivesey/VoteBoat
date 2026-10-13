# Final Linux qualification

All commands completed successfully on the same frozen411/412 production source.
Feature configurations that use target/debug executables ran sequentially.

```sh
cargo +stable fmt --all -- --check
.githooks/pre-push
cargo +stable test --locked --offline --all-features --lib --test admission --test application --test effect_owner --test provider_conformance --test raft --test runtime --test worker --test log_owned --example framework_benchmark
cargo +stable test --locked --offline --no-default-features --lib --test admission --test application --test effect_owner --test provider_conformance --test raft --test runtime --test worker --test log_owned --example framework_benchmark
cargo +stable test --locked --offline --all-features --test counter_service retry_replace_leader_and_recover_native_files
```

The tracked hook runs formatting plus strict default/all/no-default/native-only
Clippy with locked/offline/all-targets/keep-going and `-D warnings`. Broad native
and real service tests ran with local TCP/UDP socket access. The initial sandbox
binding refusal is retained separately; no source change or skipped test was used
to turn that refusal into the passing actual native run.
