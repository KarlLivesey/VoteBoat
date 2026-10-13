cargo +stable test --locked --offline --all-features --lib --test log_store --test provider_conformance --test snapshot --test raft --test effect_owner --message-format=json -vv -- --test-threads=1 
