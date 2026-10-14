cargo +stable test --locked --offline --all-features --lib --test log_store --test provider_conformance --test snapshot --test raft --message-format=json -vv -- --test-threads=1 
