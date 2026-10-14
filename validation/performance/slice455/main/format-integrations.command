cargo +stable test --locked --offline --all-features --test parser_campaign --test wire --test log_store --test striped_log_store --test snapshot --test native_member_startup -- --test-threads=1 
