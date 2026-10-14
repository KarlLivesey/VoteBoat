#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice452/main
command=(cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline voteboat_engine::client_outcomes::receiver_only_wait_layout_diagnostic_against_literal450_write -- --exact --nocapture --test-threads=1)
printf '%q ' "${command[@]}" > "$job/layout-corrected.command"
printf '\n' >> "$job/layout-corrected.command"
set +e
"${command[@]}" > "$job/layout-corrected.log" 2> "$job/layout-corrected.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/layout-corrected.status"
cat "$job/layout-corrected.log"
exit "$rc"
