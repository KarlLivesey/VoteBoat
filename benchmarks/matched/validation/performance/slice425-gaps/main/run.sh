#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
job="$PWD/target/p9/compact-gaps/main"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
step(){ printf '%s\n' "$1" > "$job/STAGE"; shift; "$@"; }
step tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline > "$job/test.log" 2>&1
step debug_build cargo +stable build --manifest-path benchmarks/matched/Cargo.toml --locked --offline > "$job/build.log" 2>&1
exe="$PWD/benchmarks/matched/target/debug/voteboat-matched-framework"
for mode in off on openraft; do
 args=(--clients 4096 --batch 4 --operations 16384 --warmup 1024 --maintenance default-retention --envelopes upstream-default --campaign-timeout-secs 60)
 if [[ "$mode" == openraft ]]; then args+=(--engine openraft); else args+=(--engine voteboat --attribution "$([[ "$mode" == on ]] && printf true || printf false)"); fi
 printf '%q ' "$exe" "${args[@]}" > "$job/$mode.command"
 printf '\n' >> "$job/$mode.command"
 step "cli_$mode" "$exe" "${args[@]}" > "$job/$mode.json" 2> "$job/$mode.stderr"
 printf '0\n' > "$job/$mode.exit"
done
step full_hook .githooks/pre-push > "$job/hook.log" 2>&1
printf 'DONE\n' > "$job/STAGE"
