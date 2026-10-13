#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
job=target/p9/slice424-root
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice421-checks
run(){
 name=$1;shift
 printf '%s\n' "$name" > "$job/STAGE"
 printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command"
 if "$@" > "$job/$name.stdout" 2> "$job/$name.stderr";then printf '0\n' > "$job/$name.exit";else rc=$?;printf '%s\n' "$rc" > "$job/$name.exit";printf '%s\n' "$rc" > "$job/STATUS";return "$rc";fi
}
run source_before sha256sum -c "$job/SOURCE.sha256"
run matched_tests cargo +stable test --locked --offline --manifest-path benchmarks/matched/Cargo.toml
run matched_build cargo +stable build --locked --offline --manifest-path benchmarks/matched/Cargo.toml
for engine in voteboat openraft; do
 run "cli-$engine" "$CARGO_TARGET_DIR/debug/voteboat-matched-framework" --engine "$engine" --clients 4096 --batch 4 --operations 16384 --warmup 1024 --maintenance default-retention --envelopes upstream-default --campaign-timeout-secs 60
done
run cli-attribution "$CARGO_TARGET_DIR/debug/voteboat-matched-framework" --engine voteboat --clients 4096 --batch 4 --operations 16384 --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution true
run full_hook .githooks/pre-push
run source_after sha256sum -c "$job/SOURCE.sha256"
printf 'DONE\n' > "$job/STAGE";printf '0\n' > "$job/STATUS"
