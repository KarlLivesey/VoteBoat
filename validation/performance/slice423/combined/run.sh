#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
job=target/p9/slice423-combined
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice421-checks
run() {
 local name=$1
 shift
 printf '%s\n' "$name" > "$job/STAGE"
 printf '%q ' "$@" > "$job/$name.command"
 printf '\n' >> "$job/$name.command"
 if "$@" > "$job/$name.stdout" 2> "$job/$name.stderr"; then
  printf '0\n' > "$job/$name.exit"
 else
  local status=$?
  printf '%s\n' "$status" > "$job/$name.exit"
  printf '%s\n' "$status" > "$job/STATUS"
  return "$status"
 fi
}
run source_before sha256sum -c "$job/SOURCE.sha256"
run library_all cargo +stable test --locked --offline --all-features --lib
run library_core cargo +stable test --locked --offline --no-default-features --lib
run model_and_contract_all cargo +stable test --locked --offline --all-features --test log_store --test provider_conformance
run contract_core cargo +stable test --locked --offline --no-default-features --test provider_conformance
run matched_tests cargo +stable test --locked --offline --manifest-path benchmarks/matched/Cargo.toml
run matched_build cargo +stable build --locked --offline --manifest-path benchmarks/matched/Cargo.toml
for engine in voteboat openraft; do
 for envelope in upstream-default common64; do
  for batch in 1 4; do
   name="cli-$engine-$envelope-b$batch"
   run "$name" "$CARGO_TARGET_DIR/debug/voteboat-matched-framework" --engine "$engine" --clients 4096 --batch "$batch" --operations 16384 --warmup 1024 --maintenance default-retention --envelopes "$envelope"
  done
 done
done
run cli-attribution "$CARGO_TARGET_DIR/debug/voteboat-matched-framework" --engine voteboat --clients 4096 --batch 4 --operations 16384 --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution true
run full_hook .githooks/pre-push
run source_after sha256sum -c "$job/SOURCE.sha256"
printf 'DONE\n' > "$job/STAGE"
printf '0\n' > "$job/STATUS"
