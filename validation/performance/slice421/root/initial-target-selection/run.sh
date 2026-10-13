#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
job=target/p9/slice421-root
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
run source_before sha256sum -c validation/performance/slice421/SOURCE.sha256
run affected_all cargo +stable test --locked --offline --all-features --test startup --test native_member_startup --test runtime --test client_admission --test effect_owner --test log_store --test maintenance --test leadership_maintenance -- --test-threads=1
run service_recovery cargo +stable test --locked --offline --all-features --test counter_service service_processes_retry_replace_leader_and_recover_native_files -- --test-threads=1
run service_maintenance cargo +stable test --locked --offline --all-features --test counter_service maintenance:: -- --test-threads=1
run native_cli_build cargo +stable build --locked --offline --all-features --example native_benchmark
for assembly in startup shared; do
 for protocol in tcp quic; do
  for quantum in 32 64; do
   name="cli-$assembly-$protocol-q$quantum"
   args=("$CARGO_TARGET_DIR/debug/examples/native_benchmark" "/home/karllivesey/Documents/ChatGPT/VoteBoat/$job/stores-$name" "$protocol" 64 32)
   if [[ "$assembly" == shared ]]; then args+=(2); fi
   args+=(--ready-items "$quantum")
   run "$name" "${args[@]}"
  done
 done
done
run full_hook .githooks/pre-push
run source_after sha256sum -c validation/performance/slice421/SOURCE.sha256
printf 'DONE\n' > "$job/STAGE"
printf '0\n' > "$job/STATUS"
