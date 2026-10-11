#!/bin/bash
set -eo pipefail
repo=$1
results=$2
host=$3
build=$4
cd "$repo"
export CARGO_TARGET_DIR="$build"
if [ "$host" = mac ]; then
 export PATH="$HOME/.cargo/bin:$PATH"
 export TMPDIR=/tmp/voteboat-macos-CzK1if/test-roots
fi
cargo +stable test --locked --offline --all-features --test effect_owner peer_driver::disconnected::authenticated_reconnect_starts_a_fresh_disconnected_output_interval -- --exact --nocapture > "$results/$host-driver-recheck.log" 2>&1
awk '/test result: ok. 1 passed/ { found=1 } END {exit !found}' "$results/$host-driver-recheck.log"
printf '%s\n' 'driver_recheck=0' >> "$results/$host-exits.txt"
