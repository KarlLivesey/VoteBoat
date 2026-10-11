#!/bin/bash
set -eu
repo=$1
results=$2
cd "$repo"
sha256sum -c "$results/source.sha256" > "$results/linux-source.log"
for attempt in 1 2 3; do
 set +e
 cargo +stable test --locked --offline --all-features --test routed native::delegation::quic_delegated_split_recovers_every_phase_from_checkpoints -- --exact --nocapture > "$results/linux-probe-$attempt.log" 2>&1
 check_exit=$?
 set -e
 printf '%s\n' "probe_$attempt=$check_exit" >> "$results/linux-exits.txt"
 if [ "$check_exit" -ne 0 ]; then break; fi
done
sha256sum -c "$results/source.sha256" > "$results/linux-terminal-source.log"
exit "$check_exit"
