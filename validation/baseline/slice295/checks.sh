#!/bin/bash
set -e
repo=$1
results=$2
host=$3
cd "$repo"
if [ "$host" = mac ]; then
 export PATH="$HOME/.cargo/bin:$PATH"
 export TMPDIR=/tmp/voteboat-macos-CzK1if/test-roots
 ulimit -n 4096
 hash=(shasum -a 256)
else
 hash=(sha256sum)
fi
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-source.log"
cargo +stable fmt --all -- --check > "$results/$host-fmt.log" 2>&1
printf '%s\n' 'fmt=0' > "$results/$host-exits.txt"
for profile in all default core native; do
 case "$profile" in
  all) flags=(--all-features);;
  default) flags=();;
  core) flags=(--no-default-features);;
  native) flags=(--no-default-features --features native);;
 esac
 cargo +stable clippy --locked --offline --keep-going --all-targets "${flags[@]}" -- -D warnings > "$results/$host-$profile-clippy.log" 2>&1
 printf '%s\n' "clippy_$profile=0" >> "$results/$host-exits.txt"
done
failed=0
for profile in all default core native; do
 case "$profile" in
  all) flags=(--all-features);;
  default) flags=();;
  core) flags=(--no-default-features);;
  native) flags=(--no-default-features --features native);;
 esac
 set +e
 cargo +stable test --locked --offline --no-fail-fast "${flags[@]}" --test secure --test transport --test peers --test quic -- --nocapture > "$results/$host-$profile-contracts.log" 2>&1
 check_exit=$?
 set -e
 printf '%s\n' "contracts_$profile=$check_exit" >> "$results/$host-exits.txt"
 if [ "$check_exit" -ne 0 ]; then failed=1; fi
 set +e
 cargo +stable test --locked --offline "${flags[@]}" --test effect_owner drain_inspection -- --nocapture > "$results/$host-$profile-wrappers.log" 2>&1
 check_exit=$?
 set -e
 printf '%s\n' "wrappers_$profile=$check_exit" >> "$results/$host-exits.txt"
 if [ "$check_exit" -ne 0 ]; then failed=1; fi
done
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-contract-source.log"
set +e
cargo +stable test --locked --offline --all-features --test routed native::delegation::quic_delegated_split_recovers_every_phase_from_checkpoints -- --exact --nocapture > "$results/$host-original-history.log" 2>&1
check_exit=$?
set -e
printf '%s\n' "original_history=$check_exit" >> "$results/$host-exits.txt"
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-terminal-source.log"
# The original history is a diagnostic, not an acceptance waiver. Its result is
# retained separately; a passing contract matrix does not erase a failure.
exit "$failed"
