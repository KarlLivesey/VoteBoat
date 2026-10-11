#!/bin/bash
set -euo pipefail
repo=$1
results=$2
host=$3
cd "$repo"
if [ "$host" = mac ]; then
 export PATH="$HOME/.cargo/bin:$PATH"
 export TMPDIR=/tmp/voteboat-macos-CzK1if/test-roots
 export VOTEBOAT_REFERENCE_TOOLS=/tmp/voteboat-macos-CzK1if/reference-tools296
 ulimit -n 4096
 hash=(shasum -a 256)
else
 hash=(sha256sum)
fi
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-source.log"
compiler="$(bash validation/reference-tools.sh 2> "$results/$host-reference-tools.log")"
if [ -n "$compiler" ]; then export PROTOC="$compiler"; fi
if [ "$host" = mac ]; then cargo +stable fetch --locked > "$results/$host-fetch.log" 2>&1; fi
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
 cargo +stable test --locked --offline --no-fail-fast "${flags[@]}" --test raft -- --nocapture > "$results/$host-$profile-raft.log" 2>&1
 check_exit=$?
 set -e
 printf '%s\n' "raft_$profile=$check_exit" >> "$results/$host-exits.txt"
 if [ "$check_exit" -ne 0 ]; then failed=1; fi
done
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-terminal-source.log"
exit "$failed"
