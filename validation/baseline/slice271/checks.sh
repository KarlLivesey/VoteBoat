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
 shasum -a 256 -c "$results/source.sha256" > "$results/$host-source.log"
else
 sha256sum -c "$results/source.sha256" > "$results/$host-source.log"
fi
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
 cargo +stable test --locked --offline "${flags[@]}" --test snapshot --test snapshot_worker --test member_recovery --test membership --test provider_conformance > "$results/$host-$profile-tests.log" 2>&1
 printf '%s\n' "tests_$profile=0" >> "$results/$host-exits.txt"
done
if [ "$host" = mac ]; then
 shasum -a 256 -c "$results/source.sha256" > "$results/$host-terminal-source.log"
else
 sha256sum -c "$results/source.sha256" > "$results/$host-terminal-source.log"
fi
