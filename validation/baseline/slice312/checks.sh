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
 export VOTEBOAT_REFERENCE_TOOLS=/tmp/voteboat-macos-CzK1if/reference-tools296
 hash=(shasum -a 256)
else
 hash=(sha256sum)
fi
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-source.log"
compiler="$(bash validation/reference-tools.sh 2> "$results/$host-reference-tools.log")"
if [ -n "$compiler" ]; then export PROTOC="$compiler"; fi
cargo +stable fmt --all -- --check > "$results/$host-fmt.log" 2>&1
printf '%s\n' 'fmt=0' > "$results/$host-exits.txt"
for profile in all default core native; do
 case "$profile" in
  all) flags=(--all-features);;
  default) flags=();;
  core) flags=(--no-default-features);;
  native) flags=(--no-default-features --features native);;
 esac
 printf '%s\n' "clippy $profile" > "$results/$host-stage.txt"
 cargo +stable clippy --locked --offline --keep-going --all-targets "${flags[@]}" -- -D warnings > "$results/$host-$profile-clippy.log" 2>&1
 printf '%s\n' "clippy_$profile=0" >> "$results/$host-exits.txt"
 printf '%s\n' "application $profile" > "$results/$host-stage.txt"
 cargo +stable test --locked --offline "${flags[@]}" --lib transaction -- --nocapture > "$results/$host-$profile-unit-tests.log" 2>&1
 printf '%s\n' "application_$profile=0" >> "$results/$host-exits.txt"
 cargo +stable test --locked --offline "${flags[@]}" --test transaction_application -- --nocapture > "$results/$host-$profile-downstream-tests.log" 2>&1
 printf '%s\n' "downstream_$profile=0" >> "$results/$host-exits.txt"
done
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-terminal-source.log"
printf '%s\n' 'terminal=0' > "$results/$host-stage.txt"
