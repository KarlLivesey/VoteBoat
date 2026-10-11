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
 printf '%s\n' "transactions $profile" > "$results/$host-stage.txt"
 cargo +stable test --locked --offline "${flags[@]}" --lib transaction -- --nocapture > "$results/$host-$profile-unit-tests.log" 2>&1
 printf '%s\n' "unit_$profile=0" >> "$results/$host-exits.txt"
 cargo +stable test --locked --offline "${flags[@]}" --test transaction_application -- --nocapture > "$results/$host-$profile-application-tests.log" 2>&1
 printf '%s\n' "application_$profile=0" >> "$results/$host-exits.txt"
 cargo +stable test --locked --offline "${flags[@]}" --test effect_owner transaction_driver_conformance -- --nocapture > "$results/$host-$profile-downstream-tests.log" 2>&1
 printf '%s\n' "downstream_$profile=0" >> "$results/$host-exits.txt"
 cargo +stable test --locked --offline "${flags[@]}" --test transaction_driver -- --nocapture > "$results/$host-$profile-native-tests.log" 2>&1
 printf '%s\n' "native_$profile=0" >> "$results/$host-exits.txt"
done
printf '%s\n' 'embedding all' > "$results/$host-stage.txt"
cargo +stable test --locked --offline --all-features --example embedded_transfer -- --nocapture > "$results/$host-example-recovery-test.log" 2>&1
printf '%s\n' 'example_recovery=0' >> "$results/$host-exits.txt"
example=(cargo +stable run --locked --offline --all-features --example embedded_transfer --)
"${example[@]}" create "$results/$host-example-data" tests/fixtures/tls submit 1 17 forward > "$results/$host-example-create.log" 2>&1
printf '%s\n' 'example_create=0' >> "$results/$host-exits.txt"
"${example[@]}" recover "$results/$host-example-data" tests/fixtures/tls submit 1 17 forward > "$results/$host-example-retry.log" 2>&1
printf '%s\n' 'example_retry=0' >> "$results/$host-exits.txt"
"${example[@]}" recover "$results/$host-example-data" tests/fixtures/tls resume 1 > "$results/$host-example-resume.log" 2>&1
printf '%s\n' 'example_resume=0' >> "$results/$host-exits.txt"
set +e
"${example[@]}" recover "$results/$host-example-data" tests/fixtures/tls resume 99 > "$results/$host-example-unknown.log" 2>&1
unknown=$?
set -e
printf '%s\n' "example_unknown=$unknown" >> "$results/$host-exits.txt"
test "$unknown" -eq 1
for history in create retry resume unknown; do
 grep -q 'workers_joined=true' "$results/$host-example-$history.log"
done
grep -q 'Unknown.*NoRetainedStart' "$results/$host-example-unknown.log"
"${hash[@]}" -c "$results/source.sha256" > "$results/$host-terminal-source.log"
printf '%s\n' 'terminal=0' > "$results/$host-stage.txt"
