#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice428
trap 'rc=$?; printf "%s\n" "$rc" > "$job/FINAL_MATCHED_STATUS"' EXIT
cd "$job/source"
export CARGO_TARGET_DIR="$job/matched-checks" CARGO_BUILD_JOBS=2
run() {
 local name=$1
 shift
 printf '%s\n' "$name" > "$job/CHECK_STAGE"
 printf '%q ' "$@" > "$job/$name.command"
 printf '\n' >> "$job/$name.command"
 set +e
 "$@" > "$job/$name.log" 2>&1
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/$name.status"
 if [[ $rc -ne 0 ]]; then tail -35 "$job/$name.log"; exit "$rc"; fi
}
run matched-focused-fmt-apply cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml --all
run freeze-final python3 "$job/freeze.py"
run matched-focused-actor cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline checkpoint_scopes_observe_actual_helpers_and_nested_retention -- --test-threads=1
run matched-final-fmt cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml --all -- --check
run matched-final-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-after sha256sum -c "$job/source.sha256"
printf 'DONE\n' > "$job/CHECK_STAGE"
