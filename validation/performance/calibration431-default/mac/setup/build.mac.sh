#!/usr/bin/env bash
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
set -euo pipefail
job="$(cd -- "$(dirname -- "$0")" && pwd)"
host=mac
mkdir -p "$job/$host/logs"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/$host/STATUS"; date -u +%Y-%m-%dT%H:%M:%SZ > "$job/$host/DONE"' EXIT
printf "%s\n" "$$" > "$job/$host/PID"
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS|CARGO_TARGET_*_LINKER) printf '%s\n' "$name" >> "$job/$host/logs/unset-env-names.txt"; unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET CARGO_INCREMENTAL
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR="$job/$host/target-release"
[[ ! -e "$CARGO_TARGET_DIR" ]] || { echo 'fresh target required' >&2; exit 65; }
python3 "$job/source-guard.py" "$job/CONFIG.$host.json" "$job/$host/SOURCE_BEFORE.json"
rustc +stable -Vv > "$job/$host/logs/rustc.txt"
cargo +stable -V > "$job/$host/logs/cargo.txt"
source_root=/tmp/voteboat-openraft230-mac
cd "$source_root/benchmarks/minimal"
printf '%s\n' 'cargo +stable build --manifest-path Cargo.toml --locked --offline --release --bin bench --message-format=json-render-diagnostics -vv' > "$job/$host/logs/build.command.txt"
cargo +stable build --manifest-path Cargo.toml --locked --offline --release --bin bench --message-format=json-render-diagnostics -vv > "$job/$host/logs/build.jsonl" 2> "$job/$host/logs/build.stderr"
python3 "$job/source-guard.py" "$job/CONFIG.$host.json" "$job/$host/SOURCE_AFTER.json" "$job/$host/SOURCE_BEFORE.json"
