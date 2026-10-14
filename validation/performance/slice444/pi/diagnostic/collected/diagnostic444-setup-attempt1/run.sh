#!/usr/bin/env bash
set -euo pipefail
job=$1
attempt="$job/diagnostic444"
mkdir -p "$attempt/logs" "$attempt/originals"
stage(){ printf '%s\n' "$1" > "$attempt/STAGE"; printf 'STAGE=%s\n' "$1"; }
run(){ name=$1; shift; printf '%q ' "$@" > "$attempt/logs/$name.command"; printf '\n' >> "$attempt/logs/$name.command"; set +e; "$@" > "$attempt/logs/$name.stdout" 2> "$attempt/logs/$name.stderr"; rc=$?; set -e; printf '%s\n' "$rc" > "$attempt/logs/$name.exit"; printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc"; return "$rc"; }
restorable=0
finish(){
 rc=$?; trap - EXIT; set +e
 if [[ "$restorable" == 1 ]]; then
  stage restore_baseline
  for rel in benchmarks/matched/src/voteboat_engine.rs benchmarks/matched/src/voteboat_engine/actor.rs; do cp -p "$attempt/originals/$rel" "$job/source/$rel" || rc=91; done
  if run restored-source python3 "$job/guard.py" "$job" source; then restored=0; else restored=$?; fi; printf '%s\n' "$restored" > "$attempt/RESTORE_STATUS"; [[ "$restored" == 0 ]] || rc=92
 fi
 printf '%s\n' "$rc" > "$attempt/STATUS"; printf 'DIAGNOSTIC_END=%s\n' "$rc"; exit "$rc"
}
trap finish EXIT
printf '%s\n' "$$" > "$attempt/PID"
export RUSTUP_TOOLCHAIN=1.98.1
export PATH="$HOME/.rustup/toolchains/1.98.1-aarch64-unknown-linux-gnu/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
while IFS='=' read -r name value; do case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) unset "$name";; esac; done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export RUSTC="$HOME/.rustup/toolchains/1.98.1-aarch64-unknown-linux-gnu/bin/rustc"
export PROTOC="$job/tools/protoc" CARGO_TARGET_DIR="$job/target-checks"
cargo="$HOME/.rustup/toolchains/1.98.1-aarch64-unknown-linux-gnu/bin/cargo"
stage preserve_baseline
run source-before python3 "$job/guard.py" "$job" source
[[ $(cat "$job/STATUS") == 101 ]]
cp -p "$job/STATUS" "$attempt/ORIGINAL_STATUS"
cp -p "$job/STAGE" "$attempt/ORIGINAL_STAGE"
cp -p "$job/SOURCE.json" "$attempt/BASELINE_SOURCE.json"
for suffix in command stdout stderr exit; do cp -p "$job/logs/matched-tests.$suffix" "$attempt/logs/original-matched-tests.$suffix"; done
for rel in benchmarks/matched/src/voteboat_engine.rs benchmarks/matched/src/voteboat_engine/actor.rs; do mkdir -p "$attempt/originals/$(dirname "$rel")"; cp -p "$job/source/$rel" "$attempt/originals/$rel"; done
restorable=1
run patch-hash sha256sum "$attempt/PATCH.diff"
[[ $(sha256sum "$attempt/PATCH.diff" | cut -d' ' -f1) == e890d968054c74a795de6574061599b50b1c0dd7f203d28b9e92f98606eec356 ]]
run cargo-version "$cargo" -V
run rustc-version "$RUSTC" -Vv
run fmt-version "$cargo" fmt --version
run clippy-version "$cargo" clippy --version
stage exact_diagnostic_patch
cd "$job/source"
run patch-check git apply --check "$attempt/PATCH.diff"
run patch-apply git apply "$attempt/PATCH.diff"
run patched-inventory python3 - "$job" "$attempt" <<'META'
import sys,json,hashlib
from pathlib import Path
job,attempt=map(Path,sys.argv[1:]);base=json.loads((job/'SOURCE.json').read_text());changed=[];records={}
for rel,expected in base['sha256'].items():
 p=job/'source'/rel;digest=hashlib.sha256(p.read_bytes()).hexdigest();mode=oct(p.stat().st_mode&0o777);assert mode==base['physical_modes'][rel],rel
 if digest!=expected: changed.append(rel);records[rel]={'baseline':expected,'diagnostic':digest,'physical_mode':mode}
assert sorted(changed)==['benchmarks/matched/src/voteboat_engine.rs','benchmarks/matched/src/voteboat_engine/actor.rs'],changed
(attempt/'DIAGNOSTIC_SOURCE.json').write_text(json.dumps({'baseline_commit':base['commit'],'changed':records,'unchanged_inputs':len(base['sha256'])-2,'patch_sha256':hashlib.sha256((attempt/'PATCH.diff').read_bytes()).hexdigest()},indent=2)+'\n')
print('exact2diagnostic changes, all1432 otherinputs+allphysicalmodes preserved')
META
stage focused_snapshot_diagnostic
if run snapshot-diagnostic "$cargo" test --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --locked --offline voteboat_engine::tests::compacted_prefix_reaches_a_lagging_follower_through_actual_snapshot_install -- --exact --nocapture --test-threads=1; then result=0; else result=$?; fi
printf '%s\n' "$result" > "$attempt/TEST_STATUS"
stage TEST_TERMINAL
exit "$result"
