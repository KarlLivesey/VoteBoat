#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice452/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";result=$?;set -e;printf '%s\n' "$result" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$result";return "$result"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run compiler rustc +stable -Vv
run cargo-version cargo +stable -V
printf 'format_freeze\n' > "$job/STAGE"
run format-write cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml
python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shutil
p=Path('target/p9/slice452/main');v=json.loads((p/'SOURCE_BEFORE_CHECKS.json').read_text());v['sha256']={r:hashlib.sha256(Path(r).read_bytes()).hexdigest() for r in v['sha256']};v['scope']='Final formatted candidate, frozen before diagnostic/full matched tests and strict checks.';(p/'SOURCE.json').write_text(json.dumps(v,indent=2)+'\n');(p/'PATCH.diff').write_bytes(subprocess.check_output(['git','diff','--',*v['changed']]))
for r in v['changed']:
 q=p/'source-final'/r;q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(r,q)
PY
printf 'focused_layout\n' > "$job/STAGE"
run focused cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline receiver_only_wait_layout_diagnostic_against_literal450_write -- --exact --nocapture --test-threads=1
printf 'matched_full\n' > "$job/STAGE"
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
printf 'matched_fmt_strict\n' > "$job/STAGE"
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-strict cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'source_guard\n' > "$job/STAGE"
run source-guard python3 - <<'PY'
from pathlib import Path
import hashlib,json
v=json.loads(Path('target/p9/slice452/main/SOURCE.json').read_text())
for r,h in v['sha256'].items(): assert hashlib.sha256(Path(r).read_bytes()).hexdigest()==h and oct(Path(r).stat().st_mode&0o777)==v['physical_modes'][r],r
print(v['inputs'],'selected source hashes/modes unchanged; only two matched files differ from450; root functional source unchanged.')
PY
printf 'DONE\n' > "$job/STAGE"
