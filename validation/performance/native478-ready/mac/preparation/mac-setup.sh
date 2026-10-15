#!/usr/bin/env bash
set -euo pipefail
base=/private/tmp/voteboat-native478-esZLG7
python=/Applications/Xcode-beta.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3.9
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
"$python" - "$base" <<'PY'
import hashlib,json,sys
from pathlib import Path
base=Path(sys.argv[1]);manifest=json.loads((base/'TRANSFER.json').read_text())
for name,item in manifest['files'].items():
 p=base/name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256'] and oct(p.stat().st_mode&0o777)==item['mode'] and p.stat().st_size==item['bytes'],name
for name,mode in manifest['directories'].items():assert oct((base/name).stat().st_mode&0o777)==mode,name
(base/'TRANSFER_PROOF.json').write_text(json.dumps({'source_commit':manifest['source_commit'],'schema_evidence_commit':manifest['schema_evidence_commit'],'files':len(manifest['files']),'exact_sha256_modes_lengths':True,'exact_directory_modes':True,'python_version':sys.version,'scope':manifest['scope']},indent=2)+'\n')
print('TRANSFER_VERIFIED8',flush=True)
PY
set +e
"$python" "$base/common/controls.py" \
  --candidate /private/tmp/voteboat-mac476-7e90682-kN01PT \
  --host mac --job "$base/controls" \
  --source-commit 7e9068296ce00dde5bf3e03bf4d852ed2833d185 \
  --publication-authority "$base/PUBLICATION_AUTHORITY.json" > "$base/controls.stdout" 2> "$base/controls.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$base/controls.exit"
cat "$base/controls.stdout"
cat "$base/controls.stderr" >&2
printf 'MAC478_CONTROLS_EXIT=%s\n' "$status"
if [ "$status" -ne 0 ]; then exit "$status"; fi
set +e
"$python" "$base/common/prepare.py" \
  --candidate /private/tmp/voteboat-mac476-7e90682-kN01PT \
  --host mac --job "$base/campaign" --wal-parent "$base/wal" \
  --source-commit 7e9068296ce00dde5bf3e03bf4d852ed2833d185 \
  --publication-authority "$base/PUBLICATION_AUTHORITY.json" \
  --schema "$base/SCHEMA.md" \
  --combination-controls "$base/controls/TERMINAL.json" > "$base/prepare.stdout" 2> "$base/prepare.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$base/prepare.exit"
cat "$base/prepare.stdout"
cat "$base/prepare.stderr" >&2
printf 'MAC478_PREPARE_EXIT=%s\n' "$status"
exit "$status"
