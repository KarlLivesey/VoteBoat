#!/usr/bin/env bash
set -euo pipefail
base=/private/tmp/voteboat-native477-kBXQpc/revision2
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
/usr/bin/python3 - "$base" <<'PY'
import hashlib,json,sys
from pathlib import Path
base=Path(sys.argv[1]);manifest=json.loads((base/'TRANSFER.json').read_text())
for name,item in manifest['files'].items():
 p=base/name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==item['sha256'] and oct(p.stat().st_mode&0o777)==item['mode'],name
for name,mode in manifest['directories'].items():assert oct((base/name).stat().st_mode&0o777)==mode,name
(base/'TRANSFER_PROOF.json').write_text(json.dumps({'source_commit':manifest['source_commit'],'files':len(manifest['files']),'exact_sha256_and_modes':True,'exact_directory_modes':True,'python_version':sys.version,'scope':'Small runner/schema/authority text transfer only; no production source, binaries, builds or WAL copied.'},indent=2)+'\n')
PY
set +e
/usr/bin/python3 "$base/common/prepare.py" \
  --candidate /private/tmp/voteboat-mac476-7e90682-kN01PT \
  --host mac --job "$base/campaign" --wal-parent "$base/wal" \
  --source-commit 7e9068296ce00dde5bf3e03bf4d852ed2833d185 \
  --publication-authority "$base/PUBLICATION_AUTHORITY.json" \
  --schema "$base/SCHEMA.md" > "$base/prepare.stdout" 2> "$base/prepare.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$base/prepare.exit"
cat "$base/prepare.stdout"
cat "$base/prepare.stderr" >&2
printf 'MAC477_PREPARE_EXIT=%s\n' "$status"
exit "$status"
