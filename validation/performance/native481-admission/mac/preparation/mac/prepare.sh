#!/usr/bin/env bash
set -euo pipefail
base=/private/tmp/voteboat-native481-9VAgbU
python=/Applications/Xcode-beta.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3.9
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
"$python" - "$base" <<'PY'
from pathlib import Path
import hashlib,json,sys
base=Path(sys.argv[1]);m=json.loads((base/'TRANSFER.json').read_text())
for name,r in m['files'].items():
 p=base/name;assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']and p.stat().st_size==r['bytes']and oct(p.stat().st_mode&0o777)==r['mode'],name
for name,mode in m['directories'].items():assert oct((base/name).stat().st_mode&0o777)==mode,name
(base/'TRANSFER_PROOF.json').write_text(json.dumps({'exit':0,'files':len(m['files']),'exact_sha256_lengths_modes':True,'source_commit':m['source_commit'],'schema_evidence_commit':m['schema_evidence_commit'],'scope':m['scope']},indent=2)+'\n')
print('MAC481_TRANSFER_VERIFIED7',flush=True)
PY
set +e
"$python" "$base/common/prepare.py" \
 --baseline /private/tmp/voteboat-mac476-7e90682-kN01PT \
 --candidate /private/tmp/voteboat-mac480-f7b0ef1-mccmJS \
 --host mac --job "$base/campaign" --wal-parent "$base/wal" \
 --source-commit f7b0ef12709bb7e7aad4f0279851aa035b508e9e \
 --baseline-publication-authority /private/tmp/voteboat-native477-kBXQpc/revision2/PUBLICATION_AUTHORITY.json \
 --publication-authority "$base/PUBLICATION_AUTHORITY.json" \
 --baseline-combination-controls /private/tmp/voteboat-native478-esZLG7/controls/TERMINAL.json \
 --schema "$base/SCHEMA.md" > "$base/prepare.stdout" 2> "$base/prepare.stderr"
status=$?
set -e
printf '%s\n' "$status" > "$base/prepare.exit"
cat "$base/prepare.stdout"
cat "$base/prepare.stderr" >&2
printf 'MAC481_PREPARE_EXIT=%s\n' "$status"
exit "$status"
