set -euo pipefail
python=/Applications/Xcode-beta.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3.9
job=/private/tmp/voteboat-native482-sxtrtW
"$python" - "$job" <<'PY'
from pathlib import Path
import hashlib,json,stat,sys
job=Path(sys.argv[1]);expected=json.loads((job/'TRANSFER.json').read_text())
for name,record in expected.items():
 p=job/name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==record['sha256']and p.stat().st_size==record['bytes']
 assert oct(stat.S_IMODE(p.stat().st_mode))==record['mode']
(job/'TRANSFER_PROOF.json').write_text(json.dumps({'exit':0,'files':len(expected),'exact_hashes_lengths_modes':True,'scope':'Only11 recipe/config/validator text files, no capture/source/binaries/buildtrees/WAL.'},indent=2)+'\n')
PY
set +e
"$python" "$job/record.py" --prepare > "$job/prepare.stdout" 2> "$job/prepare.stderr"
result=$?
echo "$result" > "$job/prepare.exit"
cat "$job/prepare.stdout"
cat "$job/prepare.stderr"
exit "$result"
