set -euo pipefail
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
python=/Applications/Xcode-beta.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3.9
base=/private/tmp/voteboat-native484-pair-NB2I6n
"$python" - "$base" <<'PY'
from pathlib import Path
import hashlib,json,stat,sys
base=Path(sys.argv[1]);expected=json.loads((base/'TRANSFER.json').read_text())
for name,r in expected.items():
 p=base/name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']and p.stat().st_size==r['bytes']
 assert oct(stat.S_IMODE(p.stat().st_mode))==r['mode']
(base/'TRANSFER_PROOF.json').write_text(json.dumps({'exit':0,'files':len(expected),'exact_hashes_lengths_modes':True,'scope':'Small comparison helper/schema/authority text only; no source/binary/buildtrees/WAL/workload.'},indent=2)+'\n')
sys.path.insert(0,str(base/'common'))
import common
candidate=common.candidate_record(Path('/private/tmp/voteboat-mac484-efd383d-JuSJyd'),'mac',common.required_commit(),base/'PUBLICATION_AUTHORITY.json')
baseline=common.candidate_record(Path('/private/tmp/voteboat-mac480-f7b0ef1-mccmJS'),'mac',common.BASELINE_COMMIT,Path('/private/tmp/voteboat-native481-9VAgbU/PUBLICATION_AUTHORITY.json'))
proof={'exit':0,'scope':'Actual fresh483 and immutable480 Mac guard only; no campaign/timing.','source_commits':{'baseline':baseline['source']['commit'],'candidate':candidate['source']['commit']},'artifacts_sha256':{arm:{a['target']:a['sha256']for a in r['artifacts']}for arm,r in [('baseline',baseline),('candidate',candidate)]},'proof_counts':{'baseline':len(baseline['proofs']),'candidate':len(candidate['proofs'])}}
(base/'ACTUAL_MAC_GUARD.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof,indent=2),flush=True)
PY
set +e
"$python" "$base/common/prepare.py" \
 --baseline /private/tmp/voteboat-mac480-f7b0ef1-mccmJS \
 --candidate /private/tmp/voteboat-mac484-efd383d-JuSJyd \
 --host mac --job "$base/campaign" --wal-parent "$base/wal" \
 --source-commit efd383dc36b4b9edf8f66021b90115824440b468 \
 --baseline-publication-authority /private/tmp/voteboat-native481-9VAgbU/PUBLICATION_AUTHORITY.json \
 --publication-authority "$base/PUBLICATION_AUTHORITY.json" --schema "$base/SCHEMA.md" \
 > "$base/prepare.stdout" 2> "$base/prepare.stderr"
result=$?
echo "$result" > "$base/prepare.exit"
cat "$base/prepare.stdout"
cat "$base/prepare.stderr"
exit "$result"
