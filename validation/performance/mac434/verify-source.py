import hashlib,json,stat
from pathlib import Path
root=Path(__file__).resolve().parent
required_commit=(root/'REQUIRED_COMMIT').read_text().strip()
assert len(required_commit)==40 and all(c in '0123456789abcdef' for c in required_commit),'unpublished candidate refused'
source=json.loads((root/'SOURCE.json').read_text())
assert source['commit']==required_commit
actual={p.relative_to(root/'source').as_posix() for p in (root/'source').rglob('*') if p.is_file()}
assert actual==set(source['sha256']),(actual-set(source['sha256']),set(source['sha256'])-actual)
for relative,digest in source['sha256'].items():
 path=root/'source'/relative
 assert hashlib.sha256(path.read_bytes()).hexdigest()==digest,relative
 assert oct(stat.S_IMODE(path.stat().st_mode))==source['modes'][relative],relative
proof={'commit':source['commit'],'inputs':len(actual),'exact_sha256_and_modes':True,'build_rs_present':True,'archive_sha256':source['archive_sha256'],'source_inventory_scope':source['inventory_scope']}
(root/'SOURCE_TRANSFER.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof,sort_keys=True))
