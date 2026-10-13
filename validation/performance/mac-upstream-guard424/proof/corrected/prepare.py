from pathlib import Path
import hashlib,json,shutil
r=Path('/tmp/voteboat-mac-upstream-guard424-corrected');a=json.loads((r/'ANCHOR.json').read_text());s=json.loads((r/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for rel,d in a['source_sha256'].items():
 p=Path(a['source_root'])/rel;assert h(p)==d,rel
 q=r/'source'/rel;q.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,q)
for p,d in a['artifact_sha256'].items():assert h(Path(p))==d,p
p=r/'source/benchmarks/minimal/src/bin/bench.rs';before=p.read_text();needle='Duration::from_secs(400)';assert before.count(needle)==1
p.write_text(before.replace(needle,'Duration::from_secs(3600)'))
for rel,d in s['sha256'].items():assert h(r/'source'/rel)==d,rel
assert (r/'source/benchmarks/minimal/Cargo.lock').read_text().count('[[package]]')==196
(r/'PREPARE.json').write_text(json.dumps({'source_files':1357,'changed_paths':['benchmarks/minimal/src/bin/bench.rs'],'independent_lock_packages':196,'original_anchor_preserved':True,'candidate_matches_qualified_linux_map':True},indent=2)+'\n')
print('original1357 anchor/exe and oneconstantpatch1357candidate/196lock guards passed')
