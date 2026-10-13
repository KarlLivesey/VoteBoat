from pathlib import Path
import json,hashlib,shutil,subprocess
r=Path('/tmp/voteboat-comparison424-mac');old=Path('/tmp/voteboat-mac424-41ee217');c=json.loads((r/'CONFIG.PREPARE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
events=[json.loads(s) for s in (old/'logs/matched-release.jsonl').read_text().splitlines() if s.startswith('{')]
for name in ['openraft','bench_minimal']:
 xs=[x for x in events if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')==name];assert len(xs)==1,(name,len(xs));x=xs[0];assert x['fresh'] is False
 original=Path(next(p for p in x['filenames'] if p.endswith('.rlib')));copy=r/'artifacts'/original.name;copy.parent.mkdir(exist_ok=True);shutil.copy2(original,copy)
 c['artifacts'].append({'target':name,'path':str(copy),'original':str(original),'sha256':h(copy),'compiler_record':x})
rustc=Path('/Users/karllivesey/.rustup/toolchains/stable-aarch64-apple-darwin/bin/rustc')
for name,path in [('rustc',rustc),('rustc_version',old/'logs/rustc.txt'),('invocations',old/'logs/matched-root-rustc-lines.txt')]:c['compiler_evidence'][name]={'path':str(path),'sha256':h(path)}
(r/'CONFIG.json').write_text(json.dumps(c,indent=2)+'\n')
ps=subprocess.check_output(['/bin/ps','-axo','pid=,comm='],text=True);active=[]
for line in ps.splitlines():
 pid,comm=line.strip().split(None,1);base=Path(comm).name
 if base in ['cargo','rustc'] or ('voteboat' in base and base not in ['ssh']):active.append(line)
(r/'HOST_QUIET_PREFLIGHT.txt').write_text('no cargo/rustc/VoteBoat service processes\n' if not active else '\n'.join(active)+'\n');assert not active,active
print('CONFIG actualcompiler+freshsharedopenraft/benchminimal+root/bin proofs prepared; hostquietpreflight pass')
