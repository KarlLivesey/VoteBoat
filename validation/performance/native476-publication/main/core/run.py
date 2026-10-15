from pathlib import Path
import hashlib,json,subprocess,time
job=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
files=sorted({p for folder in ['src','tests'] for p in Path(folder).rglob('*') if p.is_file()}|{Path(p)for p in ['Cargo.toml','Cargo.lock','build.rs','clippy.toml','rust-toolchain.toml']})
state=lambda:{str(p):{'sha256':sha(p),'mode':oct(p.stat().st_mode&0o777)} for p in files}
before=state();(job/'BEFORE.json').write_text(json.dumps({'scope':'All root library/integration source and root compiler/lock inputs; examples and evolving narrative plans are not compiled by these selected commands.','files':before},indent=2)+'\n')
commands=[('selected',['cargo','+stable','test','--locked','--offline','--all-features','--test','journal_barrier','--test','witness_log_store','--test','log_store','--test','log_reclaim','--test','provider_conformance','--test','snapshot','--test','raft','--message-format=json','-vv','--','--test-threads=1']),('native-library',['cargo','+stable','test','--locked','--offline','--all-features','--lib','native::log_store::','--message-format=json','-vv','--','--test-threads=1'])]
code=0;records=[]
for name,cmd in commands:
 print('START',name,flush=True);start=time.monotonic()
 with(job/(name+'.stdout')).open('wb')as o,(job/(name+'.stderr')).open('wb')as e:r=subprocess.run(cmd,stdout=o,stderr=e)
 rec={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-start};(job/(name+'.exit.json')).write_text(json.dumps(rec,indent=2)+'\n');records.append(rec);print('END',name,r.returncode,flush=True)
 if r.returncode:code=r.returncode;break
after=state();(job/'AFTER.json').write_text(json.dumps({'files':after},indent=2)+'\n');(job/'TERMINAL.json').write_text(json.dumps({'exit':code,'source_exact':before==after,'inputs':len(before),'commands':records},indent=2)+'\n');assert before==after
raise SystemExit(code)
