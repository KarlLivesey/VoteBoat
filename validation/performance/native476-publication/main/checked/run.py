from pathlib import Path
import subprocess,json,hashlib,time
j=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
files={p for root in ['src','tests','examples','benchmarks/matched/src','.githooks']for p in Path(root).rglob('*')if p.is_file()}
files.update(Path('validation').glob('*.mjs'))
files.update(Path(n)for n in ['Cargo.toml','Cargo.lock','build.rs','rust-toolchain.toml','clippy.toml','AGENTS.md','.gitignore','LICENSE','README.md','docs/component-contracts.json','docs/provider-conformance.json','docs/COMMAND_BYTES.md','docs/PERFORMANCE.md','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','benchmarks/matched/README.md','benchmarks/matched/SCHEMA.md','benchmarks/matched/COMPARISON.md'])
for p in Path('.cargo').rglob('*'):
 if p.is_file():files.add(p)
files=sorted(files);state=lambda:{str(p):{'sha256':sha(p),'mode':oct(p.stat().st_mode&0o777)}for p in files};before=state()
(j/'BEFORE.json').write_text(json.dumps({'base_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'scope':'Actual uncommitted476 selected compile/fixture/validator/contract/lock inputs, independent of evolving narrative and evidence.','files':before},indent=2)+'\n')
records=[];code=0
commands=[('format',['cargo','+stable','fmt','--all','--','--check']),('strict-all',['cargo','+stable','clippy','--locked','--offline','--keep-going','--all-targets','--all-features','--','-D','warnings']),('native-example-build',['cargo','+stable','build','--locked','--offline','--all-features','--example','native_benchmark','--message-format=json','-vv']),('ledger',['node','validation/check-provider-conformance.mjs']),('ledger-controls',['node','--test','validation/check-provider-conformance.test.mjs'])]
for name,cmd in commands:
 print('START',name,flush=True);start=time.monotonic()
 with(j/(name+'.stdout')).open('wb')as o,(j/(name+'.stderr')).open('wb')as e:r=subprocess.run(cmd,stdout=o,stderr=e)
 rec={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-start};(j/(name+'.exit.json')).write_text(json.dumps(rec,indent=2)+'\n');records.append(rec);print('END',name,r.returncode,flush=True)
 if r.returncode:code=r.returncode;break
after=state();(j/'AFTER.json').write_text(json.dumps({'files':after},indent=2)+'\n');(j/'TERMINAL.json').write_text(json.dumps({'exit':code,'source_exact':before==after,'inputs':len(before),'commands':records},indent=2)+'\n');assert before==after
raise SystemExit(code)
