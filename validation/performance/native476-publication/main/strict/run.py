from pathlib import Path
import json,hashlib,subprocess,time
j=Path(__file__).resolve().parent;source=json.loads((j.parent/'checked/BEFORE.json').read_text())['files'];sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();state=lambda:{p:{'sha256':sha(Path(p)),'mode':oct(Path(p).stat().st_mode&0o777)}for p in source};before=state();assert before==source;(j/'BEFORE.json').write_text(json.dumps({'files':before},indent=2)+'\n');records=[];code=0
base=['cargo','+stable','clippy','--locked','--offline','--keep-going','--all-targets']
commands=[('default',base+['--','-D','warnings']),('core',base+['--no-default-features','--','-D','warnings']),('native',base+['--no-default-features','--features','native','--','-D','warnings']),('matched-format',['cargo','+stable','fmt','--manifest-path','benchmarks/matched/Cargo.toml','--','--check']),('matched-strict',['cargo','+stable','clippy','--manifest-path','benchmarks/matched/Cargo.toml','--locked','--offline','--keep-going','--all-targets','--','-D','warnings'])]
for name,cmd in commands:
 print('START',name,flush=True);t=time.monotonic()
 with(j/(name+'.stdout')).open('wb')as o,(j/(name+'.stderr')).open('wb')as e:r=subprocess.run(cmd,stdout=o,stderr=e)
 row={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-t};records.append(row);(j/(name+'.exit.json')).write_text(json.dumps(row,indent=2)+'\n');print('END',name,r.returncode,flush=True)
 if r.returncode:code=r.returncode;break
after=state();(j/'AFTER.json').write_text(json.dumps({'files':after},indent=2)+'\n');(j/'TERMINAL.json').write_text(json.dumps({'exit':code,'source_exact':before==after,'inputs':len(source),'allfeature_reuse':'../checked/strict-all.exit.json exact shared checked/BEFORE guard','commands':records},indent=2)+'\n');assert before==after
raise SystemExit(code)
