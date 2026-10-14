import hashlib,json,subprocess
from pathlib import Path
j=Path(__file__).resolve().parent;h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert(j/'STATUS').read_text().strip()=='0' and(j/'STAGE').read_text().strip()=='DONE'
p=json.loads((j/'BUILD_PROOF.json').read_text());s=json.loads((j/'SOURCE.json').read_text());assert p['source_commit']=='590f6e787aa27e5d4752ee34109f09566c9b3fd9'
assert s['inputs']==1437 and len(s['protected_fixture_keys'])==7
for rel in s['protected_fixture_keys']:assert (j/'source'/rel).stat().st_mode&0o777==0o600
files={}
for r in p['artifacts']+p['symbols']:
 for key in ['original','copy']:
  path=Path(r[key]);assert h(path)==r['sha256'];files[str(path)]=r['sha256']
path=Path(p['samply_path']);assert h(path)==p['samply_sha256'];files[str(path)]=p['samply_sha256']
(j/'ARTIFACTS.sha256').write_text(''.join(d+'  '+f+'\n' for f,d in sorted(files.items())))
commands=[]
for cell,n,w in [('bulk',15000,32),('singleton',4096,1)]:
 assert not(j/(cell+'-workload')).exists() and not(j/(cell+'-profile.json.gz')).exists()
 commands.append({'cell':cell,'operations':n,'window':w,'warmup':64,'ready_items':16,'Core_max_items':64,'argv':[p['samply_path'],'record','--save-only','--rate','1000','--unstable-presymbolicate','--symbol-dir',str(j/'target-native-release/release/examples'),'--symbol-dir',str(j/'artifacts/symbols'),'--output',str(j/(cell+'-profile.json.gz')),'--',str(j/'artifacts/native/native_benchmark'),str(j/(cell+'-workload')),'tcp',str(n),str(w),'--ready-items','16','--journal-timings']})
(j/'COMMANDS.json').write_text(json.dumps(commands,indent=2)+'\n')
quiet=subprocess.check_output(['/bin/ps','-Ao','pid=,comm='],text=True)
active=[line for line in quiet.splitlines()if Path(line.strip().split(maxsplit=1)[1]).name in{'cargo','rustc','native_benchmark','voteboat-matched-framework','samply','perf'}]
(j/'QUIET_INVENTORY.txt').write_text('\n'.join(active)+'\n');assert not active,active
ready={'status':'BUILD_AND_CAPTURE_SETUP_READY_NO_RECORDING','original_build_ssh_handle':9297,'actual_terminal_tool':'74b54d','build_exit':0,'source_inputs':s['inputs'],'source_sha_modes':True,'private_keys0600':7,'root_example_fresh_false':True,'UUID':p['UUID'],'exe_sha256':next(r['sha256']for r in p['artifacts']if r['target']=='native_benchmark'),'profile':p['profile'],'samply':(j/'SAMPLY_VERSION.txt').read_text().strip(),'quiet_inventory_empty':True,'preflight_count':'15000/window32 instead of20000 before recording; default log16384/64warmup/64control reserve unchanged; no failed timing case','next':'Wait parent quiet authorization; two serial whole-launch cases once; no acceptance claim'}
(j/'SETUP_READY.json').write_text(json.dumps(ready,indent=2)+'\n');print(json.dumps(ready))
