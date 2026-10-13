from pathlib import Path
import json,hashlib,subprocess
r=Path('/tmp/voteboat-native421-quantum-mac');h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for name,d in json.loads((r/'HANDOFF.json').read_text()).items():assert h(r/name)==d,name
c=json.loads((r/'INPUT-mac.json').read_text());source=Path(c['source']['sourceRoot']);assert len(c['source']['sha256'])==1396
for rel,d in c['source']['sha256'].items():assert h(source/rel)==d,rel
for a in c['artifacts']:
 assert h(Path(a['copy']))==a['sha256'];x=a['compiler_record'];assert x['fresh'] is False and x['profile']=={'opt_level':'3','debuginfo':0,'debug_assertions':True,'overflow_checks':True,'test':False}
 assert Path(x['target']['src_path']).resolve().is_relative_to(source.resolve()) if hasattr(Path(),'is_relative_to') else str(Path(x['target']['src_path'])).startswith(str(source)+'/')
lines=Path('/tmp/voteboat-mac421-1091610/logs/native-root-rustc-lines.txt').read_text();assert '-C lto=' not in lines and '-C codegen-units=' not in lines and '-C linker-plugin-lto' not in lines
ps=subprocess.check_output(['/bin/ps','-axo','pid=,comm='],text=True);active=[]
for line in ps.splitlines():
 pid,comm=line.strip().split(None,1);name=Path(comm).name
 if name in ['cargo','rustc'] or 'voteboat' in name:active.append(line)
assert not active,active
(r/'PREFLIGHT.json').write_text(json.dumps({'source1396':True,'reviewed_driver_hash':True,'actual_fresh_root_library_and_executable_native_debug0':True,'default_lto_codegen_actual_invocations_guard':True,'hostquiet':True},indent=2)+'\n')
print('source1396/library/exe/nativeprofile/defaultinvocations/driver/quiet guards pass')
