import pathlib,json,hashlib,sys,shutil,subprocess,shlex,datetime
job=pathlib.Path(sys.argv[1]).resolve();host=sys.argv[2];root=job/host;c=json.loads((job/f'CONFIG.{host}.json').read_text());assert (root/'STATUS').read_text().strip()=='0';assert (root/'DONE').exists()
before=json.loads((root/'SOURCE_BEFORE.json').read_text());after=json.loads((root/'SOURCE_AFTER.json').read_text());assert before['passed'] and after['passed'];assert before['source_files']==after['source_files'];assert before['source_count']==1357 and before['lock_packages']==196
records=[]
for line in (root/'logs/build.jsonl').read_text().splitlines():
 try:x=json.loads(line)
 except ValueError:continue
 if x.get('reason')=='compiler-artifact' and x['target']['name'] in ['openraft','bench_minimal','bench']:
  assert x['fresh'] is False,x['target']['name'];assert x['profile']==c['expected_profile'],x['profile'];records.append(x)
assert len(records)==3 and {x['target']['name'] for x in records}=={'openraft','bench_minimal','bench'}
lines=[]
for line in (root/'logs/build.stderr').read_text().splitlines():
 if 'Running `' in line and any('--crate-name '+n+' ' in line for n in ['openraft','bench_minimal','bench']):
  assert all(s in line for s in ['-C opt-level=3','-C debuginfo=2','-C codegen-units=1','-C split-debuginfo=packed']),line
  assert '-C debug-assertions=on' not in line and '-C overflow-checks=on' not in line,line
  assert '-C lto=thin' in line if '--crate-name bench ' in line else '-C linker-plugin-lto' in line
  lines.append(line)
assert len(lines)==3
(root/'logs/root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
out=root/'artifacts';out.mkdir(exist_ok=True);arts=[]
for x in records:
 src=pathlib.Path(x['executable']) if x['target']['name']=='bench' else next(pathlib.Path(f) for f in x['filenames'] if f.endswith('.rlib'));dest=out/src.name;want=hashlib.sha256(src.read_bytes()).hexdigest();shutil.copy2(src,dest);got=hashlib.sha256(dest.read_bytes()).hexdigest();assert want==got
 arts.append({'name':x['target']['name'],'original':str(src),'copy':str(dest),'original_sha256':want,'copy_sha256':got,'mode':oct(src.stat().st_mode&0o777),'copy_mode':oct(dest.stat().st_mode&0o777),'bytes':src.stat().st_size,'compiler_artifact':x})
bench=next(x for x in arts if x['name']=='bench');c['executable']=bench['copy'];c['original_executable']=bench['original'];c['artifact_sha256']=bench['copy_sha256'];c['cases']=[]
for trial,order in [(1,['off','on']),(2,['on','off']),(3,['off','on'])]:
 for mode in order:c['cases'].append({'id':f'trial{trial}-latency-{mode}','argv':[c['executable']]+c['base_args']+(['--latency'] if mode=='on' else []),'latency':mode=='on'})
(job/f'CONFIG.{host}.json').write_text(json.dumps(c,indent=2)+'\n');(job/f'COMMANDS.{host}.txt').write_text('\n'.join(shlex.join(x['argv']) for x in c['cases'])+'\n')
(root/'ARTIFACTS.json').write_text(json.dumps({'scope':c['scope'],'host':host,'upstream_pin':c['pin'],'lock_sha256':c['lock_sha256'],'lock_packages':196,'source_count':1357,'source_hashes_modes_before_after':True,'compiler':before['compiler'],'artifacts':arts,'profile':c['expected_profile'],'actual_root_invocations':3,'flags_overrides_unset':True},indent=2)+'\n')
controls=root/'control';controls.mkdir(exist_ok=True);cmd=[c['executable'],'--client-workers','1','--server-workers','16','--clients','4096','--batch','4','--operations','16384','--members','3','--warmup-writes','1024','--latency'];(controls/'command.txt').write_text(shlex.join(cmd)+'\n');(controls/'START').write_text(datetime.datetime.now(datetime.timezone.utc).isoformat()+'\n')
r=subprocess.run(cmd,capture_output=True,text=True);(controls/'stdout').write_text(r.stdout);(controls/'stderr').write_text(r.stderr);(controls/'exit').write_text(str(r.returncode)+'\n');(controls/'END').write_text(datetime.datetime.now(datetime.timezone.utc).isoformat()+'\n');assert r.returncode==0,r.stderr
reports=[json.loads(line) for line in r.stdout.splitlines() if line.startswith('{')];assert len(reports)==1;v=reports[0]
for key,want in {'operations':16384,'requested_operations':16384,'samples':4096,'warmup_writes':1024,'clients':4096,'batch':4,'members':[0,1,2],'client_workers':1,'server_workers':16,'runtime_stats':True,'latency_origin':'request_start','raft_config':{},'write_rate':0,'append_delay_us':0,'flush_delay_us':0,'network_delay_us':0}.items():assert v[key]==want,(key,v[key],want)
for a in arts:assert hashlib.sha256(pathlib.Path(a['original']).read_bytes()).hexdigest()==a['original_sha256'];assert hashlib.sha256(pathlib.Path(a['copy']).read_bytes()).hexdigest()==a['copy_sha256']
(root/'CONTROL_VALIDATION.json').write_text(json.dumps({'passed':True,'operations':16384,'samples':4096,'warmup':1024,'default_controls':True,'artifacts_after_control_unchanged':True},indent=2)+'\n')
print(json.dumps({'host':host,'qualified':True,'source_count':1357,'fresh_false_artifacts':3,'profile':c['expected_profile'],'bench_sha256':c['artifact_sha256'],'executable':c['executable'],'control_operations':16384,'control_samples':4096,'no20Mtiming':True}))
