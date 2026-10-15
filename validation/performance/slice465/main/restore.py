import pathlib,subprocess,json,hashlib,stat
r=pathlib.Path.cwd();j=r/'target/p9/slice465';base='84fc509fecbcdefd872bf23f8618e092b5a79802';scopes=['src','tests','examples','docs/component-contracts.json','docs/provider-conformance.json','validation/check-joined.mjs','validation/check-joined.test.mjs','validation/check-native-benchmark.mjs'];git=lambda *a:subprocess.check_output(['git',*a]);names=[p.decode() for p in git('diff','--name-only','-z',base,'HEAD','--',*scopes).split(b'\0') if p];rows=[]
for n in names:
 p=r/n;old=git('show','HEAD:'+n);assert p.read_bytes()==old,('Uncommittedsourcechange',n)
 exists=subprocess.run(['git','cat-file','-e',base+':'+n],capture_output=True).returncode==0
 row={'path':n,'before_sha256':hashlib.sha256(old).hexdigest(),'action':'restore' if exists else 'remove_experiment_added'}
 if exists:
  data=git('show',base+':'+n);p.write_bytes(data);row['after_sha256']=hashlib.sha256(data).hexdigest()
 else:p.unlink()
 rows.append(row)
(j/'RESTORE.json').write_text(json.dumps({'base':base,'selected_paths':rows},indent=2)+'\n');print('Restored',len(rows),'explicitexperimentpaths')
# Check complete build/source/contract scope against the actual baseline blobs.
full=['Cargo.toml','Cargo.lock','clippy.toml','build.rs','rust-toolchain.toml','.cargo','.githooks','src','tests','examples','benches','benchmarks/matched','docs/component-contracts.json','docs/provider-conformance.json','validation/check-native-benchmark.mjs']
entries=git('ls-tree','-r','-z',base,'--',*full).split(b'\0');checked=[]
for entry in entries:
 if not entry:continue
 header,name=entry.split(b'\t',1);n=name.decode();mode,kind,oid=header.decode().split();assert kind=='blob';p=r/n;data=git('cat-file','blob',oid);assert p.is_file() and p.read_bytes()==data,n;assert (p.stat().st_mode&0o111)==(int(mode,8)&0o111),n;checked.append({'path':n,'sha256':hashlib.sha256(data).hexdigest(),'physical_mode':oct(stat.S_IMODE(p.stat().st_mode))})
for n in ['validation/check-joined.mjs','validation/check-joined.test.mjs']:assert not (r/n).exists()
subprocess.run(['git','diff','--exit-code',base,'--',*full],check=True,stdout=subprocess.DEVNULL)
(j/'BASELINE_EQUIVALENCE.json').write_text(json.dumps({'base':base,'full_scope':full,'inputs':len(checked),'files':checked,'experiment_added_paths_removed':True,'scope_notes':'Source/build/contracts/comparison tree byte-identical; project plans/evidence intentionally retained. Physical private fixture modes retained. Prior exact462/455 production/network/recovery evidence reusable.'},indent=2)+'\n');print('Full baseline equality',len(checked),'inputs')
