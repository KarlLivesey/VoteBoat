import ast,hashlib,importlib.util,json
from pathlib import Path
b=Path(__file__).resolve().parent;old=Path('target/p9/native484-ingress/comparison/common');sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def module(p,name):
 spec=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
newplan=module(b/'common/plan.py','newplan');oldplan=module(old/'plan.py','oldplan')
params=({'baseline':'/baseline','candidate':'/candidate'},Path('/wal'),Path('/metadata'),Path('/python'),Path('/resources.py'))
a=oldplan.cases(*params);z=newplan.cases(*params);assert a==z
assert len(z)==48 and sum(c['offers']for c in z)==391680 and sum(c['horizon_seconds']for c in z)==1344
for name in ['campaign.py','plan.py','prepare.py']:
 text=(old/name).read_text().replace('484','487').replace('483','486');assert text==(b/'common'/name).read_text()
assert sha(old/'resources.py')==sha(b/'common/resources.py')
for p in (b/'common').glob('*.py'):ast.parse(p.read_text())
readerold=Path('target/p9/native484-ingress/review/raw_readout.py').read_text().splitlines(True);readernew=(b/'readout/raw_readout.py').read_text().splitlines(True)
changed=[(x,y)for x,y in zip(readerold,readernew)if x!=y];assert len(readerold)==len(readernew)and len(changed)==1 and changed[0][0].startswith('COMMITS = ')and "'campaign487'"in changed[0][1]
assert sha(b/'SCHEMA.md')==sha('validation/performance/native487-capacity-bound/SCHEMA.md')
proof={'actual_exit':0,'argv_normalized_records_exact484':True,'all48_order_fields_exact484':True,'controller_plan_prepare_only_identity_text_changes':True,'resources_byte_exact484':True,'readout_only_change_add487_source_commit_binding':True,'reader_reference_phases_retained':['reference481','campaign484'],'cases':48,'pairs':24,'offers':391680,'scheduled_horizon_s':1344,'candidate':'dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69','baseline':'f7b0ef12709bb7e7aad4f0279851aa035b508e9e','scripts':{p.name:sha(p)for p in (b/'common').iterdir()if p.is_file()},'schema_sha256':sha(b/'SCHEMA.md'),'readout_sha256':sha(b/'readout/raw_readout.py'),'actual_config_assembled':False,'timing_started':False}
(b/'STATIC_PROOF.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof,indent=2))
