"""Finite adaptation of the completed470 native campaign; setup only, no timing."""
from pathlib import Path
import copy,hashlib,json,shutil,subprocess
repo=Path(__file__).resolve().parents[3]
job=Path(__file__).resolve().parent/'linux'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
write=lambda p,d:Path(p).write_text(json.dumps(d,indent=2)+'\n')
old=repo/'target/p9/comparison470-native/linux'
prior=json.loads((old/'CAMPAIGN_CONFIG.json').read_text())
assert not (job/'CAMPAIGN_CONFIG.json').exists(), 'Refuse overwrite/restart'
base=copy.deepcopy(prior['candidate'])
fresh=repo/'target/p9/native472-linux'
assert json.loads((fresh/'BUILD_TERMINAL.json').read_text())['actual_exit']==0
assert json.loads((fresh/'controls/TERMINAL.json').read_text())['exit']==0
source=json.loads((fresh/'SOURCE.json').read_text())
assert source['commit']=='de5a7221e49c89771dd3cd22a0b904966b1f4789'
artifacts=json.loads((fresh/'ARTIFACTS.json').read_text())
candidate={'source_commit':source['commit'],'source_inventory':str(fresh/'SOURCE.json'),'source_inventory_sha256':sha(fresh/'SOURCE.json'),'artifact':next(a for a in artifacts if a['target']=='native_benchmark'),'root_artifact':next(a for a in artifacts if a['target']=='voteboat'),'profile_proof':{'path':str(fresh/'PROFILE_PROOF.json'),'sha256':sha(fresh/'PROFILE_PROOF.json')},'compiler':{}}
for name in base['compiler']:
 p=fresh/'logs'/name
 candidate['compiler'][name]={'path':str(p),'sha256':sha(p)}
 assert p.read_bytes()==Path(base['compiler'][name]['path']).read_bytes(),name
control=job/'control-tools';control.mkdir(exist_ok=False)
files={}
for name in prior['control_tools']['files']:
 rel='validation/'+name;raw=subprocess.check_output(['git','show','de5a7221:'+rel],cwd=repo)
 assert raw==(repo/rel).read_bytes(),rel
 shutil.copy2(repo/rel,control/name)
 files[name]={'sha256':sha(control/name),'mode':oct((control/name).stat().st_mode&0o777),'committed_path':rel}
write(control/'SOURCE.json',{'commit':source['commit'],'files':files})
shutil.copy2(old/'check-selection.mjs',job/'check-selection.mjs')
# Controller remains byte-identical; four actual variants are carried by config.
shutil.copy2(old/'campaign.py',job/'campaign.py')
resource=next(iter((old/'campaign-metadata').glob('*/resources.py')))
wal=job/'campaign-wal';wal.mkdir(exist_ok=False)
metadata=job/'campaign-metadata';metadata.mkdir(exist_ok=False)
cases=[]
arms={'A':('baseline-off',base,'off'),'B':('candidate-off',candidate,'off'),'C':('baseline-on',base,'on'),'D':('candidate-on',candidate,'on')}
for protocol in ['tcp','quic']:
 for trial,rotation in enumerate(['ABCD','BADC','CDAB'],1):
  for letter in rotation:
   variant,build,selection=arms[letter]
   name=f'g1-{protocol}-r500-w64-q16-t{trial}-{variant}'
   root=wal/name;meta=metadata/name;meta.mkdir();shutil.copy2(resource,meta/'resources.py')
   cmd=[build['artifact']['copy'],str(root),protocol,'15000','64','1','--offered','500','--maintenance','2','--ready-items','16']
   if selection=='on':cmd+=['--append-window','4']
   cases.append({'name':name,'variant':variant,'arm':letter,'protocol':protocol,'groups':1,'offered_rate':500,'requested_offers':15000,'horizon_seconds':30,'window':64,'ready_items':16,'warmup':64,'trial':trial,'append_window':selection,'wal_root':str(root),'metadata':str(meta),'command':cmd,'resource_command':['python3',str(meta/'resources.py'),*cmd],'resource_collector_sha256':sha(meta/'resources.py'),'validator_commands':[['node',str(control/'check-native-benchmark.mjs'),'--storage',str(root)],['node',str(job/'check-selection.mjs'),str(root),selection]]})
deps=[job/'check-selection.mjs',fresh/'controls/ROOT_CONTROL_REVIEW.json',fresh/'controls/TERMINAL.json',repo/'target/p9/slice472/publication/TERMINAL.json',fresh/'FUNCTIONAL_QUALIFICATION.json',repo/'validation/performance/comparison472-native/SCHEMA.md']
config={'control_tools':{'directory':str(control),'inventory_sha256':sha(control/'SOURCE.json'),'files':files},'profile':prior['profile'],'scope':'24 serial finite native diagnostics: retained470 versus fresh472, default1 and selected4; TCP/QUIC, ABCD/BADC/CDAB,15000offers500/s/window64/ready16/warm64/maintenance2s/noPause. Primary selected-path instrumented472 change; not isolated pump CPU, sustained capacity/fairness/headroom/P9 acceptance.','resources':prior['resources'],'timing_authorized':False,'root_quiet_grant_required':True,'baseline':base,'candidate':candidate,'cases':cases,'primary_comparison':['baseline-on','candidate-on'],'default_regression_control':['baseline-off','candidate-off'],'observer_caveat':'Both observer Rust sources unchanged; grouping-dependent ticket scan/correlation work can differ. Instrumented end-to-end usefulness, not observer-free algorithm cost.','setup_dependencies':[{'path':str(p),'sha256':sha(p)} for p in deps],'wal_parent':str(wal)}
assert len(cases)==24
write(job/'CAMPAIGN_CONFIG.json',config)
write(job/'PREPARED.json',{'cases':24,'config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'driver_sha256':sha(job/'campaign.py'),'schema_sha256':sha(repo/'validation/performance/comparison472-native/SCHEMA.md'),'setup_sha256':sha(Path(__file__)),'scope':'Setup complete; no READY/quiet grant or workload launch.'})
print('PREPARED24; root review/preflight/quiet grant still required')
