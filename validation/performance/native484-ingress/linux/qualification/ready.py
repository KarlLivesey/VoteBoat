"""Seal actual fresh native483 build and three functional controls, not timing."""
from pathlib import Path
import hashlib,json,subprocess
job=Path(__file__).resolve().parent
load=lambda p:json.loads(Path(p).read_text())
h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert not(job/'READY.json').exists(),'No overwrite'
subprocess.run(['/usr/bin/python3.14',str(job/'guard.py'),str(job),'source'],check=True)
assert(job/'STATUS').read_text().strip()=='0'and(job/'STAGE').read_text().strip()=='DONE'
assert load(job/'EXPORT_TERMINAL.json')['actual_exit']==load(job/'BUILD_TERMINAL.json')['actual_exit']==0
terminal=load(job/'controls/TERMINAL.json');results=load(job/'controls/RESULTS.json')
assert terminal['actual_exit']==0 and terminal['source_exact'] and terminal['cases']==len(results)==3
assert terminal['all_owned_children_waited']and not terminal['received_signals']
assert{(r['protocol'],r['selection'],r['ready_quantum'],r['groups'],r['weights'])for r in results}=={('tcp','witness',64,2,'31,1'),('quic','witness',64,2,'31,1'),('tcp','default-atomic',16,1,None)}
for result in results:
 name=result['protocol']+'-'+result['selection']
 for suffix in ['', '-check']+(['-publication-check']if result['selection']=='witness'else[]):
  receipt=load(job/'controls'/(name+suffix+'.exit.json'))
  assert receipt['exit']==0 and receipt['error']is None and not receipt['received_signals']
 s=result['summary'];assert s['applied']=='512'and s['recovered_value']=='576'and s['workers_joined']==s['retry_verified']=='true'
 assert len(result['physical'])==3 and len(result['capacity_readbacks'])==6
 if result['selection']=='default-atomic':
  assert not result['diagnostic_publication_selected']and not result['publication_readbacks']
  assert '--publication'not in result['native_command']and '--ready-items'not in result['native_command']
  assert all(r['metadata_bytes']==52 for r in result['physical'])
 else:assert result['diagnostic_publication_selected']and len(result['publication_readbacks'])==6
assert load(job/'controls/BEFORE.json')==load(job/'controls/AFTER.json')
artifacts=load(job/'ARTIFACTS.json');assert all(h(a['original'])==h(a['copy'])==a['sha256']and a['compiler_record']['fresh']is False for a in artifacts)
assert load(job/'SYMBOLS.json')['root_buildscript_fresh_record']['fresh']is False
source=load(job/'SOURCE.json')
proof_names=['PUBLICATION.json','FUNCTIONAL_QUALIFICATION.json','SOURCE.json','ARTIFACTS.json','SYMBOLS.json','PROFILE_PROOF.json','EXPORT_TERMINAL.json','BUILD_TERMINAL.json','controls/TERMINAL.json','controls/RESULTS.json','controls/BEFORE.json','controls/AFTER.json','controls/RUNTIME_TOOLS.json','control-tools/SOURCE.json']
proof_names += [str(p.relative_to(job))for p in sorted((job/'controls').glob('*.exit.json'))]
ready={'source_commit':source['commit'],'source_files':len(source['sha256']),'qualified_projection_files':source['functional_projection_inputs'],'actual_export_exit':0,'actual_build_exit':0,'actual_network_controls_exit':0,'actual_native_processes_pass':3,'actual_native_storage_validators_pass':3,'actual_instrumented_witness_publication_validators_pass':2,'default_legacy_filelogio_physical_verified':True,'cases':3,'artifact_sha256':next(a['sha256']for a in artifacts if a['target']=='native_benchmark'),'root_artifact_sha256':next(a['sha256']for a in artifacts if a['target']=='voteboat'),'baseline480_artifact_sha256':'8d3530a6e801ebb857d8dff3fc1a0fb4ef5af865c3aed89d1450271f2054b84d','proof_sha256':{n:h(job/n)for n in proof_names},'limits':'Fresh native483 functional qualification only. Default omitted publication/ready flags select uninstrumented Legacy FileLogIo q16; no default publication trace expected. Explicit Witness TCP/QUIC q64 two-group31:1 controls retain individual outcomes, recovery/retry/join/maintenance, actual diagnostics and physical selectors/acknowledged lengths. Not capacity/speedup or fullP9 acceptance. Future paired baseline is immutable480.'}
(job/'READY.json').write_text(json.dumps(ready,indent=2)+'\n');print(json.dumps(ready,indent=2))
