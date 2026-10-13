import copy,importlib.util,json
from pathlib import Path
j=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('guard',j/'artifact-guard.py');guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
pair=json.loads((j/'CONFIG_PAIR.linux.json').read_text());rows=[]
for name,mutation in [
 ('unpublished_commit',lambda c:c.update(source_commit='UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING')),
 ('missing_build_rs',lambda c:c['source_sha256'].pop('build.rs')),
 ('reused_root_library',lambda c:c['artifacts'][0]['compiler_record'].update(fresh=True)),
 ('assertions_disabled',lambda c:c['artifacts'][0]['compiler_record']['profile'].update(debug_assertions=False)),
 ('wrong_dwp_hash',lambda c:c['symbols'].update(sha256='0'*64)),
 ('changed_oracle_receipt',lambda c:c['release_oracle_evidence'].update(sha256='0'*64)),
]:
 changed=copy.deepcopy(pair['candidate']);mutation(changed)
 try:guard.verify(changed)
 except ValueError as e:rows.append({'control':name,'result':'correctly rejected','reason':str(e)})
 else:raise ValueError('invalid artifact guard accepted: '+name)
(j/'ARTIFACT_GUARD_CONTROLS.json').write_text(json.dumps(rows,indent=2)+'\n');print('6 invalid artifact configurations rejected; actual pair independently passes')
