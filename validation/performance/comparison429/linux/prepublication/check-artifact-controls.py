"""Read-only published428 baseline proof and unpublished429 refusal controls."""
import copy,importlib.util,json
from pathlib import Path
j=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('guard',j/'artifact-guard.py');guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
base=json.loads((j/'BASELINE_CONFIG.linux.json').read_text());rows=[]
assert guard.REQUIRED_CANDIDATE_COMMIT=='UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING'
assert base['source_commit']==guard.REQUIRED_BASELINE_COMMIT
actual=guard.verify(base)
for name,mutation in [
 ('unpublished_commit',lambda c:c.update(source_commit='UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING')),
 ('missing_build_rs',lambda c:c['source_sha256'].pop('build.rs')),
 ('missing_release_oracle_evidence',lambda c:c.pop('release_oracle_evidence')),
 ('reused_root_library',lambda c:next(a for a in c['artifacts'] if a['target']=='voteboat')['compiler_record'].update(fresh=True)),
 ('assertions_disabled',lambda c:next(a for a in c['artifacts'] if a['target']=='voteboat')['compiler_record']['profile'].update(debug_assertions=False)),
 ('wrong_dwp_hash',lambda c:c['symbols'].update(sha256='0'*64)),
 ('changed_oracle_receipt',lambda c:c['release_oracle_evidence'].update(sha256='0'*64)),
]:
 changed=copy.deepcopy(base);mutation(changed)
 try:guard.verify(changed)
 except ValueError as e:rows.append({'control':name,'result':'correctly rejected','reason':str(e)})
 else:raise ValueError('invalid artifact guard accepted: '+name)
try:guard.verify_pair(j.parent/'comparison428/CONFIG_PAIR.linux.json')
except ValueError as error:
 assert str(error)=='candidate not yet qualified/published'
 rows.append({'control':'unpublished_pair','result':'correctly rejected','reason':str(error)})
else:raise ValueError('unpublished candidate pair accepted')
(j/'ARTIFACT_GUARD_CONTROLS.json').write_text(json.dumps({'scope':'published428 actual baseline guards plus refusal controls only; candidate429 artifact validation remains pending','actual_baseline':actual,'negative_controls':rows},indent=2)+'\n')
print('published428 baseline passes;8 invalid/unpublished configurations correctly refused')
