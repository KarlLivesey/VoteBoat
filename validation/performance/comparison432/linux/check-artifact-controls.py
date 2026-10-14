"""Qualify actual published431 artifact guards and invalid in-memory configurations."""
import copy,importlib.util,json
from pathlib import Path
j=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('guard',j/'artifact-guard.py');guard=importlib.util.module_from_spec(spec);spec.loader.exec_module(guard)
pair=json.loads((j/'CONFIG_PAIR.linux.json').read_text());candidate=pair['candidate'];rows=[]
actual=guard.verify_pair(j/'CONFIG_PAIR.linux.json')
for name,mutation in [
 ('unpublished_commit',lambda c:c.update(source_commit='UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING')),
 ('missing_build_rs',lambda c:c['source_sha256'].pop('build.rs')),
 ('missing_owned_append_module',lambda c:c['source_sha256'].pop('src/raft/owned_append_tests.rs')),
 ('missing_release_oracle_evidence',lambda c:c.pop('release_oracle_evidence')),
 ('reused_root_library',lambda c:next(a for a in c['artifacts'] if a['target']=='voteboat')['compiler_record'].update(fresh=True)),
 ('assertions_disabled',lambda c:next(a for a in c['artifacts'] if a['target']=='voteboat')['compiler_record']['profile'].update(debug_assertions=False)),
 ('wrong_dwp_hash',lambda c:c['symbols'].update(sha256='0'*64)),
 ('changed_oracle_receipt',lambda c:c['release_oracle_evidence'].update(sha256='0'*64)),
]:
 changed=copy.deepcopy(candidate);mutation(changed)
 try:guard.verify(changed)
 except ValueError as e:rows.append({'control':name,'result':'correctly rejected','reason':str(e)})
 else:raise ValueError('invalid artifact guard accepted: '+name)
(j/'ARTIFACT_GUARD_CONTROLS.json').write_text(json.dumps({'scope':'actual published430/431 pair artifact guards and8 refusal controls; prior unpublished controls retained in prepublication','actual_pair':actual,'negative_controls':rows},indent=2)+'\n')
print('actual published pair passes;8 invalid configurations rejected')
