#!/usr/bin/env bash
# Functional controls and guarded configuration only; never starts paired timing.
set -euo pipefail
job=target/p9/matched468-linux
pair=target/p9/comparison468
trap 'rc=$?; printf "%s\n" "$rc" > "$job/SETUP_STATUS"' EXIT
printf 'actual_cli_controls\n' > "$job/SETUP_STAGE"
bash "$job/cli-run.sh"
printf 'bind_actual_cli_and_committed_control_docs\n' > "$job/SETUP_STAGE"
python3 - <<'PY'
from pathlib import Path
import hashlib,json,subprocess
j=Path('target/p9/matched468-linux');p=Path('target/p9/comparison468');h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
commit=(j/'REQUIRED_COMMIT.txt').read_text().strip();assert len(commit)==40 and (p/'REQUIRED_COMMIT.txt').read_text().strip()==commit
assert (j/'CLI_STATUS').read_text().strip()=='0' and (j/'CLI_STAGE').read_text().strip()=='DONE'
rows=json.loads((j/'CLI.json').read_text());assert {v['case'] for v in rows}=={'off','on','openraft'} and all(v['actual_exit']==0 for v in rows)
proofs=[{'path':str(f.resolve()),'sha256':h(f)} for f in [j/'CLI.json',j/'CLI_STATUS',j/'CLI_TERMINAL.json',j/'phase-diagnostic-check.py',*[j/'logs'/f'{name}.{suffix}' for name in ['off','on','openraft'] for suffix in ['json','status','command','stderr']]]]
checker={'commit':commit,'actual_exit':0,'actual_cli_positive':rows,'proofs':proofs,'phase_checker_reuse':'unchanged comparison456 checker; actual468 OFF/ON/OR outputs, per-replica checkpoint correlations; no466 allocation-observer fields'}
(p/'CURRENT_CLI_CHECKER.json').write_text(json.dumps(checker,indent=2)+'\n')
docs={}
for name in ['docs/COMMAND_BYTES.md','benchmarks/matched/COMPARISON.md','validation/performance/slice468/SCHEMA.md']:
 data=subprocess.check_output(['git','show',commit+':'+name]);f=p/'control-documents'/name;f.parent.mkdir(parents=True,exist_ok=True);assert not f.exists() or f.read_bytes()==data;f.write_bytes(data)
 docs[name]={'source_commit':commit,'path':str(f.resolve()),'sha256':h(f)}
(p/'API_DOCUMENTATION.json').write_text(json.dumps(docs,indent=2)+'\n')
PY
printf 'actual_pair_guards\n' > "$job/SETUP_STAGE"
python3 "$pair/prepare-pair.py" --baseline-config "$pair/BASELINE_CONFIG.linux.json" --baseline-job target/p9/matched455-linux --candidate-job "$job" --output "$pair/CONFIG_PAIR.linux.json" > "$pair/prepare-pair.log" 2> "$pair/prepare-pair.stderr"
printf '0\n' > "$pair/prepare-pair.exit"
python3 - <<'PY'
from pathlib import Path
import json,hashlib
j=Path('target/p9/matched468-linux');p=Path('target/p9/comparison468');h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest();c=json.loads((p/'CONFIG_PAIR.linux.json').read_text())
ready={'baseline_commit':c['baseline']['source_commit'],'candidate_commit':c['candidate']['source_commit'],'guards_exit':0,'current_cli_checker_sha256':h(p/'CURRENT_CLI_CHECKER.json'),'bulk':{'requested':20000000,'actual':19988480,'cases':12},'singleton':{'requested':1048576,'actual':1048576,'cases':12},'combined_controller':'bash target/p9/comparison468/linux-run.sh','timing_authorized':False}
(p/'PAIR_SETUP_READY.json').write_text(json.dumps(ready,indent=2)+'\n')
(j/'SETUP_READY.json').write_text(json.dumps({'build_original':json.loads((j/'BUILD_TERMINAL.json').read_text())['original_handle'],'actual_exit':0,'fresh_four_artifacts':True,'CLI_three_exit0':True,'phase_scope_methods':[29,4,15],'pair_positive_guards_exit0':True,'timing_started':False},indent=2)+'\n')
print('Actual3CLI +pairguards END; combined24 controller prepared; no paired timing.')
PY
printf 'ALL_SETUP_END\n' > "$job/SETUP_STAGE"
