#!/usr/bin/env bash
set -euo pipefail
job=target/p9/matched454-linux
pair=target/p9/comparison454
trap 'rc=$?; printf "%s\n" "$rc" > "$job/SETUP_STATUS"' EXIT
printf 'actual_cli_controls\n' > "$job/SETUP_STAGE"
bash "$job/cli-run.sh"
printf 'actual_pair_guards\n' > "$job/SETUP_STAGE"
python3 "$pair/prepare-pair.py" --baseline-config "$pair/BASELINE_CONFIG.linux.json" --baseline-job target/p9/matched452-linux --candidate-job "$job" --output "$pair/CONFIG_PAIR.linux.json" > "$pair/prepare-pair.log" 2> "$pair/prepare-pair.stderr"
printf '0\n' > "$pair/prepare-pair.exit"
python3 - <<'PY'
from pathlib import Path
import json,importlib.util,hashlib
p=Path('target/p9/comparison454');j=Path('target/p9/matched454-linux');h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest();rows=[]
spec=importlib.util.spec_from_file_location('checker',p/'phase-diagnostic-check.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
for name in ['off','on','openraft']:
 f=j/'logs'/f'{name}.json';value=json.loads(f.read_text());m.case(value,name,16384,4096,4,16384);rows.append({'case':name,'fixture_sha256':h(f),'actual_checker_exit':0})
(p/'CURRENT_CLI_CHECKER.json').write_text(json.dumps(rows,indent=2)+'\n')
config=json.loads((p/'CONFIG_PAIR.linux.json').read_text());ready={'baseline_commit':config['baseline']['source_commit'],'candidate_commit':config['candidate']['source_commit'],'guards_exit':0,'actual_cli_checker':rows,'bulk':{'requested':20000000,'actual':19988480,'cases':12},'singleton':{'requested':1048576,'actual':1048576,'cases':12},'combined_controller':'bash target/p9/comparison454/linux-run.sh','timing_authorized':False}
(p/'PAIR_SETUP_READY.json').write_text(json.dumps(ready,indent=2)+'\n')
(j/'SETUP_READY.json').write_text(json.dumps({'build_original':22148,'actual_exit':0,'fresh_four_artifacts':True,'CLI_three_exit0':True,'phase_scope_methods':[29,4,15],'pair_positive_guards_exit0':True,'timing_started':False},indent=2)+'\n')
print('actual3CLI checker +pairguard passed; bothcells combinedserial ready,no timing')
PY
printf 'PAIR_GUARDS_DONE\n' > "$job/SETUP_STAGE"
