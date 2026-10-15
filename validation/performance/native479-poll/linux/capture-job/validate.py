"""Frozen native validator/readbacks only after both original capture terminals."""
import subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
from common import check_readbacks,check_settings,load,save,sha
job=Path(__file__).resolve().parent
assert sys.argv[1:]==['--authorized-postterminal'],'Requires root both-host recording-end GO'
assert not(job/'VALIDATION.json').exists(),'No overwrite'
config=load(job/'CONFIG.json');assert sha(job/'CONFIG.json')==load(job/'READY.json')['config_sha256']
assert load(job/'TERMINAL.json')['exit']==0
for name,digest in config['validators'].items():assert sha(job/'control-tools'/name)==digest
for item in config['runtime_tools']:assert sha(item['path'])==item['sha256']
case=config['cases'][0];out=job/'capture'
s=check_settings(case,Path(case['wal_root']));readbacks=check_readbacks((out/'perf-native.stderr').read_text().splitlines(),'witness')
assert s['recovered_value']==str(64+int(s['applied']))and s['workers_joined']==s['retry_verified']=='true'
assert s['unknown']==s['pending']=='0'and int(s['offers'])==8192
assert int(s['applied'])+sum(int(s[k])for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])==8192
assert int(s['checkpoints'])>0 and int(s['reclaims'])>0
command=config['validator_command'];save(out/'validator.command.json',command)
run=subprocess.run(command,capture_output=True)
(out/'validator.stdout').write_bytes(run.stdout);(out/'validator.stderr').write_bytes(run.stderr);(out/'validator.exit').write_text(str(run.returncode)+'\n')
save(job/'VALIDATION.json',{'exit':run.returncode,'settings':s,'readbacks':readbacks,'scope':'Original profiled diagnostic; conservation/settings/recovery/retries/joins/active maintenance/native physical validation. Not unprofiled acceptance.'})
raise SystemExit(run.returncode)
