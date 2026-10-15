from pathlib import Path
import json,hashlib,socket,subprocess,ast,datetime
job=Path(__file__).resolve().parent;sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();config=json.loads((job/'CAMPAIGN_CONFIG.json').read_text());ready=json.loads((job/'READY.json').read_text());assert sha(job/'campaign.py')==ready['campaign_driver_sha256'] and sha(job/'CAMPAIGN_CONFIG.json')==ready['campaign_config_sha256']
module=ast.parse((job/'campaign.py').read_text());definitions=[n for n in module.body if isinstance(n,ast.FunctionDef) and n.name=='guards'];assert len(definitions)==1;context={'config':config,'sha':sha,'Path':Path,'json':json};exec(compile(ast.Module(body=definitions,type_ignores=[]),'exact-campaign-guards','exec'),context);context['guards']()
checks=[]
for kind in [socket.SOCK_STREAM,socket.SOCK_DGRAM]:
 with socket.socket(socket.AF_INET,kind) as s:s.bind(('127.0.0.1',0));checks.append({'kind':str(kind),'bound':s.getsockname(),'passed':True})
processes=subprocess.check_output(['ps','-eo','pid=,comm='],text=True);busy=[line for line in processes.splitlines() if line.split()[-1] in ['cargo','rustc','native_benchmar','voteboat-matche','samply','perf']];assert not busy,busy
proof={'time':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_tcp_udp_binds':checks,'full_original_artifact_source_modes_compiler_validator_guards':'PASS','all12case_roots_absent':all(not Path(c['wal_root']).exists() for c in config['cases']),'no_competing_build_check_benchmark_profile_processes':not busy,'source_or_artifact_mutation':False,'campaign_not_started':not(job/'campaign').exists()};assert proof['all12case_roots_absent'] and proof['campaign_not_started'];(job/'PREFLIGHT.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
