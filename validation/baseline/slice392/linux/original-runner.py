from pathlib import Path
import json,hashlib,subprocess,sys,os,time
p=Path(__file__).resolve().parent
choice=sys.argv[1];tests={'tcp':'cold_recovery::tcp_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores','quic':'cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores'};test=tests[choice]
checks=json.loads((p/'CHECKS.json').read_text());assert len(checks)==6 and all(r['exit']==0 for r in checks)
assert not (p/(choice+'-RUN.json')).exists(),'Original launched history preserved; unchanged repeat prohibited.'
subprocess.run([sys.executable,str(p/'guard.py'),'before-'+choice],check=True)
meta=json.loads((p/'BINARY.json').read_text());binary=Path(meta['test_artifact']['executable']);sha=lambda f:hashlib.sha256(Path(f).read_bytes()).hexdigest();before=sha(binary);assert before==meta['binary_sha256']
profile=meta['test_artifact']['profile'];assert profile['opt_level']=='3' and profile['debug_assertions'] and profile['overflow_checks']
row={'test':test,'binary':str(binary),'binary_sha256':before,'profile':profile,'source_inputs':1306,'pending_write_seconds':90,'convergence_after_original_consumed_applied_seconds':90,'upper_constructor_claim_seconds':90,'cold_upper_checkpoint_seconds':90,'write_top_inactivity_gate':False,'read_immutable_total_seconds':90,'read30_inactivity_gate':False,'election_total_inactivity_seconds':[90,30],'other_until_and_physical_seconds':15,'launch':True}
(p/(choice+'-RUN.json')).write_text(json.dumps(row,indent=2)+'\n');print(json.dumps(row),flush=True)
tmp=p/'test-tmp';tmp.mkdir(exist_ok=True);env=dict(os.environ,TMPDIR=str(tmp));start=time.monotonic()
with (p/(choice+'.log')).open('w') as out:rc=subprocess.run([str(binary),'--exact',test,'--nocapture','--test-threads=1'],cwd=p/'repo',env=env,stdout=out,stderr=subprocess.STDOUT).returncode
row.update(exit=rc,seconds=round(time.monotonic()-start,2),binary_after_sha256=sha(binary),library_after_sha256={f:sha(f) for f in meta['library_sha256']});assert row['library_after_sha256']==meta['library_sha256'];assert row['binary_after_sha256']==before
(p/(choice+'-RUN.json')).write_text(json.dumps(row,indent=2)+'\n');subprocess.run([sys.executable,str(p/'guard.py'),'after-'+choice],check=True);print(json.dumps(row),flush=True);sys.exit(rc)
