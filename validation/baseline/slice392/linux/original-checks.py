import json,pathlib,subprocess,os,time,sys
p=pathlib.Path(__file__).resolve().parent;env=dict(os.environ,CARGO_TARGET_DIR=str(p/'build'));rows=[];env['CARGO_BUILD_JOBS']='2'
configs=[('fmt',['fmt','--all','--','--check']),('strict-all',['clippy','--locked','--offline','--keep-going','--all-targets','--all-features','--','-D','warnings']),('strict-default',['clippy','--locked','--offline','--keep-going','--all-targets','--','-D','warnings']),('strict-none',['clippy','--locked','--offline','--keep-going','--all-targets','--no-default-features','--','-D','warnings']),('strict-native',['clippy','--locked','--offline','--keep-going','--all-targets','--no-default-features','--features','native','--','-D','warnings'])]
for name,args in configs:
 start=time.monotonic()
 with (p/(name+'.log')).open('w') as out: rc=subprocess.run(['cargo','+stable']+args,cwd=p/'repo',env=env,stdout=out,stderr=subprocess.STDOUT).returncode
 rows.append({'stage':name,'exit':rc,'seconds':round(time.monotonic()-start,2)});(p/'CHECKS.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(rows[-1]),flush=True)
 if rc:sys.exit(rc)
