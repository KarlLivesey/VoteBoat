import subprocess,json,pathlib,datetime,os,hashlib
root=pathlib.Path.cwd();job=root/'target/p9/native485-restore/final';job.mkdir(exist_ok=True);logs=job/'logs';logs.mkdir(exist_ok=True)
commands=[('metadata-tests',['node','--test','validation/check-provider-conformance.test.mjs']),('strict',['sh','.githooks/pre-push'])]
receipts=[]
for name,argv in commands:
 (job/'STAGE').write_text(name+'\n');start=datetime.datetime.now(datetime.timezone.utc).isoformat();print('START',name,flush=True)
 with (logs/(name+'.stdout')).open('wb') as out,(logs/(name+'.stderr')).open('wb') as err:r=subprocess.run(argv,stdout=out,stderr=err)
 (logs/(name+'.exit')).write_text(str(r.returncode)+'\n');receipts.append({'name':name,'argv':argv,'start_utc':start,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_exit':r.returncode});(job/'COMMANDS.json').write_text(json.dumps(receipts,indent=2)+'\n');print('END',name,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
(job/'STAGE').write_text('DONE\n');(job/'STATUS').write_text('0\n');print('DONE',flush=True)
