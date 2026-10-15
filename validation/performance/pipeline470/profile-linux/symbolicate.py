from pathlib import Path
import os,json,subprocess,time,re,urllib.parse,signal,hashlib
repo=Path.cwd();job=repo/'target/p9/slice470/profile16';samply=repo/'target/p9/hotspot-tools/samply-linux/samply-x86_64-unknown-linux-gnu/samply'
artifacts=json.loads((job/'RECORDING.json').read_text())['artifacts']
env={**os.environ,'XDG_CACHE_HOME':str(job/'cache'),'DEBUGINFOD_URLS':''}
for ordinal,name in enumerate(['baseline','candidate']):
 folder=job/(name+'-symbolication');folder.mkdir()
 command=[str(samply),'load','--no-open','--address','127.0.0.1','--port',str(38491+ordinal),'--symbol-dir',str(Path(artifacts[name]['path']).parent),str(job/(name+'.json.gz'))]
 (folder/'server.command.json').write_text(json.dumps({'args':command,'environment':{'XDG_CACHE_HOME':env['XDG_CACHE_HOME'],'DEBUGINFOD_URLS':''}},indent=2)+'\n')
 out=(folder/'server.stdout').open('wb');err=(folder/'server.stderr').open('wb')
 process=subprocess.Popen(command,stdout=out,stderr=err,env=env)
 (folder/'server.pid').write_text(str(process.pid)+'\n');started=time.monotonic()
 try:
  while True:
   if process.poll() is not None:raise RuntimeError('headless localhost server terminated before ready')
   text=(folder/'server.stdout').read_text();match=re.search(r'symbolServer=([^\s]+)',text)
   if match:break
   if time.monotonic()-started>30:raise RuntimeError('headless server readiness timeout; source retained')
   time.sleep(0.1)
  base=urllib.parse.unquote(match.group(1));parsed=urllib.parse.urlparse(base)
  assert parsed.scheme=='http' and parsed.hostname=='127.0.0.1' and parsed.port==38491+ordinal
  endpoint=base.rstrip('/')+'/symbolicate/v5'
  helper=['python3',str(job/'symbolize-native-json.py'),str(job/(name+'.json.gz')),endpoint,str(folder/'data')]
  (folder/'query.command.json').write_text(json.dumps(helper,indent=2)+'\n')
  with (folder/'query.stdout').open('wb') as query_out,(folder/'query.stderr').open('wb') as query_err: result=subprocess.run(helper,stdout=query_out,stderr=query_err,env=env)
  (folder/'query.exit').write_text(str(result.returncode)+'\n');print(name+' query_exit='+str(result.returncode),flush=True)
  if result.returncode:raise SystemExit(result.returncode)
 finally:
  if process.poll() is None:process.send_signal(signal.SIGINT)
  try:code=process.wait(timeout=30)
  except subprocess.TimeoutExpired:
   process.terminate();code=process.wait(timeout=10)
  out.close();err.close();(folder/'server.exit').write_text(str(code)+'\n');print(name+' server_exit='+str(code),flush=True)
 assert hashlib.sha256(Path(artifacts[name]['path']).read_bytes()).hexdigest()==artifacts[name]['sha256']
print('SYMBOLICATION_COMPLETE',flush=True)
