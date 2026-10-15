"""Post-both-terminal symbol queries over owned localhost server; no UI or recording."""
from pathlib import Path
import gzip,hashlib,json,os,re,signal,subprocess,time,urllib.parse,urllib.request
job=Path('/private/tmp/voteboat-native482-sxtrtW');out=job/'symbolication';assert not out.exists();out.mkdir()
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
config=json.loads((job/'CONFIG.json').read_text())
for path,h in config['files'].items():assert sha(path)==h,path
profile=json.loads(gzip.decompress((job/'profile.json.gz').read_bytes()))
lib=next(lib for lib in profile['libs']if lib['name']=='native_benchmark')
assert lib['codeId']=='EDE59E8D89283C4E9F043AADBBEF5772'and lib['path']==config['artifact']
pids={t['pid']for t in profile['threads']if t['name']=='native_benchmark'};assert len(pids)==1
addresses=set()
for t in profile['threads']:
 if t['pid']not in pids:continue
 for f in range(t['frameTable']['length']):
  resource=t['funcTable']['resource'][t['frameTable']['func'][f]]
  if resource is None or resource<0:continue
  library=t['resourceTable']['lib'][resource];address=t['frameTable']['address'][f]
  if library is not None and library>=0 and address>=0:addresses.add((library,address))
command=[config['samply'],'load','--no-open','--address','127.0.0.1','--port','48237','--symbol-dir',str(Path(config['artifact']).parent),'--symbol-dir','/private/tmp/voteboat-mac480-f7b0ef1-mccmJS/target-native-release/release/examples',str(job/'profile.json.gz')]
(out/'COMMAND.json').write_text(json.dumps({'command':command,'scope':'Existing samply headless localhost only, exact480 Mach-O UUID verified; no browser/old maps/recapture.'},indent=2)+'\n')
stdout=(out/'server.stdout').open('wb');stderr=(out/'server.stderr').open('wb');process=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True)
(out/'OWNED_PROCESS.json').write_text(json.dumps({'pid':process.pid,'pgid':process.pid})+'\n');status=1;failure=None;records=[]
try:
 start=time.monotonic()
 while True:
  assert process.poll()is None,'Headless server exited before ready'
  text=(out/'server.stdout').read_text()+ (out/'server.stderr').read_text();match=re.search(r'symbolServer=([^\s]+)',text)
  if match:break
  assert time.monotonic()-start<30,'Headless server readiness timeout';time.sleep(.1)
 base=urllib.parse.unquote(match.group(1));url=urllib.parse.urlparse(base);assert url.scheme=='http'and url.hostname=='127.0.0.1'and url.port==48237
 endpoint=base.rstrip('/')+'/symbolicate/v5';memory=[[l['debugName'],l['breakpadId']]for l in profile['libs']];ordered=sorted(addresses)
 for start in range(0,len(ordered),256):
  points=ordered[start:start+256];request={'memoryMap':memory,'stacks':[[[lib,addr]]for lib,addr in points]};prefix='batch-'+str(start//256).zfill(3)
  encoded=(json.dumps(request)+'\n').encode();(out/(prefix+'.request.json')).write_bytes(encoded)
  req=urllib.request.Request(endpoint,data=encoded,headers={'Content-Type':'application/json'})
  with urllib.request.urlopen(req,timeout=60)as response:body=response.read()
  (out/(prefix+'.response.json')).write_bytes(body);stacks=json.loads(body)['results'][0]['stacks'];assert len(stacks)==len(points)
  for (library,address),frames in zip(points,stacks):
   assert len(frames)==1 and int(frames[0]['module_offset'],16)==address
   records.append({'library':library,'address':address,**frames[0]})
 (out/'ADDRESS_SYMBOLS.json').write_text(json.dumps(records,indent=2)+'\n');status=0
except BaseException as error:failure=repr(error);raise
finally:
 if process.poll()is None:
  try:os.killpg(process.pid,signal.SIGINT)
  except ProcessLookupError:pass
 try:server_exit=process.wait(timeout=10)
 except subprocess.TimeoutExpired:
  try:os.killpg(process.pid,signal.SIGKILL)
  except ProcessLookupError:pass
  server_exit=process.wait()
 stdout.close();stderr.close()
 (out/'TERMINAL.json').write_text(json.dumps({'exit':status,'server_exit':server_exit,'failure':failure,'addresses':len(records),'server_joined':True,'profile_sha256':sha(job/'profile.json.gz'),'scope':'All localhost request/response bodies retained; owned server interrupted and waited; no UI.'},indent=2)+'\n')
print(json.dumps({'exit':status,'addresses':len(records),'server_exit':server_exit,'known':sum('function'in r for r in records),'unknown':sum('function'not in r for r in records)}))
