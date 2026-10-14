import sys,json,pathlib,hashlib,subprocess,datetime
c=json.loads(pathlib.Path(sys.argv[1]).read_text());root=pathlib.Path(c['source_root']);errors=[]
for rel,want in c['source_sha256'].items():
 f=root/rel;got=hashlib.sha256(f.read_bytes()).hexdigest() if f.is_file() else None
 if got!=want:errors.append({'source':rel,'expected':want,'actual':got})
art={}
for name in [c['executable'],c['original_executable']]:
 f=pathlib.Path(name);got=hashlib.sha256(f.read_bytes()).hexdigest() if f.is_file() else None;art[name]=got
 if got!=c['artifact_sha256']:errors.append({'artifact':name,'expected':c['artifact_sha256'],'actual':got})
r=subprocess.run(['rustc','+stable','-Vv'],text=True,capture_output=True)
if r.returncode or r.stdout.strip()!=c['compiler']:errors.append({'compiler':r.stdout,'stderr':r.stderr,'exit':r.returncode})
for name,want in c.get('preparation_proof_sha256',{}).items():
 f=pathlib.Path(name);got=hashlib.sha256(f.read_bytes()).hexdigest() if f.is_file() else None
 if got!=want:errors.append({'proof':name,'expected':want,'actual':got})
out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'passed':not errors,'source_count':len(c['source_sha256']),'artifacts':art,'compiler':r.stdout.strip(),'errors':errors}
pathlib.Path(sys.argv[2]).write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'passed':not errors,'source_count':len(c['source_sha256']),'errors':errors}))
sys.exit(bool(errors))
