import pathlib,json,hashlib,subprocess,sys,datetime
c=json.loads(pathlib.Path(sys.argv[1]).read_text());root=pathlib.Path(c['source_root']);bad=[];actual={}
for rel,want in c['source_sha256'].items():
 f=root/rel;got=hashlib.sha256(f.read_bytes()).hexdigest() if f.is_file() else None
 actual[rel]={'sha256':got,'mode':oct(f.stat().st_mode&0o777) if f.exists() else None}
 if got!=want:bad.append({'path':rel,'expected':want,'actual':got})
r=subprocess.run(['rustc','+stable','-Vv'],capture_output=True,text=True)
if r.returncode or r.stdout.strip()!=c['compiler']:bad.append({'compiler':r.stdout,'exit':r.returncode,'stderr':r.stderr})
lock=root/'benchmarks/minimal/Cargo.lock';packages=sum(line.strip()=='[[package]]' for line in lock.read_text().splitlines())
if packages!=196:bad.append({'lock_packages':packages})
out={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'passed':not bad,'source_files':actual,'source_count':len(actual),'lock_packages':packages,'compiler':r.stdout.strip(),'errors':bad}
if len(sys.argv)>3:
 old=json.loads(pathlib.Path(sys.argv[3]).read_text())
 if old['source_files']!=actual:out['passed']=False;out['errors'].append({'source_modes_before_after':'mismatch'})
pathlib.Path(sys.argv[2]).write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'passed':out['passed'],'source_count':len(actual),'lock_packages':packages,'errors':out['errors']}));sys.exit(not out['passed'])
