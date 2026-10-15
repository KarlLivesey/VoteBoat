import hashlib,json,pathlib,shutil,stat,subprocess,sys
root=pathlib.Path.cwd(); job=root/'target/p9/slice465'; label=sys.argv[1]
roots=['src','tests','examples','benches','.cargo','.githooks','benchmarks/matched/src']
fixed=['Cargo.toml','Cargo.lock','clippy.toml','build.rs','rust-toolchain.toml','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','docs/component-contracts.json','docs/provider-conformance.json']
paths=set(p for p in fixed if (root/p).is_file())
for folder in roots:
 if (root/folder).exists(): paths.update(str(p.relative_to(root)) for p in (root/folder).rglob('*') if p.is_file())
rows=[]
for name in sorted(paths):
 p=root/name; data=p.read_bytes(); mode=stat.S_IMODE(p.stat().st_mode)
 rows.append({'path':name,'sha256':hashlib.sha256(data).hexdigest(),'mode':oct(mode),'size':len(data)})
 if label.endswith('BEFORE'):
  out=job/(label.lower()+'-source')/name; out.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(p,out)
result={'parent':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'files':rows}
(job/(label+'.json')).write_text(json.dumps(result,indent=2)+'\n')
print(label,len(rows))
