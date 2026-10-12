from pathlib import Path
import hashlib,json,sys
p=Path(__file__).resolve().parent
h=lambda f:hashlib.sha256(Path(f).read_bytes()).hexdigest()
s=json.loads((p/'SOURCE.json').read_text())['sha256'];b=json.loads((p/'BASE_SOURCE.json').read_text());c=json.loads((p/'CACHE_GUARD.json').read_text())
r={'inputs':len(s),'copy_mismatches':[n for n,v in s.items() if h(p/'repo'/n)!=v],'base397_mismatches':[n for n,v in b['sha256'].items() if h(Path(b['path'])/'repo'/n)!=v],'production_delta':[n for n,v in b['sha256'].items() if n.startswith('src/') and s[n]!=v],'original_cache_artifact_mismatches':[n for n,v in c['artifacts'].items() if h(n)!=v]}
if (p/'BINARY.json').exists():
 a=json.loads((p/'BINARY.json').read_text());r['binary_mismatch']=h(a['test_artifact']['executable'])!=a['binary_sha256'];r['library_mismatches']=[n for n,v in a['library_sha256'].items() if h(n)!=v]
(p/(sys.argv[1]+'-guard.json')).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r),flush=True);sys.exit(any(v for k,v in r.items() if k!='inputs'))
