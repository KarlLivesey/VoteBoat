from pathlib import Path
import json,hashlib,sys
p=Path(__file__).resolve().parent;h=lambda f:hashlib.sha256(Path(f).read_bytes()).hexdigest();s=json.loads((p/'SOURCE.json').read_text())['sha256'];b=json.loads((p/'BASE.json').read_text())
r={'inputs':len(s),'copyDelta':[n for n,v in s.items() if h(p/'repo'/n)!=v],'original398Delta':[n for n,v in s.items() if h(Path(b['source'])/'repo'/n)!=v],'original398ArtifactDelta':[n for n,v in b['artifacts'].items() if h(n)!=v]}
if (p/'BINARY.json').exists():
 a=json.loads((p/'BINARY.json').read_text());r['defaultArtifactDelta']=[n for n,v in a['artifacts'].items() if h(n)!=v]
(p/(sys.argv[1]+'-guard.json')).write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r),flush=True);sys.exit(any(v for k,v in r.items() if k!='inputs'))
