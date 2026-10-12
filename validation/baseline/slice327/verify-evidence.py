from pathlib import Path
import hashlib,json,subprocess
b=Path(__file__).resolve().parent
for line in (b/'evidence.sha256').read_text().splitlines():
 digest,file=line.split('  ',1);assert hashlib.sha256((b/file).read_bytes()).hexdigest()==digest,file
files=(b/'source-files.txt').read_bytes();assert len(files.splitlines())==1073
for platform in ['linux','mac']:
 p=b/platform
 actual=subprocess.run(['node',str(b/'qualify327-audit-combined.mjs'),str(p),platform],check=True,capture_output=True,text=True)
 assert json.loads(actual.stdout)==json.loads((p/(platform+'-combined-audit.json')).read_text())
 initial=subprocess.run(['node',str(b/'qualify327-audit-final.mjs'),str(p),platform],capture_output=True,text=True)
 assert initial.returncode==1 and json.loads(initial.stdout)==json.loads((p/(platform+'-original-failed-audit.json')).read_text())
 for prefix in [platform+'-before',platform+'-terminal',platform+'-tls-before',platform+'-tls-terminal']:
  assert (p/(prefix+'-files.txt')).read_bytes()==files
  lines=(p/(prefix+'-source.log')).read_text().splitlines();assert len(lines)==1073 and all(x.endswith(': OK') for x in lines)
for prefix in ['root-after','copied','mac-copied']:
 assert (b/(prefix+'-files.txt')).read_bytes()==files
 lines=(b/(prefix+'-source.log')).read_text().splitlines();assert len(lines)==1073 and all(x.endswith(': OK') for x in lines)
for name in ['runner.sha256','tls-runner.sha256','audit-tool.sha256']:
 for line in (b/name).read_text().splitlines():
  digest,file=line.split('  ',1);assert hashlib.sha256((b/file).read_bytes()).hexdigest()==digest,file
print('Saved evidence hashes, exact combined audits, preserved initial failures and1073-path guards pass.')
