from pathlib import Path
import hashlib,json,shutil,subprocess
root=Path(__file__).resolve().parents[2]
q=root/'target/divide-membership/qualification327';l=q/'results';m=q/'mac-results';dest=root/'validation/baseline/slice327'
subprocess.run(['python3',str(root/'target/divide-membership/qualify327-verify.py')],check=True)
assert not dest.exists();dest.mkdir()
shared=['source.sha256','source-files.txt','qualify315-inputs.sh','qualify327-checks.sh','qualify327-audit.mjs','runner.sha256','qualify327-tls.sh','tls-runner.sha256','qualify327-audit-final.mjs','qualify327-audit-combined.mjs','audit-tool.sha256','TLS_SCOPE_CORRECTION.md','PLAN.md','guard-summary.json']
for name in shared:shutil.copyfile(l/name,dest/name)
shutil.copyfile(q/'EXECUTION_REVIEW.md',dest/'README.md')
# No user-process inventories or build artifacts are copied into the tracked bundle.
for directory,platform in [(l,'linux'),(m,'mac')]:
 out=dest/platform;out.mkdir()
 a=json.loads((directory/f'{platform}-combined-audit.json').read_text())
 for stage in a['expected']:
  name=f'{platform}-{stage}.log';assert (directory/name).is_file();shutil.copyfile(directory/name,out/name)
 for suffix in ['exits.txt','terminal.txt','environment.log','fd-adjustment.log','cache-clone.log','driver.pid','runner-guard.log','final-runner-guard.log','final-tls-runner-guard.log','audit-tool-guard.log','tls-exits.txt','tls-terminal.txt','tls-environment.log','tls-fd-adjustment.log','tls-driver.pid','tls-runner-guard.log','original-failed-audit.json','combined-audit.json']:
  name=f'{platform}-{suffix}';assert (directory/name).is_file();shutil.copyfile(directory/name,out/name)
 for prefix in [platform+'-before',platform+'-terminal',platform+'-tls-before',platform+'-tls-terminal']:
  for suffix in ['-source.log','-files.txt']:shutil.copyfile(directory/(prefix+suffix),out/(prefix+suffix))
 if platform=='linux':
  for name in ['linux-retained-handle.txt','linux-tls-retained-handle.txt','linux-final-processes.log']:shutil.copyfile(directory/name,out/name)
 else:
  for name in ['mac-original302-after.log','mac-original-audit-terminal.txt','mac-protoc-guard.log']:shutil.copyfile(directory/name,out/name)
for directory,prefix in [(l,'root-after'),(l,'copied'),(m,'mac-copied')]:
 for suffix in ['-source.log','-files.txt']:shutil.copyfile(directory/(prefix+suffix),dest/(prefix+suffix))
shutil.copyfile(root/'target/divide-membership/qualify327-save.py',dest/'save-evidence.py')
# Baseline audit consumes its saved platform layout, with all shared tools hash-pinned.
(d:=dest/'verify-evidence.py').write_text('''from pathlib import Path
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
''')
with (dest/'evidence.sha256').open('w') as out:
 for p in sorted(dest.rglob('*')):
  if p.is_file() and p.name!='evidence.sha256':out.write(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(dest)}\n')
subprocess.run(['python3',str(d)],check=True)
print('Saved',dest,'files',len([p for p in dest.rglob('*') if p.is_file()]))
