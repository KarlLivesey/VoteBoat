"""Collect terminal Mac486 qualification text only, never build or measure."""
from pathlib import Path
import hashlib,json,re,stat,tarfile,time
job=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
assert not(job/'COLLECTED.tar.gz').exists()and not(job/'READINESS.json').exists()
original=load(job/'ORIGINAL_TERMINAL.json')
assert original['actual_exit']==0 and original['original_handle'] and original['terminal_tool_chunk']
assert(job/'STATUS').read_text().strip()=='0'and(job/'STAGE').read_text().strip()=='DONE'
source=load(job/'SOURCE.json')
assert source['commit']==(job/'REQUIRED_COMMIT').read_text().strip()
assert oct(stat.S_IMODE((job/'source').stat().st_mode))==source['source_root_mode']
assert {p.relative_to(job/'source').as_posix()for p in(job/'source').rglob('*')if p.is_file()}==set(source['sha256'])
assert {p.relative_to(job/'source').as_posix()for p in(job/'source').rglob('*')if p.is_dir()}==set(source['directory_modes'])
for n,h in source['sha256'].items():
    p=job/'source'/n;assert sha(p)==h and oct(stat.S_IMODE(p.stat().st_mode))==source['modes'][n],n
for n,m in source['directory_modes'].items():assert oct(stat.S_IMODE((job/'source'/n).stat().st_mode))==m,n
assert len(source['protected_fixture_keys'])==7 and all(source['modes'][n]=='0o600'for n in source['protected_fixture_keys'])
artifacts=load(job/'ARTIFACTS.json')
for a in artifacts:assert sha(a['copy'])==sha(a['original'])==a['sha256']and a['compiler_record']['fresh']is False
symbols=load(job/'SYMBOLS.json');assert symbols['root_buildscript_fresh_record']['fresh']is False
controls=load(job/'CONTROLS.json');assert controls['exit']==0 and controls['artifact_unchanged']is True and len(controls['cases'])==4
for c in controls['cases']:
    assert c['process']['exit']==c['validator']['exit']==0 and len(c['capacity_readbacks'])==6
    assert len(c['publication_readbacks'])==(6 if c['selection']=='witness'else 0)
expected={'application_all':23,'application_core':23,'client_all':28,'client_core':28,'unit_application_all':39,'unit_application_core':39,'provider_all':42,'provider_core':27,'counter_service':2,'matched_tests':70}
other=['tool_rustc_identity','tool_cargo_identity','tool_hash_identity','tool_protoc_identity','provider_metadata','provider_metadata_controls','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_fmt','matched_strict','native-release','artifact_guard','positive_controls','source_before','source_after']
checks={}
for n in [*expected,*other]:
    assert(job/'logs'/(n+'.exit')).read_text().strip()=='0',n
    p=job/'logs'/(n+'.stdout');text=p.read_text()if p.exists()else''
    counts=[int(v)for v in re.findall(r'test result: ok\. (\d+) passed;',text)]
    if n in expected:assert counts==[expected[n]],(n,counts,expected[n])
    checks[n]={'exit':0,'actual_rust_passed_counts':counts}
    if n=='provider_metadata_controls':
        for label,want in [('tests',42),('pass',42),('fail',0),('skipped',0)]:
            values=[int(v)for v in re.findall(r'(?m)^# '+label+r' (\d+)\s*$',text)]
            assert values==[want],(label,values)
            checks[n]['actual_node_'+label]=values[0]
ready={'exit':0,'source_commit':source['commit'],'inputs':len(source['sha256']),'directories':len(source['directory_modes']),'source_exact':True,'source_inventory_sha256':sha(job/'SOURCE.json'),'original_terminal':original,'checks':checks,'artifact_sha256':{a['target']:a['sha256']for a in artifacts},'native_uuid':symbols['uuid'],'fresh_root_executable_buildscript':True,'controls':4,'controls_scope':'Fresh defaultFileLogIo and opt-inWitness ×TCP/QUIC;64ready,2groups31:1,512offers@128/window4096; exact original ledgers/recovery/retries/joins/active maintenance. Default has no publication diagnostics; actual CURRENT/MANIFEST CRC/length proof verifies format. Witness has6actual publication readbacks. No performance gain claim.','scope':'Mac fresh published486 Counter-method scoped functional qualification and separately fresh asserted native artifact only; application integration23/core23, unit39/core39, client28/core28, provider42/core27, service2, matched70 and strict checks only; unchanged broad library/Node omitted by exact source scope. Full P9 remains incomplete.'}
(job/'READINESS.json').write_text(json.dumps(ready,indent=2)+'\n')
selected=[]
excluded={'source','target-checks','target-matched-checks','target-native-release','test-tmp','artifacts'}
for p in job.rglob('*'):
    if not p.is_file():continue
    rel=p.relative_to(job)
    if rel.parts[0]in excluded or p.name in ['source.tar','COLLECTED.tar.gz','COLLECTED.json','helpers.tar']:continue
    if rel.parts[0]=='control-wal'and p.suffix not in ['.csv','.txt']:continue
    assert p.suffix not in ['.rlib','.rmeta','.o'],p
    selected.append(p)
selected.sort()
inventory={str(p.relative_to(job)):{'sha256':sha(p),'bytes':p.stat().st_size,'mode':oct(stat.S_IMODE(p.stat().st_mode))}for p in selected}
receipt={'scope':'Collection only after actual original qualification terminal; no source/buildtrees/binaries/WAL or rerun. All original command/log/error text is retained.','collected_unix':time.time(),'qualification_status':0,'qualification_stage':'DONE','original_terminal':original,'commit':source['commit'],'source_files':len(source['sha256']),'source_directories':len(source['directory_modes']),'files':inventory}
(job/'COLLECTED.json').write_text(json.dumps(receipt,indent=2)+'\n')
with tarfile.open(job/'COLLECTED.tar.gz','w:gz')as archive:
    for p in selected+[job/'COLLECTED.json']:archive.add(p,arcname=str(p.relative_to(job)),recursive=False)
assert all(sha(job/n)==v['sha256']for n,v in inventory.items())
print(json.dumps({'files':len(selected),'raw_bytes':sum(v['bytes']for v in inventory.values()),'archive_sha256':sha(job/'COLLECTED.tar.gz'),'manifest_sha256':sha(job/'COLLECTED.json')}))
