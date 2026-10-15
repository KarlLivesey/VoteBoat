"""Two functional q64/skew histories on already qualified476 artifacts, not timing acceptance."""
import argparse,datetime,re,shutil,subprocess,sys,time
from pathlib import Path
from common import candidate_record,check_readbacks,check_settings,load,proof,save,sha

parser=argparse.ArgumentParser()
for name in ['candidate','host','job','source-commit','publication-authority']:parser.add_argument('--'+name,required=True)
args=parser.parse_args();candidate=Path(args.candidate).resolve()
record=candidate_record(candidate,args.host,args.source_commit,Path(args.publication_authority).resolve())
job=Path(args.job).resolve();assert not job.exists(),'Never overwrite/restart controls';job.mkdir(parents=True)
tools=job/'control-tools';tools.mkdir();source=Path(record['source']['root']);pending=[source/'validation/check-native-benchmark.mjs'];closure=set()
while pending:
    p=pending.pop().resolve()
    if p in closure:continue
    assert p.parent==source/'validation';closure.add(p)
    pending.extend(p.parent/name for name in re.findall(r"from\s+['\"](\./[^'\"]+)['\"]",p.read_text()))
for p in closure:
    assert sha(p)==record['source']['files'][p.relative_to(source).as_posix()];shutil.copy2(p,tools/p.name)
artifact=next(a for a in record['artifacts']if a['target']=='native_benchmark')
assert sha(artifact['original'])==sha(artifact['copy'])==artifact['sha256']
node=Path(shutil.which('node')).resolve();save(job/'ARTIFACT.json',artifact)
paths=[source/n for n in record['source']['files']]+[Path(__file__).resolve(),Path(__file__).with_name('common.py'),*tools.iterdir()]
state=lambda:{str(p):{'sha256':sha(p),'mode':oct(p.stat().st_mode&0o777)}for p in paths}
before=state();assert all(before[str(source/n)]=={'sha256':h,'mode':record['source']['modes'][n]}for n,h in record['source']['files'].items());save(job/'BEFORE.json',{'files':before});save(job/'RUN.json',{'source_commit':args.source_commit,'artifact_sha256':artifact['sha256'],'node':proof(node),'scope':'Two new-combination q64/groups2/skew functional controls only; no build, broad tests or performance acceptance.'})
commands=[];records=[];exit_code=1
def run(name,command):
    start=time.monotonic();print('START',name,flush=True)
    with(job/(name+'.stdout')).open('wb')as stdout,(job/(name+'.stderr')).open('wb')as stderr:child=subprocess.run(command,stdout=stdout,stderr=stderr)
    receipt={'command':command,'exit':child.returncode,'elapsed_s':time.monotonic()-start};save(job/(name+'.exit.json'),receipt);commands.append(receipt)
    print('END',name,child.returncode,flush=True);assert child.returncode==0
    return receipt
try:
    for protocol in ['tcp','quic']:
        root=job/(protocol+'-wal');command=[artifact['copy'],str(root),protocol,'512','4096','2','--offered','128','--maintenance','2','--ready-items','64','--offered-capacity','4096','--publication','witness','--group-weights','31,1']
        case={'ready_items':64,'groups':2,'weights':'31,1','publication':'witness','offers':512,'rate':128,'window':4096,'command':command}
        process=run(protocol,command);validator=run(protocol+'-check',[str(node),str(tools/'check-native-benchmark.mjs'),'--storage',str(root)])
        summary=check_settings(case,root);lines=(job/(protocol+'.stderr')).read_text().splitlines();readbacks=check_readbacks(lines,'witness')
        assert summary['applied']=='512'and summary['recovered_value']=='576'and summary['retry_verified']==summary['workers_joined']=='true'
        assert all(summary[k]=='0'for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
        assert int(summary['checkpoints'])>0 and int(summary['reclaims'])>0
        records.append({'protocol':protocol,'root':str(root),'case':case,'summary':summary,'process':process,'validator':validator,'capacity_readbacks':readbacks['capacity_readback'],'publication_readbacks':readbacks['publication_readback']});save(job/'RESULTS.json',records)
    exit_code=0
finally:
    after=state();save(job/'AFTER.json',{'files':after});same=before==after and sha(artifact['original'])==sha(artifact['copy'])==artifact['sha256']
    save(job/'TERMINAL.json',{'exit':exit_code if same else 1,'source_exact':same,'cases':len(records),'artifact_sha256':artifact['sha256'],'results_sha256':sha(job/'RESULTS.json')if(job/'RESULTS.json').exists()else None,'commands':commands,'end':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Actual functional combination controls only; all raw files/failures retained.'})
raise SystemExit(exit_code if same else 1)
