#!/usr/bin/env python3
"""Archive completed framework472 Linux diagnostics only; no subprocesses."""
from pathlib import Path
import gzip,hashlib,json,shutil,stat,statistics
ROOT=Path(__file__).resolve().parents[3]
JOB=ROOT/'target/p9/comparison472'
OUT=ROOT/'validation/performance/comparison472/linux'
def sha(data):return hashlib.sha256(data).hexdigest()
def load(p):return json.loads(Path(p).read_text())
def main():
 terminal=load(JOB/'TERMINAL.linux.json');assert terminal['original_handle']==66317 and terminal['actual_exit']==0 and terminal['failures']==0
 for name in ['ALL_STATUS','LINUX_STATUS','BULK_STATUS','SINGLETON_STATUS']:assert (JOB/name).read_text().strip()=='0'
 assert (JOB/'ALL_STAGE').read_text().strip()=='ALL_TIMING_END'
 assert not OUT.exists(),'No overwrite of partial/sealed archive'
 config=load(JOB/'CONFIG_PAIR.linux.json');readout={'scope':'whole470+472 framework release versus immutable455, two selected cells only; native pump is not isolated by these results','original_handle':66317,'actual_exit':0,'decision':'Retention audit owned by root; no universal win/causal/performance-acceptance claim here','identities':{},'cells':{}}
 for role in ['baseline','candidate']:
  d=config[role];assert all(a['compiler_record']['fresh'] is False for a in d['artifacts']) and len(d['artifacts'])==4
  readout['identities'][role]={'source_commit':d['source_commit'],'source_inputs':len(d['source_sha256']),'artifacts':d['artifacts'],'symbols':d['symbols'],'compiler_evidence':d['compiler_evidence']}
 for cell,folder,count in [('bulk','linux-results',19988480),('singleton','linux-singleton-results',1048576)]:
  result=load(JOB/folder/'RESULTS.json');assert len(result['cases'])==12 and not result['failures'];groups={};cellresult={'completed_originals_per_case':count,'warmup':1024,'frontiers':[count+1025]*3,'variants':{},'paired':{}}
  for variant in ['baseline','candidate','on','openraft']:
   cases=sorted([c for c in result['cases'] if c['variant']==variant],key=lambda c:c['trial']);assert len(cases)==3;rows=[]
   for c in cases:
    r=c['report'];assert (JOB/folder/(c['label']+'.exit')).read_text().strip()=='0';assert r['completed_originals']==count and r['completed_requests']==count//c['batch'] and r['applied_frontiers']==cellresult['frontiers']
    assert r['settings']['warmup']==1024 and r['client_workers']==1 and r['server_workers']==16 and r['members']==3 and r['payload_bytes']==0 and r['replication_entry_capacity']==1024 and r['settings']['maintenance']=='default_retention'
    if variant=='on':assert len(r['attribution'])==3 and all(len(a['phases'])==29 for a in r['attribution'])
    rows.append({'trial':c['trial'],'completed_originals_per_second':r['operations_per_second'],'seconds':r['seconds'],'p50_ns':r['request_p50_ns'],'p99_ns':r['request_p99_ns'],'completed_originals':r['completed_originals'],'completed_requests':r['completed_requests'],'frontiers':r['applied_frontiers'],'diagnostics':r['diagnostics']})
   groups[variant]=rows;cellresult['variants'][variant]={'trials':rows,'median_rate':statistics.median(r['completed_originals_per_second'] for r in rows),'median_p50_ms':statistics.median(r['p50_ns']/1e6 for r in rows),'median_p99_ms':statistics.median(r['p99_ns']/1e6 for r in rows)}
  for variant in ['candidate','on','openraft']:
   cellresult['paired'][variant+'_versus_baseline']={'rate_ratios':[b['completed_originals_per_second']/a['completed_originals_per_second'] for a,b in zip(groups['baseline'],groups[variant])],'p99_ratios':[b['p99_ns']/a['p99_ns'] for a,b in zip(groups['baseline'],groups[variant])]}
  readout['cells'][cell]=cellresult
 readout['limits']={'refusals':'Not separately reported by fixed completed-work framework; unavailable is not zero/headroom.','memory':'Shell timing has elapsed/user/system, no physical RSS or continuous memory/debt proof. Logical peaks remain diagnostic quantities.','CPU':'Nested attribution elapsed scopes overlap; no allocator/copy/CPU cause inferred.','scope':'One group/three voters/empty payload/in-processRPC/volatile stores; no native disk-capacity or full-seven-cell/both-host sustained acceptance.'}
 (JOB/'READOUT.json').write_text(json.dumps(readout,indent=2)+'\n')
 selected={}
 def add(p,relative):
  p=Path(p);assert p.is_file() and not p.is_symlink(),p
  relative=str(relative);assert not Path(relative).is_absolute() and '..' not in Path(relative).parts
  assert relative not in selected or sha(selected[relative].read_bytes())==sha(p.read_bytes())
  selected[relative]=p
 for p in JOB.rglob('*'):
  if p.is_file() and '__pycache__' not in p.parts:add(p,p.relative_to(JOB))
 for role in ['baseline','candidate']:
  d=config[role];job=Path(d['source_inventory_evidence']['path']).parent
  names=['SOURCE.json','ARTIFACTS.json','SYMBOLS.json','RELEASE_ORACLE.json','FUNCTIONAL_QUALIFICATION.json','PUBLICATION.json','QUALIFIED_SOURCE.json','BUILD_RUN.json','BUILD_TERMINAL.json','CLI.json','CLI_STATUS','CLI_TERMINAL.json','REQUIRED_COMMIT.txt','STATUS','STAGE','SETUP_STATUS','SETUP_STAGE','MANIFEST_LOCK_IDENTITY_SUPPLEMENT.json']
  for name in names:
   if (job/name).is_file():add(job/name,Path(role+'-artifact-proofs')/name)
  for name in ['cargo.txt','rustc.txt','cargo-path.txt','rustc-path.txt','protoc.txt','protoc-path.txt','tool-binaries.sha256','baseline-tool-preflight.log','matched-release-profile.txt','matched-root-upstream-rustc-lines.txt','off.json','off.status','off.command','off.stderr','on.json','on.status','on.command','on.stderr','openraft.json','openraft.status','openraft.command','openraft.stderr']:
   if (job/'logs'/name).is_file():add(job/'logs'/name,Path(role+'-artifact-proofs/logs')/name)
  for name,proof in d.get('api_documentation',{}).items():add(proof['path'],Path('control-documents')/role/name)
  if role=='candidate':
   for p in (job/'manifest-qualification').iterdir():
    if p.is_file():add(p,Path('candidate-artifact-proofs/manifest-qualification')/p.name)
 fresh=ROOT/'validation/performance/pipeline472/fresh-linux/SHA256SUMS';assert sha(fresh.read_bytes())=='af2b60150a81a0e9757a4ae10ac2c451058b6bf25384cfc5f6b5a318fa973cb7'
 OUT.mkdir(parents=True);packing=[];selection=[]
 for name,p in sorted(selected.items()):
  raw=p.read_bytes();assert not raw.startswith((b'\x7fELF',b'!<arch>\n')),p
  archived=name+'.gz' if len(raw)>65536 else name;target=OUT/archived;target.parent.mkdir(parents=True,exist_ok=True)
  if archived!=name:
   target.write_bytes(gzip.compress(raw,compresslevel=9,mtime=0));target.chmod(stat.S_IMODE(p.stat().st_mode));packing.append({'original_path':name,'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'container_bytes':target.stat().st_size,'container_sha256':sha(target.read_bytes())})
  else:shutil.copy2(p,target)
  selection.append({'source_path':str(p),'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'source_mode':oct(stat.S_IMODE(p.stat().st_mode))})
 for name,value in [('PACKING.json',packing),('SELECTION.json',selection),('FRESH_ARTIFACT_REFERENCE.json',{'path':str(fresh.parent),'manifest_sha256':sha(fresh.read_bytes()),'scope':'Sealed357-payload candidate fresh native/framework build+controls+first missing-manifest failure+corrected frozen70 records. Large build logs/artifact bytes not duplicated here.'})]: (OUT/name).write_text(json.dumps(value,indent=2)+'\n')
 lines=['# Linux472 framework diagnostics','', 'Original66317 ended0/ALL_TIMING_END. All24 process exits, both final validators and pre/post identity guards passed. Retention audit is separate; preserve all losses/tails without a universal win or causal claim. Candidate is whole470+472 compared with retained455, not isolated native pump behavior.','', '| Cell | Variant | Completed originals/s median | Request p99 median ms |','| --- | --- | ---: | ---: |']
 for cell,result in readout['cells'].items():
  for variant,r in result['variants'].items():lines.append(f"| {cell} | {variant} | {r['median_rate']:,.3f} | {r['median_p99_ms']:.6f} |")
  pair=result['paired']['candidate_versus_baseline'];lines.extend(['',cell+' OFF rate ratios: '+', '.join(f'{x:.6f}' for x in pair['rate_ratios'])+'; p99 ratios: '+', '.join(f'{x:.6f}' for x in pair['p99_ratios'])+'.',''])
 lines.extend(['Bulk OFF loses rates in trials1/3 and p99 in trials1/3 despite a higher median rate. Singleton median rate falls and median p99 rises; trial1 loses both rate/tail, all other actual pairs remain in READOUT/raw JSON. ON and OpenRaft are complete parallel diagnostics, not replacements for those losses.','',
 'Bulk requested20M, actual19,988,480 originals per case; singleton1,048,576, warmups1024. Every case has exact completed counts and finalfrontiers actual+1025 on all3replicas. Client1/server16 workers, onegroup/threevoters, empty payload, volatile stores/in-process actorRPC. API batch4 hard bound16384 originals/4096requestmessages; ordinary replication1024. Upstream append capacity remains a soft merge threshold. Default active maintenance/catch-up, historical VoteBoat snapshots and current-state OpenRaft snapshot differences remain disclosed. Timer ends after all client joins, before latency flatten/sort, follower verification and shutdown.','',
 'Both releases use identical within-host Linux1.98.1 toolchain/protoc hashes and assertedopt3/debug2packed/assert+overflow/thinLTO/CGU1. All4 actual root/executable/pinnedupstream/minimal fresh:false compiler records, source modes, release rootbuildscript oracle and original/copy ELF+DWP identities remain in config and compact artifact proofs. Candidate1467source includes original1453qualifiedprojection+14supplemental config/docs; two matched manifest/lock omissions were closed by actual frozen-source70 and fullSOURCE before/after equality. The sealed fresh archive preserves the first framework export101; no claim those files were originally captured. Mac data remains separate.','',
 'Preparation-attempt1 is a PRE-WORKLOAD failedsetup1: root extended the preparer to8proofs while the guard still required the old5proof exactset. No CONFIG/workload was produced by that attempt. Receipt identifies original toolchunk84b661; stderr is explicitly a TRANSCRIPTION from original tool output in conversation, not a separately captured original stderr file. Current guard requires all8actual proofs. Historical PREPARATION fields remain historical; actual READY/CLI/terminal records supersede pending labels. No workload repeated.','',
 'Attribution ON29phases/4scopes/15methods are nested/overlapping wall scopes, not additive CPU or allocator-cause proof. Logical queue/retained-entry peaks, persistence/checkpoint/snapshot counters and histograms remain raw; fixed completed-work framework has no separately reported refusal/headroom counter. Shelltime elapsed/user/system is wholeprocess context, not engine RSS. No continuous memory/debt plateau, native durability gain or fullP9/healthy seven-cell both-host sustained qualification follows.','',
 'Allraw JSON/commands/stdout-asJSON/stderr/shelltime/exits/status/checker+beforeafter identity logs, config/READY/quietgrant/initialprep/transcription and CLI/source/compiler/symbol metadata are retained. No binaries/DWPbytes/fullsource/buildtrees/WAL are copied. Files>64KiB use losslessgzip with original/containerSHA+length in PACKING. SELECTION records original paths/modes. Every nested inventory/manifest remains a payload. Outer SHA256SUMS covers exact recursive regular files except itself. Historical absolute paths are unchanged.'])
 (OUT/'README.md').write_text('\n'.join(lines)+'\n')
 payloads=sorted(p for p in OUT.rglob('*') if p.is_file());assert all(not p.is_symlink() for p in payloads)
 (OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in payloads))
 expected={r:d for d,r in [line.split('  ',1) for line in (OUT/'SHA256SUMS').read_text().splitlines()]};assert set(expected)=={str(p.relative_to(OUT)) for p in OUT.rglob('*') if p.is_file() and p!=OUT/'SHA256SUMS'}
 for name,digest in expected.items():assert sha((OUT/name).read_bytes())==digest
 packed={r['archived_path']:r for r in packing}
 for row in selection:
  data=(OUT/row['archived_path']).read_bytes();raw=gzip.decompress(data) if row['archived_path'] in packed else data
  assert len(raw)==row['original_bytes'] and sha(raw)==row['original_sha256']==sha(Path(row['source_path']).read_bytes())
 for row in packing:assert (OUT/row['archived_path']).stat().st_size==row['container_bytes'] and sha((OUT/row['archived_path']).read_bytes())==row['container_sha256']
 print(json.dumps({'archive':str(OUT),'payloads':len(payloads),'gzip':len(packing),'manifest_sha256':sha((OUT/'SHA256SUMS').read_bytes()),'exact_set_original_container_readback':'PASS','cells':{cell:{v:{'rate':r['median_rate'],'p99_ms':r['median_p99_ms']} for v,r in c['variants'].items()} for cell,c in readout['cells'].items()}}))
if __name__=='__main__':main()
