import csv,json,statistics,sys
from pathlib import Path
j=Path(__file__).resolve().parent
r=json.loads((j/'results/RESULTS.json').read_text());cases=r['cases'];assert len(cases)==18
assert all((j/f'results/native-{c["protocol"]}-w64-t{c["trial"]}-q{c["quantum"]}.{kind}-checker.exit').read_text().strip()=='0'for c in cases for kind in ['result','publication'])
percase=[]
for c in cases:
 label=f'native-{c["protocol"]}-w64-t{c["trial"]}-q{c["quantum"]}'
 with(j/'results'/label/'publication.csv').open()as f:rows=list(csv.DictReader(f))
 pub=[]
 for replica in [1,2,3]:
  for step in ['open','write','file_sync','rename','directory_sync']:
   before=next(x for x in rows if x['stage']=='before_measurement'and int(x['replica'])==replica and x['step']==step)
   after=next(x for x in rows if x['stage']=='after_measurement'and int(x['replica'])==replica and x['step']==step)
   pub.append({'replica':replica,'step':step,**{k:int(after[k])-int(before[k])for k in ['calls','elapsed_ns','manifest_calls','manifest_ns']}})
 s=c['summary'];percase.append({'label':label,'protocol':c['protocol'],'quantum':c['quantum'],'trial':c['trial'],'completed':c['completed'],'rate':float(s['applied_ops_s']),'p99_ms':float(s['p99_us'])/1000,'max_ms':float(s['max_us'])/1000,'recovered_value':int(s['recovered_value']),'recovery_retries':int(s['recovery_retries']),'joins':s['workers_joined'],'retry_verified':s['retry_verified'],'max_inflight':int(s['max_inflight']),'measurement_widths':c['histograms']['measurement'],'journal':c['physical'],'publication':pub})
med=lambda xs:statistics.median(xs)
groups=[]
for protocol in ['tcp','quic']:
 baseline=sorted([c for c in percase if c['protocol']==protocol and c['quantum']==16],key=lambda c:c['trial'])
 for quantum in [16,32,64]:
  cells=sorted([c for c in percase if c['protocol']==protocol and c['quantum']==quantum],key=lambda c:c['trial']);assert len(cells)==3
  leader=lambda c:next(x for x in c['journal']if x['replica']==1)
  step=lambda c,name:next(x for x in c['publication']if x['replica']==1 and x['step']==name)
  groups.append({'protocol':protocol,'quantum':quantum,'rates_originals_s':[c['rate']for c in cells],'p99_ms':[c['p99_ms']for c in cells],'rate_median_originals_s':med(c['rate']for c in cells),'p99_median_ms':med(c['p99_ms']for c in cells),'rate_trial_ratios_to_q16':[round(c['rate']/b['rate'],6)for c,b in zip(cells,baseline)],'p99_trial_ratios_to_q16':[round(c['p99_ms']/b['p99_ms'],6)for c,b in zip(cells,baseline)],'max_latency_max_ms':max(c['max_ms']for c in cells),'leader_width_histograms':[c['measurement_widths']['widths']for c in cells],'leader_sync_calls':[leader(c)['sync_calls']for c in cells],'leader_publish_calls':[leader(c)['publish_calls']for c in cells],'leader_sync_median_ms':med(leader(c)['sync_ns']/1e6 for c in cells),'leader_publish_median_ms':med(leader(c)['publish_ns']/1e6 for c in cells),'leader_file_sync_median_ms':med(step(c,'file_sync')['elapsed_ns']/1e6 for c in cells),'leader_directory_sync_median_ms':med(step(c,'directory_sync')['elapsed_ns']/1e6 for c in cells)})
out={'scope':'Native445 finite15000-original quorum-durable Counter diagnostic,64warmups/window64; same frozen Macdebug0 assertednativeartifact; no sustained20M/framework/headroom/fairness/defaultchange claim','case_count':18,'original_capture_exit':0,'result_checkers':18,'publication_checkers':18,'completed_originals_per_case':15000,'recovered_value_per_case':15064,'max_inflight':sorted(set(c['max_inflight']for c in percase)),'recovery_retries':sorted(set(c['recovery_retries']for c in percase)),'all_retry_verified':all(c['retry_verified']=='true'for c in percase),'all_joined':all(c['joins']=='true'for c in percase),'guards':{k:r[k]for k in ['sourceUnchanged','artifactsUnchanged','sameArtifactAllQuantums']},'groups':groups,'cases':percase,'limits':'Per-case active measurement rates; original raw outputs and slowtails retained. Publication/journal times are inclusive elapsed intervals, may overlap/nest; do not sum as CPU or clientcriticalpath. No offered-load/refusal/headroom/physicalRSS/debt claim. Existing LMStudio/backgrounduserapps leftunchanged.'}
(j/'READOUT.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items()if k!='cases'},separators=(',',':')))
