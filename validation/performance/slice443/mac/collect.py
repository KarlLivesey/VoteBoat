import csv,hashlib,json,re,tarfile
from pathlib import Path
j=Path(__file__).resolve().parent;h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert(j/'STATUS').read_text().strip()=='0' and(j/'STAGE').read_text().strip()=='DONE'
steps=['root_scope_reuse','ready_items','lanes_bounds','result_checkers','root_fmt','native_debug_build','native_window64','native_result_checker','publication_checker','strict_default','strict_all','source_final']
for n in steps:assert(j/f'logs/{n}.exit').read_text().strip()=='0',n
assert re.search(r'test result: ok\. 3 passed; 0 failed;', (j/'logs/ready_items.log').read_text())
assert re.search(r'test result: ok\. 1 passed; 0 failed;', (j/'logs/lanes_bounds.log').read_text())
for n in ['normal','lanes']:assert(j/f'logs/invalid_{n}.exit').read_text().strip()=='1'and not(j/f'invalid-{n}').exists()
s=json.loads((j/'SOURCE.json').read_text());read=lambda name:list(csv.DictReader((j/'window64'/name).open()))
rows=read('samples.csv');assert len(rows)==128 and sorted(int(x['operation'])for x in rows)==list(range(65,193))
assert all(int(x['value'])==int(x['operation']) and int(x['group'])==1 and int(x['completed_ns'])>=int(x['submitted_ns'])for x in rows)
widths={phase:{}for phase in ['warmup','measurement']}
for r in read('proposal-batches.csv'):
 w=int(r['width']);n=int(r['visits']);assert int(r['original_proposals'])==w*n
 if w:widths[r['phase']][str(w)]=widths[r['phase']].get(str(w),0)+n
assert widths=={'warmup':{'64':1},'measurement':{'64':2}},widths
fields=dict(re.findall(r'(\w+)=([^\s]+)',(j/'window64/summary.txt').read_text()));assert fields['window']==fields['max_inflight']=='64' and fields['recovered_value']=='192'and fields['retry_verified']==fields['workers_joined']=='true' and fields['ready_visit_items_effective']==fields['core_proposal_entries']=='64'
events=[json.loads(line)for line in(j/'logs/native_debug_build.jsonl').read_text().splitlines()if line.startswith('{')];artifacts=[]
for name in ['voteboat','native_benchmark']:
 rs=[r for r in events if r.get('reason')=='compiler-artifact'and r.get('target',{}).get('name')==name];assert len(rs)==1
 r=rs[0];p=r['profile'];assert p['opt_level']=='0'and p['debuginfo']==2 and p['debug_assertions']and p['overflow_checks']and not p['test']
 src=Path(r['target']['src_path']);assert h(src)==s['sha256'][src.resolve().relative_to((j/'source').resolve()).as_posix()]
 file=Path(r['executable'])if r.get('executable')else Path(next(f for f in r['filenames']if f.endswith('.rlib')))
 artifacts.append({'target':name,'original':str(file),'sha256':h(file),'compiler_record':r,'source_sha256':h(src),'scope':'actualdebug qualification; fresh flags reported honestly, root reused same-job earlier test compilation'})
(j/'ARTIFACT.json').write_text(json.dumps(artifacts,indent=2)+'\n')
q={'source_commit':s['commit'],'source_inputs':s['inputs'],'source_modes_unchanged':True,'actual_ready_items_tests':3,'actual_lanes_tests':1,'actual_existing_JS_fixtures':2,'format_strict_default_all_exits':0,'noTLS_root_source_tool_receipt_reuse':'ROOT_REUSE.json; protectedfixture600 excluded, no broadTLS/currentroot functional claim','debug_profile':'actualopt0/debug2/assertions+overflow; notperformanceartifact','actual_CLI':{'originals':128,'warmup':64,'max_inflight':64,'widths':widths,'sample_operation_value_latencies_checked':True,'sample_index_range':[min(int(x['applied_index'])for x in rows),max(int(x['applied_index'])for x in rows)],'recovered_value':192,'first_last_retry_and_worker_joins_verified':True,'result_publication_independent_checkers':0},'invalid65_and_lanes33_refused_before_root':True,'original_remote_pid':int((j/'PID').read_text()),'terminal':'DONE','no_performance_build_profile_old_benchmark_or_gain_claim':True}
(j/'QUALIFICATION.json').write_text(json.dumps(q,indent=2)+'\n')
files=[f for f in j.rglob('*')if f.is_file()and f.relative_to(j).parts[0]not in{'source','target-debug-checks','target-default-strict','test-tmp','window64','__pycache__'}and f.name not in{'source.tar','qualification.tgz'}and'__pycache__'not in f.parts]
files.extend(f for f in(j/'window64').iterdir()if f.is_file()and f.suffix in{'.csv','.txt','.json'})
with tarfile.open(j/'qualification.tgz','w:gz')as t:
 for f in sorted(files):assert not f.is_symlink();t.add(f,arcname=str(f.relative_to(j)),recursive=False)
print(json.dumps(q))
