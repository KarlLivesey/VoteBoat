from pathlib import Path
import hashlib,json,gzip,shutil
P=Path(__file__).resolve().parent
R=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat')
def read(p):return json.loads(p.read_text())
def write(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
readout={}
for cell,expected,requests,frontier in [('bulk',19988480,4997120,19989505),('singleton',1048576,1048576,1049601)]:
 d=P/'symbolication'/(cell+'-symbolicated');s=read(d/'SUMMARY.json');a=read(d/'SUBSETS.json');c=read(d/'CLIENT_CONTEXTS.json');v=read(P/'perf-quiet'/(cell+'.stdout'));n=s['denominator']
 assert (v['completed_originals'],v['completed_requests'],v['applied_frontiers'])==(expected,requests,[frontier]*3)
 assert v['settings']['warmup']==1024 and v['client_workers']==1 and v['server_workers']==16 and v['members']==3 and v['payload_bytes']==0
 assert int((P/'perf-quiet'/(cell+'.exit')).read_text())==0
 write(P/'perf-quiet'/(cell+'.counts-validation.json'),{'completed_originals':expected,'completed_requests':requests,'frontiers':[frontier]*3,'warmup':1024,'all_originals_applied':True,'actual_profiled_diagnostic_only':True})
 raw=(P/'perf-quiet'/(cell+'.stderr')).read_text();imp=(P/'perf-quiet'/(cell+'.import.stderr')).read_text()
 readout[cell]={'selected_sample_weight':n,'unit':s['weight'],'totals':s['totals'],'threads':s['threads'],'physical_depth_sample_weights':s['physical_depth_sample_weights'],'physical_depth_le2_percent':100*sum(w for k,w in s['physical_depth_sample_weights'].items() if int(k)<=2)/n,'unresolved_leaf_percent':100*s['totals']['unresolved_leaf_sample_weight']/n,'any_unresolved_percent':100*s['totals']['any_unresolved_stack_sample_weight']/n,'top12_native_self':s['self_native'][:12],'top12_inline_self':s['self_including_inlines'][:12],'top12_native_inclusive':s['inclusive_native'][:12],'top12_inline_inclusive':s['inclusive_including_inlines'][:12],'inclusive_contexts':a['subsets'],'entry_clone_top5_project_ancestries':a['entry_clone_top5_project_ancestries'],'client_contexts':c,'profiled_workload':{'completed_originals':expected,'completed_requests':requests,'originals_per_second':v['operations_per_second'],'p99_ms':v['request_p99_ns']/1e6,'seconds':v['seconds'],'applied_frontiers':v['applied_frontiers'],'not_unprofiled_capacity_or_acceptance':True},'libraries':s['libraries'],'unique_symbol_queries':s['unique_symbol_queries'],'capture_stderr':raw,'import_stderr':imp,'loss':{'reported_lost_chunks':1 if cell=='bulk' else 0,'reported_perf_samples':5616 if cell=='bulk' else 17721,'selected_samples':n,'excluded_thread_samples':6,'bulk_bad_timestamp_ordering_zero_warning':cell=='bulk'},'limits':s['limits']+a['limitations']+['Actor::include has no observed resolved symbol: zero subset is not proof of zero execution.','Current exact libc build-ID+bytes match archived slice440 proof, so raw fun_1b7ac0 belongs to shared memcpy/memmove implementation family; no allocator/call/byte inference.']}
write(P/'symbolication/READOUT.json',readout)
raw=[]
for cell in ['bulk','singleton']:
 f=P/'perf-quiet'/(cell+'.data');raw.append({'path':str(f),'bytes':f.stat().st_size,'sha256':sha(f),'retention':'Original unmodified ignored local perf.data; not duplicated into compact publication archive. Converted JSON, perf text, raw stderr, address queries and folded stacks are archived.'})
write(P/'RAW_PERF_LOCATIONS.json',raw)
settings={n:int(Path('/proc/sys/kernel/'+n).read_text()) for n in ('perf_event_paranoid','kptr_restrict','perf_event_mlock_kb')};assert settings=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
assert read(P/'symbolication/SERVER_STOP.json')['actual_terminal_exit']==0
assert int((P/'guard-after.exit').read_text())==0
write(P/'FINAL_STATUS.json',{'capture_original_handle':32690,'capture_actual_terminal':0,'imports':0,'symbol_queries':0,'subsets':0,'client_contexts':0,'perf_script':0,'after_artifact_guard':0,'owned_server_actual_terminal':0,'kernel_settings_unchanged':settings,'raw_perf_retained':raw,'all_processing_end':True})
A=R/'validation/performance/hotspot446/linux';assert not A.exists();A.mkdir(parents=True)
containers=[]
for src in sorted(P.rglob('*')):
 if not src.is_file() or '__pycache__' in src.parts or src.suffix=='.data':continue
 assert not src.is_symlink()
 dst=A/src.relative_to(P);dst.parent.mkdir(parents=True,exist_ok=True)
 if src.stat().st_size>1024*1024 and src.suffix!='.gz':
  dst=Path(str(dst)+'.gz')
  with src.open('rb') as inp,dst.open('wb') as out,gzip.GzipFile(filename='',mode='wb',fileobj=out,mtime=0,compresslevel=5) as gz:shutil.copyfileobj(inp,gz)
  h=hashlib.sha256();size=0
  with gzip.open(dst,'rb') as inp:
   for chunk in iter(lambda:inp.read(1024*1024),b''):h.update(chunk);size+=len(chunk)
  assert h.hexdigest()==sha(src) and size==src.stat().st_size
  containers.append({'archive':str(dst.relative_to(A)),'original':str(src.relative_to(P)),'original_bytes':size,'original_sha256':h.hexdigest(),'encoding':'gzip lossless','compressed_sha256':sha(dst)})
 else:shutil.copy2(src,dst)
write(A/'LOSSLESS_CONTAINERS.json',containers)
(A/'README.md').write_text("""# Current446 Linux hotspot evidence (slice447)

Original capture32690 and both cases exited0; imports, current per-address symbol queries, subsets, client contexts, perf-text exports and after artifact guard exited0. Owned localhost-only Samply server17420/PID838355 was verified, stopped with SIGINT and polled to actual0. No browser, source/build/tool/policy changes or recapture. Kernel2/2/516 remained unchanged. Both hosts finished before conversion/query/archive work. Published source6c7f09b2/current evidencef81ac21a uses the existing genuinely fresh asserted446 ELF+DWP; PREP hashes link its published artifact proof, avoiding duplicate historical source/build trees.

Profiler-perturbed bulk completed19,988,480 originals/4,997,120 requests with frontiers[19,989,505;3],7,803,609.56 PUTS/s and p99 2.974516ms. Singleton completed1,048,576 originals with frontiers[1,049,601;3],105,203.77 PUTS/s and p99 .018274ms. These are separate from unprofiled paired capacity and P9 acceptance.

Bulk selected5,610 saved on-CPU sample weights: physical client-write wrapper15.134%, oneshot send8.075%, LogEntry-to-Vec6.417%. Inclusive overlapping clientwrite21.640% (receiverpoll7.825%, channelconstruct6.774%, mpscSend4.260%, alloc/dealloc2.799%); resultdelivery13.939%, PendingReplies1.979%, cohort dictionary.677%, entryclone6.827% and independent pending plan_copy5.882%. No resolved old durable-completion clone samples; missing optimized frames cannot establish zero executions. Current PendingReplies/dictionary spellings were verified against current RVA responses. Actor::include has no observed resolved frame and its zero subset is not zero work.

Bulk perf reports ONE LOST CHUNK; import also reports `bad timestamp ordering; 0 is earlier but arrived after 151741864475621`. No missing samples were reconstructed and no capture repeated. Bulk rawperf5,616/selected5,610; null0, unresolvedleaf1.319%, physicaldepth<=2 .749%. Singleton rawperf17,721/selected17,715; no reported lost chunk/timestamp warning, null0/unresolvedleaf1.270%. Singleton physical self has shared libc memcpy/memmove-family15.230%, schedulerworker5.916% and notifyPoll4.149%. Raw synthetic names remain; exact libc build-ID and bytes match the existing slice440 dlsym/dladdr proof. This identifies a shared copy implementation, not allocation/free causation or call/byte counts.

Whole-launch captures include warmup, post-clock sorting, verification and cleanup. Sample counts are not perf-period cycle weights, throughput-timer CPU, leader CPU or inclusive elapsed wall time. Inline/native views and context unions overlap; do not add percentages. Thread names do not establish leader/follower ownership. Current responses, source lines, full folded stacks, perf text, JSON and warnings are retained. Original rawperf.data stays ignored locally with exact hashes/locations in RAW_PERF_LOCATIONS.json; this compact archive excludes raw data, binaries, full sources/build trees and duplicated old profiles.

Initial metadata typo used Darwin SYMBOL_ARTIFACTS.json instead of actual Linux SYMBOLS.json; it was corrected without rerunning passed guards. The first stop probe could not see the escalated server from the sandbox namespace and issued no signal; exactPID verification/stop in its namespace succeeded. Both actual failures are recorded in PREP_INITIAL_FAILURE and PROCESSING_PROVENANCE, with original tool IDs/status rather than invented command output.

SHA256SUMS covers every regular archive file except itself; no nested manifests or symlinks. Files over1MiB are losslessly compressed; LOSSLESS_CONTAINERS records original hashes/sizes and reread verification. No next optimization is selected by this archive; inspect both current host profiles before one separately planned bounded change. Full P9 remains active/incomplete.
""")
files=sorted(f for f in A.rglob('*') if f.is_file());assert not any(f.is_symlink() for f in A.rglob('*'))
(A/'SHA256SUMS').write_text(''.join(sha(f)+'  '+str(f.relative_to(A))+'\n' for f in files))
for line in (A/'SHA256SUMS').read_text().splitlines():expected,name=line.split('  ',1);assert sha(A/name)==expected
print(json.dumps({'archive':str(A),'regular_files':len(files)+1,'payload_hashes':len(files),'manifest_sha256':sha(A/'SHA256SUMS'),'archive_bytes':sum(f.stat().st_size for f in A.rglob('*') if f.is_file()),'lossless_containers':len(containers),'all_hashes_reread_verified':True,'all_processing_end':True},indent=2))
