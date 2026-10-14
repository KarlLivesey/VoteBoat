from pathlib import Path
import hashlib,json,gzip,shutil,re
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
 readout[cell]={'selected_sample_weight':n,'unit':s['weight'],'totals':s['totals'],'threads':s['threads'],'physical_depth_sample_weights':s['physical_depth_sample_weights'],'physical_depth_le2_percent':100*sum(w for k,w in s['physical_depth_sample_weights'].items() if int(k)<=2)/n,'unresolved_leaf_percent':100*s['totals']['unresolved_leaf_sample_weight']/n,'any_unresolved_percent':100*s['totals']['any_unresolved_stack_sample_weight']/n,'top12_native_self':s['self_native'][:12],'top12_inline_self':s['self_including_inlines'][:12],'top12_native_inclusive':s['inclusive_native'][:12],'top12_inline_inclusive':s['inclusive_including_inlines'][:12],'inclusive_contexts':a['subsets'],'entry_clone_top5_project_ancestries':a['entry_clone_top5_project_ancestries'],'client_contexts':c,'profiled_workload':{'completed_originals':expected,'completed_requests':requests,'originals_per_second':v['operations_per_second'],'p99_ms':v['request_p99_ns']/1e6,'seconds':v['seconds'],'applied_frontiers':v['applied_frontiers'],'not_unprofiled_capacity_or_acceptance':True},'libraries':s['libraries'],'unique_symbol_queries':s['unique_symbol_queries'],'capture_stderr':raw,'import_stderr':imp,'loss':{'reported_lost_chunks':sum(int(v) for v in re.findall(r'lost (\d+) chunks',raw)),'reported_perf_samples':int(re.search(r'\((\d+) samples\)',raw)[1]),'selected_samples':n,'excluded_thread_samples':6,'bad_timestamp_ordering_warning':'bad timestamp ordering' in imp,'truncation':'No explicit truncation indicator available; fixed DWARF16KiB and optimized/unwinder limits remain.'},'limits':s['limits']+a['limitations']+['Actor::include has no observed resolved symbol: zero subset is not proof of zero execution.','Current exact libc build-ID+bytes match archived slice440 proof, so raw fun_1b7ac0 belongs to shared memcpy/memmove implementation family; no allocator/call/byte inference.']}
write(P/'symbolication/READOUT.json',readout)
raw=[]
for cell in ['bulk','singleton']:
 f=P/'perf-quiet'/(cell+'.data');raw.append({'path':str(f),'bytes':f.stat().st_size,'sha256':sha(f),'retention':'Original unmodified ignored local perf.data; not duplicated into compact publication archive. Converted JSON, perf text, raw stderr, address queries and folded stacks are archived.'})
write(P/'RAW_PERF_LOCATIONS.json',raw)
settings={n:int(Path('/proc/sys/kernel/'+n).read_text()) for n in ('perf_event_paranoid','kptr_restrict','perf_event_mlock_kb')};assert settings=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
assert read(P/'symbolication/SERVER_STOP.json')['actual_terminal']==0
assert int((P/'guard-after.exit').read_text())==0
write(P/'FINAL_STATUS.json',{'capture_original_handle':4448,'capture_actual_terminal':0,'imports':0,'symbol_queries':0,'subsets':0,'client_contexts':0,'perf_script':0,'after_artifact_guard':0,'owned_server_actual_terminal':0,'kernel_settings_unchanged':settings,'raw_perf_retained':raw,'all_processing_end':True})
A=R/'validation/performance/hotspot452/linux';assert not A.exists();A.mkdir(parents=True)
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
bulk=readout['bulk'];single=readout['singleton']
(A/'README.md').write_text(f"""# Current452 Linux hotspot evidence (slice453)

Original4448 actual0 after jointquiet grant; bothhostcaptures actualEND before processing. Imports/current452 per-RVA queries/converters/perftext exports/afterartifact guard0. Headless localhost Samplyserver53519/PID350886 verifiedSIGINT and originalterminal0. Initial sandboxbind failed1 Operationnotpermitted; raw failure preserved, focusedsame localhostserver launched with scopedescalation. No recording/build/test/CLI/baseline recapture/install/policy/source/API/Pi changes. Settings2/2/516 unchanged. PREP proves1440 selected source hashes/physicalmodes match immutable452source5201bfea, compiler/locks/fouractualfresh:false/rootbuildscript/releasecfg and ELF4bf462…/DWP0fdf….

Perturbed bulk completed19,988,480 originals/4,997,120requests/frontiers[19,989,505;3], {bulk['profiled_workload']['originals_per_second']:,.2f} PUTS/s,p99{bulk['profiled_workload']['p99_ms']:.6f}ms. Singleton1,048,576/frontiers[1,049,601;3], {single['profiled_workload']['originals_per_second']:,.2f}/s,p99{single['profiled_workload']['p99_ms']:.6f}ms. Rates are profiled diagnostics, never paired capacity or acceptance.

Bulk5,786 selectedweights: physicalself Clientwritewrapper14.034%, LogEntry-to_vec9.229%, oneshotsend7.933%, sharedlibccopyfamily4.632%. Inclusive client21.120%(receiverpoll6.758/channelconstruction6.205/mpsc4.580/alloc-dealloc3.474, overlapping); current receiver-only waitVecconstruction4weights/.069%, source line2155weights/.086% are different attribution scopes. Entryclone9.869%, independentplan_copy5.513% (receive3.370%, proposal1.573%, othermaterialization overlaps), materialize1.158%. Resultdelivery14.017%, application15.486%, persist15.088%, oneshotsend13.204%, pending1.988%, cohortdictionary.674%. No allocator/call/byte cause or projected removedcost inferred. No empty-byte clone hypothesis retuning.

Singleton17,487selectedweights: sharedlibccopyfamily15.486%, schedulerworker6.165%, LogMetadataEq3.849%; clientunion2.419%, entryclone.823%, pendingplan_copy.429%, persist23.246%. Exact savedlibc codeID+bytes match existingaliasproof for shared memcpy/memmove implementation; rawFDE names remain preserved, other fun_hex symbols retain unknown semantics. Source ownership observation: coreplan_copy is an independent owned copy guarding mutablepublicEffect.Persist/LogUpdate; removal requires protectedownership pluslegacyexactfallback. No newoptimization selected in this archive.

Quality: perf reports5,792/17,493 samples vsselected5,786/17,487, sixexcludedthread samples each. Bulk no reportedlostchunks/timestampwarning; singleton reports LOST1CHUNK and badtimestampordering (0 earlierthan166049659994722), original warnings retained, no recapture/missing-sample reconstruction. Null0 both, unresolvedleaf{bulk['unresolved_leaf_percent']:.3f}%/{single['unresolved_leaf_percent']:.3f}%, physicaldepth<=2 {bulk['physical_depth_le2_percent']:.3f}%/{single['physical_depth_le2_percent']:.3f}%. Fixed DWARF16KiB/optimized/unwinder limits and missing ancestry remain; no explicit truncation indicator. Wholelaunch includeswarmup/postclocksort/verification/shutdown, notthroughput-clock/leader CPU. Linuxweights are samples, notperfperiodcycles orCPUus; overlappingnative/inline unions cannot be added. SeparateMacCPUdelta/waitsamplebias stays separate.

Original451 genericconverters preservedbyhash. Focusedcurrent452 clientconstructor predicate adapts ONLY oldIDtupletypename tocurrentRxVec and commentlabels; prior output+code preserved. One ad-hoc savedrecord name query initially KeyErrorfunction on unresolvedrecord, noted withactualtool446ef8; optionalfieldread fixed, no capture or converterdata loss. No fabricated zeroallocation/work from absent frames.

Originalperf.data remainsignored local withhashes/sizes/locations. CurrentJSON/text/folded/per-RVArequests/responses/callerchains/metrics and allcommands/exits/statuses are archived. No binaries/fullsource/buildtrees/oldprofiles. SHA256SUMS covers everyregularpayload exceptitself, no nestedmanifests/symlinks; large textlosslesslycompressed/rereadverified. Fullseven-cell/durable/headroom/fairness/memory/debt/placement/sharedbarriers/P8 objective remains active; Pi stopped.
""")
files=sorted(f for f in A.rglob('*') if f.is_file());assert not any(f.is_symlink() for f in A.rglob('*'))
(A/'SHA256SUMS').write_text(''.join(sha(f)+'  '+str(f.relative_to(A))+'\n' for f in files))
for line in (A/'SHA256SUMS').read_text().splitlines():expected,name=line.split('  ',1);assert sha(A/name)==expected
print(json.dumps({'archive':str(A),'regular_files':len(files)+1,'payload_hashes':len(files),'manifest_sha256':sha(A/'SHA256SUMS'),'archive_bytes':sum(f.stat().st_size for f in A.rglob('*') if f.is_file()),'lossless_containers':len(containers),'all_hashes_reread_verified':True,'all_processing_end':True},indent=2))
