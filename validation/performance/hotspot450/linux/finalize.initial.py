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
 readout[cell]={'selected_sample_weight':n,'unit':s['weight'],'totals':s['totals'],'threads':s['threads'],'physical_depth_sample_weights':s['physical_depth_sample_weights'],'physical_depth_le2_percent':100*sum(w for k,w in s['physical_depth_sample_weights'].items() if int(k)<=2)/n,'unresolved_leaf_percent':100*s['totals']['unresolved_leaf_sample_weight']/n,'any_unresolved_percent':100*s['totals']['any_unresolved_stack_sample_weight']/n,'top12_native_self':s['self_native'][:12],'top12_inline_self':s['self_including_inlines'][:12],'top12_native_inclusive':s['inclusive_native'][:12],'top12_inline_inclusive':s['inclusive_including_inlines'][:12],'inclusive_contexts':a['subsets'],'entry_clone_top5_project_ancestries':a['entry_clone_top5_project_ancestries'],'client_contexts':c,'profiled_workload':{'completed_originals':expected,'completed_requests':requests,'originals_per_second':v['operations_per_second'],'p99_ms':v['request_p99_ns']/1e6,'seconds':v['seconds'],'applied_frontiers':v['applied_frontiers'],'not_unprofiled_capacity_or_acceptance':True},'libraries':s['libraries'],'unique_symbol_queries':s['unique_symbol_queries'],'capture_stderr':raw,'import_stderr':imp,'loss':{'reported_lost_chunks':0,'reported_perf_samples':5857 if cell=='bulk' else 17298,'selected_samples':n,'excluded_thread_samples':6,'bad_timestamp_ordering_warning':False,'truncation':'No explicit truncation indicator available; fixed DWARF16KiB and optimized/unwinder limits remain.'},'limits':s['limits']+a['limitations']+['Actor::include has no observed resolved symbol: zero subset is not proof of zero execution.','Current exact libc build-ID+bytes match archived slice440 proof, so raw fun_1b7ac0 belongs to shared memcpy/memmove implementation family; no allocator/call/byte inference.']}
write(P/'symbolication/READOUT.json',readout)
raw=[]
for cell in ['bulk','singleton']:
 f=P/'perf-quiet'/(cell+'.data');raw.append({'path':str(f),'bytes':f.stat().st_size,'sha256':sha(f),'retention':'Original unmodified ignored local perf.data; not duplicated into compact publication archive. Converted JSON, perf text, raw stderr, address queries and folded stacks are archived.'})
write(P/'RAW_PERF_LOCATIONS.json',raw)
settings={n:int(Path('/proc/sys/kernel/'+n).read_text()) for n in ('perf_event_paranoid','kptr_restrict','perf_event_mlock_kb')};assert settings=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
assert read(P/'symbolication/SERVER_STOP.json')['actual_terminal_exit']==0
assert int((P/'guard-after.exit').read_text())==0
write(P/'FINAL_STATUS.json',{'capture_original_handle':57929,'capture_actual_terminal':0,'imports':0,'symbol_queries':0,'subsets':0,'client_contexts':0,'perf_script':0,'after_artifact_guard':0,'owned_server_actual_terminal':0,'kernel_settings_unchanged':settings,'raw_perf_retained':raw,'all_processing_end':True})
A=R/'validation/performance/hotspot450/linux';assert not A.exists();A.mkdir(parents=True)
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
(A/'README.md').write_text("# Current450 Linux hotspot evidence (slice451)\n\nOriginal57929 actual0 and bothcaptures0; imports/currentRVAqueries/existing exactbytehashconverters/perftextexports/afterartifactguard0. Ownedlocalheadless Samplyserver92474/PID3377209 verifiedSIGINT and originalterminal0. BOTHhostsetupEND beforejointquietgrant and BOTHcapturesactualEND beforeconversion. No browser/build/test/CLIrerun/install/policy/source/API/Pi orbaseline recapture. Kernel2/2/516 remainedunchanged. Current8813342b source1440/modesmatch compiledimmutable d6395be7 artifactELF70ae…6945/DWP3749…e149; PREPlinks existingactualroot/exe/upstreamfourfresh:false/compilerlocks/rootbuildscript/releaseflags proof.\n\nProfiled bulk19,988,480 originals/4,997,120requests/frontiers[19,989,505;3],7,867,759.40 PUTS/s/p993.231140ms. Singleton1,048,576/frontiers[1,049,601;3],116,209.68 PUTS/s/p99.016310ms. Separateprofilerperturbed diagnostics, notunprofiledpairedcapacity orhealthyacceptance.\n\nBulk selected5,851 weights: physicalselfClientwritewrapper13.878%, LogEntry-to_vec9.280%, oneshotsend7.435%. Inclusiveoverlapclientwrite21.296% (receiverpoll7.042%, construction6.478%, mpscSend4.598%, alloc/dealloc3.418%); resultdelivery13.639%, oneshotsend12.613%, PendingReplies2.547% andcohortdictionary.701%. Entryclone10.032%; independentpending plan_copy5.896% (receive3.316%, proposal1.607%, snapshot.974%). Largesttopappendcloneparents includecommitbroadcast1.607% andackpath1.094%; fullunion isnotthesum ofonlytopfivecallchains. Nativeframecategories move as450's append_for now calls genericto_vec ratherthaninlinepushclone; sharescannotestablish causalaggregateCPU increase/removal. No resolvedoldcommittedcompletionclone orreceivesuffixcopy observed; optimizedmissingframesdonotprovezerocalls.\n\nSingleton17,292selectedweights: sharedlibccopyfamily15.290%, schedulerworker6.032%, notifyPoll4.621%, LogMetadataEq4.355%; clientunion2.342%, pending.503%, dictionary.399%, entryclone.798%. Rawfun_1b7ac0 remainsplaceholder; exactcurrentlibccodeID+bytes match existing slice440dlsym/dladdrproof and identifysharedmemcpy/memmoveimplementation, notallocation/freecause orcall/bytecounts. Remainingfun_hexnames stayunresolvedsemanticboundaries.\n\nRawperf5,857/17,298 vsselected5,851/17,292 (sixexcludedthreadsamples each); noreportedlostchunks/timestamporderingwarnings onrecord/import/perftext. Bulknull0/unresolvedleaf1.812%/depth<=2 .684%; singletonnull0/unresolvedleaf1.428%/depth<=2 .266%. No explicittruncationindicator is available: optimizedframes andfixed16KiB unwindcapture canstillloseancestry. Wholelaunchincludeswarmup/postclocksort/verification/shutdown, notthroughputclockCPU/leaderCPU. Savedsampleweights are notperfperiodcycles orCPUus; inclusive/native/inlinecontextsoverlap andmustnotbeadded. Threadnamesdonotproveleader/followerownership. Macsamplingbias andsymbolcoalescing mustremainexplicit inseparatehostreadout.\n\nSource-groundedobservationonly: OwnedNextLog::plan_copy retainsanindependentownedcopy becausepublic Effect::Persist/LogUpdate remainsmutableandpreflightmustdetectallalteredfields. Removingthatcopy requiresprotectedownership pluslegacyexactfallback, notdigest/revision shortcuts. Currentclient/channel work remainsdominant, butrejected448isnotretuned andnonewoptimizationisselectedbythisarchive. Fullseven-cell/durable/headroom/fairness/memory/debt/placement/sharedbarrier/P8 goalstaysactive.\n\nOriginalperf.data staysignoredlocal withhashes/sizes/locations; archivedcurrentJSON/text/folded/perRVArequests+responses/sourcechains/metrics+originalcommands/exits providecompactreviewableevidence. No binaries/fullsource/buildtrees/oldprofiles copied. SHA256SUMS covers eachregularfileexceptitself, nonestedmanifests/symlinks; >1MiBtextlosslesslycompressed/rereadverified withoriginalhash/size. No recapture tohidefailures/warnings orinvent speedup.\n")
files=sorted(f for f in A.rglob('*') if f.is_file());assert not any(f.is_symlink() for f in A.rglob('*'))
(A/'SHA256SUMS').write_text(''.join(sha(f)+'  '+str(f.relative_to(A))+'\n' for f in files))
for line in (A/'SHA256SUMS').read_text().splitlines():expected,name=line.split('  ',1);assert sha(A/name)==expected
print(json.dumps({'archive':str(A),'regular_files':len(files)+1,'payload_hashes':len(files),'manifest_sha256':sha(A/'SHA256SUMS'),'archive_bytes':sum(f.stat().st_size for f in A.rglob('*') if f.is_file()),'lossless_containers':len(containers),'all_hashes_reread_verified':True,'all_processing_end':True},indent=2))
