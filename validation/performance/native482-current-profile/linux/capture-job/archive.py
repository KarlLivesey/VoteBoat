"""Adapted479 lossless archive; one current480 profile, raw perf retained ignored."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent;out=Path('validation/performance/native482-current-profile/linux');assert not out.exists()
load=lambda p:json.loads(Path(p).read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
config=load(job/'CONFIG.json');record=load(job/'capture/RECORDING.json');terminal=load(job/'TERMINAL.json');validation=load(job/'VALIDATION.json');perf=load(job/'readouts/ANALYSIS.json');context=load(job/'samply-symbolication/data/CONTEXTS.json');samply=load(job/'samply-symbolication/data/SUMMARY.json');diagnostic=load(job/'DIAGNOSTIC.json')
assert terminal['exit']==terminal['actual_profiler_exit']==terminal['actual_native_exit']==validation['exit']==0 and terminal['all_owned_processes_terminated']
for n in ['record','prepare','validate','convert','analyse','symbolicate','samply-contexts']:assert(job/(n+'.exit')).read_text().strip()=='0',n
quiet=load(job.parent/'ROOT_QUIET_END.json');assert quiet['linux']['actual_terminal_exit']==quiet['mac']['actual_terminal_exit']==0
assert sha(job/'CONFIG.json')==load(job/'READY.json')['config_sha256']and sha(job/'capture/perf.data')==perf['raw_perf']['sha256']
assert record['kernel_policy_before']==record['kernel_policy_after']==config['kernel_policy']=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
assert record['post_guard_exit']==0 and not record['errors']and not record['received_signals']
assert context['sample_count']==samply['denominator']==perf['native_samples']==3683 and context['period_weight']==perf['native_period_weight']==17333009497
assert(job/'samply-symbolication/server.exit').read_text().strip()==(job/'samply-symbolication/query.exit').read_text().strip()=='0'
proof={'scope':'One original current480 loaded native CPU diagnostic; not unprofiled acceptance. No recapture/browser/SVG/sysctl/sudo/install/rebuild. Same saved data converted after both original captures ended.','source_commit':config['source_commit'],'schema_publication_commit':'33de924687b4240083f9e31f80fa7ac886fb905c','original_handle':11490,'actual_terminal_tool_chunk':'d4fe2c','actual_terminal_exit':0,'frozen_validation_exit':0,'same_native480_artifact':load(job/'READY.json')['native_artifact_sha256'],'source_inputs':len(config['candidates']['candidate']['source']['files']),'source_directories':len(config['candidates']['candidate']['source']['directories']),'recording_post_source_artifact_compiler_runtime_tool_policy_guard_exit':0,'raw_perf_ignored_location':perf['raw_perf'],'native_exec_samples':context['sample_count'],'native_exec_period_weight':context['period_weight'],'whole_launch_samples':perf['all_samples'],'whole_launch_period_weight':perf['all_period_weight'],'reported_lost_samples':perf['lost_samples_reported'],'profiler_resource_owner_native_all_terminated':True,'symbol_server_exit':0,'native_build_id':diagnostic['native_build_id'],'raw_source_native_WAL_binary_buildtrees_excluded':True,'independent_review':'Parent independent raw reviewer has paths; archive publication remains parent-owned.'}
save(job/'ARCHIVE_PROOF.json',proof)
selected={}
for p in job.rglob('*'):
 if not p.is_file()or any(x in p.relative_to(job).parts for x in ['symbol-cache','__pycache__']):continue
 rel=p.relative_to(job)
 if rel==Path('capture/perf.data')or p.name=='ARCHIVE_RECEIPT.json':continue
 wal=job/'capture/wal'
 if wal in p.parents and p.parent!=wal:continue
 if wal in p.parents:assert p.suffix in ['.csv','.txt','.json']
 assert not p.is_symlink();selected[str(Path('capture-job')/rel)]=p
for name in ['ROOT_QUIET_GO.json','ROOT_QUIET_END.json','INDEPENDENT_PREPARATION_REVIEW.json']:selected['root-authority/'+name]=job.parent/name
candidate=Path(config['candidates']['candidate']['source']['path']).parent
for item in config['candidates']['candidate']['proofs']:
 p=Path(item['path']);assert sha(p)==item['sha256']
 try:rel='qualification/current480/'+str(p.relative_to(candidate))
 except ValueError:rel='qualification/authority/'+p.parent.name+'-'+p.name
 assert rel not in selected;selected[rel]=p
for n in ['elf-notes.log','elf-notes.exit','elf-notes.stderr']:
 p=candidate/'logs'/n;selected['qualification/current480/logs/'+n]=p
out.mkdir(parents=True);packing=[];selection=[]
for relative,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'));saved=relative+('.gz'if (len(b)>65536 or p.suffix in ['.stdout','.stderr','.log','.txt']) and p.suffix!='.gz' else'');target=out/saved;target.parent.mkdir(parents=True,exist_ok=True)
 if saved!=relative:
  target.write_bytes(gzip.compress(b,compresslevel=9,mtime=0));assert gzip.decompress(target.read_bytes())==b
  packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(b),'container_sha256':sha(target),'container_bytes':target.stat().st_size})
 else:shutil.copy2(p,target);assert target.read_bytes()==b
 selection.append({'source_path':str(p.resolve()),'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(b),'source_mode':oct(p.stat().st_mode&0o7777)})
save(out/'PACKING.json',packing);save(out/'SELECTION.json',selection)
s=diagnostic;r=load(job/'capture/metadata/RESOURCES.json');c=context['contexts'];quality=context['quality'];den=context['period_weight'];count=context['sample_count']
lines=['# 482 Linux current480 loaded CPU diagnostic','',
'One original handle11490 actually terminated0 (final tool chunkd4fe2c): perf, native, resource owner, exact postguard and frozen native/storage/publication validator all passed. Three quorum-durable voters share the guarded Btrfs Sabrent NVMe. Published480 source f7b0ef12709bb7e7aad4f0279851aa035b508e9e and its unchanged fresh native opt3/debug0/assertions/overflow/defaultLTO+codegen artifact8d3530a6… run TCP/TLS, WitnessLogIo, ready64,16000 offers at2000/s, window4096, onegroup,64warmups and2s active maintenance. No recapture, build, policy change, browser or SVG.',
'',f"Profiled diagnostic: {s['completed']} completed originals, {s['window_refused']} window refusals, all other refusals/unknown/pending zero. Completed {s['completed_originals_per_second']:.3f} originals/s; during-offer {s['during_originals_per_second']:.3f}/s. Backlog at horizon and max inflight4096; drain {s['drain_seconds']:.6f}s. Recovery8283=64+8219, retries/joins verified;3 checkpoints,9 reclaims, no maintenance skips. This is loaded, unlike479's max65pending; profiles are not paired throughput acceptance.",
'','| Applied-original latency | p50 ms | p95 ms | p99 ms | max ms |','| --- | ---: | ---: | ---: | ---: |']
for label in ['intended','service','dispatch']:
 a=s['latencies_nearest_rank'][label];lines.append('| '+label+' | '+' | '.join(f"{a[k]:.3f}"for k in ['p50_ms','p95_ms','p99_ms','max_ms'])+' |')
lines+=['','Intended/service distributions cover8219 actual applied originals; dispatch covers all16000 actual offers, including refused ones. Percentiles use nearest-rank ceil(n*p/100)-1. All original offers and raw summaries are preserved.','','| Exact physical-frame union | Sample-count share | Perf-period share |','| --- | ---: | ---: |']
for label in ['Counter_validate_proposal','OperationId_set_insert','Counter_applied_map_get','pending_client_btree','client_reconcile','check_ready_execution','ready_collect','core_raft']:
 a=c[label];lines.append(f"| {label} | {a['sample_count_percent']:.3f}% | {a['period_percent']:.3f}% |")
lines+=['','These inclusive unions overlap and cannot be added. Counter validation is the largest observed named work stage:57.003% native-period/44.746% counts. Its exact check_submit-iterator instantiation covers55.257% period; physical ClientRouter::submit/propose_maintenance ancestry covers55.643%. check_submit is present in a generic type name, not a captured physical frame. Direct caller of Counter validation leaf work is97.823% ClientRouter::submit. Among OperationId-set insertion leaf periods94.874% directly name the Counter submission validation instantiation; Counter applied-map get has93.655% that caller. Source application.rs225 rebuilds reserved BTreeSet across pending after dedup checks; clients.rs327 invokes it separately during submission on log.chain(queued). This source structure fits ancestry, without measuring call/allocation counts or proving a future optimization gain.',
'',f"Native-exec branch {context['native_exec_branch']} has {count} unit-weight samples and period sum {den}; all samples match raw perf leaves one-to-one by TID/ordinal and timestamps within {context['maximum_timestamp_difference_ms']*1000:.3f}us. No preexec Python samples occurred in this native PID. Whole perf launch has {perf['all_samples']} samples/{perf['all_period_weight']} period, including wrappers. Counts and periods differ; neither is elapsed CPU, exact cycles, blocked I/O time or per-original cause. Whole native launch includes startup/warmup/maintenance/drain/recovery/cleanup; there is no exact measurement-only CPU boundary. Worker names do not identify leader ownership.",
'',f"Perf reports zero lost samples, but its libdw text conversion has {perf['quality']['single_frame_weight']*100/perf['native_period_weight']:.3f}% native-period leaf-only. Headless saved-data Samply import recovers depths up to29, leaving6 single-frame samples; no second capture. Native debug0 still limits inlines/source, and no zero-truncation or complete-stack claim is made. Final unresolved leaves:131 samples/{quality['unresolved_leaf_period']*100/den:.3f}% period; synthetic fun_<hex> leaves266samples/{quality['synthetic_fun_leaf_period']*100/den:.3f}% period are separate unnamed identities. Many outer stacks contain synthetic names; those are not actual function labels.",
'',f"The exact480 ELF buildID {diagnostic['native_build_id']} matches imported codeId and symbol requests; no476maps used. Raw addresses, imported profile, symbol request/response JSON, all converter reports, folded count/period data and warnings remain inspectable. The temporary127.0.0.1 JSON-only Samply symbol server was owned, stopped and joined0; no browser. Original policies remain2/2/516 before/after. One read-only source-path observation error (nonexistent src/application/counter.rs; actual source application.rs read subsequently) is recorded, without inventing a raw file.",
'',f"Whole native-child resources: user {r['user_seconds']:.6f}s, system {r['system_seconds']:.6f}s over {r['elapsed_seconds']:.6f}s, peak RSS {r['max_rss_bytes']} bytes (rawLinux KiB converted). This boundary includes verification/cleanup and differs from the11.488s original timer. Block counters are operations, not bytes; inclusive/elapsed storage scopes are not additive CPU.",
'',f"Raw perf.data remains ignored at {perf['raw_perf']['path']}, SHA256 {perf['raw_perf']['sha256']}, {perf['raw_perf']['bytes']} bytes. Archive omits binaries, buildtrees, source trees, caches and generated WAL. Frozen source inventories/modes, exact artifact/root/compiler/protoc/provenance proofs, native metadata CSVs, all raw stdout/stderr, commands, exits and errors are retained losslessly. Text/stdout/stderr/log files gzip-packed upfront; PACKING/SELECTION preserve original hashes/lengths/modes and SHA256SUMS seals every payload.",
'','Linux loaded TCP and Mac moderate QUIC answer different questions: do not compare their percentages or profiled rates cross-host. This evidence selects submission validation for a bounded design review; it does not change code/defaults or satisfy full P9 sustained/headroom/fairness/placement/composition or pending ext4/XFS acceptance.']
# Expand legacy compact prose retained from the prior archive pattern for readability.
for i,line in enumerate(lines):
 for old,new in [('handle11490','handle 11490'),('terminated0','terminated 0'),('chunkd4fe2c','chunk d4fe2c'),('Published480','Published 480'),('artifact8d','artifact 8d'),('ready64,16000','ready64, 16000'),('at2000','at 2000'),('onegroup,64warmups','one group, 64 warmups'),('and2s','and 2s'),('inflight4096','inflight 4096'),('Recovery8283','Recovery 8283'),('verified;3','verified; 3'),(',9 reclaims',', 9 reclaims'),('unlike479','unlike 479'),('max65pending','max 65 pending'),('cover8219','cover 8219'),('all16000','all 16000'),('stage:57','stage: 57'),('covers55','covers 55'),('is97','is 97'),('periods94','periods 94'),('has93','has 93'),('has {count}','has {count}'),('up to29','up to 29'),('leaves266samples','leaves 266 samples'),('exact480','exact 480'),('no476maps','no 476 maps'),('temporary127','temporary 127'),('joined0','joined 0'),('remain2/2/516','remain 2/2/516'),('user3','user 3'),('system0','system 0'),('over12','over 12'),('peakRSS','peak RSS '),('29454336bytes','29454336 bytes'),('rawLinux','raw Linux'),('the11.488s','the 11.488s'),('61970732bytes','61970732 bytes')]:line=line.replace(old,new)
 lines[i]=line
(out/'README.md').write_text('\n'.join(lines)+'\n')
manifest=''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in sorted(out.rglob('*'))if p.is_file());(out/'SHA256SUMS').write_text(manifest)
for r in selection:
 p=out/r['archived_path'];b=gzip.decompress(p.read_bytes())if r['archived_path'].endswith('.gz')and not r['source_path'].endswith('.gz')else p.read_bytes()
 assert len(b)==r['original_bytes']and hashlib.sha256(b).hexdigest()==r['original_sha256']
receipt={**proof,'payloads':len(manifest.splitlines()),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'archive_bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'all_original_hashes_lengths_gzip_roundtrip':'PASS','archive':str(out)}
save(job/'ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
