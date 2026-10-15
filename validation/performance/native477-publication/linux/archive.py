"""Pack terminal477 Linux evidence, including failed preparation; never executes workloads."""
from pathlib import Path
import datetime,gzip,hashlib,json,shutil

base=Path('target/p9/native477-publication').resolve();job=base/'linux3';out=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();load=lambda p:json.loads(Path(p).read_text())
assert {p.name for p in out.iterdir()}=={'archive.py'}, 'refuse archive overwrite'
r=load(job/'READOUT.json');config=load(job/'CAMPAIGN_CONFIG.json');assert r['actual_processes']==r['actual_validators']==r['actual_readback_checks']==48 and r['actual_terminal_exit']==0
assert load(base/'ROOT_QUIET_END.json')['linux']['actual_terminal_exit']==load(base/'ROOT_QUIET_END.json')['mac']['actual_terminal_exit']==0
provenance=load(job/'READOUT_PROVENANCE.json');assert sha(job/'analyse.py')==provenance['analyse_py_sha256']
for p,proof in provenance['inputs'].items():assert sha(p)==proof['sha256']and Path(p).stat().st_size==proof['bytes']and oct(Path(p).stat().st_mode&0o7777)==proof['mode']
selected={}
def add(p,name):
 p=Path(p);name=str(name);assert p.is_file()and not p.is_symlink();assert name not in selected;selected[name]=p
# All final Linux controller outputs and result tables, excluding generated voter state.
for p in job.rglob('*'):
 if p.is_file()and '__pycache__'not in p.parts:add(p,Path('final')/p.relative_to(job))
for case in config['cases']:
 root=Path(case['wal_root'])
 for p in root.iterdir():
  if p.is_file():assert p.suffix in ['.csv','.txt','.json'];add(p,Path('case-results')/case['name']/p.name)
# Earlier preparations never ran a VoteBoat workload; retain exact scripts,
# imported validators and partial outputs rather than relabel them final.
for version in ['linux','linux2']:
 for p in(base/version).rglob('*'):
  if p.is_file()and '__pycache__'not in p.parts:add(p,Path('preparation')/version/p.relative_to(base/version))
for p in(base/'common').iterdir():
 if p.is_file():add(p,Path('preparation/common')/p.name)
for p in base.iterdir():
 if p.is_file()and(p.suffix=='.json' or p.name.startswith(('prepare-linux','plan-tests','resource-tests'))):add(p,Path('preparation')/p.name)
for directory in sorted(base.glob('resource-*')):
 if directory.is_dir():
  for p in directory.rglob('*'):
   if p.is_file():add(p,Path('preparation')/directory.name/p.relative_to(directory))
# Hash-bound build/compiler/qualification identities are recorded once here;
# their full original build and release histories are separately sealed in476.
for i,proof in enumerate(config['candidate']['proofs']):
 assert sha(proof['path'])==proof['sha256'];add(proof['path'],Path('candidate-proofs')/(f'{i:02d}-'+Path(proof['path']).name))
for p in config['candidate']['compiler_tools']+config['runtime_tools']:assert sha(p['path'])==p['sha256']
for a in config['candidate']['artifacts']:assert sha(a['copy'])==sha(a['original'])==a['sha256']
selection=[];packing=[]
for name,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'));relative=name+('.gz'if len(b)>65536 else'');target=out/relative;assert not target.exists();target.parent.mkdir(parents=True,exist_ok=True)
 if name!=relative:
  target.write_bytes(gzip.compress(b,compresslevel=9,mtime=0));packing.append({'original_path':name,'archived_path':relative,'original_bytes':len(b),'original_sha256':sha(p),'container_bytes':target.stat().st_size,'container_sha256':sha(target)})
 else:shutil.copy2(p,target)
 selection.append({'source_path':str(p),'archived_path':relative,'original_bytes':len(b),'original_sha256':sha(p),'source_mode':oct(p.stat().st_mode&0o7777)})
for name,value in [('SELECTION.json',selection),('PACKING.json',packing)]: (out/name).write_text(json.dumps(value,indent=2)+'\n')
regression_inventory=[]
for p in r['pairs']:
 flags={'completed_rate':p['completed_rate_ratio']<1,'during_rate':p['during_rate_ratio']<1,'mean_barrier':p['mean_barrier_ratio']>1,'mean_wal_sync':p['mean_wal_sync_ratio']>1,'mean_publication':p['mean_publication_ratio']>1}
 for kind,metrics in p['latency_ratios'].items():
  for metric,value in metrics.items():flags[kind+'_'+metric]=value>1
 regression_inventory.append({'cell':p['cell'],'protocol':p['protocol'],'trial':p['trial'],'flags':flags,'paired_ratios':p})
(out/'REGRESSION_INVENTORY.json').write_text(json.dumps({'scope':'Every paired rate/quantile/maximum/mean physical-duration regression flagged independently; no significance or causal claim from three trials. READOUT.regressions is the narrower completed-rate or intended-p99 subset.','pairs':regression_inventory},indent=2)+'\n')
proof={'collection_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'original_handle':58029,'actual_terminal_exit':0,'processes':48,'validators':48,'readback_checks':48,'source_commit':config['source_commit'],'config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'actual_offers':297984,'actual_applied':249383,'actual_window_refused':48601,'all_other_outcomes_zero':True,'independent_raw_sha_modes_exact':True,'all_actual_exit_ledger':'0','no_workload_repeated':True,'earlier_preparation_versions':['linux(sandboxdeviceinvisible exit32)','linux2(pre-aliasfreeze, no timing)','linux3(final original58029)'],'physical_device':config['device_identity'],'scope':'Raw-only collection after both original host campaigns terminated and root authorized collection. No source/build tree, binaries, generated WAL or repeated test/workload. Native476 artifact proofs retained by SHA; cross-filesystem/fullP9 acceptance open.'}
(out/'COLLECTION.json').write_text(json.dumps(proof,indent=2)+'\n')
lines=['#477 Linux paired native publication','','Original58029 terminates0; all48 processes,48 independent validators and48 six-plus-six readback checks pass. No case retries/replacements. Root authorized collection only after both Linux58029 and Mac94749 actually terminated. Source476 `7e9068296ce00dde5bf3e03bf4d852ed2833d185`; schema/evidence published `c3aeed5c800fcb47e70a8b0df1cbd5f655d9dfe2`. Same fresh native executable `0bf40f6b4547c446f9db2a86a4bffe6d9ab223640d470154ec266a0d7cc2d064` serves both explicit atomic and witness arms, with identical native diagnostics. Fresh root/example/build-script proofs and compiler/source identities are sealed in476/linux and bound here by exact hashes.','','Linux stable1.98.1/LLVM22.1.8/protoc3.9.0; native opt3/debug0/assertions+overflow/defaultLTO/defaultcodegen. One group, three voters sharing one Sabrent NVMe Btrfs compress=zstd:1 volume;8B Counter/64 warmups/ready16/capacity4096/maintenance2s/120s drain guard. Device and CPU policies match before/after; powersave/balance_performance remains disclosed and unchanged. Three trials rotate workloads/protocols/adjacent arm order;12 pairs start each arm. Singleton256offers/8s-1/window1; moderate8192/128s-1/window256; bulk8192/512s-1/window4096; overload8192/1024s-1/window4096.','','Completed rates below include drain. During rates end at each offered horizon. P99 uses intended-to-completed latency of successful originals. All per-arm medians, ranges and trial values, plus nearest-rank p50/p95/p99/max intended/service/dispatch, are in final/READOUT.json.gz. Dispatch quantiles cover all scheduled offers.','','| Cell | Protocol | Arm | Completed originals/s median | During/s median | p99 ms median | Window refusals trials1/2/3 |','| --- | --- | --- | ---: | ---: | ---: | --- |']
for c in r['cells']:
 lines.append(f"| {c['cell']} | {c['protocol']} | {c['publication']} | {c['rates']['completed_originals_per_second']['median']:.3f} | {c['rates']['during_originals_per_second']['median']:.3f} | {c['latencies']['intended']['p99_ms']['median']:.3f} | {','.join(str(v)for v in c['refusals']['window_refused']['trials'])} |")
lines+=['','All24 paired total-completion rates improve, but intended p99 regresses in singletonTCPt2 (1.028132), singletonQUICt1 (1.086547) and moderateTCPt3 (1.028301). During-rate regressions are moderateTCPt3 (0.993445) and overloadTCPt2 (0.501507); the latter admits fewer originals and refuses3267 versus2437 despite its slightly higher total rate. Keep these original tails and refusals. Full service/dispatch ratios and every quantile/maximum/physical-duration regression flag remain in REGRESSION_INVENTORY.','','| Cell | Protocol | Trial | Witness/atomic completed | During | Intended p50 | p95 | p99 | Max |','| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
for p in r['pairs']:
 q=p['latency_ratios']['intended'];lines.append(f"| {p['cell']} | {p['protocol']} | {p['trial']} | {p['completed_rate_ratio']:.6f} | {p['during_rate_ratio']:.6f} | {q['p50_ms']:.6f} | {q['p95_ms']:.6f} | {q['p99_ms']:.6f} | {q['max_ms']:.6f} |")
lines+=['','297984 offers reconcile to249383 applied+48601 window refusals; other refusal/unknown/pending classes are zero. Exact unique tickets, contiguous application values, timing/window conservation, recovery64+applied, retries and worker joins pass.668 checkpoints/2004 reclaims/4 explicit skips are retained. No scalar storage error deltas; every physical publication step and manifest error count is zero. Raw checkpoints/reclaims/bases and all original per-operation outcomes remain available.','','Bulk leader mean barrier/WALsync/publication medians atomic→witness: TCP41.605/14.203/27.395→28.152/14.115/14.026ms; QUIC48.612/16.108/32.496→24.963/12.717/12.286ms. These are nested elapsed wall contexts, not additive CPU. Counter deltas and each replica/step are retained; lifetime maxima are never subtracted and recovery-handle counters remain absolute. Cold checkpoint/reclaim latency sums include queuing/overlap and do not establish critical-path CPU or all file primitives. Ordinary witness namespace counters are zero; initialization/reclamation/repair namespace work is outside that scope.','','Bulk TCP still reaches4096 inflight and roughly4096 pending at horizon in both arms; witnessQUIC accepts all8192 but retains3813–3892 pending at16s and drains afterward. Overload witnesses refuse1953–3267 TCP and2089–2606 QUIC originals. These cells are not maximum capacity or healthy/refusal-free headroom. Mean commands/appendunit is a physical suffix-command ratio including metadata transitions/retries, not literal proposal width; encoded bytes are not device I/O. Wholeprocess RSS/context-switch/CPU/block counts include warmup, drain, recovery and cleanup; block counts are not bytes and RSS peaks are not continuous memory/debt plateaus.','','The native completion timer is unchanged: before measure loop to immediately after it, before ledger persistence/history validation/follower verification/worker joins/recovery. This differs from the framework all-client-join boundary. All successful cases still verify joins afterward. Independent validators run only after ALL_TIMING_END; source/tool/artifact and host identity guards also pass. No same-host builds/checks/copies/archiving/profiling competed with timing.','','Preparation versions and failures remain distinct: initial Linux setup cannot see the real device inside the sandbox (lsblk32, no READY/workload); Linux2 is the preserved pre-alias freeze with no timing; Linux3 alone ran. Harmless Python ownership probes preserve success, signal and intentional metadata-write failure, not VoteBoat benchmark replacements. All actual scripts/configuration/commands/start/end/exits/readbacks, guard and host snapshots, raw histories and derived data are retained. Large files are lossless gzip with original/container hashes and lengths; SELECTION retains source paths/bytes/modes. Full source/build trees, executables/libraries, Python bytecode and generated voter WALs are excluded.','','FileLogIo remains default. Finite Btrfs improvement does not close APFS/real ext4-XFS gain acceptance, sustained20M framework floors, bounded memory/debt, fair/skewed traffic, independent placement/shared physical barriers or P8 composition. No filesystem-only causation, universal speed claim or fullP9 completion follows.']
(out/'README.md').write_text('\n'.join(lines)+'\n')
files=sorted(p for p in out.rglob('*')if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in files))
manifest={s[66:]:s[:64]for s in(out/'SHA256SUMS').read_text().splitlines()};assert set(manifest)=={str(p.relative_to(out))for p in out.rglob('*')if p.is_file()and p.name!='SHA256SUMS'}
assert all(sha(out/p)==h for p,h in manifest.items())
for p in packing:
 raw=gzip.decompress((out/p['archived_path']).read_bytes());assert len(raw)==p['original_bytes']and hashlib.sha256(raw).hexdigest()==p['original_sha256']
print(json.dumps({'payloads':len(manifest),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'all_original_and_container_SHA_lengths':'PASS','archive':str(out)}))
