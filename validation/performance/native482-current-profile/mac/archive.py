"""Seal one original Mac482 saved JSON profile and postterminal numerical evidence."""
from pathlib import Path
import gzip,hashlib,json
job=Path(__file__).resolve().parent;root=job.parent
destination=Path('validation/performance/native482-current-profile/mac')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
assert not destination.exists(),'Never replace sealed evidence'
assert load(job/'raw/TERMINAL.json')['exit']==load(job/'raw/VALIDATION.json')['exit']==0
assert load(root/'INDEPENDENT_PROFILE_REVIEW.json')['exit']==0
assert load(root/'ROOT_QUIET_END.json')['mac']['actual_terminal_exit']==0
assert int((job/'live/controller.exit').read_text())==0
cpu=load(job/'COMPACT_CPU.json');quality=load(job/'QUALITY.json');work=load(job/'WORKLOAD_READOUT.json')
symbols=load(job/'symbolication/TERMINAL.json');addresses=load(job/'symbolication/ADDRESS_SYMBOLS.json')
assert symbols['exit']==symbols['server_exit']==0 and symbols['server_joined']
proof={'original_handle':18694,'actual_exit':0,'validation_exit':0,'headless_symbol_server_exit':0,'headless_symbol_server_joined':True,
 'profile_sha256':sha(job/'raw/profile.json.gz'),'source_commit':'f7b0ef12709bb7e7aad4f0279851aa035b508e9e',
 'schema_publication_commit':load(root/'ROOT_QUIET_GO.json')['published_evidence_schema_commit'],
 'native_executable_sha256':'fef05adc73179359ea2acc0d9b92978c57c4795410a727aefc30f1aac422fa52','native_uuid':'EDE59E8D-8928-3C4E-9F04-3AADBBEF5772',
 'raw_original_files':len(load(job/'raw/COLLECTED.json')['files']),'cpu_delta_us':cpu['cpu_us'],'sample_rows':quality['totals']['samples'],'wall_sample_weight':quality['wall_sample_weight'],
 'addresses_queried':len(addresses),'known_address_functions':sum('function'in r for r in addresses),'unknown_address_functions':sum('function'not in r for r in addresses),
 'scope':'One original current 480 Mac moderate QUIC q16 Witness8192@128/window256/group1 diagnostic. No repeat/rebuild/default/policy change, browser, SVG, old476maps or production source/binary/buildtree/WAL transfer. Symbol queries/conversion only after both captures actually ended. Saved JSON serialized inherently by record without presymbolication.',
 'sampling_limits':quality['limits'],'transfer_scope':'Original JSON.gz, exact address queries/responses, guards/commands/streams, CSV/text ledgers and numerical readout only. Packed large text retains original hashes/lengths.'}
(job/'ARCHIVE_PROOF.json').write_text(json.dumps(proof,indent=2)+'\n')
excluded=['COLLECTED.tar.gz','ARCHIVE_REVIEW.json','archive.stdout','archive.stderr','archive.exit']
payloads={p.relative_to(job).as_posix():p for p in job.rglob('*')if p.is_file()and '__pycache__'not in p.parts and p.name not in excluded and p.suffix!='.tar'}
for name in ['ROOT_QUIET_GO.json','ROOT_QUIET_END.json','INDEPENDENT_PREPARATION_REVIEW.json','independent-profile-review.py','INDEPENDENT_PROFILE_REVIEW.json','independent-profile-review.stdout','independent-profile-review.stderr']:
 payloads['shared/'+name]=root/name
destination.mkdir(parents=True);packing=[]
for name,p in sorted(payloads.items()):
 content=p.read_bytes();pack=len(content)>65536 and p.suffix!='.gz';saved=name+'.gz'if pack else name
 target=destination/saved;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(gzip.compress(content,mtime=0)if pack else content)
 assert(gzip.decompress(target.read_bytes())if pack else target.read_bytes())==content
 if pack:packing.append({'original_path':name,'archived_path':saved,'original_bytes':len(content),'original_sha256':sha(p)})
(destination/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n')
lines=['# 482 Mac headless current 480 moderate QUIC profile','',
 'Original SSH handle 18694 and the owned Samply capture actually terminated with exit 0. Frozen validation, outcome conservation, recovery 8256=64+8192, retries, worker joins, active maintenance and six capacity/six provider readbacks pass. All conversion and symbol queries began after both hosts actually ended. No capture was repeated.', '',
 'The current 480 native opt3/debug0/assertions/overflow/defaultLTO+codegen binary serves three voters on one shared APFS device. Exact original/copy SHA256 fef05adc73179359ea2acc0d9b92978c57c4795410a727aefc30f1aac422fa52 and Mach-O UUID EDE59E8D-8928-3C4E-9F04-3AADBBEF5772 match. Existing Samply 0.13.1 records at 1000Hz with save-only/no-open. No browser, SVG, sudo/settings change, tool install, rebuild or presymbolication during capture. No 476 address maps.', '',
 '| Profiled diagnostic | Result |','|---|---:|',
 '| QUIC offered originals / rate / window / ready |8192 /128 per second /256 /16|',
 '| Applied / all refusal classes / unknown / pending |8192 /0 /0 /0|',
 '| Completed / during originals per second |127.821 /127.766|',
 '| Native elapsed / drain |64.089596s /0.089596s|',
 '| Backlog at horizon / peak in flight |15 /125|',
 '| Intended p50 / p95 / p99 / max ms |106.643 /346.095 /793.179 /976.217|',
 '| Service p99 / dispatch p99 ms |793.030 /0.214|',
 '| Checkpoints / reclaims |31 /93|',
 '| Max whole-fixture host poll ms |1.562|','',
 'These rates/tails are a profiled diagnostic, capped at 128 offered/s, not paired improvement, headroom, maximum capacity or sustained acceptance. This capture does not reproduce the worst 481 moderate QUIC tails; their all-three-pair regressions remain in the separate481 archive. WORKLOAD_READOUT.json retains successful-original intended/service and all-offer dispatch nearest-rank quantiles, individual outcomes and original native timer boundaries.', '',
 'Each threadCPUDelta is summed once, never multiplied by coalesced sample weight. The native PID has 13 threads, 139360 sample rows, 12103830µs observed CPU delta and 458696 wall sample weight. Positive-delta rows 110284; zero-delta rows 29076. The main thread accounts for 87.734% of observed delta. These are whole-launch interval observations including startup, warmup, measurement, recovery/retry verification, sorting and cleanup, not precisely cropped native throughput-timer CPU.', '',
 '| Visible sampled context | Observed CPU-delta µs | Share |','|---|---:|---:|',
 '| ClientRouter::reconcile inclusive |2402134|19.846%|',
 '| ClientRouter::cleanup inclusive, overlaps reconcile |443132|3.661%|',
 '| Pending-receipt BTreeMap::get self |827069|6.833%|',
 '| Pending lower-bound search self |463899|3.833%|',
 '| Pending range span search self |262081|2.165%|',
 '| Pending upper-bound search self |162369|1.341%|',
 '| ClientRouter::reconcile self |207721|1.716%|',
 '| Counter legacy+prepared admission union |4797|0.03963%|',
 '| Ready execution inclusive |4210|0.03478%|',
 '| Client submission inclusive |2230|0.01842%|','',
 'Exact Counter legacy-ready validation is 3142µs, submission 1062µs and prepared-admission contexts 593µs; their union 4797µs is counted once per sampled physical stack. Full self/inclusive/caller rankings, physical stacks, exact labels and request/response addresses are retained. Reconcile and its pending-map work are a source-audit candidate; this profile does not prove a safe transformation or a future speedup. Different diagnostic workloads prevent comparison of these percentages with 479 skew.', '',
 'Interval attribution has substantial bias. semaphore_timedwait_trap receives 5114093µs (42.252%) at sampled wait stacks; this is not CPU spent sleeping. Other prominent syscall leaves are fcntl 9.138%, recvfrom 8.835%, write 3.459% and sendto 3.271%; these are interval-to-current-stack assignments rather than isolated syscall execution costs. First main-thread sample appears at 376.501833ms with 11408µs delta. Coalesced wall weights and sample-row counts cannot rank CPU. Inclusive contexts overlap and must not be summed.', '',
 'Headless localhost symbol queries cover 4055 library/address pairs against current 480 and matching system UUIDs: 3836 resolve a function and 219 remain unknown. The owned server is interrupted and joined with exit 0. Unknown-leaf delta 154401µs (1.276%); any unknown ancestry 5455817µs (45.075%). Null/malformed stacks are zero. Native debug0 limits source/inline precision; unmarked unwind truncation/capture loss cannot be quantified. Absence of a visible frame is not proof of no cost. Linux uses a different loaded workload and cycles:u weighting; no cross-host percentage/rate comparison follows.', '',
 'Original commands, guards, cleanup, raw saved JSON.gz, all address requests/responses and CSV ledgers remain available. An automatic approval-review timeout occurred before the initial preparation process was created; its allowed identical-scope retry ended0. The first numerical readout used old ready-instantiation names; its source/outputs are retained under readout-attempt1, and a focused classifier correction recognizes 480 ready_pending. Raw profile, symbol responses and sampled weights were unchanged. These were preparation/analysis observations, not recording retries.', '',
 'PACKING.json records original hashes/lengths of packed large text; SHA256SUMS seals every payload. FileLogIo and ready 16 remain defaults. Full P9, third-filesystem acceptance, sustained framework floors, durable headroom, bounded memory/debt, fairness and independent placement remain open.']
(destination/'README.md').write_text('\n'.join(lines)+'\n')
manifest=''.join(f'{sha(p)}  {p.relative_to(destination)}\n'for p in sorted(destination.rglob('*'))if p.is_file())
(destination/'SHA256SUMS').write_text(manifest)
print(json.dumps({'archive':str(destination),'payloads':len(manifest.splitlines()),'packed':len(packing),'manifest_sha256':sha(destination/'SHA256SUMS')},indent=2))
