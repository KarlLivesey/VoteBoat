"""Losslessly seal actual Mac481 evidence; never replace a sealed archive."""
from pathlib import Path
import gzip
import hashlib
import json

job = Path(__file__).resolve().parent
base = job.parent
raw = job / 'raw'
destination = Path('validation/performance/native481-admission/mac')
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
load = lambda p: json.loads(Path(p).read_text())
analysis = load(job / 'ANALYSIS.json')
config = load(raw / 'CAMPAIGN_CONFIG.json')
collected = load(raw / 'COLLECTED.json')
assert analysis['cases'] == analysis['validators'] == analysis['readback_checks'] == 48
assert analysis['actual_exit'] == 0 and analysis['source_device_cpu_guards_match']
assert (base / 'mac/live/controller.exit').read_text().strip() == '0'
assert load(base / 'ROOT_QUIET_END.json')['mac']['actual_terminal_exit'] == 0
independent = load(base / 'independent-readout-receipt.json')
assert independent['exit'] == 0 and independent['cases'] == 96 and independent['failures'] == 0
for name, record in independent['files'].items():
    assert sha(base / name) == record['sha256']
assert not destination.exists(), 'Refuse replacing sealed evidence'

payloads = {str(p.relative_to(raw)): p for p in raw.rglob('*') if p.is_file()}
for p in job.iterdir():
    if p.is_file() and p.name not in ['COLLECTED.tar.gz', 'ARCHIVE_REVIEW.json', 'archive.stdout', 'archive.stderr', 'archive.exit']:
        payloads['collection/' + p.name] = p
for p in (base / 'mac').rglob('*'):
    if p.is_file() and p.suffix != '.tar' and '__pycache__' not in p.parts:
        payloads['preparation/mac/' + str(p.relative_to(base / 'mac'))] = p
for p in base.iterdir():
    if p.is_file():
        prefix = 'independent-review/' if p.name.startswith('independent-') else 'preparation/'
        payloads[prefix + p.name] = p
for folder in ['common', 'setup-initial-authority-path', 'linux/setup-before-mac-scope-fix']:
    for p in (base / folder).rglob('*'):
        if p.is_file() and '__pycache__' not in p.parts:
            payloads['preparation/' + folder + '/' + str(p.relative_to(base / folder))] = p

proof = {
    'scope': '48 original Mac481 published 476/480 Witness paired processes; original 81015 actually0, every process/validator/readback 0, no retry/replacement. Both hosts actually ended before collection. No full P9, sustained 20M, default promotion or three-filesystem acceptance.',
    'source_commits': analysis['source_commits'],
    'native_executable_sha256': analysis['artifacts_sha256'],
    'native_profile': 'Separate genuinely fresh 476/480 opt3/debug0/assertions+overflow/default LTO+codegen artifacts, same within-host compiler/tool bytes. Compiled candidate remains f7b0ef1 despite later evidence commits.',
    'qualified_source_files': {k: len(v['source']['files']) for k, v in config['candidates'].items()},
    'source_device_cpu_guards_before_after_exact': True,
    'raw_transfer_members': len(collected['files']),
    'raw_transfer_manifest_sha256': sha(raw / 'COLLECTED.json'),
    'totals': analysis['totals'],
    'maintenance_status_counts': analysis['maintenance_status_counts'],
    'latency_convention': 'Nearest rank ceil(n*p/100)-1. Intended/service successful originals; dispatch all offers. Per-group completed rates share whole-case elapsed boundary; during rates use offered horizon.',
    'timer_limit': '185 late offer decisions retained; no explicit missed-timer counter. Late decisions are not timer misses.',
    'counter_scope': 'Ordinary provider primitives exclude cold repair, selectors and reclamation. Metadata-inclusive command/unit means are not proposal widths; codec bytes are not drive I/O. Nested elapsed scopes are not additive CPU.',
    'resource_scope': 'Whole child including warmup/recovery/cleanup; Mac RSS raw bytes, not continuous memory/debt plateau.',
    'host': 'M2 Max, APFS Data volume/container/internal SSD shared by all voters and skew groups. stable1.98.0/LLVM22.1.8/protoc3.21.12. Linux1.98.1 is a separate within-host comparison.',
    'transfer_scope': 'Only post-terminal text/CSV/JSON; no binaries, source/build trees or generated WAL transferred. Original stores remain on Mac.',
    'failure_scope': 'All48 workload/validator/readback exits0; no timing observation errors. Shared initial preparation authority-path failure and prestart guard-revision records retained. Mac480 qualification initial 101, resume 0 and separate runtime-scope append 0 are preserved in the separately sealed native480-admission/mac archive; they are not relabeled as one successful scope.',
    'independent_review': {'cases': 96, 'exit': 0, 'input_files': independent['input_files'], 'receipt_sha256': sha(base / 'independent-readout-receipt.json')},
}
(job / 'ARCHIVE_PROOF.json').write_text(json.dumps(proof, indent=2) + '\n')
payloads['collection/ARCHIVE_PROOF.json'] = job / 'ARCHIVE_PROOF.json'

lines = ['# 481 Mac APFS admission comparison', '',
    'Original SSH handle 81015 actually terminated with exit 0 after ALL_TIMING_END. All 48 native processes, frozen validators and readback checks passed; no case was retried or replaced. Collection began only after both original hosts terminated. Baseline is published 476 (7e9068296ce00dde5bf3e03bf4d852ed2833d185); candidate is published 480 (f7b0ef12709bb7e7aad4f0279851aa035b508e9e). Both explicitly select WitnessLogIo, with separately fresh native artifacts and identical within-host compiler/profile/tool settings.', '',
    '| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms | Baseline window refusals | Candidate window refusals |',
    '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for cell in analysis['cells']:
    a, b = cell['arms']['baseline'], cell['arms']['candidate']
    lines.append(f"| {cell['cell']} | {cell['protocol']} | {a['completed_originals_s']['median']:.3f} | {b['completed_originals_s']['median']:.3f} | {a['during_originals_s']['median']:.3f} | {b['during_originals_s']['median']:.3f} | {a['intended_latency']['p99_ms']['median']:.3f} | {b['intended_latency']['p99_ms']['median']:.3f} | {a['refusals']['window_refused']['median']:.0f} | {b['refusals']['window_refused']['median']:.0f} |")
lines += ['',
    'Three-trial arm medians, including refused offers rather than removing failed admissions. ANALYSIS.json retains every raw-recomputed trial and all 24 paired comparisons, intended/service/dispatch p50/p95/p99/max, completed/during/drain rates, all refusal classes, per-group backlog, resources, physical deltas and cold maintenance. Singleton is 256 offers at 8/s, window 1, ready 16; moderate 8192 at 128/s, window 256, ready 16; loaded 16000 at 2000/s, window 4096, ready 64; skew 8192 at 1024/s, window 4096, ready 64, two groups 31:1 with 2048 pending/group fixture bound. Every case has 64 warmups and active 2s maintenance. These finite offered caps are not maximum capacity, spare headroom or sustained 20M acceptance.', '',
    'All 391680 offers reconcile to 296506 applied plus 95174 window refusals. Other no-leader/admission-refused/not-proposed/unknown/pending classes are zero. Every case recovers 64+applied, verifies original-operation retries and joins workers. Maintenance completes 624 checkpoints and 1872 reclaims. Every actual group has leader replica 1. All voters and both skew groups share the same physical APFS device.', '',
    'All six loaded pairs improve completed throughput and p99. TCP medians rise 794.133 to 1065.505 completed originals/s and QUIC 809.012 to 1177.929/s. TCP refusals fall from 8230/7597/8383 to 6821/5279/5631; QUIC 8742/7653/8035 to 3989/3753/4579. Both still refuse many offers and drain after the horizon, so the 2000/s offered rate is not established useful capacity.', '',
    'Regressions are retained. Moderate QUIC intended p99 worsens in all three pairs (ratios 2.541/1.753/2.136); its median rises 779.149 to 1664.171ms and candidate trial 1 adds 38 window refusals versus zero baseline. Moderate TCP p99 worsens in two pairs. Singleton TCP first pair loses 1.25% completed rate; median throughput stays 7.406/s while p99 rises 158.366 to 160.100ms. Skew TCP completed throughput loses in two pairs and its median falls 754.503 to 738.781/s.', '',
    '| Skew group | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms | Baseline median refused | Candidate median refused |',
    '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for cell in analysis['cells']:
    if cell['cell'] != 'skew':
        continue
    for group in ['1', '2']:
        a, b = (cell['arms'][arm]['groups'][group] for arm in ['baseline', 'candidate'])
        name = 'Hot 7936 offers' if group == '1' else 'Cold 256 offers'
        lines.append(f"| {name} | {cell['protocol']} | {a['completed_originals_s']['median']:.3f} | {b['completed_originals_s']['median']:.3f} | {a['during_originals_s']['median']:.3f} | {b['during_originals_s']['median']:.3f} | {a['intended_latency']['p99_ms']['median']:.3f} | {b['intended_latency']['p99_ms']['median']:.3f} | {a['refusals']['window_refused']['median']:.0f} | {b['refusals']['window_refused']['median']:.0f} |")
lines += ['',
    'Every cold skew case applies all 256 offers with zero cold refusals. Cold TCP p99 nevertheless worsens in every pair: 454.874/454.299/923.457 to 761.284/835.434/997.624ms, median 454.874 to 835.434ms. Cold QUIC p99 improves in all three pairs, median 786.090 to 434.037ms. Per-group completed rates share the hot-drain elapsed denominator; they are not independent cold capacity. Hot candidate refusals remain TCP 1373/1501/1900 and QUIC 1200/806/1009, with hot backlog approaching the 2048/group ceiling.', '',
    'Physical work remains visible. Loaded TCP leader barrier wall/native elapsed medians rise 0.347 to 0.502, with metadata-inclusive commands/unit 27.107 to 27.400 and barriers/completed original 0.03689 to 0.03650. Loaded QUIC fractions 0.352 to 0.546, commands/unit 28.345 to 26.394 and barriers/original 0.03528 to 0.03789. Skew barrier fractions remain about 0.89–0.90; whole-child CPU falls roughly 0.42 to 0.32 core-equivalent for TCP and 0.44 to 0.35 for QUIC. These overlapping elapsed fractions are not CPU attribution. Resources include warmup/recovery/cleanup. Ordinary namespace counters exclude cold selectors/reclaim/repair; append-unit counts include metadata and are not literal proposal widths, and codec bytes are not drive I/O.', '',
    '185 late offer decisions and all dispatch distributions are retained. No explicit missed-timer count exists. Mac RSS is whole-process bytes, not a continuous memory/debt plateau. Shared preparation failures, superseded guard records, actual controller commands/streams and all 48 resource/validator/readback receipts remain available. The separate native480-admission/mac archive preserves qualification initial 101, resume 0 and missing runtime-scope append 0 distinctly.', '',
    'Large originals are losslessly packed with original hashes/lengths in PACKING.json; SHA256SUMS seals every payload. No source trees, binaries, build trees or generated WAL are included. FileLogIo and ready 16 remain defaults. No full P9, sustained acceptance, ext4/XFS acceptance or independent physical placement claim follows.']
destination.mkdir(parents=True)
(destination / 'README.md').write_text('\n'.join(lines) + '\n')
packing = []
for relative, original in sorted(payloads.items()):
    content = original.read_bytes()
    saved = relative + '.gz' if len(content) > 65536 else relative
    target = destination / saved
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(gzip.compress(content, mtime=0) if saved != relative else content)
    assert (gzip.decompress(target.read_bytes()) if saved != relative else target.read_bytes()) == content
    if saved != relative:
        packing.append({'original_path': relative, 'archived_path': saved, 'original_bytes': len(content), 'original_sha256': sha(original)})
(destination / 'PACKING.json').write_text(json.dumps(packing, indent=2) + '\n')
manifest = ''.join(f'{sha(p)}  {p.relative_to(destination)}\n' for p in sorted(destination.rglob('*')) if p.is_file())
(destination / 'SHA256SUMS').write_text(manifest)
print(json.dumps({'archive': str(destination), 'payloads': len(manifest.splitlines()), 'packed': len(packing), 'manifest_sha256': sha(destination / 'SHA256SUMS')}, indent=2))
