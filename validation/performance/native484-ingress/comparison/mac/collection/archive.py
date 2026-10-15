"""Seal original484 Mac campaign evidence once, without rewriting raw bytes."""
from pathlib import Path
import gzip
import hashlib
import json
import stat

job = Path(__file__).resolve().parent
base = job.parent.parent
raw = job / 'collected'
review = base / 'review'
dest = Path('validation/performance/native484-ingress/comparison/mac')
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
load = lambda p: json.loads(Path(p).read_text())
readout = load(review / 'actual484-mac/READOUT.json')
compact = load(job / 'COMPACT_READOUT.json')
transfer = load(job / 'LOCAL_TRANSFER_PROOF.json')
independent = load(review / 'independent-actual/RECEIPT.json')
assert readout['valid_cases'] == readout['planned_cases'] == 48 and not readout['failures']
assert readout['actual_terminal']['exit'] == 0
assert transfer['all_hashes_lengths_modes_exact'] and transfer['collected_originals'] == 1036
assert independent['actual_exit'] == 0 and independent['cases'] == 96
assert load(base / 'ROOT_QUIET_END.json')['mac']['actual_terminal_exit'] == 0
assert (review / 'actual484-mac.exit').read_text().strip() == '0'
assert sha(review / 'raw_readout.py') == '02209e3306d1fc1c1685dec733f7633b146154834df2a414b768ada03baaa516'
assert not dest.exists(), 'Never replace sealed evidence'
inputs = {}
def add(prefix, folder):
    for p in sorted(folder.rglob('*')):
        if p.is_file() and '__pycache__' not in p.parts:
            inputs[prefix + str(p.relative_to(folder))] = p
add('raw/', raw)
add('original-owner/', base / 'mac-campaign-owner')
add('readout/actual484-mac/', review / 'actual484-mac')
add('independent-review/', review / 'independent-actual')
for name in ['raw_readout.py', 'PREPARED_READOUT.json', 'actual484-mac.command.json', 'actual484-mac.stdout', 'actual484-mac.stderr', 'actual484-mac.exit', 'config_review.py', 'INDEPENDENT_CONFIG_REVIEW.json', 'config-review.stdout', 'config-review.stderr', 'config-review.exit']:
    inputs['readout/' + name] = review / name
for name in ['ROOT_QUIET_GO.json', 'ROOT_QUIET_END.json']:
    inputs['authority/' + name] = base / name
inputs['authority/SCHEMA.md'] = base / 'comparison/SCHEMA.md'
inputs['authority/PREPARED_SCRIPTS.json'] = base / 'comparison/PREPARED_SCRIPTS.json'
# Root-level collector and preparation receipts only: no duplicate transfer bundles,
# source/build/binary/WAL trees, and never this live archiver's output or receipt.
for p in sorted(job.iterdir()):
    if p.is_file() and p.name not in ['COLLECTED.tar.gz', 'transfer.tar', 'prepared.tar', 'ARCHIVE_RECEIPT.json', 'ARCHIVE_REVIEW.json', 'archive.stdout', 'archive.stderr', 'archive.exit']:
        inputs['collection/' + p.name] = p
selection = []
packing = []
dest.mkdir(parents=True)
for relative, p in sorted(inputs.items()):
    data = p.read_bytes()
    trailing = any(line.rstrip(b' \t') != line for line in data.splitlines()) or data.endswith(b'\n\n')
    packed = len(data) > 65536 or trailing
    saved = relative + '.gz' if packed else relative
    out = dest / saved
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(gzip.compress(data, mtime=0) if packed else data)
    mode = stat.S_IMODE(p.stat().st_mode)
    out.chmod(mode)
    assert (gzip.decompress(out.read_bytes()) if packed else out.read_bytes()) == data
    entry = {'original_path': relative, 'source_path': str(p), 'archived_path': saved, 'original_sha256': sha(p), 'original_bytes': len(data), 'original_mode': oct(mode), 'archived_sha256': sha(out)}
    selection.append(entry)
    if packed:
        packing.append(entry)
lines = ['# 484 Mac APFS scoped ingress comparison', '',
    'Original Mac controller 2749 terminated with exit 0 after all 48 original cases, validators, readback checks and post-guards. No case was retried or replaced. Collection began after both original hosts terminated. Baseline is published 480 f7b0ef12709bb7e7aad4f0279851aa035b508e9e; candidate is published 483 efd383dc36b4b9edf8f66021b90115824440b468. Both use WitnessLogIo and separately fresh native opt3/debug0/assertions/overflow/default-LTO-codegen artifacts with identical within-host compiler/tool identities. The separate mac qualification archive records fresh builds and functional qualification.', '',
    '| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms |',
    '|---|---|---:|---:|---:|---:|---:|---:|']
for c in compact['cells']:
    a,b = c['baseline'],c['candidate']
    lines.append(f"| {c['cell']} | {c['protocol']} | {a['completed_s']:.3f} | {b['completed_s']:.3f} | {a['during_s']:.3f} | {b['during_s']:.3f} | {a['p99_ms']:.3f} | {b['p99_ms']:.3f} |")
lines += ['',
    'These are three-trial arm medians. READOUT.json retains every trial, all 24 adjacent paired comparisons, all refusal classes, per-group latencies, physical deltas and resource scopes. All 391680 offers reconcile to 330860 applied and 60820 window refusals; other refusal/outcome classes are zero. Every case checks recovery, original-operation retries, worker joins, active maintenance and physical provider diagnostics. All actual exits are zero. All 1036 original transferred payloads have exact SHA256, length and mode verification.', '',
    'All six loaded pairs improve completed throughput and p99, but the candidate fails portable retention. Mac skew TCP loses completed throughput in all three pairs; skew QUIC loses two. All six cold-skew p99 pairs regress, while every cold case still applies all 256 originals with zero cold refusals. Cold TCP p99 median rises 383.173 to 854.009 ms; cold QUIC 411.649 to 809.077 ms. Moderate TCP candidate trial 2 has 67 window refusals and p99 ratio 2.770; moderate QUIC p99 worsens in two pairs. Singleton QUIC completed rate loses in all three pairs. Independent recomputation agrees with both hosts and records Linux 17/24 completed-rate losses. The candidate is rejected as a portable optimization; no universal gain claim follows.', '',
    'Singleton is 256 offers at 8/s/window1/ready16; moderate8192 at128/s/window256/ready16; loaded16000 at2000/s/window4096/ready64; skew8192 at1024/s/window4096/ready64/groups2/weights31:1 with2048 pending per group. All cases retain64 warmups, active2s maintenance and120s drain. Finite offered caps and horizons do not establish maximum sustained capacity, healthy-margin floors, memory/debt plateau or full P9 acceptance. Per-group rates share the whole-case elapsed boundary and cannot replace a one-group floor.', '',
    'All three voters and both skew groups share the M2 Max APFS physical device. Actual AC Power was captured before timing. Source, compiler, artifact, device and CPU guards are retained before/after. Snapshot-install evidence is aggregated on replica3 and cannot locate a particular delayed original. No missed-timer counter exists; late offer decisions are not timer misses. Whole-child CPU and raw RSS bytes include startup, warmup, drain, verification, recovery and cleanup. They are not timer-only attribution or a continuous memory plateau.', '',
    'Nested elapsed scopes overlap and cannot be summed as CPU or per-original waiting. Append units include metadata transitions and are not literal proposal widths. Ordinary publication primitives exclude cold selectors, repair and reclaim; encoded bytes are not drive-I/O bytes. There is no independent physical placement or ext4/XFS acceptance here.', '',
    'The first display-only compact helper field error is retained; it did not alter raw data or the frozen reader. Independent review retains its initial parser failure and focused correction. Original process streams, collection commands, errors, validators, source/tool guards and every slower trial remain available. No browser, images, profiling, policy changes or replacement workload was used for this readout.', '',
    'PACKING.json records lossless gzip of large or whitespace-sensitive originals. SELECTION.json binds every selected original hash, byte length and mode to its archived representation. SHA256SUMS seals all payloads recursively. No source trees, executables, build trees or generated WAL are included.']
(dest / 'README.md').write_text('\n'.join(lines) + '\n')
(dest / 'PACKING.json').write_text(json.dumps(packing, indent=2) + '\n')
(dest / 'SELECTION.json').write_text(json.dumps(selection, indent=2) + '\n')
manifest = ''.join(f'{sha(p)}  {p.relative_to(dest)}\n' for p in sorted(dest.rglob('*')) if p.is_file())
(dest / 'SHA256SUMS').write_text(manifest)
receipt = {'exit': 0, 'archive': str(dest), 'payloads': len(manifest.splitlines()), 'packed_originals': len(packing), 'selected_originals': len(selection), 'raw_originals':1036, 'manifest_sha256': sha(dest / 'SHA256SUMS'), 'archived_bytes':sum(p.stat().st_size for p in dest.rglob('*') if p.is_file()), 'actual_original_handle':2749, 'actual_original_exit':0, 'all48_cases_validators_readbacks_exit':0, 'readout_sha256':sha(review / 'actual484-mac/READOUT.json'), 'independent_receipt_sha256':sha(review / 'independent-actual/RECEIPT.json'), 'no_new_workloads':True}
(job / 'ARCHIVE_RECEIPT.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
