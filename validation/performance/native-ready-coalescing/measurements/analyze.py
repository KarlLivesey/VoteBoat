"""Summarize every accepted native414 trial without selecting or rerunning cases."""
import json
import statistics
import sys
from pathlib import Path

raw, output = map(Path, sys.argv[1:])
result = json.loads((raw / 'RESULTS.json').read_text())
assert len(result['cases']) == 24
assert all(result[k] for k in ('sourceUnchanged', 'artifactsUnchanged', 'benchmarkSourceIdentical'))
rows = []
for protocol in ('tcp', 'quic'):
    for window in (32, 1):
        cell = {'protocol': protocol, 'window': window, 'variants': {}, 'pairs': []}
        for variant in ('reference', 'candidate'):
            cases = [c for c in result['cases'] if c['protocol'] == protocol
                     and c['window'] == window and c['variant'] == variant]
            assert len(cases) == 3 and {c['trial'] for c in cases} == {1, 2, 3}
            for c in cases:
                assert c['completed'] == 256
                assert c['summary']['recovered_value'] == '320'
                assert c['summary']['retry_verified'] == c['summary']['workers_joined'] == 'true'
                assert c['histograms']['measurement']['weightedOriginals'] == 256
            med = lambda field: statistics.median(float(c['summary'][field]) for c in cases)
            cell['variants'][variant] = {
                'median_completed_writes_s': med('applied_ops_s'),
                'median_p50_us': med('p50_us'), 'median_p99_us': med('p99_us'),
                'trials': [{
                    'trial': c['trial'], 'completed_writes_s': float(c['summary']['applied_ops_s']),
                    'p99_us': float(c['summary']['p99_us']),
                    'measurement_histogram': c['histograms']['measurement'],
                    'physical': c['physical'],
                } for c in sorted(cases, key=lambda c: c['trial'])],
                'median_leader_sync_calls': statistics.median(c['physical'][0]['sync_calls'] for c in cases),
                'median_leader_publish_calls': statistics.median(c['physical'][0]['publish_calls'] for c in cases),
                'median_leader_sync_total_ms': statistics.median(c['physical'][0]['sync_ns'] / 1e6 for c in cases),
                'median_leader_publish_total_ms': statistics.median(c['physical'][0]['publish_ns'] / 1e6 for c in cases),
            }
        for trial in (1, 2, 3):
            pair = {c['variant']: c for c in result['cases'] if c['protocol'] == protocol
                    and c['window'] == window and c['trial'] == trial}
            cell['pairs'].append({
                'trial': trial,
                'candidate_reference_completed_ratio': float(pair['candidate']['summary']['applied_ops_s'])
                    / float(pair['reference']['summary']['applied_ops_s']),
                'candidate_reference_p99_ratio': float(pair['candidate']['summary']['p99_us'])
                    / float(pair['reference']['summary']['p99_us']),
            })
        cell['median_paired_completed_ratio'] = statistics.median(
            p['candidate_reference_completed_ratio'] for p in cell['pairs'])
        cell['median_paired_p99_ratio'] = statistics.median(
            p['candidate_reference_p99_ratio'] for p in cell['pairs'])
        rows.append(cell)
output.write_text(json.dumps({
    'scope': result['scope'], 'cases': 24, 'original_measured_completions': 6144,
    'valid_campaign': True, 'method': 'median of three trials; paired ratios before aggregation; all originals retained',
    'limitations': ['three replicas share one host/device; 256 measured originals and 64 warmup per trial',
                    'three pairs are a focused before/after campaign, not a capacity sweep or confidence interval',
                    'physical I/O timing is substantial and varies across trials; no queue-selection-cause tracing',
                    'RPL code and diagnostic are compared only; no OpenRaft matched-floor claim'],
    'cells': rows,
}, indent=2) + '\n')
