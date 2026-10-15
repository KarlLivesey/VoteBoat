"""Maintenance interval union after both original campaigns ended; no workloads."""
import csv, hashlib, json, statistics
from pathlib import Path

base = Path(__file__).resolve().parent
records = []
inputs = {}
for host, root in [('linux', base/'linux3'), ('mac', base/'mac-collection/raw')]:
    campaign = json.loads((root/'campaign/CAMPAIGN.json').read_text())
    for entry in campaign['cases']:
        path = Path(entry['wal_root']) if host == 'linux' else root/'results'/entry['name']
        source = path/'maintenance.csv'
        inputs[str(source)] = hashlib.sha256(source.read_bytes()).hexdigest()
        rows = list(csv.DictReader(source.open()))
        valid = [r for r in rows if r['status'] == 'completed']
        intervals = sorted((int(r['submitted_ns']), int(r['completed_ns'])) for r in valid)
        union = 0
        start = end = 0
        for left, right in intervals:
            if left > end:
                union += end-start
                start, end = left, right
            else:
                end = max(end, right)
        union += end-start
        maximum = lambda kind: max((int(r['completed_ns'])-int(r['submitted_ns']))/1e6 for r in valid if r['kind'] == kind)
        records.append({'host': host, 'case': entry['name'], 'union_ms': union/1e6,
                        'checkpoint_max_ms': maximum('checkpoint'), 'reclaim_max_ms': maximum('reclaim'),
                        'completed': len(valid)})
result = {'scope': 'Union of submitted-to-completed maintenance intervals across all three replicas; overlapping waits, never additive CPU or isolated disk time.',
          'inputs': inputs, 'records': records}
(base/'INDEPENDENT_COLD_WORK.json').write_text(json.dumps(result, indent=2)+'\n')
for name in ['t1-moderate-quic-atomic', 't1-moderate-quic-witness', 't3-moderate-quic-atomic', 't3-moderate-quic-witness']:
    print(next(r for r in records if r['host'] == 'mac' and r['case'] == name))
for host in ['linux', 'mac']:
    for protocol in ['tcp', 'quic']:
        selected = [r for r in records if r['host'] == host and '-bulk-'+protocol+'-witness' in r['case']]
        print(host, protocol, {field: statistics.median(r[field] for r in selected)
                              for field in ['union_ms', 'checkpoint_max_ms', 'reclaim_max_ms']})
