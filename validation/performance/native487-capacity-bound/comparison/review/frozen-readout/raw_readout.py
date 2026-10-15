"""Independent saved-ledger readout; never launches a workload or a converter.

Adapted from481's independent raw reader, retaining per-original arithmetic.
Requires actual both-host END and matching quiet-GO/config bindings before I/O.
"""
import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path
import statistics
import sys
import traceback

sys.dont_write_bytecode = True
STATES = ['applied', 'window_refused', 'no_leader', 'admission_refused', 'not_proposed', 'unknown', 'pending']
STAGES = ['before_measurement', 'after_measurement', 'after_create_join', 'after_recover_join']
FIELDS = ['append_calls', 'append_units', 'append_commands', 'append_ns', 'barrier_calls', 'barrier_tickets', 'barrier_ns', 'errors', 'encoded_bytes', 'io_append_calls', 'io_append_ns', 'sync_calls', 'sync_ns', 'publish_calls', 'publish_ns']
PRIMITIVES = ['open', 'write', 'file_sync', 'rename', 'directory_sync']
COMMITS = {'reference481': {'baseline': '7e9068296ce00dde5bf3e03bf4d852ed2833d185', 'candidate': 'f7b0ef12709bb7e7aad4f0279851aa035b508e9e'}, 'campaign484': {'baseline': 'f7b0ef12709bb7e7aad4f0279851aa035b508e9e', 'candidate': 'efd383dc36b4b9edf8f66021b90115824440b468'}, 'campaign487': {'baseline': 'f7b0ef12709bb7e7aad4f0279851aa035b508e9e', 'candidate': 'dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69'}}
MATRIX = {'singleton': (16, 256, 8, 1, 1, None), 'moderate': (16, 8192, 128, 256, 1, None), 'loaded': (64, 16000, 2000, 4096, 1, None), 'skew': (64, 8192, 1024, 4096, 2, '31,1')}
inputs = {}


def read(path):
    path = Path(path)
    raw = path.read_bytes()
    inputs[str(path.resolve())] = {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw), 'mode': oct(path.stat().st_mode & 0o7777)}
    return raw.decode()


def obj(path):
    return json.loads(read(path))


def table(path):
    return list(csv.DictReader(read(path).splitlines()))


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def ratio(a, b):
    return a / b if a is not None and b else None


def quantiles(values):
    ordered = sorted(values)
    if not ordered:
        return {**{f'p{p}_ms': None for p in [50, 95, 99]}, 'max_ms': None, 'count': 0}
    return {**{f'p{p}_ms': ordered[(len(ordered) * p + 99) // 100 - 1] / 1e6 for p in [50, 95, 99]}, 'max_ms': ordered[-1] / 1e6, 'count': len(ordered)}


def distributions(rows):
    applied = [r for r in rows if r['status'] == 'applied']
    return {'intended': quantiles([r['completed_ns'] - r['intended_ns'] for r in applied]), 'service': quantiles([r['completed_ns'] - r['dispatched_ns'] for r in applied]), 'dispatch': quantiles([r['dispatched_ns'] - r['intended_ns'] for r in rows])}


def stats(values):
    known = [v for v in values if v is not None]
    return {'trials': values, 'available': len(known), 'median': statistics.median(known) if known else None, 'min': min(known) if known else None, 'max': max(known) if known else None}


def histogram(text):
    return {int(k): int(v) for k, v in (item.split(':') for item in text.split('|') if item)}


def diff(before, after, keys):
    result = {k: int(after.get(k, 0)) - int(before.get(k, 0)) for k in keys}
    assert all(v >= 0 for v in result.values())
    return result


def readbacks(lines, saved):
    expected = {(phase, str(replica)) for phase in ['Create', 'Recover'] for replica in [1, 2, 3]}
    actual = {}
    for prefix in ['capacity_readback', 'publication_readback']:
        selected = [line for line in lines if line.startswith(prefix + ' ')]
        rows = [dict(item.split('=', 1) for item in line.split()[1:]) for line in selected]
        assert len(rows) == 6 and {(r['mode'], r['replica']) for r in rows} == expected
        required = ({'offered_capacity_requested': '4096', 'offered_capacity_effective': '4096', 'client_requests': '4096', 'client_group_requests': '4096', 'client_bytes': '33554432', 'client_receipt_bytes': '1048576', 'shard_max_items': '8192', 'shard_control_items': '1024', 'shard_group_items': '4112', 'shard_group_control_items': '16', 'shard_group_bytes': '4194304'} if prefix == 'capacity_readback' else {'publication_requested': 'witness', 'publication_effective': 'witness', 'publication_provider': 'WitnessLogIo', 'publication_timings': 'native', 'publication_scope': 'ordinary_barrier', 'storage_error_scope': 'overlapping_observations'})
        assert all(all(r[k] == v for k, v in required.items()) for r in rows)
        actual[prefix] = selected
    assert actual == saved
    return actual


def analyze(case, args, job):
    name = case['name']
    wal = args.case_root / name if args.case_root else Path(case['wal_root'])
    meta = args.resource_root / name if args.resource_root else Path(case['metadata'])
    prefix = job / 'campaign' / name
    assert case['exit'] == case['validator_exit'] == case['readbacks_exit'] == 0
    for suffix in ['.exit', '-check.exit']:
        assert read(prefix.with_name(name + suffix)).strip() == '0'
    assert obj(prefix.with_name(name + '.native-command.json')) == case['command']
    assert obj(prefix.with_name(name + '.command.json')) == case['resource_command']
    summary_text = read(wal / 'summary.txt')
    assert summary_text == read(prefix.with_name(name + '.stdout'))
    s = dict(item.split('=', 1) for item in summary_text.split())
    run = dict(item.split('=', 1) for item in read(wal / 'run.txt').split())
    n = lambda key: int(s[key])
    assert tuple(case[k] for k in ['ready_items', 'offers', 'rate', 'window', 'groups', 'weights']) == MATRIX[case['cell']]
    for key, value in [('offers', case['offers']), ('offered_rate', case['rate']), ('window', case['window']), ('groups', case['groups'])]:
        assert s[key] == run[key] == str(value)
    assert s['publication_requested'] == s['publication_effective'] == run['publication_effective'] == 'witness'
    assert s['publication_provider'] == 'WitnessLogIo' and s['publication_timings'] == 'native'
    assert n('warmup') == 64 and n('payload_bytes') == 8 and n('replicas') == 3
    assert n('ready_visit_items_requested') == n('ready_visit_items_effective') == case['ready_items']
    assert n('core_proposal_entries') == 64 and n('ready_visit_bytes') == 1048576 and n('ready_visit_ms') == 2 and s['ready_batch_capable'] == 'true'
    command = case['command']
    for flag, value in [('--publication', 'witness'), ('--ready-items', str(case['ready_items'])), ('--offered', str(case['rate'])), ('--maintenance', '2'), ('--offered-capacity', '4096')]:
        assert command[command.index(flag) + 1] == value
    assert s['leader_placement'] == ','.join(f'{g}:1' for g in range(1, case['groups'] + 1))
    if case['weights']:
        assert s['workload_group_weights'] == run['workload_group_weights'] == case['weights']
        assert s['workload_group_phase'] == run['workload_group_phase'] == 'absolute_operation_minus_one'
    else:
        assert 'workload_group_weights' not in s and '--group-weights' not in command
    readbacks(read(prefix.with_name(name + '.stderr')).splitlines(), obj(prefix.with_name(name + '.readbacks.json')))
    rows = table(wal / 'offers.csv')
    numeric = ['operation', 'group', 'intended_ns', 'dispatched_ns', 'completed_ns', 'replica', 'sequence', 'applied_index', 'value']
    assert len(rows) == case['offers']
    for index, row in enumerate(rows):
        row.update({k: int(row[k]) for k in numeric})
        assert row['operation'] == 65 + index and row['intended_ns'] == index * 1000000000 // case['rate']
        assert row['group'] == (2 if case['groups'] == 2 and (row['operation'] - 1) % 32 == 31 else 1)
        assert row['intended_ns'] <= row['dispatched_ns'] <= row['completed_ns'] <= n('elapsed_ns')
        assert row['status'] in STATES
    assert all(b['dispatched_ns'] >= a['dispatched_ns'] for a, b in zip(rows, rows[1:]))
    status = collections.Counter(row['status'] for row in rows)
    assert all(status[k] == n(k) for k in STATES)
    applied = [r for r in rows if r['status'] == 'applied']
    admitted = [r for r in rows if r['sequence']]
    assert len(admitted) == n('admitted') == len(applied) + n('not_proposed')
    assert len({(r['replica'], r['sequence']) for r in admitted}) == len(admitted)
    assert n('unknown') == n('pending') == 0 and n('recovered_value') == 64 + len(applied)
    assert s['workers_joined'] == s['retry_verified'] == 'true'
    horizon, elapsed, drain = n('horizon_ns'), n('elapsed_ns'), n('drain_ns')
    assert horizon == case['offers'] * 1000000000 // case['rate'] and elapsed - horizon == drain
    during = sum(r['completed_ns'] < horizon for r in applied)
    assert during == n('applied_during') and len(applied) - during == n('applied_drain')
    assert sum(r['dispatched_ns'] < horizon for r in admitted) == n('admitted_during')
    assert sum(r['dispatched_ns'] >= horizon for r in rows) == n('late_decisions')
    backlog = lambda selected: sum(r['dispatched_ns'] < horizon <= r['completed_ns'] for r in selected if r['sequence'])
    assert backlog(rows) == n('backlog_at_horizon')
    live = peak = 0
    group_live, group_peak = collections.Counter(), collections.Counter()
    ceiling = (case['window'] + case['groups'] - 1) // case['groups']
    events = [(r['dispatched_ns'], 0, i, r) for i, r in enumerate(rows)] + [(r['completed_ns'], -1, i, r) for i, r in enumerate(admitted)]
    for _, kind, _, r in sorted(events, key=lambda event: event[:3]):
        group = r['group']
        if kind == 0 and r['status'] == 'window_refused':
            assert live >= case['window'] or group_live[group] >= ceiling
            continue
        if kind == 0 and not r['sequence']:
            continue
        change = -1 if kind == -1 else 1
        live += change
        group_live[group] += change
        assert 0 <= live <= case['window'] and 0 <= group_live[group] <= ceiling
        peak, group_peak[group] = max(peak, live), max(group_peak[group], group_live[group])
    assert live == 0 and not any(group_live.values()) and peak == n('max_inflight')
    latencies = distributions(rows)
    for key, field in [('intended', 'p99_us'), ('service', 'service_p99_us'), ('dispatch', 'dispatch_p99_us')]:
        if latencies[key]['p99_ms'] is None:
            assert s[field] == 'NA'
        else:
            assert abs(latencies[key]['p99_ms'] - float(s[field]) / 1000) <= .00000051
    rates = {'completed_ops_s': len(applied) * 1e9 / elapsed, 'during_ops_s': during * 1e9 / horizon, 'drain_ops_s': ratio((len(applied) - during) * 1e9, drain)}
    for key, field in [('completed_ops_s', 'applied_total_ops_s'), ('during_ops_s', 'applied_during_ops_s')]:
        assert abs(rates[key] - float(s[field])) <= .00051
    groups = {}
    for group in range(1, case['groups'] + 1):
        offered = [r for r in rows if r['group'] == group]
        done = [r for r in applied if r['group'] == group]
        ordered = sorted(done, key=lambda r: r['applied_index'])
        assert all(r['value'] == 64 // case['groups'] + 1 + i for i, r in enumerate(ordered))
        assert all(b['applied_index'] > a['applied_index'] for a, b in zip(ordered, ordered[1:]))
        if case['groups'] == 2:
            assert len(offered) == (7936 if group == 1 else 256) and ceiling == 2048
        gd = sum(r['completed_ns'] < horizon for r in done)
        groups[str(group)] = {'offers': len(offered), 'completed': len(done), 'during': gd, 'drain': len(done) - gd, 'completed_ops_s': len(done) * 1e9 / elapsed, 'during_ops_s': gd * 1e9 / horizon, 'drain_ops_s': ratio((len(done) - gd) * 1e9, drain), 'statuses': {k: sum(r['status'] == k for r in offered) for k in STATES}, 'backlog_at_horizon': backlog(offered), 'max_inflight': group_peak[group], 'pending_ceiling': ceiling, 'quantiles': distributions(offered), 'last_applied_value': ordered[-1]['value'] if ordered else 64 // case['groups']}
    storage = table(wal / 'storage.csv')
    by_stage = {(r['stage'], r['replica']): r for r in storage}
    assert len(storage) == len(by_stage) == 12
    physical = {}
    for replica in ['1', '2', '3']:
        before, after, joined = (by_stage[(stage, replica)] for stage in STAGES[:3])
        d, broad = diff(before, after, FIELDS), diff(before, joined, FIELDS)
        assert all(broad[k] >= d[k] for k in FIELDS) and d['errors'] == broad['errors'] == 0
        for field in ['batch_histogram', 'group_units']:
            a, b = histogram(before[field]), histogram(after[field])
            d[field] = diff(a, b, set(a) | set(b))
        assert sum(d['batch_histogram'].values()) == d['append_calls']
        assert sum(k * v for k, v in d['batch_histogram'].items()) == d['append_units']
        physical[replica] = {'measurement_delta': d, 'after_create_join_delta': broad, 'after_recover_join_absolute': by_stage[('after_recover_join', replica)], 'barriers_per_completed_original': ratio(d['barrier_calls'], len(applied)), 'mean_barrier_ms': ratio(d['barrier_ns'], d['barrier_calls'] * 1e6), 'mean_wal_sync_ms': ratio(d['sync_ns'], d['sync_calls'] * 1e6), 'mean_publication_ms': ratio(d['publish_ns'], d['publish_calls'] * 1e6), 'commands_per_append_unit': ratio(d['append_commands'], d['append_units']), 'units_per_append_call': ratio(d['append_units'], d['append_calls'])}
    primitive = {}
    publications = table(wal / 'publication.csv')
    assert len(publications) == 60
    for replica in ['1', '2', '3']:
        primitive[replica] = {}
        for step in PRIMITIVES:
            rs = {r['stage']: r for r in publications if r['replica'] == replica and r['step'] == step}
            assert set(rs) == set(STAGES)
            d = diff(rs['before_measurement'], rs['after_measurement'], ['calls', 'errors', 'elapsed_ns', 'manifest_calls', 'manifest_errors', 'manifest_ns'])
            assert d['errors'] == d['manifest_errors'] == 0
            if step in ['open', 'rename', 'directory_sync']:
                assert d['calls'] == d['elapsed_ns'] == 0
            primitive[replica][step] = {'measurement_delta': d, 'after_create_join': rs['after_create_join'], 'after_recover_join': rs['after_recover_join']}
    maintenance = table(wal / 'maintenance.csv')
    counts = collections.Counter((r['kind'], r['status']) for r in maintenance)
    assert counts[('checkpoint', 'completed')] == n('checkpoints') and counts[('reclaim', 'completed')] == n('reclaims') and counts[('skip', 'skipped')] == n('maintenance_skips')
    reclaim = sum(max(0, int(r['before_bytes']) - int(r['after_bytes'])) for r in maintenance if (r['kind'], r['status']) == ('reclaim', 'completed'))
    assert reclaim == n('reclaimed_bytes')
    cold = {kind: {'count': counts[(kind, 'completed')], 'queue_plus_operation_ms': sum(int(r['completed_ns']) - int(r['intended_ns']) for r in maintenance if (r['kind'], r['status']) == (kind, 'completed')) / 1e6, 'submitted_to_complete_ms': sum(int(r['completed_ns']) - int(r['submitted_ns']) for r in maintenance if (r['kind'], r['status']) == (kind, 'completed')) / 1e6} for kind in ['checkpoint', 'reclaim']}
    bases = table(wal / 'bases.csv')
    mapped = {(r['stage'], r['replica'], r['group']): int(r['base_index']) for r in bases}
    assert len(bases) == len(mapped) == 6 * case['groups']
    assert all(mapped[('before_close', str(replica), str(group))] == mapped[('after_recover', str(replica), str(group))] for replica in [1, 2, 3] for group in range(1, case['groups'] + 1))
    resource = obj(meta / 'RESOURCES.json')
    assert resource['exit'] == resource['helper_exit'] == 0 and resource['helper_error'] is None
    assert resource['max_rss_unit'] == ('KiB' if args.host == 'linux' else 'bytes')
    assert resource['max_rss_bytes'] == resource['max_rss_raw'] * (1024 if args.host == 'linux' else 1)
    resource['cpu_us_per_completed_original'] = ratio((resource['user_seconds'] + resource['system_seconds']) * 1e6, len(applied))
    return {'case': name, 'pair': case['pair'], 'trial': case['trial'], 'cell': case['cell'], 'protocol': case['protocol'], 'arm': case['arm'], 'offers': len(rows), 'completed': len(applied), 'statuses': {k: status[k] for k in STATES}, 'rates': rates, 'quantiles': latencies, 'groups': groups, 'backlog_at_horizon': backlog(rows), 'max_inflight': peak, 'elapsed_ns': elapsed, 'horizon_ns': horizon, 'drain_ns': drain, 'leader_placement': s['leader_placement'], 'late_decisions': n('late_decisions'), 'host_poll_ms': float(s['host_poll_ms']), 'host_poll_max_ms': float(s['max_host_poll_ms']), 'host_poll_ms_per_completed_original': ratio(float(s['host_poll_ms']), len(applied)), 'poll_rounds': n('poll_rounds'), 'recovery': {k: s[k] for k in ['recovered_value', 'recovery_retries', 'retry_verified', 'workers_joined']}, 'maintenance': {'counts': {str(k): v for k, v in counts.items()}, 'cold_elapsed_sums': cold, 'skips': n('maintenance_skips'), 'reclaimed_logical_bytes': reclaim, 'catchup_counters': {k: n(k) for k in ['snapshot_events', 'snapshot_supplies', 'follower_snapshot_installs', 'installs_at_resume']}, 'base_readbacks': bases}, 'replica_storage': physical, 'publication_primitives': primitive, 'wholechild_resources': resource, 'actual_exits': {k: case[k] for k in ['exit', 'validator_exit', 'readbacks_exit']}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', choices=COMMITS, required=True)
    parser.add_argument('--host', choices=['linux', 'mac'], required=True)
    parser.add_argument('--job', type=Path, required=True)
    parser.add_argument('--quiet-go', type=Path, required=True)
    parser.add_argument('--both-end', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--case-root', type=Path)
    parser.add_argument('--resource-root', type=Path)
    args = parser.parse_args()
    assert not args.output.exists(), 'No overwrite or score replacement'
    assert args.quiet_go.parent == args.both_end.parent
    go, end = obj(args.quiet_go), obj(args.both_end)
    assert all(end[host]['actual_terminal_exit'] is not None for host in ['linux', 'mac'])
    job = args.job
    config = obj(job / 'CAMPAIGN_CONFIG.json')
    assert config['source_commits'] == COMMITS[args.phase]
    digest = inputs[str((job / 'CAMPAIGN_CONFIG.json').resolve())]['sha256']
    assert go[args.host]['config_sha256'] == digest == obj(job / 'READY.json')['campaign_config_sha256']
    before, after = obj(job / 'before-host.json'), obj(job / 'after-host.json')
    assert before['device_identity'] == after['device_identity'] == config['device_identity']
    assert before['cpu_policy'] == after['cpu_policy'] == config['cpu_policy']
    terminal, campaign = obj(job / 'TERMINAL.json'), obj(job / 'campaign/CAMPAIGN.json')
    assert terminal['all_timing_ended'] and terminal['cases_terminated'] == 48
    assert len(campaign['cases']) == len(config['cases']) == 48
    assert [r['name'] for r in campaign['cases']] == [r['name'] for r in config['cases']]
    records, failures = [], []
    if terminal['exit'] or campaign['post_timing_errors']:
        failures.append({'campaign_terminal': terminal, 'post_timing_errors': campaign['post_timing_errors']})
    for case in campaign['cases']:
        try:
            record = analyze(case, args, job)
            assert all(case[k] == frozen[k] for frozen in config['cases'] if frozen['name'] == case['name'] for k in frozen)
            records.append(record)
        except Exception:
            failures.append({'case': case['name'], 'actual_exits': {k: case.get(k) for k in ['exit', 'validator_exit', 'readbacks_exit']}, 'readout_error': traceback.format_exc()})
    pairs = []
    for pair in dict.fromkeys(c['pair'] for c in config['cases']):
        selected = {r['arm']: r for r in records if r['pair'] == pair}
        if set(selected) != {'baseline', 'candidate'}:
            pairs.append({'pair': pair, 'valid': False})
            continue
        a, b = selected['baseline'], selected['candidate']
        assert a['leader_placement'] == b['leader_placement']
        per_group = {g: {'completed_rate_ratio': ratio(b['groups'][g]['completed_ops_s'], a['groups'][g]['completed_ops_s']), 'during_rate_ratio': ratio(b['groups'][g]['during_ops_s'], a['groups'][g]['during_ops_s']), 'baseline': a['groups'][g], 'candidate': b['groups'][g], 'p99_ratio': ratio(b['groups'][g]['quantiles']['intended']['p99_ms'], a['groups'][g]['quantiles']['intended']['p99_ms'])} for g in a['groups']}
        pairs.append({'pair': pair, 'valid': True, 'cell': a['cell'], 'protocol': a['protocol'], 'trial': a['trial'], 'rates': {k: {'baseline': a['rates'][k], 'candidate': b['rates'][k], 'ratio': ratio(b['rates'][k], a['rates'][k])} for k in a['rates']}, 'latency_ratios': {kind: {key: ratio(b['quantiles'][kind][key], a['quantiles'][kind][key]) for key in ['p50_ms', 'p95_ms', 'p99_ms', 'max_ms']} for kind in a['quantiles']}, 'cpu_us_per_completed_original_ratio': ratio(b['wholechild_resources']['cpu_us_per_completed_original'], a['wholechild_resources']['cpu_us_per_completed_original']), 'host_poll_max_ratio': ratio(b['host_poll_max_ms'], a['host_poll_max_ms']), 'host_poll_ms_per_completed_original_ratio': ratio(b['host_poll_ms_per_completed_original'], a['host_poll_ms_per_completed_original']), 'barriers_per_completed_original_ratio': {r: ratio(b['replica_storage'][r]['barriers_per_completed_original'], a['replica_storage'][r]['barriers_per_completed_original']) for r in a['replica_storage']}, 'mean_barrier_ratio': {r: ratio(b['replica_storage'][r]['mean_barrier_ms'], a['replica_storage'][r]['mean_barrier_ms']) for r in a['replica_storage']}, 'groups': per_group, 'rate_loss': b['rates']['completed_ops_s'] < a['rates']['completed_ops_s'], 'during_loss': b['rates']['during_ops_s'] < a['rates']['during_ops_s'], 'p99_loss': b['quantiles']['intended']['p99_ms'] > a['quantiles']['intended']['p99_ms'] if a['quantiles']['intended']['p99_ms'] is not None and b['quantiles']['intended']['p99_ms'] is not None else None, 'baseline_statuses': a['statuses'], 'candidate_statuses': b['statuses']})
    for pair in pairs:
        if not pair['valid']:
            continue
        a, b = [next(r for r in records if r['pair'] == pair['pair'] and r['arm'] == arm) for arm in ['baseline', 'candidate']]
        pair['replica_counter_ratios'] = {replica: {k: ratio(b['replica_storage'][replica]['measurement_delta'][k], a['replica_storage'][replica]['measurement_delta'][k]) for k in FIELDS} for replica in a['replica_storage']}
        pair['mean_wal_sync_ratios'] = {replica: ratio(b['replica_storage'][replica]['mean_wal_sync_ms'], a['replica_storage'][replica]['mean_wal_sync_ms']) for replica in a['replica_storage']}
        pair['mean_publication_ratios'] = {replica: ratio(b['replica_storage'][replica]['mean_publication_ms'], a['replica_storage'][replica]['mean_publication_ms']) for replica in a['replica_storage']}
        pair['primitive_ratios'] = {replica: {step: {k: ratio(b['publication_primitives'][replica][step]['measurement_delta'][k], a['publication_primitives'][replica][step]['measurement_delta'][k]) for k in ['calls', 'elapsed_ns', 'manifest_calls', 'manifest_ns']} for step in PRIMITIVES} for replica in a['replica_storage']}
    cells = []
    group_cells = []
    for cell in MATRIX:
        for protocol in ['tcp', 'quic']:
            for arm in ['baseline', 'candidate']:
                rs = sorted([r for r in records if (r['cell'], r['protocol'], r['arm']) == (cell, protocol, arm)], key=lambda r: r['trial'])
                if len(rs) != 3:
                    continue
                cells.append({'cell': cell, 'protocol': protocol, 'arm': arm, 'rates': {k: stats([r['rates'][k] for r in rs]) for k in rs[0]['rates']}, 'quantiles': {kind: {key: stats([r['quantiles'][kind][key] for r in rs]) for key in ['p50_ms', 'p95_ms', 'p99_ms', 'max_ms']} for kind in rs[0]['quantiles']}, 'statuses': {k: stats([r['statuses'][k] for r in rs]) for k in STATES}, 'cpu_us_per_completed_original': stats([r['wholechild_resources']['cpu_us_per_completed_original'] for r in rs]), 'host_poll_max_ms': stats([r['host_poll_max_ms'] for r in rs]), 'backlog_at_horizon': stats([r['backlog_at_horizon'] for r in rs]), 'drain_ns': stats([r['drain_ns'] for r in rs])})
    for cell in cells:
        rs = sorted([r for r in records if (r['cell'], r['protocol'], r['arm']) == (cell['cell'], cell['protocol'], cell['arm'])], key=lambda r: r['trial'])
        for group in rs[0]['groups']:
            gs = [r['groups'][group] for r in rs]
            group_cells.append({'cell': cell['cell'], 'protocol': cell['protocol'], 'arm': cell['arm'], 'group': group, 'metrics': {k: stats([g[k] for g in gs]) for k in ['offers', 'completed', 'completed_ops_s', 'during_ops_s', 'drain_ops_s', 'backlog_at_horizon', 'max_inflight', 'pending_ceiling', 'last_applied_value']}, 'statuses': {k: stats([g['statuses'][k] for g in gs]) for k in STATES}, 'quantiles': {kind: {metric: stats([g['quantiles'][kind][metric] for g in gs]) for metric in ['p50_ms', 'p95_ms', 'p99_ms', 'max_ms']} for kind in ['intended', 'service', 'dispatch']}})
    output = {'phase': args.phase, 'host': args.host, 'source_commits': config['source_commits'], 'config_sha256': digest, 'device_identity': config['device_identity'], 'cpu_policy': config['cpu_policy'], 'actual_terminal': terminal, 'quiet_end': end[args.host], 'valid_cases': len(records), 'planned_cases': 48, 'failures': failures, 'totals': {k: sum(r['statuses'][k] for r in records) for k in STATES}, 'records': records, 'cells': cells, 'group_cells': group_cells, 'pairs': pairs, 'regressions': [p for p in pairs if p.get('valid') and (p['rate_loss'] or p['during_loss'] or p['p99_loss'] or any(g['p99_ratio'] is not None and g['p99_ratio'] > 1 for g in p['groups'].values()))], 'limits': ['This exact phase uses the source commits above; reference481 is retained context only and must never be spliced into484 scores.', 'Intended/service quantiles cover applied originals; dispatch covers all offers. Nearest-rank ceil(n*p/100)-1. No estimated or reconstructed outcomes.', 'Whole-child CPU/RSS includes startup, warmup, measurement, drain, verification, recovery and cleanup. CPUus/completed has that wider numerator; it is not native timer CPU or causal attribution.', 'Inclusive/nested storage and publication/cold elapsed scopes overlap; do not sum them as CPU or per-original waiting.', 'Append units include metadata transitions; histogram width and commands/unit are not literal proposal width. Encoded bytes and process block counters are not device-I/O bytes.', 'All voters/groups share the same physical device. Per-group skew is7936hot/256cold with2048pending ceiling, not an engine fairness guarantee or one-group floor.', 'After-recover counters have a new observer lifetime and are absolute; no counter subtraction across reopen.', 'Reported catch-up counts and before-close/recovered bases do not locate individual delayed originals. No missed-timer count exists; late decisions are not timer misses.', 'Cap-limited/small finite horizons do not establish maximum/sustained capacity, memory/debt plateau or fullP9/three-filesystem acceptance.']}
    args.output.mkdir(parents=True)
    save(args.output / 'READOUT.json', output)
    save(args.output / 'PROVENANCE.json', {'readout_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'command': sys.argv, 'inputs': inputs, 'no_workloads_rerun': True})
    print(json.dumps({'phase': args.phase, 'host': args.host, 'valid_cases': len(records), 'failures': len(failures), 'totals': output['totals'], 'pairs': len(pairs), 'regressions': len(output['regressions'])}, indent=2))
    return int(bool(failures))


if __name__ == '__main__':
    raise SystemExit(main())
