"""Validate every original and observer scope; no shorter/faster replacement cases."""
import json
import math
import sys
from pathlib import Path
import importlib.util

spec = importlib.util.spec_from_file_location('comparison_guard', Path(__file__).with_name('artifact-guard.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
need = module.need
PHASES = [
    'host_admission', 'core_direct', 'persist_helper_inclusive', 'store_append_nested',
    'store_barrier_nested', 'application', 'results', 'send', 'maintenance_tick_inclusive',
    'snapshot_helpers_inclusive', 'host_wait', 'actor_lifetime_inclusive',
    'checkpoint_reserve_inclusive', 'checkpoint_publish_inclusive', 'checkpoint_compact_inclusive',
    'checkpoint_effects_inclusive', 'store_state_clone_nested', 'snapshot_begin_nested',
    'snapshot_write_chunk_nested', 'snapshot_seal_nested', 'snapshot_publish_nested',
    'snapshot_abort_nested', 'snapshot_load_nested', 'snapshot_latest_reference_nested',
    'snapshot_pin_for_log_nested', 'snapshot_load_pinned_nested', 'snapshot_reconcile_log_inclusive_nested',
]
PHASES.append('store_matches_state_exact_nested')
SCOPES = PHASES[12:16]
METHODS = [PHASES[3], PHASES[4], *PHASES[16:]]
FIELDS = ['calls', 'failures', 'total_elapsed_ns', 'items']
SCOPE = 'whole_actor_lifetime_including_campaign_warmup_follower_drain_and_shutdown;provider_maxima_whole_provider_lifetime_including_setup'


def integer(value, label):
    need(type(value) is int and 0 <= value <= 2**64 - 1, 'invalid counter: ' + label)


def counters(value, maximum):
    for field in FIELDS:
        integer(value[field], field)
    need(value['failures'] == 0, 'observer failure')
    if maximum:
        integer(value['maximum_elapsed_ns'], 'maximum_elapsed_ns')
        need(value['maximum_elapsed_ns'] <= value['total_elapsed_ns'], 'maximum exceeds total')
    else:
        need('maximum_elapsed_ns' not in value, 'invented scoped maximum')
    if value['calls'] == 0:
        need(all(value[field] == 0 for field in FIELDS), 'nonempty zero-call interval')
        if maximum:
            need(value['maximum_elapsed_ns'] == 0, 'nonempty zero-call maximum')


def observer(record, diagnostics, expected):
    need(record['scope'] == SCOPE, 'wrong lifetime/maxima scope')
    need(record['clock'] == 'monotonic_elapsed_wall_intervals_not_cpu_or_critical_path', 'wrong clock')
    need(all(token in record['nesting'] for token in ['reconcile_contains_load_pinned', 'replicas_overlap', 'do_not_sum']), 'missing nesting caveat')
    need('not_pure_Core' in record['residual'] and 'not_additive_when_nested' in record['residual'], 'missing unattributed residual')
    need(len(record['phases']) == len(PHASES), 'wrong phase count')
    phases = {value['phase']: value for value in record['phases']}
    need(set(phases) == set(PHASES), 'wrong/duplicate phase names')
    for phase in phases.values():
        counters(phase, True)
    need(phases['actor_lifetime_inclusive']['calls'] == 1, 'wrong actor lifetime count')
    need(phases['actor_lifetime_inclusive']['maximum_elapsed_ns'] == phases['actor_lifetime_inclusive']['total_elapsed_ns'], 'wrong actor lifetime maximum')
    need(phases['send']['items'] == phases['send']['calls'], 'send units differ')
    need(phases['store_append_nested']['calls'] == phases['store_barrier_nested']['calls'], 'append/barrier completions differ')
    if record['replica_slot'] == 0:
        for name in ['host_admission', 'core_direct', 'application', 'results']:
            need(phases[name]['items'] == expected, 'leader original units: ' + name)
    else:
        direct = phases['application']['items']
        need(direct <= expected, 'follower direct original units exceed campaign')
        if diagnostics['snapshot_installs'] == 0:
            need(direct == expected, 'follower originals missing without snapshot installation')
    checkpoint_count = diagnostics['checkpoint_count']
    for name in SCOPES:
        need(phases[name]['calls'] == checkpoint_count, 'checkpoint count correlation: ' + name)
    scopes = record['checkpoint_provider_scopes']
    need(len(scopes) == 4 and {v['scope'] for v in scopes} == set(SCOPES), 'wrong scope names/count')
    totals = {name: {field: 0 for field in FIELDS} for name in METHODS}
    scope_maps = {}
    for scope in scopes:
        need(len(scope['methods']) == len(METHODS), 'wrong provider method count')
        methods = {m['method']: m for m in scope['methods']}
        need(set(methods) == set(METHODS), 'wrong/duplicate provider names')
        scope_maps[scope['scope']] = methods
        for name, method in methods.items():
            counters(method, False)
            for field in FIELDS:
                totals[name][field] += method[field]
    for name in METHODS:
        need(all(totals[name][field] <= phases[name][field] for field in FIELDS), 'scoped methods exceed whole provider: ' + name)
    for name in ['snapshot_begin_nested', 'snapshot_write_chunk_nested', 'snapshot_seal_nested', 'snapshot_publish_nested']:
        need(scope_maps['checkpoint_publish_inclusive'][name]['calls'] == checkpoint_count, 'publication method correlation: ' + name)
    compact = scope_maps['checkpoint_compact_inclusive']
    for name in ['store_matches_state_exact_nested', 'store_append_nested', 'store_barrier_nested', 'snapshot_pin_for_log_nested', 'snapshot_reconcile_log_inclusive_nested']:
        need(compact[name]['calls'] == checkpoint_count, 'compaction method correlation: ' + name)
    need(compact['store_state_clone_nested']['calls'] == 0, 'borrowed compaction unexpectedly materializes state')
    need(compact['snapshot_load_pinned_nested']['calls'] == 2 * checkpoint_count, 'nested load/reconcile correlation')

    need('store_matches_state=retained_entries_in_successfully_equal_state_false_or_error_zero_not_physical_traversal' in record['item_units'], 'comparison units absent/misleading')
    need('state_to_pin_starts_after_exact_comparison' in record['checkpoint_compact_gap_scope'], 'historical gap scope reused')
    gaps = record['checkpoint_compact_gaps']
    need(len(gaps) == 3 and [gap['gap'] for gap in gaps] ==
         ['state_to_pin', 'pin_to_append', 'barrier_to_reconcile'], 'wrong gap names/count')
    need(all(token in record['checkpoint_compact_gap_scope'] for token in
             ['whole_successful', 'nested_pinned_load', 'elapsed_not_CPU', 'do_not_add']),
         'missing gap scope/nesting caveat')
    for gap in gaps:
        counters(gap, True)
        need(gap['calls'] == checkpoint_count and gap['items'] == 0,
             'gap observations do not certify exactly each successful compaction')


def case(report, variant, originals, clients=4096, batch=4):
    settings = report['settings']
    engine = 'openraft' if variant == 'openraft' else 'voteboat'
    enabled = variant == 'on'
    expected = originals + 1024
    need(report['engine'] == engine, 'wrong engine')
    need(report['upstream_commit'] == module.UPSTREAM_COMMIT, 'wrong upstream commit')
    need(settings['operations'] == settings['requested_operations'] == report['completed_originals'] == originals, 'wrong original completion')
    need(report['completed_requests'] == originals // batch, 'wrong request completion')
    need(settings['clients'] == clients and settings['batch'] == batch and settings['warmup'] == 1024 and settings['attribution'] is enabled, 'wrong workload')
    need(settings['maintenance'] == 'default_retention' and settings['envelopes'] == 'upstream_default', 'wrong maintenance/envelopes')
    need(settings['campaign_timeout_secs'] == 3600 and report['cleanup_timeout_secs'] == 30, 'wrong shared guards')
    need(report['throughput_timing_scope'] == 'ready_client_barrier_to_all_client_joins_excludes_warmup_latency_flatten_sort_follower_verification_and_shutdown', 'wrong timed boundary')
    need(report['applied_frontiers'] == [expected + 1] * 3, 'wrong follower frontier')
    need(report['payload_bytes'] == 0 and report['members'] == 3 and report['client_workers'] == 1 and report['server_workers'] == 16, 'wrong resources')
    need(report['durability'] == 'volatile_no_disk' and report['transport'] == 'in_process_actor_messages', 'wrong provider model')
    need(report['completion'] == 'every_original_locally_applied_after_quorum_commit', 'wrong completion model')
    need(report['snapshot_model'] == 'openraft_current_snapshot_keep1000_voteboat_historical_snapshot_retained_suffix1000', 'wrong snapshot model')
    need(report['primary_upstream_envelopes_preserved'] is True, 'rival envelope altered')
    capacities = [report[name] for name in ['api_entry_batch_capacity', 'append_entry_batch_capacity', 'replication_entry_capacity']]
    need(capacities == ([4096, 4096, 1024] if engine == 'openraft' else [4096 * batch, 4096 * batch, 1024]), 'wrong selected capacities')
    contract = report['capacity_contract']
    need(contract['api_merge_original_bound_for_fixed_request_batch'] == 4096 * batch, 'wrong API original bound')
    need(contract['api_capacity_unit'] == ('client_write_request_messages' if engine == 'openraft' else 'original_entries'), 'wrong capacity units')
    need(contract['append_capacity_unit'] == 'original_entries' and contract['replication_capacity_unit'] == 'original_entries_per_ordinary_replication_rpc', 'wrong append/replication units')
    if engine == 'voteboat':
        envelope = contract['voteboat_empty_owned_envelope']
        need(envelope['original_capacity'] == 4096 * batch, 'wrong byte envelope width')
        for name, value in envelope.items():
            integer(value, name)
        need(envelope['proposal_input_bytes'] < envelope['proposal_and_staged_entries_bytes'] < envelope['complete_empty_envelope_bytes'] <= envelope['selected_log_batch_bytes'] <= 4 * 1024 * 1024, 'owned staging byte envelope mismatch')
        diagnostics = report['diagnostics']
        need(len(diagnostics) == 3, 'missing replica diagnostics')
        for slot, row in enumerate(diagnostics):
            for name, value in row.items():
                if name != 'proposal_widths': integer(value, name)
            widths = row['proposal_widths']
            need(len({w['original_entries'] for w in widths}) == len(widths), 'duplicate histogram width')
            for width in widths:
                need(type(width['original_entries']) is int and 1 <= width['original_entries'] <= 4096 * batch, 'proposal width exceeds bound')
                need(type(width['batches']) is int and width['batches'] > 0, 'invalid histogram count')
            need(sum(w['original_entries'] * w['batches'] for w in widths) == row['proposed_originals'] == (expected if slot == 0 else 0), 'histogram originals mismatch')
            need(sum(w['batches'] for w in widths) == row['proposal_batches'], 'histogram batch mismatch')
            need(max((w['original_entries'] for w in widths), default=0) == row['maximum_proposal_width'], 'maximum width mismatch')
            need(row['retained_entries'] <= row['peak_retained_entries'] <= 65536, 'retention budget exceeded')
            need(row['peak_peer_queue'] <= 65536 and row['peak_client_queue'] <= 65536, 'queue budget exceeded')
        if enabled:
            records = report['attribution']
            need(len(records) == 3 and sorted(r['replica_slot'] for r in records) == [0, 1, 2], 'wrong observer replicas')
            for record in records:
                observer(record, diagnostics[record['replica_slot']], expected)
        else:
            need('attribution' not in report, 'disabled observer emitted data')
    else:
        need(report['diagnostics'] is None and 'attribution' not in report, 'unavailable upstream diagnostics invented')
        need('voteboat_empty_owned_envelope' not in contract, 'upstream bytes invented')
    need(math.isfinite(report['seconds']) and report['seconds'] > 0, 'invalid measured interval')
    need(math.isfinite(report['operations_per_second']) and math.isclose(report['operations_per_second'], originals / report['seconds'], rel_tol=1e-12), 'throughput arithmetic differs')
    for name in ['request_p50_ns', 'request_p99_ns']: integer(report[name], name)
    need(report['request_p99_ns'] >= report['request_p50_ns'], 'latency order differs')


def validate(directory):
    directory = Path(directory)
    results, failures = [], []
    for clients, batch in [(4096, 4), (1, 1)]:
        for trial in range(1, 4):
            for variant in ['baseline', 'candidate', 'on', 'openraft']:
                label = f'{variant}-c{clients}-b{batch}-t{trial}'
                try:
                    need((directory / f'{label}.exit').read_text().strip() == '0',
                         label + ': actual process failed')
                    report = json.loads((directory / f'{label}.json').read_text())
                    mode = 'on' if variant == 'on' else 'openraft' if variant == 'openraft' else 'off'
                    case(report, mode, 1048576, clients, batch)
                    timers = dict(line.split('=', 1) for line in
                                  (directory / f'{label}.time').read_text().splitlines())
                    need(set(timers) == {'elapsed_seconds', 'user_seconds', 'system_seconds'},
                         'missing resource timers')
                    need(all(math.isfinite(float(value)) and float(value) >= 0
                             for value in timers.values()), 'invalid resource timers')
                    results.append({'label': label, 'variant': variant, 'trial': trial,
                                    'clients': clients, 'batch': batch, 'report': report})
                except (ValueError, KeyError, TypeError, OSError) as error:
                    failures.append({'label': label, 'failure': str(error)})
    summary = {'scope': 'paired short equal-work diagnostic; not sustained/minimum qualification',
               'cases': results, 'failures': failures}
    (directory / 'RESULTS.json').write_text(json.dumps(summary, indent=2) + '\n')
    need(not failures, 'invalid cases retained: ' + repr(failures))
    print(f'{len(results)} exact paired original/frontier/envelope/observer cases pass')


if __name__ == '__main__':
    validate(sys.argv[1])
