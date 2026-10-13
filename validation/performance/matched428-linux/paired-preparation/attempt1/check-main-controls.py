"""Qualify adapted checker with preserved actual CLI outputs and negative copies."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path

here = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('phase_checker', here / 'phase-diagnostic-check.py')
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
main = here.parent / 'matched428-linux/logs'
actual, negatives = [], []
for mode in ['off', 'on', 'openraft']:
    raw = (main / (mode + '.json')).read_bytes()
    checker.need((main / (mode + '.status')).read_text().strip() == '0', 'actual CLI exit failed')
    report = json.loads(raw)
    checker.case(report, mode, 16384, 4096, 4)
    actual.append({'mode': mode, 'path': str(main / (mode + '.json')), 'sha256': hashlib.sha256(raw).hexdigest(), 'completed_originals': 16384, 'frontiers': report['applied_frontiers'], 'result': 'pass'})
base = json.loads((main / 'on.json').read_text())

def remove_phase(d):
    phases=d['attribution'][0]['phases']
    phases[:]=[p for p in phases if p['phase']!='store_checkpoint_state_protected_inclusive_nested']

def remove_method(d):
    d['attribution'][0]['checkpoint_provider_scopes'][2]['methods'].pop()

def duplicate_phase(d):
    d['attribution'][0]['phases'][-1]=copy.deepcopy(d['attribution'][0]['phases'][-2])

def bad_units(d):
    d['attribution'][0]['item_units']=d['attribution'][0]['item_units'].replace('not_actual_compared_or_skipped_entries_full_fallback_inside_this_phase','actual_compared_entries')

def bad_gap_units(d):
    d['attribution'][0]['checkpoint_compact_gaps'][0]['items']=1

def invented_scoped_max(d):
    d['attribution'][0]['checkpoint_provider_scopes'][0]['methods'][0]['maximum_elapsed_ns']=0

def old_scope(d):
    d['attribution'][0]['checkpoint_compact_gap_scope'] = 'historical_state_to_pin_includes_full_Eq'

def clone_compact(d):
    compact = next(s for s in d['attribution'][0]['checkpoint_provider_scopes'] if s['scope'] == 'checkpoint_compact_inclusive')
    next(m for m in compact['methods'] if m['method'] == 'store_state_clone_nested')['calls'] = 1

mutations = {
    'missing_original': lambda d: d.update(completed_originals=16383),
    'missing_frontier': lambda d: d['applied_frontiers'].__setitem__(2, 17408),
    'missing_protected_phase': remove_phase,
    'missing_scoped_method':remove_method,
    'duplicate_phase':duplicate_phase,
    'misleading_protected_units':bad_units,
    'invented_gap_units':bad_gap_units,
    'invented_scoped_maximum':invented_scoped_max,
    'historical_gap_scope': old_scope,
    'unexpected_owned_compaction': clone_compact,
}
for name, mutation in mutations.items():
    changed = copy.deepcopy(base)
    mutation(changed)
    try:
        checker.case(changed, 'on', 16384, 4096, 4)
    except ValueError as error:
        negatives.append({'control': name, 'result': 'correctly rejected', 'reason': str(error)})
    else:
        raise ValueError('invalid mutation accepted: ' + name)
summary = {'scope': 'actual16384 CLI checker qualification and negative in-memory copies; no new benchmarks, checker-only actual CLI qualification; artifact proof is separate; no sustained performance claim', 'actual': actual, 'negative_controls': negatives, 'checker_sha256': hashlib.sha256((here / 'phase-diagnostic-check.py').read_bytes()).hexdigest()}
(here / 'MAIN_CHECKER_CONTROLS.json').write_text(json.dumps(summary, indent=2) + '\n')
print('3 actual CLI outputs and10 refusal controls pass')
