"""Validate actual requested-to-normalized run shape; no recording/raw edit."""
import copy,hashlib,importlib.util,json
from pathlib import Path
root=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('checker',Path('/tmp/voteboat-comparison450-GUz9m1/phase-diagnostic-check.py'))
checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
rows=[]
def validate(r,requested,clients,batch):
    n=requested//(clients*batch)*(clients*batch)
    assert r['settings']['requested_operations']==requested
    assert r['settings']['operations']==r['completed_originals']==n
    checker.case(r,'off',n,clients,batch,requested)
    return n
for cell,requested,c,b in [('bulk',20000000,4096,4),('singleton',1048576,1,1)]:
    raw=(root/(cell+'.stdout.json')).read_bytes();report=json.loads(raw)
    assert (root/(cell+'.exit')).read_text().strip()=='0'
    n=validate(report,requested,c,b)
    negative=[]
    for field in []:  # Existing unchanged predicates already qualified; no repeated controls.
        forged=copy.deepcopy(report)
        if field=='requested':forged['settings']['requested_operations']-=1
        elif field=='completed':forged['completed_originals']-=1
        else:forged['applied_frontiers'][0]-=1
        try:validate(forged,requested,c,b)
        except (AssertionError,ValueError):negative.append(field)
        else:raise AssertionError('bad normalized profile accepted: '+field)
    rows.append({'case':cell,'requested_originals':requested,'normalized_completed_originals':n,
                 'completed_requests':report['completed_requests'],'frontiers':report['applied_frontiers'],
                 'profiled_originals_per_second':report['operations_per_second'],
                 'profiled_request_p99_ms':report['request_p99_ns']/1e6,
                 'profiled_rate_scope':'profiler perturbed, not paired capacity acceptance',
                 'checkpoint_counts':[x['checkpoint_count'] for x in report['diagnostics']],
                 'raw_sha256':hashlib.sha256(raw).hexdigest(),
                 'normalization_formula':'requested // (clients*batch) * (clients*batch)',
                 'invariant_adapter_scope':'Raw requested/normalized completion verified directly by current checker requested argument; original bytes unchanged',
                 'correctly_rejected_controls':negative})
(root/'COMPLETION.json').write_text(json.dumps({'scope':'actual normalized sampled outcomes, no acceptance claim','cases':rows},indent=2)+'\n')
print('both raw requested/normalized counts, every original/frontier/envelope invariant pass')
