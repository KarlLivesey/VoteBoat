"""Independent raw saved-profile arithmetic; no profiler/child/source mutation."""
from pathlib import Path
from collections import Counter, defaultdict
from decimal import Decimal
import csv, gzip, hashlib, json, re

B = Path(__file__).resolve().parent
inputs = {}
def digest(p):
    p = Path(p)
    value = hashlib.sha256(p.read_bytes()).hexdigest()
    inputs[str(p)] = value
    return value
def load(p):
    digest(p)
    return json.loads(Path(p).read_text())
def profile(p):
    digest(p)
    return json.loads(gzip.decompress(Path(p).read_bytes()))
def frames_for(t, symbols):
    cache = {}
    def stack(index):
        if index in cache:
            return cache[index]
        result, seen = [], set()
        while index is not None:
            assert index not in seen and 0 <= index < t['stackTable']['length']
            seen.add(index)
            frame = t['stackTable']['frame'][index]
            f = t['frameTable']['func'][frame]
            resource = t['funcTable']['resource'][f]
            lib = t['resourceTable']['lib'][resource] if resource is not None and resource >= 0 else None
            info = symbols.get((lib, t['frameTable']['address'][frame]), {})
            result.append(info.get('function', '[unresolved]'))
            index = t['stackTable']['prefix'][index]
        return tuple(result)
    return stack

router = '<voteboat::runtime::clients::ClientRouter<voteboat::application::CounterReceipt>>::'
counter = '<voteboat::application::Counter as voteboat::application::ProposalAdmission>::'
predicates = {
    'counter_validate': lambda n: n.startswith(counter + 'validate_proposal::'),
    'counter_submit_monomorph': lambda n: n.startswith(counter + 'validate_proposal::') and 'check_submit' in n,
    'counter_ready_monomorph': lambda n: n.startswith(counter + 'validate_proposal::') and ('ready_pending' in n or 'check_ready_execution' in n),
    'counter_prepared': lambda n: n.startswith(counter + 'prepare_ready_admission::') or n.startswith('voteboat::application::ready_admission::prepare'),
    'client_submit_physical': lambda n: n.startswith(router + 'submit::'),
    'client_reconcile': lambda n: n.startswith(router + 'reconcile::'),
    'client_cleanup': lambda n: n.startswith(router + 'cleanup'),
    'ready_execution': lambda n: n.startswith(router + 'check_ready_execution::'),
    'pending_btree': lambda n: n.startswith('<alloc::collections::btree::') and 'runtime::clients::Pending<' in n,
}
def unions(frames, weight, counts, weights):
    for name, predicate in predicates.items():
        if any(predicate(n) for n in frames):
            counts[name] += 1
            weights[name] += weight

end = load(B/'ROOT_QUIET_END.json')
assert end['linux']['actual_terminal_exit'] == end['mac']['actual_terminal_exit'] == 0
linux = B/'linux'
lc = load(linux/'samply-symbolication/data/CONTEXTS.json')
lp = profile(linux/'readouts/native.json.gz')
symbols = {(r['library'], r['address']): r for r in load(linux/'samply-symbolication/data/ADDRESS_SYMBOLS.json')}
pid = load(linux/'capture/metadata/RESOURCES.json')['pid']
leaf = linux/'readouts/leaves.stdout'
digest(leaf)
header = re.compile(r'^\s*(.*?) (\d+)/(\d+) ([\d.]+):\s+(\d+) cycles:u:')
raw = defaultdict(list)
for line in leaf.read_text().splitlines():
    match = header.match(line)
    assert match
    comm, p, tid, stamp, period = match.groups()
    if int(p) == pid and comm != 'python3.14':
        raw[int(tid)].append((Decimal(stamp)*1000, int(period)))
counts, weights, quality = Counter(), Counter(), Counter()
total = number = 0
offset = None
max_delta = Decimal(0)
for t in lp['threads']:
    if str(t['pid']) != str(pid)+'.1':
        continue
    samples = t['samples']
    rows = raw[int(str(t['tid']).split('.')[0])]
    assert len(rows) == samples['length'] and samples['weight'] == [1]*len(rows)
    resolve = frames_for(t, symbols)
    for i, stack in enumerate(samples['stack']):
        stamp, period = rows[i]
        time = Decimal(str(samples['time'][i]))
        if offset is None:
            offset = stamp-time
        max_delta = max(max_delta, abs(stamp-time-offset))
        assert abs(stamp-time-offset) <= Decimal('.0011')
        frames = resolve(stack)
        assert frames
        number += 1
        total += period
        unions(frames, period, counts, weights)
        for name, yes in [('single_frame', len(frames)==1), ('unresolved_leaf', frames[0].startswith('[unresolved]')), ('any_unresolved', any(n.startswith('[unresolved]') for n in frames)), ('synthetic_leaf', frames[0].startswith('fun_'))]:
            if yes:
                quality[name+'_count'] += 1
                quality[name+'_period'] += period
assert number == lc['sample_count'] and total == lc['period_weight']
assert weights['counter_validate'] == lc['contexts']['Counter_validate_proposal']['period_weight']
assert weights['client_reconcile'] == lc['contexts']['client_reconcile']['period_weight']
for own, published in [('unresolved_leaf','unresolved_leaf'),('single_frame','single_frame'),('synthetic_leaf','synthetic_fun_leaf')]:
    assert quality[own+'_count'] == lc['quality'][published+'_count']
    assert quality[own+'_period'] == lc['quality'][published+'_period']
linux_result = {'native_pid':pid, 'samples':number, 'period':total, 'maximum_join_delta_ms':str(max_delta), 'contexts_nonadditive':{n:{'samples':counts[n],'count_percent':100*counts[n]/number,'period':w,'period_percent':100*w/total} for n,w in weights.items()},'quality':dict(quality)}

mac = B/'mac'
mp = profile(mac/'raw/profile.json.gz')
ms = load(mac/'analysis/SUMMARY.json')
mc = load(mac/'COMPACT_CPU.json')
symbols = {(r['library'],r['address']):r for r in load(mac/'symbolication/ADDRESS_SYMBOLS.json')}
pids = {str(t['pid']) for t in mp['threads'] if t['name']=='native_benchmark'}
assert len(pids)==1
cpu = wall = rows = 0
counts, weights, quality, thread_cpu = Counter(), Counter(), Counter(), Counter()
for t in mp['threads']:
    if str(t['pid']) not in pids:
        continue
    s=t['samples']
    assert len(s['threadCPUDelta'])==len(s['weight'])==len(s['stack'])==s['length']
    resolve=frames_for(t,symbols)
    rows += s['length']
    for i,stack in enumerate(s['stack']):
        delta=s['threadCPUDelta'][i]
        wall+=s['weight'][i]
        assert delta is not None and isinstance(delta,int) and delta>=0
        cpu+=delta
        thread_cpu[str(t['tid'])]+=delta
        frames=resolve(stack)
        assert frames
        unions(frames,delta,counts,weights)
        for n,yes in [('unresolved_leaf',frames[0].startswith('[unresolved]')),('any_unresolved',any(f.startswith('[unresolved]')for f in frames))]:
            if yes:quality[n+'_cpu_us']+=delta
assert cpu==ms['cpu_denominator_us']==mc['cpu_us']
assert rows==ms['totals']['samples'] and wall==ms['totals']['wall_sample_weight']
for n in ['unresolved_leaf','any_unresolved']:
    assert quality[n+'_cpu_us']==ms['totals'][n+('_stack_cpu_us'if n=='any_unresolved'else'_cpu_us')]
for n,owner in [('client_reconcile','client_reconcile'),('client_cleanup','client_cleanup'),('counter_validate','all_counter_admission'),('counter_ready_monomorph','ready_admission'),('counter_submit_monomorph','submission_admission'),('counter_prepared','prepared_ready_admission')]:
    assert weights[n]==mc['contexts_nonadditive'][owner]['cpu_us'],n
mac_result={'native_pid':next(iter(pids)),'rows':rows,'cpu_delta_us_once':cpu,'wall_sample_weight_separate':wall,'thread_cpu_us':dict(thread_cpu),'contexts_nonadditive':{n:{'cpu_us':w,'percent':100*w/cpu}for n,w in weights.items()},'quality':dict(quality)}

def workload(host, validation, offers):
    v=load(validation)
    assert v['exit']==0
    s=v.get('settings',v.get('summary'))
    digest(offers)
    with offers.open() as stream:rows=list(csv.DictReader(stream))
    statuses=Counter(r['status']for r in rows)
    assert len(rows)==int(s['offers']) and sorted(int(r['operation'])for r in rows)==list(range(65,65+len(rows)))
    assert statuses['applied']==int(s['applied']) and sum(statuses.values())==len(rows)
    for name in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']:assert statuses[name]==int(s[name])
    applied=[r for r in rows if r['status']=='applied']
    latency=sorted(int(r['completed_ns'])-int(r['intended_ns'])for r in applied)
    p99=latency[(len(latency)*99+99)//100-1]/1e6
    assert abs(p99-float(s['p99_us'])/1000)<.000001
    assert int(s['recovered_value'])==64+len(applied) and s['workers_joined']==s['retry_verified']=='true'
    return {'completed_originals_s':len(applied)*1e9/int(s['elapsed_ns']),'during_originals_s':sum(int(r['completed_ns'])<int(s['horizon_ns'])for r in applied)*1e9/int(s['horizon_ns']),'p99_ms':p99,'status_counts':dict(statuses),'max_inflight':int(s['max_inflight']),'recovered_value':int(s['recovered_value'])}
linux_result['workload']=workload('linux',linux/'VALIDATION.json',linux/'capture/wal/offers.csv')
mac_result['workload']=workload('mac',mac/'raw/VALIDATION.json',mac/'raw/results/offers.csv')
for p in [linux/'CONFIG.json',linux/'TERMINAL.json',linux/'samply-contexts.py',linux/'DIAGNOSTIC.json',mac/'raw/CONFIG.json',mac/'raw/TERMINAL.json',mac/'collapse.py',mac/'readout.py',mac/'QUALITY.json',B/'INDEPENDENT_PREPARATION_REVIEW.json',B.parents[2]/'src/runtime/clients.rs',B.parents[2]/'src/runtime/clients/ready.rs',B.parents[2]/'src/application.rs',B.parents[2]/'src/native/worker.rs']:
    if p.exists():digest(p)
digest(__file__)
result={'exit':0,'scope':'Independent same-data raw frame/context/weight reconstruction and ledger arithmetic after both actual original terminals. No captures/browser/tests/source edits. Physical sampled-stack unions overlap.','linux':linux_result,'mac':mac_result,'inputs':inputs}
out=B/'INDEPENDENT_PROFILE_REVIEW.json'
assert not out.exists()
out.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'exit':0,'linux':linux_result,'mac':mac_result,'input_count':len(inputs)},indent=2))
