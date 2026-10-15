"""Original serial481 controller. No retry, resume, deadline extension or cleanup."""
import os, signal, subprocess, sys, time, traceback
from pathlib import Path
from common import check_readbacks, check_settings, guards, host_snapshot, load, save, sha

job = Path(__file__).resolve().parent
assert sys.argv[1:] == ['--authorized-quiet'], 'Separate explicit root quiet-window grant required'
ready = load(job/'READY.json'); assert sha(job/'CAMPAIGN_CONFIG.json') == ready['campaign_config_sha256']
config = load(job/'CAMPAIGN_CONFIG.json'); assert config['job'] == str(job)
assert sha(job/'SCHEMA.md') == config['schema_sha256']
assert not (job/'STARTED.json').exists() and not (job/'campaign').exists(), 'No overwrite/restart'
guards(config)
before = host_snapshot(config['host'],config['wal_parent']); save(job/'before-host.json',before)
assert before['device_identity'] == config['device_identity'], 'Selected volume/device changed'
assert before['cpu_policy'] == config['cpu_policy'], 'CPU policy changed since setup; review before timing'
active = subprocess.check_output(['ps','-eo','comm='],text=True).splitlines()
assert not any(Path(line.strip()).name in ['cargo','rustc','native_benchmark','native_benchmar','samply','perf','voteboat-matched-framework','voteboat-matche'] for line in active), 'Competing build/profile/benchmark'
out = job/'campaign'; out.mkdir()
save(job/'STARTED.json',{'pid':os.getpid(),'start_unix':time.time(),'scope':config['scope']})
record = {'scope':config['scope'],'cases':[],'post_timing_errors':[]}
save(out/'CAMPAIGN.json',record)
(job/'CAMPAIGN_STATUS').write_text('running\n')
child = None; active_case = None; received_signal = None; timing_end = False
def signal_child(number):
    if child is not None and child.poll() is None:
        try: os.killpg(child.pid,number)
        except ProcessLookupError: pass
def interrupted(number, frame):
    global received_signal
    received_signal = number
    signal_child(number)
for number in [signal.SIGINT,signal.SIGTERM]: signal.signal(number,interrupted)
failed = False
try:
    for case in config['cases']:
        if received_signal: raise InterruptedError('Controller interrupted before next case')
        name = case['name']; assert not Path(case['wal_root']).exists()
        (job/'CAMPAIGN_STAGE').write_text(name+'\n')
        save(out/(name+'.command.json'),case['resource_command'])
        save(out/(name+'.native-command.json'),case['command'])
        save(out/(name+'.started.json'),{'start_unix':time.time(),'case':name})
        print('START',name,flush=True); start = time.time()
        active_case = {**case,'start_unix':start}
        with (out/(name+'.stdout')).open('wb') as stdout,(out/(name+'.stderr')).open('wb') as stderr:
            child = subprocess.Popen(case['resource_command'],stdout=stdout,stderr=stderr,start_new_session=True)
            if received_signal: signal_child(received_signal)
            (job/'ACTIVE_CHILD_PID').write_text(str(child.pid)+'\n'); code = child.wait()
        entry = {**case,'start_unix':start,'end_unix':time.time(),'exit':code}
        (out/(name+'.exit')).write_text(str(code)+'\n')
        record['cases'].append(entry); save(out/'CAMPAIGN.json',record)
        child = None; active_case = None
        failed |= code != 0; print('END',name,code,flush=True)
        if received_signal: raise InterruptedError('Controller interrupted after owned child terminated')
    timing_end = True
    (job/'CAMPAIGN_STAGE').write_text('ALL_TIMING_END\n'); print('ALL_TIMING_END',flush=True)
    for label, action in [('after-guards',lambda:guards(config)),('after-host',lambda:host_snapshot(config['host'],config['wal_parent']))]:
        try:
            value = action()
            if label == 'after-host':
                save(job/'after-host.json',value)
                assert value['device_identity'] == before['device_identity'], 'Volume/device drift'
                assert value['cpu_policy'] == before['cpu_policy'], 'CPU-policy drift'
        except BaseException:
            record['post_timing_errors'].append({'stage':label,'traceback':traceback.format_exc()}); failed = True
    for entry in record['cases']:
        name = entry['name']
        if entry['exit'] != 0: continue
        try:
            readbacks = check_readbacks((out/(name+'.stderr')).read_text().splitlines(),entry['publication'])
            check_settings(entry,Path(entry['wal_root']))
            save(out/(name+'.readbacks.json'),readbacks); entry['readbacks_exit'] = 0
        except BaseException:
            entry['readbacks_exit'] = 1; entry['readback_error'] = traceback.format_exc(); failed = True
        command = [config['node'],str(job/'control-tools/check-native-benchmark.mjs'),'--storage',entry['wal_root']]
        save(out/(name+'-check.command.json'),command)
        checked = subprocess.run(command,capture_output=True)
        (out/(name+'-check.stdout')).write_bytes(checked.stdout); (out/(name+'-check.stderr')).write_bytes(checked.stderr)
        (out/(name+'-check.exit')).write_text(str(checked.returncode)+'\n'); entry['validator_exit'] = checked.returncode
        failed |= checked.returncode != 0
        save(out/'CAMPAIGN.json',record)
except BaseException:
    record['controller_error'] = traceback.format_exc(); failed = True
    # A controller failure cannot announce termination while its original child
    # still owns benchmark workers. Signal only that child's new process group.
    if child is not None:
        signal_child(signal.SIGTERM)
        actual = child.wait()
        if active_case is not None:
            entry = {**active_case,'end_unix':time.time(),'exit':actual,'controller_cleanup':True}
            if not any(c['name'] == entry['name'] for c in record['cases']): record['cases'].append(entry)
            (out/(entry['name']+'.exit')).write_text(str(actual)+'\n')
        child = None
finally:
    save(out/'CAMPAIGN.json',record)
    code = 128+received_signal if received_signal else int(failed)
    save(job/'TERMINAL.json',{'exit':code,'pid':os.getpid(),'end_unix':time.time(),
                             'all_timing_ended':timing_end,'cases_terminated':len(record['cases']),
                             'received_signal':received_signal,'no_retries_or_replacements':True})
    (job/'CAMPAIGN_STATUS').write_text(str(code)+'\n')
    print('CAMPAIGN_EXIT',code,flush=True)
raise SystemExit(code)
