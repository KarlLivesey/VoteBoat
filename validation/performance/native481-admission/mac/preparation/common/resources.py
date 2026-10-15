"""Existing475 getrusage scope, with explicit host units and owned-child signals."""
import json, platform, resource, signal, subprocess, sys, time, traceback
from pathlib import Path

metadata = Path(sys.argv[1])
start = time.monotonic()
signals = []; process = None; failure = None; code = None
def forward(number, frame):
    signals.append(number)
    if process is not None:
        try: process.send_signal(number)
        except ProcessLookupError: pass
for signum in [signal.SIGINT, signal.SIGTERM]:
    signal.signal(signum, forward)
try:
    process = subprocess.Popen(sys.argv[2:])
    if signals:
        try: process.send_signal(signals[-1])
        except ProcessLookupError: pass
    (metadata/'NATIVE_PID').write_text(str(process.pid)+'\n')
    code = process.wait()
except BaseException:
    failure = traceback.format_exc()
    if process is not None:
        if process.poll() is None:
            try: process.terminate()
            except ProcessLookupError: pass
        code = process.wait()
    print(failure,file=sys.stderr,flush=True)
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
unit = 'bytes' if platform.system() == 'Darwin' else 'KiB'
helper_exit = 1 if failure or code is None else (code if code >= 0 else 128-code)
result = {'pid': process.pid if process is not None else None, 'exit': code,
          'helper_exit':helper_exit,'helper_error':failure,'elapsed_seconds': time.monotonic()-start,
          'user_seconds': usage.ru_utime, 'system_seconds': usage.ru_stime,
          'max_rss_raw': usage.ru_maxrss, 'max_rss_unit': unit,
          'max_rss_bytes': usage.ru_maxrss*(1 if unit == 'bytes' else 1024),
          'voluntary_context_switches': usage.ru_nvcsw,
          'involuntary_context_switches': usage.ru_nivcsw,
          'block_inputs': usage.ru_inblock, 'block_outputs': usage.ru_oublock,
          'received_signals': signals,
          'scope': 'Whole child process including warmup, horizon, drain, maintenance, recovery and cleanup; not measurement-only or continuous RSS/debt. Block I/O counts are not bytes.'}
(metadata/'RESOURCES.json').write_text(json.dumps(result, indent=2)+'\n')
raise SystemExit(helper_exit)
