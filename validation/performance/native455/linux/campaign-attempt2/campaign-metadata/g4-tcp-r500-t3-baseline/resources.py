import json
import resource
import subprocess
import sys
import time
from pathlib import Path

job = Path(__file__).resolve().parent
start = time.monotonic()
process = subprocess.Popen(sys.argv[1:])
(job / 'NATIVE_PID').write_text(str(process.pid) + '\n')
exit_code = process.wait()
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
result = {
    'pid': process.pid,
    'exit': exit_code,
    'elapsed_seconds': time.monotonic() - start,
    'user_seconds': usage.ru_utime,
    'system_seconds': usage.ru_stime,
    'max_rss_raw': usage.ru_maxrss,
    'max_rss_unit': 'KiB',
    'max_rss_bytes': usage.ru_maxrss * 1024,
    'voluntary_context_switches': usage.ru_nvcsw,
    'involuntary_context_switches': usage.ru_nivcsw,
    'block_inputs': usage.ru_inblock,
    'block_outputs': usage.ru_oublock,
    'scope': 'Linux native benchmark wholeprocess and children; includes campaign/warmup/horizon/drain/maintenance/recovery/cleanup; not measurement-only or continuous RSS plateau. Block I/O counts are not bytes.',
}
(job / 'RESOURCES.json').write_text(json.dumps(result, indent=2) + '\n')
sys.exit(exit_code)
