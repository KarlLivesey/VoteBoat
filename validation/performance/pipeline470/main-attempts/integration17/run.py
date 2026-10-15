import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

repo = Path.cwd()
job = repo / 'target/p9/slice470/integration17'
env = os.environ.copy()
env['CARGO_TARGET_DIR'] = str(repo / 'target/p9/slice470/integration-checks')
env['CARGO_BUILD_JOBS'] = '2'
(job / 'test-tmp').mkdir(exist_ok=True)
env['TMPDIR'] = str(job / 'test-tmp')
commands = [('format', ['cargo', '+stable', 'fmt', '--all', '--', '--check']), ('connections', ['cargo', '+stable', 'test', '--locked', '--offline', '--all-features', '--lib', 'runtime::connections::tests', '--', '--test-threads=1']), ('append-callbacks', ['cargo', '+stable', 'test', '--locked', '--offline', '--all-features', '--lib', 'runtime::ready_batch::tests::append_window', '--', '--test-threads=1'])]

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def write(name, value): (job / name).write_text(json.dumps(value, indent=2) + '\n')
(job / 'STATUS').write_text('running\n')
write('RUN.json', {'pid': os.getpid(), 'start': now(), 'commands': commands,
    'environment': {k: env.get(k) for k in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','TMPDIR','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','PROTOC']},
    'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
exit_code = 1
try:
    for name, command in commands:
        (job / 'STAGE').write_text(name + '\n')
        print('START ' + name, flush=True)
        start = now(); clock = time.monotonic()
        with (job / (name + '.stdout')).open('wb') as out, (job / (name + '.stderr')).open('wb') as err:
            result = subprocess.run(command, cwd=repo, env=env, stdout=out, stderr=err)
        write(name + '.exit.json', {'command': command, 'start': start, 'end': now(),
            'elapsed_s': time.monotonic() - clock, 'exit': result.returncode})
        print('END ' + name + ' exit=' + str(result.returncode), flush=True)
        if result.returncode:
            exit_code = result.returncode
            break
    else:
        exit_code = 0
finally:
    before = json.loads((job / 'BEFORE.json').read_text())
    after = {name: {'sha256': hashlib.sha256(Path(name).read_bytes()).hexdigest(),
        'mode': oct(Path(name).stat().st_mode & 0o7777)} for name in before['files'] if Path(name).is_file()}
    write('AFTER.json', {'files': after, 'same_selected_inputs': after == before['files']})
    write('TERMINAL.json', {'exit': exit_code, 'end': now(), 'same_selected_inputs': after == before['files']})
    (job / 'STATUS').write_text('DONE\n' if exit_code == 0 else 'FAILED\n')
    print('INTEGRATION470_EXIT=' + str(exit_code), flush=True)
raise SystemExit(exit_code)
