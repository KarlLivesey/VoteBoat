import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

repo = Path.cwd()
job = repo / 'target/p9/native470-linux/controls'
env = os.environ.copy()
(job / 'test-tmp').mkdir(exist_ok=True)
env['TMPDIR'] = str(job / 'test-tmp')
commands = [('tcp-startup', ['/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/artifacts/native/native_benchmark', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-startup', 'tcp', '128', '64', '--append-window', '4', '--journal-timings']), ('tcp-startup-raw', ['node', 'validation/check-native-benchmark.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-startup']), ('tcp-startup-selection', ['node', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/check-selection.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-startup']), ('tcp-startup-journal', ['node', 'validation/check-journal-timings.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-startup/journal.csv']), ('tcp-shared', ['/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/artifacts/native/native_benchmark', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-shared', 'tcp', '128', '64', '1', '--append-window', '4']), ('tcp-shared-raw', ['node', 'validation/check-native-benchmark.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-shared', '--storage']), ('tcp-shared-selection', ['node', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/check-selection.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/tcp-shared']), ('quic-startup', ['/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/artifacts/native/native_benchmark', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-startup', 'quic', '128', '64', '--append-window', '4', '--journal-timings']), ('quic-startup-raw', ['node', 'validation/check-native-benchmark.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-startup']), ('quic-startup-selection', ['node', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/check-selection.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-startup']), ('quic-startup-journal', ['node', 'validation/check-journal-timings.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-startup/journal.csv']), ('quic-shared', ['/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/artifacts/native/native_benchmark', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-shared', 'quic', '128', '64', '1', '--append-window', '4']), ('quic-shared-raw', ['node', 'validation/check-native-benchmark.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-shared', '--storage']), ('quic-shared-selection', ['node', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/check-selection.mjs', '/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native470-linux/controls/quic-shared'])]

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def write(name, value): (job / name).write_text(json.dumps(value, indent=2) + '\n')
assert not (job / 'RUN.json').exists(), 'Refuse to restart or overwrite controls'
for name in ['tcp-startup', 'tcp-shared', 'quic-startup', 'quic-shared']:
    assert not (job / name).exists()
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
