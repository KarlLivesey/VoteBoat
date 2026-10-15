"""Three functional controls after actual build; importing never launches work."""
from pathlib import Path
import hashlib
import importlib.util
import json
import re
import shlex
import subprocess
import sys


def main():
    job = Path(__file__).resolve().parent
    assert (job / 'STATUS').read_text().strip() == '0'
    assert (job / 'STAGE').read_text().strip() == 'DONE'
    assert json.loads((job / 'BUILD_TERMINAL.json').read_text())['actual_exit'] == 0
    h = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    artifacts = json.loads((job / 'ARTIFACTS.json').read_text())
    exe = next(row for row in artifacts if row['target'] == 'voteboat-matched-framework')
    binary = Path(exe['copy'])
    symbols = json.loads((job / 'SYMBOLS.json').read_text())
    sidecar = Path(symbols['copy'])
    assert h(binary) == exe['sha256'] and h(sidecar) == symbols['sha256']
    spec = importlib.util.spec_from_file_location('phase_checker', job / 'phase-diagnostic-check.py')
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    observer = (job / 'source/benchmarks/matched/src/attribution.rs').read_text()
    phase_count = int(re.search(r'const PHASES: usize = (\d+);', observer)[1])
    scope_count = int(re.search(r'const SCOPES: \[Phase; (\d+)\]', observer)[1])
    assert (phase_count, scope_count, len(checker.METHODS)) == (29, 4, 15)
    base = [str(binary), '--clients', '4096', '--batch', '4', '--operations', '16384',
            '--warmup', '1024', '--maintenance', 'default-retention',
            '--envelopes', 'upstream-default', '--campaign-timeout-secs', '3600']
    rows = []
    for name, engine, trace in [('off', 'voteboat', False), ('on', 'voteboat', True),
                                ('openraft', 'openraft', False)]:
        output = job / 'logs' / (name + '.json')
        status = job / 'logs' / (name + '.status')
        assert h(binary) == exe['sha256'] and h(sidecar) == symbols['sha256']
        (job / 'CLI_STAGE').write_text(name + '\n')
        args = base + ['--engine', engine] + (['--attribution', 'true' if trace else 'false'] if engine == 'voteboat' else [])
        if status.exists():
            assert int(status.read_text()) == 0 and output.is_file(), 'Refuse to rerun failed/partial control'
        else:
            assert not output.exists(), 'Refuse to overwrite partial output'
            (job / 'logs' / (name + '.command')).write_text(shlex.join(args) + '\n')
            with output.open('wb') as stdout, (job / 'logs' / (name + '.stderr')).open('wb') as stderr:
                result = subprocess.run(args, stdout=stdout, stderr=stderr)
            status.write_text(str(result.returncode) + '\n')
            assert result.returncode == 0, (name, result.returncode)
        value = json.loads(output.read_text())
        checker.case(value, name, 16384, 4096, 4, 16384)
        rows.append({'case': name, 'actual_exit': int(status.read_text()),
                     'completed_originals': value['completed_originals'],
                     'completed_requests': value['completed_requests'],
                     'applied_frontiers': value['applied_frontiers'],
                     'phase_scope_method_counts': [29, 4, 15] if trace else None,
                     'guarded_output_sha256': h(output),
                     'checker_sha256': h(job / 'phase-diagnostic-check.py'),
                     'scope': 'Functional control only; rates are not paired capacity evidence; no466 entry-allocation observer fields.'})
        assert h(binary) == exe['sha256'] and h(sidecar) == symbols['sha256']
    with (job / 'logs/cli-source-after.log').open('wb') as stdout:
        subprocess.run([sys.executable, str(job / 'guard.py'), str(job), 'source'], stdout=stdout, check=True)
    (job / 'CLI.json').write_text(json.dumps(rows, indent=2) + '\n')
    (job / 'CLI_TERMINAL.json').write_text(json.dumps({'actual_exit': 0, 'scope': 'three actual functional controls, no paired timing'}, indent=2) + '\n')
    (job / 'CLI_STAGE').write_text('DONE\n')
    print('All three functional controls/current phase checker/source and ELF+DWP hashes passed; no paired timing.')


if __name__ == '__main__':
    main()
