"""No build/toolchain invocation: verify frozen input and actual artifact identities."""
import hashlib
import json
import sys
from pathlib import Path


def need(condition, label):
    if not condition:
        raise ValueError(label)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify(config):
    mode = config['mode']
    need(mode in ('asserted_release', 'functional_debug_smoke'), 'unknown mode')
    need(config['source_commit'] == 'ccee8eca1fbc8e2584f148e4e69ef34a614aa2bc', 'wrong published source')
    root = Path(config['source_root']).resolve()
    need(bool(config['source_sha256']), 'missing source map')
    for relative, expected in config['source_sha256'].items():
        path = (root / relative).resolve()
        need(root in path.parents, 'source path escapes frozen root')
        need(digest(path) == expected, 'changed source: ' + relative)
    need({'Cargo.toml', 'Cargo.lock', 'benchmarks/matched/Cargo.toml',
          'benchmarks/matched/Cargo.lock', 'src/lib.rs',
          'benchmarks/matched/src/main.rs',
          'benchmarks/matched/src/attribution/gaps.rs',
          'benchmarks/matched/src/attribution/gaps/tests.rs'} <= set(config['source_sha256']),
         'essential source/lock identities absent')
    artifacts = {a['target']: a for a in config['artifacts']}
    need('voteboat-matched-framework' in artifacts, 'executable proof absent')
    binary = artifacts['voteboat-matched-framework']
    need(Path(binary['path']).resolve() == Path(config['executable']).resolve(),
         'driver does not use guarded executable')
    for name, artifact in artifacts.items():
        need(digest(artifact['path']) == artifact['sha256'], 'changed artifact: ' + name)
    for path, expected in config['driver_sha256'].items():
        need(digest(path) == expected, 'changed driver input: ' + path)
    if mode == 'asserted_release':
        need('voteboat' in artifacts, 'root library proof absent')
        for name, source in [('voteboat', 'src/lib.rs'),
                             ('voteboat-matched-framework', 'benchmarks/matched/src/main.rs')]:
            artifact = artifacts[name]
            record = artifact['compiler_record']
            need(record['fresh'] is False, 'reused compiler record: ' + name)
            need(record['target']['name'] == name, 'wrong compiler target: ' + name)
            need(Path(record['target']['src_path']).resolve() == root / source,
                 'compiler source not frozen input: ' + name)
            profile = record['profile']
            need(profile == {'opt_level': '3', 'debuginfo': 2, 'debug_assertions': True,
                             'overflow_checks': True, 'test': False},
                 'wrong actual compiler profile: ' + name)
            need(artifact['original'] in record['filenames'], 'uncorrelated original artifact')
            need(digest(artifact['original']) == artifact['sha256'], 'original/copy differ')
        evidence = config['compiler_evidence']
        for identity in ['rustc', 'rustc_version', 'invocations']:
            need(digest(evidence[identity]['path']) == evidence[identity]['sha256'],
                 'changed compiler evidence: ' + identity)
        lines = Path(evidence['invocations']['path']).read_text().splitlines()
        bin_lines = [line for line in lines if '--crate-name voteboat_matched_framework' in line]
        need(bool(bin_lines), 'actual binary rustc invocation absent')
        need(any('-C lto=thin' in line and '-C codegen-units=1' in line for line in bin_lines),
             'actual thin/one compiler invocation absent')
        need(bool(Path(evidence['rustc_version']['path']).read_text().strip()), 'empty rustc identity')
    else:
        need(config['qualification'] == 'debug functional smoke only; no release/timing claim',
             'smoke must disclose unavailable release proof')
    return {'source_inputs': len(config['source_sha256']), 'artifacts': sorted(artifacts),
            'mode': mode, 'checks': 'all frozen source, artifact, driver and applicable compiler guards pass'}


if __name__ == '__main__':
    print(json.dumps(verify(json.loads(Path(sys.argv[1]).read_text())), sort_keys=True))
