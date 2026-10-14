"""No build/toolchain invocation: verify frozen input and actual artifact identities."""
import hashlib
import json
import sys
import subprocess
from pathlib import Path


REQUIRED_CANDIDATE_COMMIT = '9d2b7852862a633e188afb38de8e2d3578efb5ca'  # Checked429 published; actual Mac qualification still required.
UPSTREAM_COMMIT = 'e88bb40707f2e511d8ecc44d31edcb1d814a11a8'

def need(condition, label):
    if not condition:
        raise ValueError(label)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify(config):
    need(len(REQUIRED_CANDIDATE_COMMIT)==40 and all(c in '0123456789abcdef' for c in REQUIRED_CANDIDATE_COMMIT), 'candidate not yet qualified/published')
    mode = config['mode']
    need(mode in ('asserted_release', 'functional_debug_smoke'), 'unknown mode')
    need(config['source_commit'] in ('832479d7dc11db176549baea422b98b00b3e01dc', REQUIRED_CANDIDATE_COMMIT), 'wrong published source')
    if config['source_commit']==REQUIRED_CANDIDATE_COMMIT:
        need('tests/provider_conformance/log_store/entry_equality.rs' in config['source_sha256'], 'published equality oracle module absent')
    if config['source_commit']==REQUIRED_CANDIDATE_COMMIT:
        need('benchmarks/matched/src/voteboat_engine/actor/tests/replies.rs' in config['source_sha256'], '430 actor reply contracts absent')
    root = Path(config['source_root']).resolve()
    need(bool(config['source_sha256']), 'missing source map')
    actual = {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()}
    need(actual == set(config['source_sha256']), 'source inventory differs')
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
    need(len(artifacts) == len(config['artifacts']), 'duplicate artifact targets')
    need('voteboat-matched-framework' in artifacts, 'executable proof absent')
    binary = artifacts['voteboat-matched-framework']
    need(Path(binary['path']).resolve() == Path(config['executable']).resolve(),
         'driver does not use guarded executable')
    for name, artifact in artifacts.items():
        need(digest(artifact['path']) == artifact['sha256'], 'changed artifact: ' + name)
    for path, expected in config['driver_sha256'].items():
        need(digest(path) == expected, 'changed driver input: ' + path)
    if mode == 'asserted_release':
        need(set(artifacts) == {'voteboat', 'voteboat-matched-framework', 'openraft', 'bench_minimal'}, 'four actual library/executable proofs required')
        for name, source in [('voteboat', 'src/lib.rs'),
                             ('voteboat-matched-framework', 'benchmarks/matched/src/main.rs'),
                             ('openraft', None), ('bench_minimal', None)]:
            artifact = artifacts[name]
            record = artifact['compiler_record']
            need(record['fresh'] is False, 'reused compiler record: ' + name)
            need(record['target']['name'] == name, 'wrong compiler target: ' + name)
            compiled = Path(record['target']['src_path']).resolve()
            manifest = Path(record['manifest_path']).resolve()
            need(digest(compiled) == artifact['compiled_source_sha256'], 'changed compiled source: ' + name)
            need(digest(manifest) == artifact['manifest_sha256'], 'changed compiler manifest: ' + name)
            if source is not None:
                need(compiled == root / source, 'compiler source not frozen input: ' + name)
                need(record['features'] == [], 'unexpected root/executable features')
                need(manifest == root / ('Cargo.toml' if name == 'voteboat' else 'benchmarks/matched/Cargo.toml'), 'wrong root manifest')
            else:
                need(UPSTREAM_COMMIT in record['package_id'], 'upstream pin changed: ' + name)
                checkout = next((parent for parent in compiled.parents if (parent / '.git').exists()), None)
                need(checkout is not None, 'upstream checkout absent')
                head = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'], text=True).strip()
                need(head == UPSTREAM_COMMIT, 'upstream checkout commit changed')
                status = subprocess.check_output(['git', '-C', str(checkout), 'status', '--porcelain'], text=True).splitlines()
                need(all(line == '?? .cargo-ok' for line in status), 'dirty upstream checkout')
                for path in [compiled, manifest]:
                    relative = path.relative_to(checkout).as_posix()
                    committed = subprocess.check_output(['git', '-C', str(checkout), 'show', 'HEAD:' + relative])
                    need(hashlib.sha256(committed).hexdigest() == digest(path), 'upstream committed input differs: ' + relative)
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
        for name in ['voteboat', 'voteboat_matched_framework', 'openraft', 'bench_minimal']:
            actual_lines = [line for line in lines if '--crate-name ' + name + ' ' in line]
            need(len(actual_lines) == 1, 'actual unique rustc invocation absent: ' + name)
            line = actual_lines[0]
            need(all(flag in line for flag in ['-C opt-level=3', '-C debuginfo=2', '-C debug-assertions=on', '-C codegen-units=1','-C split-debuginfo=packed']), 'wrong actual rustc flags: ' + name)
            need('-C overflow-checks=off' not in line, 'overflow disabled: ' + name)
        bin_lines = [line for line in lines if '--crate-name voteboat_matched_framework' in line]
        need(bool(bin_lines), 'actual binary rustc invocation absent')
        need(any('-C lto=thin' in line and '-C codegen-units=1' in line and '-C split-debuginfo=packed' in line for line in bin_lines),
             'actual thin/one compiler invocation absent')
        need(bool(Path(evidence['rustc_version']['path']).read_text().strip()), 'empty rustc identity')
    else:
        need(config['qualification'] == 'debug functional smoke only; no release/timing claim',
             'smoke must disclose unavailable release proof')
    source_evidence=config['source_inventory_evidence']
    need(digest(source_evidence['path'])==source_evidence['sha256'],'changed source provenance')
    inventory=json.loads(Path(source_evidence['path']).read_text())
    need(inventory['commit']==config['source_commit'] and inventory['sha256']==config['source_sha256'],'source provenance mismatch')
    symbols=config['symbols']
    need(symbols['format']=='darwin_dsym_bundle', 'wrong Mac debug sidecar format')
    need(bool(symbols['files']), 'empty dSYM inventory')
    original_bundle=Path(symbols['original_bundle']).resolve()
    copied_bundle=Path(symbols['copy_bundle']).resolve()
    need(str(original_bundle)==str(Path(binary['original']+'.dSYM').resolve()), 'dSYM does not belong to original executable')
    need(original_bundle in [Path(path).resolve() for path in binary['compiler_record']['filenames']], 'dSYM absent from actual compiler artifact')
    for location,bundle in [('original',original_bundle),('copy',copied_bundle)]:
        actual_symbols={p.relative_to(bundle).as_posix() for p in bundle.rglob('*') if p.is_file()}
        need(actual_symbols==set(symbols['files']), 'changed dSYM inventory: '+location)
        for relative,record in symbols['files'].items():
            path=(bundle/relative).resolve()
            need(bundle in path.parents, 'symbol path escapes bundle')
            need(digest(path)==record['sha256'], 'changed dSYM file: '+location+'/'+relative)
    need(symbols['executable_sha256']==binary['sha256'],'dSYM/executable correlation differs')
    need('build.rs' in config['source_sha256'],'essential candidate build.rs absent')
    receipt=config['release_oracle_evidence']
    need(digest(receipt['path'])==receipt['sha256'],'changed release oracle proof')
    oracle=json.loads(Path(receipt['path']).read_text())
    need(oracle['build_rs_sha256']==config['source_sha256']['build.rs'],'build.rs compiler proof differs')
    need(oracle['profile']=='release' and oracle['reference_cfg_absent'] and oracle['ordinary_assertions_overflow_enabled'],'release oracle semantics differ')
    need('voteboat_checkpoint_debug_oracle' not in oracle['build_script_record']['cfgs'],'release oracle cfg emitted')
    need(oracle['build_script_compilation']['fresh'] is False,'root buildscript reused')
    need(Path(oracle['build_script_compilation']['target']['src_path']).resolve()==root/'build.rs','wrong buildscript source')
    need('PROFILE=release' in oracle['actual_PROFILE_release_invocation'],'actual PROFILE release absent')
    root_line=next(line for line in lines if '--crate-name voteboat ' in line)
    need(root_line==oracle['actual_root_rustc'],'root invocation receipt differs')
    need('--cfg voteboat_checkpoint_debug_oracle' not in root_line,'reference oracle remains in timed path')
    need("--check-cfg 'cfg(voteboat_checkpoint_debug_oracle)'" in root_line,'root cfg declaration absent')
    return {'source_inputs': len(config['source_sha256']), 'artifacts': sorted(artifacts),
            'mode': mode, 'checks': 'all frozen source, artifact, driver and applicable compiler guards pass'}


def verify_pair(path):
    pair = json.loads(Path(path).read_text())
    need(len(REQUIRED_CANDIDATE_COMMIT) == 40 and all(c in '0123456789abcdef' for c in REQUIRED_CANDIDATE_COMMIT),
         'candidate not yet qualified/published')
    baseline, candidate = pair['baseline'], pair['candidate']
    need(baseline['source_commit'] == '832479d7dc11db176549baea422b98b00b3e01dc',
         'wrong baseline source')
    need(candidate['source_commit'] == REQUIRED_CANDIDATE_COMMIT, 'wrong candidate source')
    need(baseline['mode'] == candidate['mode'] == 'asserted_release', 'unasserted artifact')
    for identity in ['Cargo.lock', 'Cargo.toml', 'benchmarks/matched/Cargo.lock',
                     'benchmarks/matched/Cargo.toml', 'clippy.toml']:
        need(baseline['source_sha256'][identity] == candidate['source_sha256'][identity],
             'comparison config/dependency changed: ' + identity)
    need(baseline['compiler_evidence']['rustc']['sha256'] ==
         candidate['compiler_evidence']['rustc']['sha256'], 'compiler differs')
    need(baseline['compiler_evidence']['rustc_version']['sha256'] ==
         candidate['compiler_evidence']['rustc_version']['sha256'], 'compiler version differs')
    return {'baseline': verify(baseline), 'candidate': verify(candidate)}


if __name__ == '__main__':
    print(json.dumps(verify_pair(sys.argv[1]), sort_keys=True))
