"""Prepare read-only guards from actual build evidence, outside timed campaigns."""
import argparse
import copy
import hashlib
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location('artifact_guard', Path(__file__).with_name('artifact-guard.py'))
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


def h(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def proof(path):
    path = Path(path).resolve()
    return {'path': str(path), 'sha256': h(path)}


def enrich(config, invocation_path, drivers):
    config = copy.deepcopy(config)
    for artifact in config['artifacts']:
        record = artifact['compiler_record']
        artifact['path'] = artifact.get('path', artifact['copy'])
        compiled = Path(record['target']['src_path'])
        manifest = Path(record['manifest_path'])
        # Historical records with hashes are checked, never replaced.
        for field, path in [('compiled_source_sha256', compiled), ('manifest_sha256', manifest)]:
            actual = h(path)
            if field in artifact:
                guard.need(artifact[field] == actual, 'historical compiled input changed: ' + str(path))
            else:
                artifact[field] = actual
    config['compiler_evidence']['invocations'] = proof(invocation_path)
    config['driver_sha256'] = {str(path.resolve()): h(path) for path in drivers}
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline-config', required=True)
    parser.add_argument('--baseline-job', required=True)
    parser.add_argument('--candidate-job', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    guard.need(guard.REQUIRED_CANDIDATE_COMMIT != 'UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING', 'candidate not yet qualified/published')
    here = Path(__file__).resolve().parent
    drivers = [here / name for name in ['artifact-guard.py', 'phase-diagnostic-check.py', 'phase-diagnostic.sh', 'prepare-pair.py']]
    base_job, job = Path(args.baseline_job).resolve(), Path(args.candidate_job).resolve()
    baseline = json.loads(Path(args.baseline_config).read_text())
    baseline = enrich(baseline, base_job / 'logs/matched-release.stderr', drivers)
    baseline['source_inventory_evidence']=proof(base_job/'SOURCE.json')
    baseline['release_oracle_evidence']=proof(base_job/'RELEASE_ORACLE.json')
    baseline['symbols']=json.loads((base_job/'SYMBOLS.json').read_text())
    baseline['comparison_scope']='immutable published430 baseline, four fresh asserted compiler artifacts, packed debug sidecar'
    source = json.loads((job / 'SOURCE.json').read_text())
    guard.need(source['commit'] == guard.REQUIRED_CANDIDATE_COMMIT, 'candidate committed source differs')
    artifacts = json.loads((job / 'ARTIFACTS.json').read_text())
    rustc_path = Path((job / 'logs/rustc-path.txt').read_text().strip())
    candidate = {
        'mode': 'asserted_release', 'source_commit': source['commit'],
        'source_root': str(job / 'source'), 'source_sha256': source['sha256'],
        'executable': str(job / 'artifacts/matched/voteboat-matched-framework'),
        'artifacts': artifacts,
        'compiler_evidence': {'rustc': proof(rustc_path), 'rustc_version': proof(job / 'logs/rustc.txt')},
        'comparison_scope': 'published431 candidate versus immutable430 on same host/compiler; opt3/debug2/assertions/overflow/thinLTO/codegen1; full source inventory and four fresh:false compiler artifacts; short diagnostic only',
    }
    candidate['source_inventory_evidence']=proof(job/'SOURCE.json')
    candidate['release_oracle_evidence']=proof(job/'RELEASE_ORACLE.json')
    candidate['symbols']=json.loads((job/'SYMBOLS.json').read_text())
    candidate = enrich(candidate, job / 'logs/matched-release.stderr', drivers)
    pair = {'baseline': baseline, 'candidate': candidate,
            'scope': 'same-host paired short diagnostic; Linux and Mac compiler identities differ; no sustained floor or durable acceptance claim',
            'source_inventory_scope': {'baseline': len(baseline['source_sha256']), 'candidate': len(candidate['source_sha256'])}}
    # Validate before output becomes usable by the runner.
    guard.verify(baseline)
    guard.verify(candidate)
    for identity in ['Cargo.lock', 'Cargo.toml', 'benchmarks/matched/Cargo.lock', 'benchmarks/matched/Cargo.toml', 'clippy.toml']:
        guard.need(baseline['source_sha256'][identity] == candidate['source_sha256'][identity], 'comparison inputs differ: ' + identity)
    for identity in ['rustc', 'rustc_version']:
        guard.need(baseline['compiler_evidence'][identity]['sha256'] == candidate['compiler_evidence'][identity]['sha256'], 'within-host compiler differs: ' + identity)
    output = Path(args.output)
    guard.need(not output.exists(), 'refuse existing CONFIG_PAIR')
    output.write_text(json.dumps(pair, indent=2) + '\n')
    print(json.dumps(guard.verify_pair(output), sort_keys=True))


if __name__ == '__main__':
    main()
