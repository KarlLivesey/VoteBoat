"""Prepare read-only guards from actual build evidence, outside timed campaigns."""
import argparse
import copy
import hashlib
import importlib.util
import json
import re
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


def symbol_proof(config, copied_bundle):
    binary=next(a for a in config['artifacts'] if a['target']=='voteboat-matched-framework')
    original=Path(binary['original']+'.dSYM').resolve()
    copied=original if copied_bundle is None else Path(copied_bundle).resolve()
    files={}
    for file in sorted(original.rglob('*')):
        if file.is_file():
            relative=file.relative_to(original).as_posix()
            guard.need(h(file)==h(copied/relative),'symbol original/copy differs')
            files[relative]={'sha256':h(file)}
    return {'format':'darwin_dsym_bundle','original_bundle':str(original),'copy_bundle':str(copied),
            'files':files,'executable_sha256':binary['sha256'],
            'baseline_copy_scope':'existing original bundle retained read-only' if copied_bundle is None else 'qualification copied every symbol file'}


def release_proof(job, output_root):
    events=[json.loads(line) for line in (job/'logs/matched-release.jsonl').read_text().splitlines() if line.startswith('{')]
    source=job/'source'
    scripts=[e for e in events if e.get('reason')=='compiler-artifact' and Path(e.get('target',{}).get('src_path','')).resolve()==(source/'build.rs').resolve()]
    guard.need(len(scripts)==1 and scripts[0]['fresh'] is False,'fresh root buildscript compilation absent')
    root_event=next(e for e in events if e.get('reason')=='compiler-artifact' and e.get('target',{}).get('name')=='voteboat')
    executed=[e for e in events if e.get('reason')=='build-script-executed' and e['package_id']==root_event['package_id']]
    guard.need(len(executed)==1 and executed[0]['cfgs']==[],'actual release cfg emission differs')
    lines=(job/'logs/matched-release.stderr').read_text().splitlines()
    profile_lines=[]
    for line in lines:
        match=re.search(r'CARGO_MANIFEST_DIR=(\S+)',line)
        if 'PROFILE=release' in line and '/build-script-build' in line and match and Path(match.group(1).strip("'\"")).resolve()==source.resolve():
            profile_lines.append(line)
    root_lines=[line for line in lines if '--crate-name voteboat ' in line]
    guard.need(len(profile_lines)==1 and len(root_lines)==1,'actual release profile/root invocation proof absent')
    receipt={'profile':'release','build_rs_sha256':h(source/'build.rs'),'reference_cfg_absent':True,
             'ordinary_assertions_overflow_enabled':root_event['profile']['debug_assertions'] and root_event['profile']['overflow_checks'],
             'build_script_record':executed[0],'build_script_compilation':scripts[0],
             'actual_PROFILE_release_invocation':profile_lines[0],'actual_root_rustc':root_lines[0],
             'scope':'derived from original qualified Mac435 raw compiler records; no new builds'}
    output=output_root/'MAC_RELEASE_ORACLE.json'
    guard.need(not output.exists(),'refuse replacing existing release receipt')
    output.write_text(json.dumps(receipt,indent=2)+'\n')
    return proof(output)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline-config', required=True)
    parser.add_argument('--baseline-job', required=True)
    parser.add_argument('--candidate-job', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    drivers = [here / name for name in ['artifact-guard.py', 'phase-diagnostic-check.py', 'phase-diagnostic.sh', 'prepare-pair.py']]
    base_job, job = Path(args.baseline_job).resolve(), Path(args.candidate_job).resolve()
    baseline = json.loads(Path(args.baseline_config).read_text())
    baseline = enrich(baseline, base_job / 'logs/matched-release.stderr', drivers)
    baseline['source_inventory_evidence']=proof(base_job/'SOURCE.json')
    baseline['comparison_scope']='immutable published434 Mac baseline; same packed profile; original and qualification-copied dSYM bundle remain read-only'
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
        'comparison_scope': 'published435 candidate versus immutable434 on same host/compiler; opt3/debug2/assertions/overflow/thinLTO/codegen1; full source inventory and four fresh:false compiler artifacts; requested20M bulk sustained diagnostic first, singleton1M only if bulk retained; not full acceptance',
    }
    candidate['source_inventory_evidence']=proof(job/'SOURCE.json')
    candidate['release_oracle_evidence']=release_proof(job,here)
    candidate['symbols']=symbol_proof(candidate,copied_bundle=job/'artifacts/symbols/voteboat-matched-framework.dSYM')
    candidate = enrich(candidate, job / 'logs/matched-release.stderr', drivers)
    pair = {'baseline': baseline, 'candidate': candidate,
            'scope': 'same-host repeated sustained bulk20M cell then eligible short singleton1M; Linux and Mac compiler identities differ; no full curve, durable or margin acceptance claim',
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
