import collections, datetime, hashlib, json, os, plistlib, re, subprocess, sys
from pathlib import Path

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def load(path): return json.loads(Path(path).read_text())
def save(path, value): Path(path).write_text(json.dumps(value, indent=2)+'\n')
def proof(path):
    path = Path(path).resolve()
    return {'path': str(path), 'sha256': sha(path)}

def source_record(candidate):
    inventory = candidate/'SOURCE.json'; source = load(inventory)
    root = Path(source.get('source_root', candidate/'source')).resolve()
    return {**proof(inventory), 'commit': source['commit'], 'root': str(root),
            'files': source['sha256'], 'modes': source.get('physical_modes', source.get('modes')),
            'directories': source.get('physical_directory_modes', source.get('directory_modes')),
            'root_mode': source.get('source_root_mode', oct(root.stat().st_mode&0o777))}

def check_readbacks(lines, provider):
    result = {}
    expected = {(mode, str(replica)) for mode in ['Create', 'Recover'] for replica in [1, 2, 3]}
    for prefix in ['capacity_readback', 'publication_readback']:
        selected = [line for line in lines if line.startswith(prefix+' ')]
        rows = [dict(token.split('=', 1) for token in line.split()[1:]) for line in selected]
        assert len(rows) == 6 and {(r['mode'],r['replica']) for r in rows} == expected, prefix
        required = ({'offered_capacity_requested':'4096','offered_capacity_effective':'4096',
                     'client_requests':'4096','client_group_requests':'4096','client_bytes':'33554432',
                     'client_receipt_bytes':'1048576','shard_max_items':'8192','shard_control_items':'1024',
                     'shard_group_items':'4112','shard_group_control_items':'16','shard_group_bytes':'4194304'}
                    if prefix == 'capacity_readback' else
                    {'publication_requested':provider,'publication_effective':provider,
                     'publication_provider':'FileLogIo' if provider == 'atomic' else 'WitnessLogIo',
                     'publication_timings':'native','publication_scope':'ordinary_barrier',
                     'storage_error_scope':'overlapping_observations'})
        assert all(all(row.get(k)==v for k,v in required.items()) for row in rows), prefix
        result[prefix] = selected
    return result

def check_settings(case, root):
    summary = dict(token.split('=',1) for token in (root/'summary.txt').read_text().split())
    run = dict(token.split('=',1) for token in (root/'run.txt').read_text().split())
    quantum = str(case['ready_items']); command = case['command']
    assert command[command.index('--ready-items')+1] == quantum
    assert summary['ready_visit_items_requested'] == summary['ready_visit_items_effective'] == quantum
    assert summary['core_proposal_entries'] == '64' and summary['ready_visit_bytes'] == '1048576' and summary['ready_visit_ms'] == '2'
    assert summary['ready_batch_capable'] == 'true'
    for key,value in [('groups',case['groups']),('offers',case['offers']),('offered_rate',case['rate']),('window',case['window'])]:
        assert summary[key] == run[key] == str(value)
    assert summary['publication_effective'] == run['publication_effective'] == 'witness'
    assert summary['leader_placement'] == ','.join(str(g)+':1' for g in range(1,case['groups']+1))
    if case['weights'] is not None:
        assert command[command.index('--group-weights')+1] == case['weights']
        assert summary['workload_group_weights'] == run['workload_group_weights'] == case['weights']
        assert summary['workload_group_phase'] == run['workload_group_phase'] == 'absolute_operation_minus_one'
    else: assert 'workload_group_weights' not in summary and 'workload_group_weights' not in run
    return summary

def combination_record(path, candidate):
    terminal = load(path); assert terminal['exit'] == 0 and terminal['source_exact'] is True and terminal['cases'] == 2
    artifact = next(a for a in candidate['artifacts'] if a['target']=='native_benchmark')
    assert terminal['artifact_sha256'] == artifact['sha256']
    records = load(path.parent/'RESULTS.json'); assert sha(path.parent/'RESULTS.json') == terminal['results_sha256']
    assert {r['protocol'] for r in records} == {'tcp','quic'} and len(records) == 2
    for record in records:
        case = record['case']; assert case['ready_items']==64 and case['groups']==2 and case['weights']=='31,1' and case['publication']=='witness'
        assert record['process']['exit']==record['validator']['exit']==0
        s = record['summary']; assert s['applied']=='512' and s['recovered_value']=='576'
        assert s['retry_verified']==s['workers_joined']=='true' and all(s[k]=='0' for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
        check_settings(case,Path(record['root']))
        check_readbacks(record['capacity_readbacks']+record['publication_readbacks'],'witness')
    return [proof(path),proof(path.parent/'RESULTS.json'),proof(path.parent/'BEFORE.json'),proof(path.parent/'AFTER.json')]

def candidate_record(candidate, host, commit, authority):
    revision480 = commit == 'f7b0ef12709bb7e7aad4f0279851aa035b508e9e'
    assert revision480 or commit == '7e9068296ce00dde5bf3e03bf4d852ed2833d185'
    published = load(authority)
    assert re.fullmatch('[0-9a-f]{40}', commit)
    assert published['commit'] == published['remote_main'] == commit
    assert published['exit'] == published['remote_exit'] == 0 and published['source_exact'] is True
    source = source_record(candidate); assert source['commit'] == commit
    artifacts = load(candidate/'ARTIFACTS.json'); assert len(artifacts) == 2
    for artifact in artifacts:
        record = artifact['compiler_record']; profile = record['profile']
        assert not record['fresh'] and profile == {'opt_level':'3','debuginfo':0,'debug_assertions':True,'overflow_checks':True,'test':False}
        assert record['features'] == ['default','native','quic','tls']
        assert Path(record['manifest_path']).resolve() == Path(source['root'])/'Cargo.toml'
        expected = 'src/lib.rs' if artifact['target'] == 'voteboat' else 'examples/native_benchmark.rs'
        assert Path(record['target']['src_path']).resolve() == Path(source['root'])/expected
    assert {a['target'] for a in artifacts} == {'voteboat','native_benchmark'}
    symbols = load(candidate/'SYMBOLS.json')
    assert symbols['reference_oracle_cfg_absent_PROFILErelease'] and not symbols['root_buildscript_fresh_record']['fresh']
    assert 'voteboat_checkpoint_debug_oracle' not in symbols['root_buildscript_record']['cfgs']
    logs = candidate/'logs'
    rustc_lines = (logs/'native-root-rustc-lines.txt').read_text().splitlines(); assert len(rustc_lines) == 2
    for line in rustc_lines:
        assert '-C opt-level=3' in line and '-C debug-assertions=on' in line
        assert all(flag not in line for flag in ['-C lto=', '-C codegen-units=', '-C linker-plugin-lto', '-C overflow-checks=off', '-C debuginfo=2'])
    invocation = ((logs/'native-buildscript-invocation.txt').read_text()
                  if host == 'mac' else load(candidate/'PROFILE_PROOF.json')['actual_PROFILE_release_invocation'])
    assert 'PROFILE=release' in invocation and 'CARGO_MANIFEST_DIR='+source['root']+' ' in invocation
    proofs = [proof(authority), proof(candidate/'SOURCE.json'), proof(candidate/'ARTIFACTS.json'), proof(candidate/'SYMBOLS.json')]
    if host == 'linux':
        ready = load(candidate/'READY.json')
        assert ready['source_commit'] == commit and ready['actual_build_exit'] == 0
        for name,digest in ready['proof_sha256'].items(): assert sha(candidate/name) == digest, name
        if revision480:
            assert ready['actual_export_exit']==ready['actual_postterminal_completion_exit']==0
            assert ready['original_controls_exit']==1 and ready['actual_native_processes_pass']==ready['actual_native_storage_validators_pass']==3
            assert ready['default_legacy_filelogio_physical_verified'] is True
            completed=load(candidate/'controls/COMPLETION.json');assert completed['exit']==0 and completed['source_exact'] and completed['original_controller_exit']==1 and completed['no_workload_or_validator_rerun']
            all_controls=load(candidate/'controls/COMPLETED_RESULTS.json');assert len(all_controls)==3
            default=next(c for c in all_controls if c['selection']=='default-atomic')
            assert default['ready_quantum']==16 and not default['diagnostic_publication_selected'] and len(default['physical'])==3
            assert default['summary']['applied']=='512'and default['summary']['recovered_value']=='576'and not default['publication_readbacks']
            terminal=load(candidate/'controls/TERMINAL.json');assert terminal['actual_exit']==1 and terminal['source_exact']
            controls=[c for c in all_controls if c['selection']=='witness'];assert len(controls)==2
            proofs += [proof(candidate/name)for name in ['READY.json','controls/COMPLETION.json','controls/COMPLETED_RESULTS.json','controls/TERMINAL.json']]
        else:
            assert ready['actual_network_controls_exit']==0
            controls=load(candidate/'controls/RESULTS.json');terminal=load(candidate/'controls/TERMINAL.json')
            assert terminal['actual_exit']==0 and terminal['source_exact'] is True and terminal['cases']==4
            proofs += [proof(candidate/name)for name in ['READY.json','controls/RESULTS.json','controls/TERMINAL.json']]
        proofs += [proof(candidate/name)for name in ready['proof_sha256']]
        tool_file = logs/'tool-binaries.sha256'
    else:
        assert (candidate/'STATUS').read_text().strip() == '0' and (candidate/'STAGE').read_text().strip() == 'DONE'
        required = (['selected_tests','selected_core','client_runtime_all','client_runtime_core','all_library','counter_service','provider_metadata','provider_metadata_controls','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_tests','matched_fmt','matched_strict','native-release','artifact_guard','wide_controls','source_before','source_after'] if revision480 else ['selected_tests','native_library','publication_example','selected_js','provider_metadata','provider_metadata_controls','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_fmt','matched_strict','native-release','artifact_guard','wide_controls','source_before','source_after'])
        for name in required:
            assert (logs/(name+'.exit')).read_text().strip() == '0', name
        if revision480:
            appended=load(candidate/'APPEND_ONLY_RUNTIME_QUALIFICATION.json')
            assert appended['exit']==appended['actual_terminal_exit']==0 and appended['source_exact'] is True and appended['source_commit']==commit
            for name,digest in appended['proof_sha256'].items():assert sha(candidate/name)==digest,name
            proofs += [proof(candidate/'APPEND_ONLY_RUNTIME_QUALIFICATION.json')]
            proofs += [proof(candidate/name)for name in appended['proof_sha256']]
        controls_container = load(candidate/'CONTROLS.json')
        assert controls_container['artifact_unchanged'] and controls_container['end']
        controls = controls_container['cases']
        proofs += [proof(candidate/name) for name in ['STATUS','STAGE','CONTROLS.json']]
        proofs.append(proof(logs/'native-buildscript-invocation.txt'))
        proofs += [proof(p) for p in sorted(logs.glob('*.exit'))]
        tool_file = logs/'tool-sha256.txt'
    assert len(controls) == (2 if revision480 and host=='linux' else 4)
    observed = set()
    for control in controls:
        summary = control['summary']; provider = summary['publication_effective']
        assert summary['protocol'] in ['TcpTls','Quic']
        protocol = 'tcp' if summary['protocol'] == 'TcpTls' else 'quic'
        observed.add((protocol,provider))
        assert summary['applied'] == '512' and summary['recovered_value'] == '576'
        assert summary['retry_verified'] == summary['workers_joined'] == 'true'
        assert all(summary[k] == '0' for k in ['unknown','pending','window_refused','no_leader','admission_refused','not_proposed'])
        if host == 'linux': lines = control['capacity_readbacks'] + control['publication_readbacks']
        else:
            assert control['process']['exit'] == control['validator']['exit'] == 0
            lines = control['actual_capacity_readbacks'] + control['actual_publication_readbacks']
        check_readbacks(lines,provider)
    assert observed == ({(p,'witness')for p in ['tcp','quic']}if revision480 and host=='linux' else {(p,a)for p in ['tcp','quic']for a in ['atomic','witness']})
    tools = []
    for line in tool_file.read_text().splitlines():
        digest,path = line.split(None,1); path = path.lstrip('* ')
        assert re.fullmatch('[0-9a-f]{64}',digest) and sha(path) == digest
        tools.append({'path':path,'sha256':digest})
    for name in ['rustc.txt','cargo.txt','protoc.txt','native-release-profile.txt','native-root-rustc-lines.txt']:
        proofs.append(proof(logs/name))
    proofs.append(proof(tool_file))
    return {'source':source, 'artifacts':artifacts,
            'proofs':list({p['path']:p for p in proofs}.values()), 'compiler_tools':tools}

def guards(config):
    assert Path(sys.executable).resolve() == Path(config['python'])
    executables={arm:Path(next(a['copy']for a in record['artifacts']if a['target']=='native_benchmark')).resolve()for arm,record in config['candidates'].items()}
    for case in config['cases']:
        assert Path(case['resource_command'][0]).resolve() == Path(config['python'])
        assert case['resource_command'][3:] == case['command'] and Path(case['command'][0]).resolve() == executables[case['arm']]
    for record in config['candidates'].values():
        source = record['source']; root = Path(source['root'])
        assert sha(source['path']) == source['sha256']
        assert {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()} == set(source['files'])
        assert {p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_dir()} == set(source['directories'])-{'.'}
        assert oct(root.stat().st_mode&0o777) == source['root_mode']
        for name,digest in source['files'].items():
            path = root/name
            assert sha(path) == digest and oct(path.stat().st_mode&0o777) == source['modes'][name], name
        for name,mode in source['directories'].items(): assert oct((root/name).stat().st_mode&0o777) == mode, name
        for artifact in record['artifacts']:
            assert sha(artifact['copy']) == sha(artifact['original']) == artifact['sha256']
        for item in record['proofs'] + record['compiler_tools'] + config['runtime_tools']:
            assert sha(item['path']) == item['sha256'], item['path']
    for name,digest in config['scripts'].items(): assert sha(Path(config['job'])/name) == digest, name
    for name,digest in config['validators'].items(): assert sha(Path(config['job'])/'control-tools'/name) == digest, name

def command(args):
    run = subprocess.run(args, capture_output=True)
    result = {'command':args,'exit':run.returncode,'stdout':run.stdout.decode(errors='replace'),'stderr':run.stderr.decode(errors='replace')}
    assert run.returncode == 0, result
    return result

def host_snapshot(host, wal):
    wal = Path(wal)
    commands = []
    if host == 'linux':
        mount = command(['findmnt','--json','-T',str(wal),'-o','TARGET,SOURCE,FSTYPE,OPTIONS,UUID']); commands.append(mount)
        selected = json.loads(mount['stdout'])['filesystems']; assert len(selected) == 1
        device = selected[0]['source'].split('[',1)[0]
        physical = command(['lsblk','--json','--bytes','--inverse','--paths','-o','NAME,TYPE,FSTYPE,UUID,MODEL,SERIAL,TRAN',device]); commands.append(physical)
        identity = {'st_dev':wal.stat().st_dev,'mount':selected[0],'physical_chain':json.loads(physical['stdout'])}
        fields = ['scaling_driver','scaling_governor','energy_performance_preference','scaling_min_freq','scaling_max_freq']
        policy = {field:dict(sorted(collections.Counter(p.read_text().strip() for p in Path('/sys/devices/system/cpu').glob('cpu[0-9]*/cpufreq/'+field)).items())) for field in fields}
        commands.append(command(['ps','-eo','pid,ppid,stat,pcpu,pmem,etime,comm','--sort=-pcpu']))
        commands.append(command(['lsattr','-d',str(wal)]))
    else:
        df = command(['/bin/df','-P',str(wal)]); commands.append(df)
        device = df['stdout'].splitlines()[-1].split()[0]
        info = command(['/usr/sbin/diskutil','info','-plist',device]); commands.append(info)
        volume = plistlib.loads(info['stdout'].encode())
        stores = []
        for physical in volume['APFSPhysicalStores']:
            raw = command(['/usr/sbin/diskutil','info','-plist',physical['APFSPhysicalStore']]); commands.append(raw)
            store = plistlib.loads(raw['stdout'].encode())
            stores.append({k:store.get(k) for k in ['DeviceIdentifier','DiskUUID','ParentWholeDisk','MediaName','Internal','SolidState','DeviceTreePath','Protocol','TotalSize']})
        identity = {'st_dev':wal.stat().st_dev,'device':device,'volume':{k:volume.get(k) for k in ['DeviceIdentifier','VolumeUUID','DiskUUID','MountPoint','FilesystemType','APFSContainerReference']},'physical_stores':stores}
        custom = command(['/usr/bin/pmset','-g','custom']); commands.append(custom); policy = {'pmset_custom':custom['stdout']}
        commands += [command(['/usr/bin/pmset','-g','therm']), command(['/bin/ps','-axo','pid,ppid,state,%cpu,%mem,etime,comm']), command(['/usr/sbin/sysctl','-n','machdep.cpu.brand_string','hw.ncpu','hw.memsize'])]
    return {'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'device_identity':identity,'cpu_policy':policy,'commands':commands,
            'scope':'Read-only outside timing; no policy/filesystem/settings changes. Three voters share this volume/device. CPU policy bounds are not workload frequencies; process percentages are lifetime observations.'}
