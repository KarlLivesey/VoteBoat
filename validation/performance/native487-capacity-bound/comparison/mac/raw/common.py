import collections, datetime, hashlib, json, os, plistlib, re, subprocess, sys
from pathlib import Path

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def load(path): return json.loads(Path(path).read_text())
def save(path, value): Path(path).write_text(json.dumps(value, indent=2)+'\n')
def proof(path):
    path = Path(path).resolve()
    return {'path': str(path), 'sha256': sha(path)}

BASELINE_COMMIT = 'f7b0ef12709bb7e7aad4f0279851aa035b508e9e'

def required_commit():
    value = (Path(__file__).resolve().parent/'REQUIRED_COMMIT').read_text().strip()
    assert re.fullmatch('[0-9a-f]{40}', value) and value != BASELINE_COMMIT, '486 REQUIRED_COMMIT is UNSET/unpublished or incorrectly equals baseline480'
    return value

def source_record(candidate):
    inventory = candidate/'SOURCE.json'; source = load(inventory)
    root = Path(source.get('source_root', candidate/'source')).resolve()
    return {**proof(inventory), 'commit': source['commit'], 'root': str(root),
            'files': source['sha256'], 'modes': source.get('physical_modes', source.get('modes')),
            'directories': source.get('physical_directory_modes', source.get('directory_modes')),
            'root_mode': source.get('source_root_mode', oct(root.stat().st_mode&0o777))}

def check_readbacks(lines, provider, publication=True):
    result = {}
    expected = {(mode, str(replica)) for mode in ['Create', 'Recover'] for replica in [1, 2, 3]}
    assert publication or not any(line.startswith('publication_readback ') for line in lines), 'Omitted publication control must not invent diagnostic readbacks'
    for prefix in ['capacity_readback'] + (['publication_readback'] if publication else []):
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

def candidate_record(candidate, host, commit, authority):
    revision480 = commit == BASELINE_COMMIT
    revision486 = not revision480 and commit == required_commit()
    assert revision480 or revision486, 'Comparison supports only immutable480 and the actual published486 candidate'
    published = load(authority)
    assert re.fullmatch('[0-9a-f]{40}', commit)
    assert published['commit'] == published['remote_main'] == commit
    if revision480:
        assert published['exit'] == published['remote_exit'] == 0 and published['source_exact'] is True
    else:
        assert published['actual_exit']==0 and published['enabled_hook']=='.githooks/pre-push' and published['selected_inputs']==1496
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
        assert ready['artifact_sha256']==next(a['sha256']for a in artifacts if a['target']=='native_benchmark')
        assert ready['root_artifact_sha256']==next(a['sha256']for a in artifacts if a['target']=='voteboat')
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
        elif revision486:
            assert ready['actual_export_exit']==ready['actual_network_controls_exit']==0
            assert ready['cases']==ready['actual_native_processes_pass']==ready['actual_native_storage_validators_pass']==3
            assert ready['actual_instrumented_witness_publication_validators_pass']==2 and ready['default_legacy_filelogio_physical_verified'] is True
            terminal=load(candidate/'controls/TERMINAL.json');all_controls=load(candidate/'controls/RESULTS.json')
            assert terminal['actual_exit']==0 and terminal['source_exact'] is True and terminal['cases']==len(all_controls)==3
            assert terminal['all_owned_children_waited'] and not terminal['received_signals']
            assert {(c['protocol'],c['selection'],c['ready_quantum'],c['groups'],c['weights'])for c in all_controls}=={('tcp','witness',64,2,'31,1'),('quic','witness',64,2,'31,1'),('tcp','default-atomic',16,1,None)}
            default=next(c for c in all_controls if c['selection']=='default-atomic')
            assert not default['diagnostic_publication_selected'] and not default['publication_readbacks']
            assert '--publication'not in default['native_command'] and '--ready-items'not in default['native_command']
            assert not any(k.startswith('publication_')for k in default['summary'])
            assert len(default['physical'])==3 and all(r['metadata_bytes']==52 for r in default['physical'])
            assert default['summary']['ready_visit_items_requested']==default['summary']['ready_visit_items_effective']=='16'
            assert default['summary']['applied']=='512'and default['summary']['recovered_value']=='576'and default['summary']['retry_verified']==default['summary']['workers_joined']=='true'
            assert all(default['summary'][key]=='0'for key in ['unknown','pending','window_refused','no_leader','admission_refused','not_proposed'])
            check_readbacks(default['capacity_readbacks'],'atomic',publication=False)
            controls=[c for c in all_controls if c['selection']=='witness']
            for control in all_controls:
                name=control['protocol']+'-'+control['selection']
                for suffix in ['','-check']+(['-publication-check']if control['selection']=='witness'else[]):
                    outcome=load(candidate/'controls'/(name+suffix+'.exit.json'))
                    assert outcome['exit']==0 and outcome['error'] is None and not outcome['received_signals']
            qualification=load(candidate/'FUNCTIONAL_QUALIFICATION.json');assert qualification['source_commit']==commit
            inventory_ref=qualification['source_inventory'];assert sha(inventory_ref['path'])==inventory_ref['sha256']
            qualified=load(inventory_ref['path'])
            assert len(qualified)==ready['qualified_projection_files']
            if 'selected_inputs'in published:assert published['selected_inputs']==len(qualified)
            for name,item in qualified.items():
                assert source['files'][name]==item['sha256']and source['modes'][name]==item['mode'],name
            proofs += [proof(inventory_ref['path'])]
            assert qualification['final_source_guards']
            for pair in qualification['final_source_guards']:
                assert set(pair)=={'before','after'}
                for reference in pair.values():
                    assert sha(reference['path'])==reference['sha256']
                    assert load(reference['path'])==qualified
                    proofs += [proof(reference['path'])]
            required_checks={'fmt','application-all','application-unit-all','clients-all','providers-all','application-core','application-unit-core','clients-core','providers-core','services','matched','metadata','metadata-negative','strict'}
            assert set(qualification['actual_exit_files'])==required_checks
            for reference in qualification['actual_exit_files'].values():
                assert sha(reference['path'])==reference['sha256']and Path(reference['path']).read_text().strip()=='0'
                proofs += [proof(reference['path'])]
            for reference in qualification['proofs'].values():
                assert sha(reference['path'])==reference['sha256']
                proofs += [proof(reference['path'])]
            checked=load(qualification['proofs']['checked']['path'])
            assert checked['actual_exit']==0 and checked['source_inputs']==len(qualified)and checked['source_hashes_modes_unchanged']is True
            assert checked['functional']=={'application':{'all':23,'core':23,'new_controls':6,'oracle_comparisons_per_configuration':880968},'application_unit':{'all':39,'core':39},'client_admission':{'all':28,'core':28},'provider':{'all':42,'core':27},'actual_tcp_quic_three_service_recovery_retry':2,'standalone_matched':70}
            assert checked['metadata']=={'partial_contracts':44,'reviewed_operations':415,'negative_checks':42}
            commands=load(qualification['proofs']['commands']['path'])
            assert {r['name']for r in commands}==required_checks and all(r['actual_exit']==0 for r in commands)
            assert next(r['argv']for r in commands if r['name']=='strict')==['sh','.githooks/pre-push']
            publication_guards=[]
            for reference in qualification['publication_source_guards']:
                assert sha(reference['path'])==reference['sha256']
                publication_guards.append(load(reference['path']));proofs += [proof(reference['path'])]
            assert len(publication_guards)==2 and publication_guards[0]==publication_guards[1]and publication_guards[0]['exact_selected_inputs']==len(qualified)
            proofs += [proof(candidate/name)for name in ['READY.json','controls/RESULTS.json','controls/TERMINAL.json','FUNCTIONAL_QUALIFICATION.json']]
        proofs += [proof(candidate/name)for name in ready['proof_sha256']]
        tool_file = logs/'tool-binaries.sha256'
    else:
        assert (candidate/'STATUS').read_text().strip() == '0' and (candidate/'STAGE').read_text().strip() == 'DONE'
        required = ['selected_tests','selected_core','client_runtime_all','client_runtime_core','all_library','counter_service','provider_metadata','provider_metadata_controls','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_tests','matched_fmt','matched_strict','native-release','artifact_guard','wide_controls','source_before','source_after']
        if revision486:
            required=['application_all','application_core','client_all','client_core','unit_application_all','unit_application_core','provider_all','provider_core','counter_service','matched_tests','provider_metadata','provider_metadata_controls','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_fmt','matched_strict','native-release','artifact_guard','positive_controls','source_before','source_after','tool_rustc_identity','tool_cargo_identity','tool_hash_identity','tool_protoc_identity']
            ready=load(candidate/'READINESS.json');original=load(candidate/'ORIGINAL_TERMINAL.json')
            assert ready['exit']==0 and ready['source_exact'] is True and ready['source_commit']==commit
            assert ready['source_inventory_sha256']==sha(candidate/'SOURCE.json')
            assert ready['inputs']==len(source['files'])
            qualified=load(candidate/'ROOT_FINAL_INPUTS.json')
            assert qualified['published_commit']==commit and set(qualified['files'])==set(source['files'])
            for name,item in qualified['files'].items():
                assert source['files'][name]==item['sha256']and source['modes'][name]==item['mode'],name
            proofs += [proof(candidate/'ROOT_FINAL_INPUTS.json')]
            assert ready['fresh_root_executable_buildscript'] is True and ready['controls']==4
            assert ready['artifact_sha256']=={a['target']:a['sha256']for a in artifacts}and ready['native_uuid']==symbols['uuid']
            assert original['actual_exit']==0 and original['original_handle'] and original['terminal_tool_chunk']
            assert ready['original_terminal']==original
            for name in required:assert ready['checks'][name]['exit']==0,name
            for name,count in {'application_all':23,'application_core':23,'client_all':28,'client_core':28,'unit_application_all':39,'unit_application_core':39,'provider_all':42,'provider_core':27,'counter_service':2,'matched_tests':70}.items():
                assert ready['checks'][name]['actual_rust_passed_counts']==[count],name
            for label,count in [('tests',42),('pass',42),('fail',0)]:
                assert ready['checks']['provider_metadata_controls']['actual_node_'+label]==count
            proofs += [proof(candidate/name)for name in ['READINESS.json','ORIGINAL_TERMINAL.json']]
        for name in required:
            assert (logs/(name+'.exit')).read_text().strip() == '0', name
        if revision480:
            appended=load(candidate/'APPEND_ONLY_RUNTIME_QUALIFICATION.json')
            assert appended['exit']==appended['actual_terminal_exit']==0 and appended['source_exact'] is True and appended['source_commit']==commit
            for name,digest in appended['proof_sha256'].items():assert sha(candidate/name)==digest,name
            proofs += [proof(candidate/'APPEND_ONLY_RUNTIME_QUALIFICATION.json')]
            proofs += [proof(candidate/name)for name in appended['proof_sha256']]
        controls_container = load(candidate/'CONTROLS.json')
        assert controls_container['artifact_unchanged']
        if revision486:assert controls_container['exit']==0
        else:assert controls_container['end']
        controls = controls_container['cases']
        proofs += [proof(candidate/name) for name in ['STATUS','STAGE','CONTROLS.json']]
        proofs.append(proof(logs/'native-buildscript-invocation.txt'))
        proofs += [proof(p) for p in sorted(logs.glob('*.exit'))]
        tool_file = logs/'tool-sha256.txt'
    assert len(controls) == (2 if host=='linux' else 4)
    observed = set()
    for control in controls:
        summary = control['summary']
        if revision486 and host=='mac'and control['selection']=='default':
            assert control['process']['exit']==control['validator']['exit']==0
            assert not any(k.startswith('publication_')for k in summary)and not control['publication_readbacks']
            assert len(control['physical'])==3 and all(not row['witness_selector_present']for row in control['physical'])
            assert all(re.fullmatch('[0-9a-f]{64}',row['manifest_sha256'])for row in control['physical'])
            assert summary['ready_visit_items_requested']==summary['ready_visit_items_effective']=='64' and summary['groups']=='2'
            assert summary['applied']=='512'and summary['recovered_value']=='576'and summary['retry_verified']==summary['workers_joined']=='true'
            assert all(summary[key]=='0'for key in ['unknown','pending','window_refused','no_leader','admission_refused','not_proposed'])
            assert control['ledger_status_counts']=={'applied':512}
            protocol='tcp'if summary['protocol']=='TcpTls'else'quic';assert summary['protocol']in ['TcpTls','Quic']
            observed.add((protocol,'atomic'));check_readbacks(control['capacity_readbacks'],'atomic',publication=False)
            continue
        provider = summary['publication_effective']
        assert summary['protocol'] in ['TcpTls','Quic']
        protocol = 'tcp' if summary['protocol'] == 'TcpTls' else 'quic'
        observed.add((protocol,provider))
        assert summary['applied'] == '512' and summary['recovered_value'] == '576'
        assert summary['retry_verified'] == summary['workers_joined'] == 'true'
        assert all(summary[k] == '0' for k in ['unknown','pending','window_refused','no_leader','admission_refused','not_proposed'])
        if host == 'linux': lines = control['capacity_readbacks'] + control['publication_readbacks']
        else:
            assert control['process']['exit'] == control['validator']['exit'] == 0
            lines = (control['capacity_readbacks']+control['publication_readbacks']if revision486 else control['actual_capacity_readbacks'] + control['actual_publication_readbacks'])
        check_readbacks(lines,provider)
    assert observed == ({(p,'witness')for p in ['tcp','quic']}if host=='linux' else {(p,a)for p in ['tcp','quic']for a in ['atomic','witness']})
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
    assert config['source_commit']==required_commit()
    assert config['candidates']['baseline']['source']['commit']==BASELINE_COMMIT
    assert config['candidates']['candidate']['source']['commit']==config['source_commit']
    assert config['source_commits']=={arm:r['source']['commit']for arm,r in config['candidates'].items()}
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
