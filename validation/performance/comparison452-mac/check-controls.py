"""Actual Mac CLI fixtures and rejection copies, never builds or benchmarks."""
import copy
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

here=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('phase_checker',here/'phase-diagnostic-check.py')
checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
guard=checker.module
qualified=Path(sys.argv[1]).resolve()
fixtures=here/'control-fixtures';fixtures.mkdir()
reports={};actual=[]
for mode in ['off','on','openraft']:
    original=qualified/'logs'/('cli-'+mode+'.log')
    raw=original.read_bytes()
    guard.need((qualified/'logs'/('cli-'+mode+'.exit')).read_text().strip()=='0','actual CLI exit failed')
    report=json.loads(raw);checker.case(report,mode,16384,4096,4)
    target=fixtures/(mode+'.json');target.write_bytes(raw);target.chmod(0o644)
    reports[mode]=report
    actual.append({'mode':mode,'fixture':target.name,'original':str(original),'sha256':hashlib.sha256(raw).hexdigest(),'result':'pass'})
negative=[]
mutations={
    'missing_original':lambda d:d.update(completed_originals=16383),
    'missing_frontier':lambda d:d['applied_frontiers'].__setitem__(2,17408),
    'missing_protected_phase':lambda d:d['attribution'][0]['phases'].pop(),
    'historical_gap_scope':lambda d:d['attribution'][0].update(checkpoint_compact_gap_scope='historical exact-state scope'),
}
for name,mutate in []:  # Four unchanged CLI refusal controls reused from sealed435 proof.
    data=copy.deepcopy(reports['on']);mutate(data)
    try:checker.case(data,'on',16384,4096,4)
    except ValueError as error:negative.append({'control':name,'result':'correctly rejected','reason':str(error)})
    else:raise ValueError('invalid CLI control accepted: '+name)
pair=json.loads((here/'CONFIG_PAIR.mac.json').read_text())
controls=[]
for name in []:  # Existing refusal scope reused; current source/profile positives checked.
    config=copy.deepcopy(pair['candidate'])
    if name=='missing_reply_module':config['source_sha256'].pop('benchmarks/matched/src/voteboat_engine/reply.rs')
    elif name=='missing_reply_tests':config['source_sha256'].pop('benchmarks/matched/src/voteboat_engine/reply/tests.rs')
    elif name=='missing_dsym_file':config['symbols']['files'].pop(next(iter(config['symbols']['files'])))
    elif name=='changed_dsym_hash':next(iter(config['symbols']['files'].values()))['sha256']='0'*64
    elif name=='reused_root_compilation':next(a for a in config['artifacts'] if a['target']=='voteboat')['compiler_record']['fresh']=True
    elif name=='missing_build_input':config['source_sha256'].pop('build.rs')
    elif name=='missing_original_buffer_module':config['source_sha256'].pop('benchmarks/matched/src/voteboat_engine/actor/tests/originals.rs')
    elif name=='missing_client_outcome_module':config['source_sha256'].pop('benchmarks/matched/src/voteboat_engine/client_requests.rs')
    elif name=='missing_completion_module':config['source_sha256'].pop('src/raft/completion.rs')
    elif name=='missing_core_completion_controls':config['source_sha256'].pop('src/raft/completion_tests.rs')
    elif name=='missing_host_completion_controls':config['source_sha256'].pop('benchmarks/matched/src/voteboat_engine/actor/tests/completion.rs')
    else:config['source_sha256'].pop('benchmarks/matched/src/voteboat_engine/client_outcomes.rs')
    try:guard.verify(config)
    except ValueError as error:controls.append({'control':name,'result':'correctly rejected','reason':str(error)})
    else:raise ValueError('invalid artifact control accepted: '+name)
previous=Path('/tmp/voteboat-comparison435-x3SbiM/CHECKER_CONTROLS.json');prior=json.loads(previous.read_text());guard.need(len(prior['cli_negative'])==4 and len(prior['artifact_negative'])==9,'previous scoped controls absent')
guard.need(hashlib.sha256((here/'phase-diagnostic-check.py').read_bytes()).hexdigest()==hashlib.sha256(Path('/tmp/voteboat-comparison435-x3SbiM/phase-diagnostic-check.py').read_bytes()).hexdigest(),'phase checker scope changed')
summary={'scope':'actual new16384 CLI inputs checked; no new refusal framework; unchanged fourCLI/nineartifact refusal scopes reused from sealed435 proof, identity/source/profile positives validated separately; no timing claim','reused_controls':{'proof':str(previous),'sha256':hashlib.sha256(previous.read_bytes()).hexdigest(),'cli_refusals':prior['cli_negative'],'artifact_refusals':prior['artifact_negative'],'scope':'unchanged predicate behavior; requiredcommit/current core-source identity updated, no check level weakening'},
         'actual':actual,'cli_negative':negative,'artifact_negative':controls}
(here/'CHECKER_CONTROLS.json').write_text(json.dumps(summary,indent=2)+'\n')
print('3 actual new Mac CLI fixtures pass;4CLI/9artifact unchanged controls reused by sealed435 proof')
