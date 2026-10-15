import hashlib,json,re,shutil,subprocess,stat
from pathlib import Path
root=Path(__file__).resolve().parent
required_commit=(root/'REQUIRED_COMMIT').read_text().strip()
assert len(required_commit)==40 and all(c in '0123456789abcdef' for c in required_commit),'unpublished candidate refused';h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
actual={p.relative_to(root/'source').as_posix() for p in (root/'source').rglob('*') if p.is_file()}
assert actual==set(source['sha256']),(actual-set(source['sha256']),set(source['sha256'])-actual)
assert source['commit']==required_commit
for rel,digest in source['sha256'].items():
 assert h(root/'source'/rel)==digest,rel
 assert oct(stat.S_IMODE((root/'source'/rel).stat().st_mode))==source['modes'][rel],rel
assert all(rel in source['sha256'] for rel in ['src/log/entry_batch.rs','src/log/entry_batch/tests.rs','src/log/entry_history.rs','src/log/entry_history/tests.rs','tests/provider_conformance/log_store/entry_batches.rs','docs/ENTRY_BATCHES.md','src/format_crc.rs', 'src/format_crc/tests.rs', 'src/log/command_bytes.rs', 'src/log/command_bytes/tests.rs', 'tests/provider_conformance/log_store/command_bytes.rs', 'docs/COMMAND_BYTES.md', 'docs/PERFORMANCE.md', 'validation/check-native-benchmark.mjs', 'validation/check-offered.mjs', 'benchmarks/matched/src/voteboat_engine/client_outcomes.rs','benchmarks/matched/src/voteboat_engine/actor/pending_replies.rs','benchmarks/matched/src/voteboat_engine/actor/pending_replies/tests.rs','src/raft/append_prefix_tests.rs', 'src/raft/completion.rs','src/raft/completion_tests.rs','benchmarks/matched/src/voteboat_engine/actor/tests/completion.rs','benchmarks/matched/src/voteboat_engine/client_requests.rs','benchmarks/matched/src/voteboat_engine/actor/tests/originals.rs','tests/provider_conformance/log_store/entry_clone.rs','clippy.toml','rust-toolchain.toml','build.rs','src/log/comparison.rs','src/log/comparison/tests.rs','src/log/owned/tests/checkpoint.rs','src/native/log_store/accepted/tests/checkpoint.rs','src/raft/checkpoint_tests.rs','tests/log_store/matches.rs','tests/provider_conformance/log_store/default_errors.rs','tests/provider_conformance/log_store/matches.rs','tests/snapshot/log_match.rs','benchmarks/matched/src/store/tests.rs','docs/provider-conformance.json','tests/provider_conformance/log_store/entry_equality.rs','src/raft/owned_append_tests.rs','benchmarks/matched/src/voteboat_engine/actor/tests/replies.rs'])
assert not {'benchmarks/matched/src/voteboat_engine/reply.rs','benchmarks/matched/src/voteboat_engine/reply/tests.rs'} & set(source['sha256'])
assert not {'benchmarks/matched/src/voteboat_engine/waits.rs','benchmarks/matched/src/voteboat_engine/waits/tests.rs'} & set(source['sha256'])
assert not {'benchmarks/matched/src/voteboat_engine/actor/pending_key.rs','benchmarks/matched/src/voteboat_engine/actor/pending_key/tests.rs'} & set(source['sha256'])
expected=(root/'EXPECTED_MATCHED_TESTS').read_text().strip()
counts={}
for name in ['root_lib_all','root_protocol_all','root_lib_core','root_protocol_core','root_three_services','root_member_services','matched_tests']:
 values=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', (root/'logs'/f'{name}.log').read_text())
 assert values and all(failed=='0' for passed,failed in values),(name,values)
 counts[name]=[int(passed) for passed,failed in values]
assert counts['matched_tests']==[int(expected)],counts['matched_tests']
assert counts['root_three_services']==[2] and counts['root_member_services']==[2],counts
steps=list(counts)+['root_fmt','root_default_strict','root_all_strict','root_core_strict','root_native_strict','matched_fmt','matched_strict','provider_metadata','source_preflight']
for name in steps: assert (root/'logs'/f'{name}.exit').read_text().strip()=='0',name
baseline=Path('/tmp/voteboat-mac454-8da7192-jrwVXF')
assert (root/'logs/rustc.txt').read_bytes()==(baseline/'logs/rustc.txt').read_bytes(),'within-host compiler changed'
assert 'libprotoc 3.21.12' in (root/'logs/protoc.txt').read_text()
proof={'commit':required_commit,'source_inputs':len(actual),'source_sha256_and_physical_modes':True,'counts':counts,'commands':steps,'all_command_exits':0,'compiler':(root/'logs/rustc.txt').read_text(),'qualification_only':True,'fresh_performance_build':False,'performance_claim':False,'original455_job_preserved':'/tmp/voteboat-mac455-7a99ae5-fENMzQ; original handle84206'}
(root/'QUALIFICATION.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof,sort_keys=True))
