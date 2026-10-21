# PR22 final source reconciliation: rejected/closed as parked

Parent final decision: reject/close; GitHub closure 2026-10-21T06:28:21Z, unmerged, parent comment https://github.com/KarlLivesey/VoteBoat/pull/22#issuecomment-6309613305. Original remote head463c50d4ec06b5e87d435c41427ccb63c60a9dfd remains unchanged. This agent did not push or edit/close/reopen the PR, and has no main-writer grant.

## Reconstructable uncommitted source

HEAD stayed original463c50d4; pending MERGE_HEAD was main20a8d08fb85f43c5496aefaa295753324f9b3b37. **No unpushed merge or source commit was created.** Candidate index tree f7cd571692a82c80bbd8b3aa12c6329c043ca634 was archived before cleanup. Full binary/full-index patch candidate-from-main20a8d08f.patch applies to exact main20a8d08f; it includes all ten changed paths, including BOTH originally untracked tests/transfer_service/root.rs and root_tests.rs. Private-index reconstruction yielded the identical tree; no source checkout/copy/build/test was used for that archive verification. Changed exact blobs, file modes, SHA256/Git identities and a tar of all ten files are retained. Original staged/unstaged patches and pre-archive status are also available in the raw report directory. Parent archives only; never apply to main as part of this disposition.

**Uncommitted-input limitation:** tests ran the working-tree merge, not a committed/pushed candidate. Input manifest covers2605 build/test/script/hook paths and was captured after the default pass; after all-feature failure all those bytes still matched. Documentation is outside that manifest. No exact published-head qualification, fresh source reviews, full-gate or performance qualification is claimed.

## Preserved source scope

Original benchmark diagnostic mechanism unchanged (no exact revision instrument). Comments document poll-level durable rises, merged completion undercount, truncation/rewrite ambiguity and restricted single-group/one-message-per-frame follower proxy. Python adds actual fake-child runs with/without optional lp/cop/ef fields plus skip-any-missing-per-node/counter coverage; raw absent fields remain NA and unavailable per-write medians are omitted. Existing meaningful cluster assertions are retained with leader commit_only_persists>=1 added (both TCP/QUIC passed it). storage.csv appended counter position unchanged. Historical Linux under1% stop rule and accurate TCP15k ratios/scope retained; N1c brief is historical input to now CLOSED parkedPR23, not shipped or merged. Current/next2 wind-down schema plan preceded edits.

Production src/Cargo/hooks/build/clippy exactly qualifiedbd714771 (also unchanged from main20a8d08f). No PR14/ST2, PR15/DR1, PR19/N1 or PR23/N1c production enters this source. The explicitly authorized exception is exactly five TEST blobs from reviewed66fcf3e000441c7fae0bb1fed231bdc28e146c9f: tests/support/ports.rs, tests/fixture_isolation.rs, tests/transfer_service.rs, tests/transfer_service/root.rs, tests/transfer_service/root_tests.rs. These prevent the known transfer port-overflow/base-root fixture defect from obstructing a possible final gate; no fixture redesign, benchmark socket/clock edits or BrokenPipe/WAL-lock work occurred. Dedicated fixture controls were NOT launched because the focused stop intervened.

## Actual focused results and stop

- Python cluster_test.py:15 passed,0 failed,rc0 (new optional-counter tests included).
- Default native_benchmark:56 passed,0 failed,rc0. All existing command/cluster tests, new leader assertion and existing storage CSV consumer passed.
- All-features native_benchmark:**63 passed,1 failed,rc101**. Failure shared::drain::quic_mixed_voter_and_learner_assignments_resume_together, examples/benchmark/shared_drain.rs:304:17, Unknown(LeadershipChanged). Both TCP/QUIC three-process counter tests and Python self-test passed within this failed run.
- Cause remains unproven. Historical similar text is not a diagnosis or proof of host noise/counter defect. No counter production-defect inference. No assertions were weakened; no retry, isolation, diagnosis or repair was run.
- No further fixture tests, seven strict checks, hook, build, full-suite, performance test, fresh reviews or new slice was launched after the failure. Parent requires no unqualified source publication.

## Resource limitations

Corrected test runs explicitly reset actual priority19 after stock guard and in child setup, verify child priority/affinity at launch, sample all controller descendants/threads, cores0-3/max4 Cargo and test jobs, sequential configurations, incremental0, own checkout target only. Debug info disabled for disk size; debug assertions/test semantics remain enabled. Corrected Python/default/all cases total88 resource samples/2345 task records, zero sampled violations, peak own target466111109 bytes (<10GiB). Stock guards passed before/after completed focused cases.

Initial Rust launch aborted immediately BEFORE compilation/tests: resource monitor found controller/child at actual-4 despite initial absolute setter, outer rc1. Raw empty log and violation JSON retained (that JSON predates later rc-recording fix and omits inner rc). No passing check arose from this attempt. Early inspection commands initially inherited T3 defaults; three T3 CUA helpers also had inherited-4/16 and unpinned affinity until positively identified and corrected. Later all helper threads were constrained19/0-3. No whole-task/all-attempt resource-compliance claim. Own helper identity/cleanup evidence is retained; no foreign process/target/worktree or /tmp/voteboat-test-ports touched.

Raw reports: main target/claude/research-r4/pr22-final-main. Compact evidence and full reconstruction patch are for the parent's docs-only durable archive. Cleanup/final stock guard are recorded separately; this report does not clear any merge gate.
