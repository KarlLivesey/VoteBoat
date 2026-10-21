# One bounded absent-follower diagnostic — INCONCLUSIVE

Parent released one diagnostic case only. Executed once, no retry:
`cargo +stable test --locked --offline --test startup backpressure::absent_follower_does_not_stop_the_leader_checkpointing -- --exact --nocapture`.
Result: 1 passed / 0 failed,55 filtered, rc0,290.07s libtest (297.99s guarded
Cargo command including build/guard). Unoptimized debug semantics retained.

Input is published base3aed5d671e735f7cd622b4ae4441a54ca5b3d5e7 PLUS the
uncommitted diagnostics-only patch captured BEFORE compilation in
diag-absent-once.patch, SHA256
7bfa83cb786169884304c8203fc3d06c265d4fe17f65fa57dc3b9c9819817c8d.
It is not a test of clean3aed or a new published qualification head.
Schema was written in branch docs/IMPLEMENTATION.md before fixture edits.
Changed files only that doc and tests/startup/backpressure.rs; no src/public
changes, clock/admission/yield changes, bound changes or acceptance changes.
All49,152 exact Applied writes,64 window,5000ms protocol/600s wall guards,
term/entry-budget/checkpoint/cap checks and cleanup remain unchanged.

| Phase/receipt | Applied writes | Protocol ms | Host elapsed ms | Last real Applied host ms |
| --- | ---: | ---: | ---: | ---: |
| Election predicate complete | 0 | 0 | 42 | None |
| Quarter1 | 12,288 | 1,604 | 51,361 | 51,361 |
| Quarter2 | 24,576 | 4,878 | 118,648 | 118,648 |
| Quarter3 | 36,864 | 8,074 | 195,902 | 195,902 |
| Quarter4 / enter drain | 49,152 | 11,213 | 289,994 | 289,994 |
| Drain and joins complete | 49,152 | 11,214 | 289,998 | 289,994 |

All sampled quarter/drain node states: node1 Leader and node2 Follower,term1,
no pending dependency, zero persistence/snapshot requests and connector
requests/dialing/anonymous/handshaking, no WAL high water/parked/reclaim failure
or pending checkpoint. Final leader last/commit/applied49153,base47211;
follower last49153,commit/applied49151,base41018. WAL bytes2266130/1926288
under usable66060288; no reclaim pending. Zero write refusals, max retained10284
<16384,5 checkpoint advances with bases[0,10281,19441,28658,38103,47211].
Final last real Applied progress age was3ms at joined observation. The absent
node3 remains identified by the existing replica-stall report; it did not
prevent completion of this run.

At election observation only, existing predicate had returned with leader
last/commit0 and pending_dependency=true, one persistence request for its noop;
follower last/commit0. This is an observation of unchanged setup, not evidence
that this caused the original hang; no setup repair was made.

Write completion:1,561,059 polls,1,448,921 native-gated polls. Joined:
1,561,063 polls,1,448,922 gated. Counts observe the original short-circuit
native-pending boolean; they do not duplicate request ownership or ascribe
historical gating to usage sampled later at a receipt. No per-poll logs.

Classification: INCONCLUSIVE for original3aed full-default failure. This run
shows finite write progress and clean completion in its own isolated context;
it does not reveal original600s failure phase, applied count, last-progress age
or usage. Its four-core isolated context differs from the original startup
binary's normal concurrent tests on13–15. Context-dependent slowdown and a
stuck predicate remain unclassified hypotheses, not host-noise attribution.
No smallest cause-specific repair/control is justified by the present evidence.
If further diagnostics are authorized, the narrow next observation would be
this instrumented existing startup binary under original13–15/normal libtest
concurrency, unchanged bounds, capturing the original failure context. That
has NOT been run. No clock/yield/serialization/timeout change is proposed as
a proven repair, and no acceptance gate is cleared.

Resource audit: one Cargo/test command,39 sampled task entries, zero violations;
controller/children/observed threads actual nice19,cores12–15,max4 Cargo jobs.
Pre-launch guardPASS and post-run guardPASS: free2852GiB,main64/150GiB,
own/shared2/120GiB,live2/5,Linux242/300 and1/10GiB,Mac10/40GiB and2/3dirs.
Own target2,003,836,928 bytes. No owned build/test controller or executable
remains, including deleted-cwd scan. See diag-absent-once-audit.json.

No source edit after the run. Diagnostic patch remains uncommitted for parent
direction; no commit/push/review request/strict repetition/fullmatrix/main edit.
Published source and PR remain3aed; original complete FAILED2667/1/1 and older
failed/control records are preserved. Further edits and runs HELD.
