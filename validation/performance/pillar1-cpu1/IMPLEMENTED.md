# CPU1 implementation receipts

Status: bounded candidate implemented and checked for parent review.
Performance, independent source review and full qualification remain PENDING.
Branch: cpu/p1-commit-notices. Base: 0a752089; production baseline:
0bf9271ecb5a34c4089c6bb10381303232624170. Performance gate PENDING.

Resource limitation: the first two read-only shell launches did not use the full
stock guard/envelope; no builds/tests ran then. Subsequent controllers explicitly
set absolute nice19/affinity0–3. Own automatic tool hosts were identified by exact
cwd/exe/start ticks (2165160:4667880, 2165194:4667881, 2165344:4667884)
and every existing thread constrained. No retrospective all-task claim.
Stock guard passed at initial baseline: root1GiB, worktree0GiB, live2 including
one foreign worktree; Mac10GiB. Original design pack read from main's untracked
VoteBoat-Design-Pack; it is absent from this worktree and was not copied/edited.

## Preserved first expanded-run failure

Default focused raft run:193 passed /1 failed, rc101. New fallback fixture failed
at membership_tests.rs166 before RPC assertions: invalid configuration journal.
Source tree800df77284423415d55c500dac1bc9b5f8288851 and full binary patch,
manifest, original log/resource receipts retained in raw report directory.
No runtime state dump was emitted; exact fixture source and error are preserved.
Dependent launches stopped. Parent explicitly released bounded new-fixture repair
before any sweep. Cause: Final incorrectly used prior Learners operation100;
the validator requires the active Joint operation101. Existing valid Final
builders use101. Parent briefly suggested fresh102, then corrected it before
any edit adopted that suggestion. Production validation is unchanged.
Both accepted Final and completed retirement coverage are retained.

The first repair launch used an unqualified --exact filter and ran0 cases;
not counted as a pass. Correct exact filter then ran1 and exposed another
new-fixture misuse: retired follower passed to leader-only broadcast helper
(debug role assertion). Corrected retirement to public Heartbeat (empty output)
and kept leader helper assertions for learner/joint/accepted Final states.
No production behavior or validator was changed for either fixture correction.

Expanded new consumer control initially14/1 rc101: native owning Shard refused
a volatile fixture (WrongOwner at native.rs24). This is expected assembly
validation; corrected new native fixture to the durable core seam supported by
Shard. Production owner validation unchanged. Earlier14 successful cases retained.


## Implemented result and scope

Explicit Rpc::CommitNotice and public startup enable_commit_notices select an
equal-guarantee physical composition. Only installed current-term tip commitment
and exact durable data ACK progress permit commit-purpose emission. One exact
scalar stamp per peer records real output; request counters cannot authorize it.
Stamps never become requests/aliases, quorum/progress/readiness/read evidence or
notice ACK drain obligations. Actual retained Vec capacity is checked in
reservation; 1000 steady cycles retain exactly two peer keys without growth.
Role/term/configuration/fence/compaction/snapshot boundaries invalidate stamps.

Strict receive identity/current configuration/origin/term validation precedes
state changes. A matching local boundary alone can advance commit; existing
completion gates still precede application. Valid nonmatching contact resets
elections, invalid/old traffic does not. A higher-term follower sends no notice
reply; the next acknowledged heartbeat discovers its term. Ordinary Append,
heartbeat, probe, catch-up, retry, transfer, learner/joint/Final/retirement and
authorization-bridge paths retain acknowledgements.

Static summary refinement is confirmed: ordinary checkpoints keep membership
None and summary plain. A 5000-original checkpoint/compaction control resumes
the six-message cycle; a positive compacted boundary is contact without commit.
Matched default-retention maintenance completes12000 originals with all three
applied frontiers12005 and at least two checkpoints per traced replica, retaining
fewer than5000 entries. Retain-all was not substituted for this primary control.

Wire9/tag19 is explicit codec/HELLO consumer integration only; older codecs
refuse the RPC. Native defaults, ALPN, network policies and storage formats
are unchanged. Logical authorization explicitly refuses even a valid wire9
notice against a committed parent image; logical assemblies stay capability-off.
Matched's only source change is public capability enabling during construction.

## Actual checks (22–23 October; sequential configurations)

| Check | Actual result |
| --- | --- |
| Final core default / all-features |202 /202 passed, rc0 each |
| Public Raft + volatile + wire, default / all-features |83+11+21 passed per configuration, rc0 |
| Logical application, default / all-features |66 passed, one existing ignored timing report each, rc0 |
| Native relay notice controls | genuine one-way frame, Sent/Failed/Cancelled ownership/drain, bounded control reserve/refusal, label/family refusal passed in default/all-features |
| Matched locked offline workspace, default / workspace all-features |75 /75 passed, rc0; no optional matched features, contracts-only VoteBoat dependency |
| Pinned off/on cycle, volatile and durable |messages8→6, events9→7, effects14→12, persist6→6; identical logs/commits/applied originals |
| Causal mutations |4/4 killed by executed assertions (each rc101/one failed case): omit boundary term, omit contact reset, infer stamp from counter, route heartbeat through notices; source restored and controls green |
| Strict root formatting + four root Clippy configs + matched formatting/Clippy |seven rc0, zero diagnostics; exact commands in raw logs |

Counts overlap; they are not a whole-repository suite total. The final core
202-case runs include18 CPU1 core/fallback cases; companion relay-kind and
logical-authorization cases passed separately (20 CPU1 cases across those
namespaces). Other consumer cases are in their stated harnesses.
The final extra core case explicitly checks delayed higher-term discovery.
The enabled actual pre-push hook repeats all seven strict checks at publication;
its command/exit/resource receipt and remote head are kept with raw reports.

Other normal development failures remain in raw logs: initial new-test module/
Counter API compile errors; a new bridge fixture borrow error; first Clippy
lookup/borrowed-slice diagnostics; and a test-only vec! replacement parse error
caught by formatting before dependent launches. Each had a focused correction.
No original failure was hidden or cleared by an unchanged retry. No lint level,
threshold, production journal validation or pre-push check was weakened.

## Consumer audit

| Consumers | Result |
| --- | --- |
| core receive/source, progress broadcast, admitted ranges, requests, send windows | typed purpose and exact stamps; ordinary requests/replies unchanged; new input is sender-originated |
| leadership, readiness, configuration, authority, joint/batched/snapshot repair and connections | existing proof gates/rosters retained; new enabled-path fallback controls plus existing core suite |
| outbound and runtime effects/ingress/connections/ready batch | scalar Control cost/reserve; normal leased effects and serialized ingress; no new batching, membership import or queue owner |
| wire encoder/counting/decoder/capacity and TLS/startup selection | explicit9/tag19; shared checked budgeting and exact-version refusal; default selections unchanged |
| native relay frame/forwarder ledger/provider ingress | exact OneWay output; Request/Reply relabel refused; no ReplyScope/RequestRecord or return collector for notice |
| logical authorization/release/bridge/node | explicit refusal; assemblies capability-off; generic existing capture, child durability and reply windows unchanged |
| matched construction/actor/client/maintenance | construction-only enable; generic send/persist/apply and all original accounting/shutdown controls retained |

No CPU gain, allocation reduction, power-loss proof, full source approval or
complete release is claimed. No long matrix, full suite, exhaustive relay,
native performance, Mac work, unrelated reliability repair or other agent ran.
Known historical native relay BrokenPipe, benchmark filesystem lock and QUIC
drain failures remain unproven. Parent owns source reviews, equal-core primary
default-retention CPU measurements, keep/reject and integration. Macro P1 and
the next CPU2/CPU3 dependency plan remain open.

## Resource/evidence boundary

All recorded Cargo controllers used direct Python setpriority(...,19) and
sched_setaffinity(0,{0,1,2,3}) immediately before exec, max4 Cargo jobs,
incremental0, debug symbols0 and unoptimized normal-debug assertions. No extra
CARGO_HOME, source copies, per-revision targets, detached job or browser ran.
Own target stayed below15GiB; final pre-publication measured size is in raw
resource summary. Stock guards used unchanged global caps before configurations
and after work. Sampled descendant task records report zero observed violations;
this does not erase the early read/tool-host limitation above or establish
unsampled all-task compliance.

Raw safe logs, command environments, every recorded rc, mutation patches,
resource samples, preserved first-failure full source patch/manifest/tree,
artifact hashes and publication/cleanup receipts live under main's
target/claude/pillar1-20261022/cpu1-builder (reports only). Own target, checkout,
registration, seed branch and exact identified automatic hosts are swept after
checked publication; final stock guard/zero-owned-process receipts are recorded
there by cleanup, not inferred from test success.
