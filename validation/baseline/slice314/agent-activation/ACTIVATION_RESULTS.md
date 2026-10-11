# Slice314 C/D/E activation models — frozen source

Owned compilation inputs: `tests/logical_voter_activation.rs` and its eight
children `actions`, `controls`, `journal`, `oracle`, `protocol`, `scenarios`,
`search`, `types`. No production/shared docs/git changes. The frozen input hashes
are `activation-frozen-source.sha256`; `activation-source-guard.log` confirms all
nine matched after validation.

All/default/core/native profiles ran sequentially in
`target/divide-provider/build`, each with six passing tests. Exact complete logs:
`activation-{all,default,core,native}-frozen.log`. The matching
`activation-{all,default,core,native}-clippy-frozen.log` each reports zero
diagnostics for the owned target. Whole repository formatting and requested
strict all-target/all-feature Clippy are zero in `activation-fmt-frozen.log` and
`activation-all-targets-clippy.log`. Root owns final integrated Linux/macOS
qualification; these executions are Linux only.

| Profile | States | Generated edges | Maximum BFS depth | Terminal states |
|---|---:|---:|---:|---:|
| C, backing-child activation | 19,357 | 61,185 | 259 | 1 |
| D, distinct parent-cell activation | 8,463 | 19,833 | 372 | 1 |
| E, authoritative parent intent | 55,366 | 150,209 | 682 | 2, both orders |

Every frontier is zero; every state/edge/depth cap flag is false; all three
selected graphs exhaust. Limits: depth1,024, states500,000, edges5,000,000,
64 records per backing child,32 parent control records, two queued original
intent requests, one observed mutation per serial admission/catch-up cut, one
observation loss and physical restart per cut. Witness sets and accepted versus
committed policies are independently enumerated/replayed from source records.
There are no imported production quorum/log-transition helpers.

C adds a fourth physical learner, received Joint/Final policy activation,
minority-synchronized uncommitted suffix replacement, removed-representative
claim eligibility/refusal, rollback re-eligibility and actual committed eligible
successor claim preserving the original parent promise/payload. D imports actual
source application data into a distinct fourth child with three physical stores;
unready/new-only evidence refuses, an uncommitted parent configuration suffix
is durably replaced, and own physical checkpoints plus later suffix reconstruct
the imported image before Joint/Final progress. E's two requests are explicitly
queued before their nondeterministically ordered parent-log admissions. Applied
Begin records retain the losing original refusal, including retries; immutable
Outcome/Release records precede admission of a fresh second operation. Ordinary
child Noop writes acquire no new ancestor intent. Parent observation loss and
restart resolve original retained outcomes through matching fresh quorum data.

Actual negative traces are generated and replayed from initial physical storage:
child NewOnly112 actions (Joint committed with only physical1/3), parent
NewOnly125 (Joint read/election evidence only logical1/3), omitted intent170
(the other layer really admits a transition without its active replicated
intent). The first two violate the actual joint witness predicate; no conflicting
parent commit is claimed. Running the same original schedules with correct
guards refuses the violation and reaches terminals with148/247/512 further
actions respectively. Full action traces are in each final test log.

Failed preflights retained: `activation-first.log` caught an oracle mistakenly
requiring minority uncommitted tails already match a replacement committed by
another quorum, and E depth384 truncation (31,580 states,87,003 edges,no terminal).
`activation-second.log` exhausts the positives after the justified depth increase
but catches an expected-error string mismatch: the independent durable-prefix
oracle reports the child defect before the explicit activation-event oracle.
`activation-clippy-first.log` catches a derivable Default implementation, fixed
by deriving. `activation-all-final.log` / `activation-safe-progress-preflight.log`
retain the deterministic safe-schedule error that continued replication past
the selected minority-uncommitted cut; the scheduler now takes that offered cut.
`activation-fmt-final.log` records a whole-repo formatting attempt while another
owned model module had not yet been published; final formatting is zero.

Scope: these are finite local activation models assuming qualified ordinary
child/parent Raft committed-prefix preservation, not a new proof of every RPC.
Detailed I/O, commit/apply, observation/loss and physical power-cut restoration
remain explicit inside selected serial cuts. Whole representative recovery
discards/rebuilds its cache from selected physical synchronized checkpoint/suffix
and fresh matching quorum at a surviving qualified child commit boundary;
parent control is rebuilt from its own recovered entries at surviving qualified
parent commit authority. There is no oracle restoration. Simultaneous all-child
crash, arbitrary concurrent layered activation, network RPC interleavings,
recursive native construction, payload/capsule profiles A/B and native315 are
outside these graphs. Terminal reachability does not claim every deliberately
restricted intermediate schedule is live. Source is frozen for root integration.
