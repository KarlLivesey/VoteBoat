# Profile B slice314 owned results

Owned source frozen: tests/logical_voter_payload.rs and its seven child modules.
Hashes: PAYLOAD_MODEL_SOURCE_SHA256.txt. No production/shared documentation/Cargo
changes were made. Parent owns integrated Linux/macOS four-profile qualification.

## Selected caller graph

Final all-feature target: five tests passed, zero failed. Raw log:
payload-all-final.log. BFS exhausts168133 unique states,874442 accepted edges,
4035192 attempted edges, deepest visited depth195, remaining frontier0. Constants
are depth384/states500000/accepted edges5000000; every cap flag is false.
Reachable terminal counts:4860 total,3240 after selected power/process cut,2430
with an anchored own child checkpoint,480 combining cut, own recovered image,
old child checkpoint and nonempty committed suffix. These counts overlap.

Fixed workload:18 immutable original commands plus two typed early submissions;
term1/configuration1, parent positions1/2 with exact X/Y bytes, representative
epochs1/2, one suffix replacement generation. The caller schedules all three prior
physical deliveries before its next original Admit or Submit. Inside a round,
ordinary child majority commit/apply/output/checkpoint/cut can precede third sync.
Model.step and its two-witness consensus guards have no added all-three predicate.
Accumulated lag across multiple original admissions is excluded; native315/316
still owe that history. Three logical parent cells exist, but cells1/2 use an
explicit matching X/Y written/synchronized witness prelude; only cell0's detailed
payload, nested snapshots, pins, suffix, observation and recovery interleave.
Graph child-checkpoint paths select physical1; controls exercise all three stores.

Ordinary child consensus is a qualified conditional abstraction, not an RPC proof.
CommitChild mutates no physical persisted commit marker. A selected physical0 cut
leaves surviving child runtime authority; all-child simultaneous power loss is
excluded. Fresh child quorum discovery precedes own-store recovery. The decoder
uses only that selected store's synchronized child checkpoint/suffix, never an
oracle/newest canonical image. The independent oracle has its own byte/commit/
snapshot decoder; it compares parent commit and snapshot boundaries separately
from ChildIndex, as well as bytes, promise/vote, configuration, epoch, suffix
generation and dedup state.

## Actual controls

Missing-payload weakening retains the original X identity but omits its bytes:
43 accepted typed actions reach acknowledgement/cut/fresh child quorum, own
recovery refuses, and independent oracle finds zero exact-byte witnesses. Safe
counterpart refuses Commit1/ack early, then accepts an actual FillX child command,
durable write/sync/commit/apply, and acknowledgement:51 accepted actions.

EarlyAnchor submits Anchor1 before Bytes1/Publish1 through the same actual typed
transition path, then Reclaim0 and stage/sync/anchor on all three child stores.
153 accepted actions reach cut/quorum; own recovery refuses and oracle finds zero
recoverable acknowledged X witnesses. Safe counterpart refuses both early steps,
keeps old pin, checkpoints/restarts from actual old image, then completes bytes,
publish, anchor, pin replacement, original epoch2 claim and service:198 actions.

Additional control rejects prefix holes, stale original completion without losing
new output, original old suffix binding against replacement, and recovery without
fresh quorum. Actual typed link loss/refusal then repair succeeds without deleting
durable bytes. A34-action own-prefix control leaves physical1 genuinely empty,
refuses missing quorum and missing own prefix separately, repairs links and
catches up physical1 through three actual ordered Write/Sync pairs before recovery.

## Preserved unqualified graph

Both retained negative action vectors replay from Model::initial through actual
Model.step with their original weakening. Replayed models match the original
full state; Recover(1) again refuses without mutation and the independent oracle
returns the same failure. The replay-only final control run passes4/0 in
payload-replay-final.log; no main graph source was changed by this final addition.

payload-unqualified/search.rs is the original capped search source;
payload-unqualified/schema.md and graph.log preserve its declared scope/result.
That accumulated-lag graph reached500000 states,2773215 accepted edges,
11371744 attempts,depth89,remaining frontier26177; state_cap=true, other capsfalse,
zero terminals. It failed its cap assertion and remains unqualified. The later
independent parent-boundary/type refinements are in the frozen final modules; this
archive does not claim a full earlier-module source snapshot. No cap increase or
hidden frontier pruning was used to qualify the revised selected caller graph.

## Commands

All use the isolated target/divide-shutdown/build cache except formatting.

- cargo +stable test --locked --offline --test logical_voter_payload --all-features -- --nocapture:5/0; payload-all-final.log
- cargo +stable test --locked --offline --test logical_voter_payload --all-features control:: -- --nocapture:4/0 after actual replay addition; payload-replay-final.log
- cargo +stable fmt --all -- --check:exit0; payload-fmt-final.log
- cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings:exit0, zero diagnostics; payload-strict-all-final.log

Default/core/native repeats are deferred to the parent's integrated frozen matrix.
Earlier focused controls and target strict results are retained in payload-* logs;
The main graph all-feature5/0 predates only the replay addition; final owned
source has the controls4/0 rerun and subsequent repository fmt/full strict0.
