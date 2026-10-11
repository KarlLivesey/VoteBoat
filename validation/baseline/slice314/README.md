# Logical-voter protocol gate314

Linux and macOS qualification are terminal against782 frozen compilation/test
inputs. Integrated results are distinct from the agents' focused Linux checks.
The refinement is described in [LOGICAL_VOTERS.md](../../../docs/LOGICAL_VOTERS.md).
No native logical-voter implementation or P8 completion is claimed.

## Actual integrated checks

| Check | Linux | macOS |
| --- | --- | --- |
| Promise/capsule target |6/0 each profile|6/0 each profile|
| Payload/checkpoint target |5/0 each profile|5/0 each profile|
| Activation/intent target |6/0 each profile|6/0 each profile|
| Formatting and four strict all-target Clippy profiles |Zero diagnostics|Zero diagnostics|
| Before/terminal source guards |782 match|782 match|

All/default/core/native run sequentially using the retained `checks.sh`. Each
host passes68 test executions, repeating17 distinct tests. Nine positive graph
reports per profile have zero frontier and no cap reached. Root source guards
also match. The110-contract inventory checker passes metadata/path validation;
this slice adds no runtime seam. Raw integrated outputs retain the commands,
results and complete guard-control traces. This affected model matrix does not
replace the original302 platform sweep, which remains separately tracked.

## Selected protocol and scope

The three independent test targets cover five protocol profiles and nine positive
finite graphs. Their oracles do not import production transition helpers or supply
recovery images. Physical written/synchronized bytes, ordinary child commitment,
application and original caller observations are separate transitions.

| Positive graph | States | Edges | Maximum depth | Terminals |
| --- | ---: | ---: | ---: | ---: |
| A promises |146148|397527|120|76|
| A repeated logical identity |70110|193434|108|38|
| A read authority |316692|864081|293|76|
| A written dependency |56050|160612|95|38|
| A stale completion |86450|244095|119|152|
| B payload/checkpoint recovery |168133|874442|195|4860|
| C backing-child activation |19357|61185|259|1|
| D parent-cell activation |8463|19833|372|1|
| E competing replicated intents |55366|150209|682|2|

These graph counts reproduce in all four profiles on both platforms.
All selected graphs report frontier0, no cap reached and positive terminals.
The limits are500,000 states/5,000,000 edges per graph; depth384 for A/B and1024
for C/D/E. Negative searches stop when a concrete counterexample is found and
do not claim exhaustion. Detailed workload, storage and recovery bounds are in
the retained [A](agent-promises/LOGICAL_VOTER_PROMISES_EVIDENCE.md),
[B](agent-payload/PAYLOAD_MODEL_RESULTS.md) and
[C/D/E](agent-activation/ACTIVATION_RESULTS.md) evidence.

The models assume qualified ordinary consensus and retain surviving child runtime
authority across a selected physical cut. Fresh quorum discovery establishes the
commit boundary; recovery decodes the selected store's own synchronized bytes.
This is not simultaneous all-child crash recovery or a complete Raft RPC proof.
A/B wait for physical catch-up before the next semantic request; C/D/E explore
serial admission/catch-up cuts. Native315/316 still owe accumulated lag, real
whole-child restart, authenticated transport and supported recursive histories.

## Guard controls and review

Five A controls generate/replay stale worker-only reads, local-written release,
physical representatives counted as extra votes, stale completion reuse and
erased promises. Two B controls execute missing payload and early checkpoint
anchor/pin reclamation. C and D separately execute new-only joint activation;
E additionally executes a real layer transition without its replicated intent.
Each safe counterpart refuses the missing dependency and reaches actual progress.
The raw logs retain complete typed traces, rather than fabricated oracle flags.

Review corrected selected-store recovery, original applier synchronization,
quorum-based original-result resolution, actual response-time packet provenance,
independent parent commit/snapshot boundaries, original-cell repair and replay of
the negative B traces. C/D/E now discard and rebuild runtime caches from their
own bytes instead of comparing an unchanged canonical image.

The first B accumulated-lag graph remains **unqualified**:500,000 states,
2,773,215 edges, depth89,26,177 queued states and zero terminals. Its original
search source, schema and failure log are retained under
[payload-unqualified](agent-payload/payload-unqualified/); this is not a complete
snapshot of every earlier module. The later graph declares a different caller
workload; its protocol guards and negative histories retain majority semantics.
The first E graph also reached its depth384 cap without a terminal. Its actual
Begin/Outcome/Release workload requires the declared1024 depth bound; state,
edge and record limits remain bounded. Other compilation, oracle, scheduler and
lint preflights are retained beside the terminal agent logs.

## Native work remains

The [native schema](PLANNED_NATIVE_SCHEMA.md) and
[release/identity review](LOGICAL_RUNTIME_RELEASE_PLAN.md) are plans. The focused
[application](LOGICAL_APPLICATION315_SCHEMA.md),
[release integration](LOGICAL_RELEASE315_SCHEMA.md) and
[native harness](LOGICAL_NATIVE315_SCHEMA.md) sketches identify actual API,
identity, codec and ownership changes; they are not implemented evidence. They require
actual child-backed parent persistence and snapshots, mandatory response gating,
distinct physical/logical transport identities and owned recovery/drain through
public providers. This protocol gate supplies no new runtime seam. It does not
finish logical voters, the remaining broader transactions or the full platform
sweep. P9 performance and P10 security stay outside the current goal.
