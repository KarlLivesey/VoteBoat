# Like-for-like comparison contract

The shared-driver comparison controls external conditions. The untouched pinned
OpenRaft driver remains the reference for its published curve. Neither is an
acceptance win until sustained results and the policy/unit audit pass.

| Condition | Required comparison |
| --- | --- |
| Hardware | Both engines run sequentially on the same Linux host or M2 Max; no builds/copies during timing |
| Build | Same pinned compiler, dependency lock and asserted opt3/thin-LTO/codegen1 profile; genuine root-library/executable identities |
| Workload | Same zero-payload no-op, one group, three voters, clients, entries/request, rounded total and1024 singleton warmups |
| Driver | Shared closed-loop original IDs and timer/latency collection, one client worker and16 server workers |
| Success | Every original applied after quorum commit; full batch consumed; all three final frontiers checked |
| Durability/transport | Both framework providers volatile and RPC in-process; real disk-backed TLS/TCP/QUIC results separate |
| Maintenance | Same declared5000/1000/1024 cadence/retention/purge policy; actual snapshot representation differs below |
| Observation | Attribution off for capacity comparison; on/off diagnostics separate; all trials and regressions retained |

## Capacity units and qualified correction

The legacy JSON name `api_entry_batch_capacity` reports a configuration value,
not equal units. Pinned OpenRaft `BatchRaftMsgReceiver` counts incoming messages
and extends every message's complete payload batch. Its4096-message capacity can
therefore collect up to16384 originals with four entries/request. VoteBoat's
4096 is a hard original-entry ceiling. `common64` has the same mismatch at batch4.
OpenRaft's4096 append setting limits merging additional append commands; it does
not split an already larger first command. These are source-derived upper bounds,
not observed upstream batch widths. Existing raw records remain unchanged.

Current422/423 selects a bounded VoteBoat original envelope of4096*batch for the
primary profile and64*batch for common64, while preserving upstream settings.
Explicit empty-payload staging-byte accounting and effective readback qualify on
Linux with exact original results, partial1024-entry acknowledgements and crash
controls. Mac qualification and short fresh release measurements now pass. Sustained
capacity and the complete minimum curve remain outstanding.
The legacy JSON field remains for historical continuity; capacity_contract records
requests versus originals, hard versus soft limits and source-derived bounds.
A separately tuned common profile is diagnostic and cannot replace the published
reference floor. Same-engine418→419 comparisons retain their original envelopes.

424 now stops the throughput clock after all client joins and before flattening
or sorting latency vectors, matching the pinned driver's boundary. Its finite
campaign budget defaults3600s and is recorded; incomplete work is failure. Async
cleanup has a separate30s guard and retains unfinished actor ownership on cancel.
The pinned untouched400s driver remains distinct. A separately qualified reference
changes only its400→3600 constant to permit slow full20M cells; source patch, fresh
compiler/library/driver identities and original failures are disclosed.
[424 controls](validation/performance/slice424-campaign/REPORT.md) and
[guard-extended reference](../../validation/performance/upstream-guard424/README.md).

## Engine and representation differences

VoteBoat's image defines its compacted log base, so its historical no-op snapshot
is at applied−1000. OpenRaft snapshots the current application and retains older
log entries. Equal declared cadence does not mean equal snapshot work. Keep actual
checkpoint/retention/catch-up evidence and this label; use a separate both-retain-all
diagnostic to isolate maintenance cost, never to qualify sustained maintenance.

Both engines create one completion channel per original, including batch writes;
OpenRaft's result stream consumes those individual receivers. VoteBoat additionally
uses its operation-ID pending map, while upstream indexes responders by log position.
Removing results is not parity. Any cheaper ownership structure must preserve exact
IDs, partial-prefix outcomes, failure/cancellation and the original completion boundary.
448 experimentally shared backing allocation across independent per-original
channels. It regressed every bulk rate trial on Linux and Mac and was rejected
in449; the retained446 path uses pinned Tokio oneshots per original. Keep the
[448 Linux](../../validation/performance/comparison448/linux/README.md) and
[Mac](../../validation/performance/comparison448-mac/MEASUREMENTS.md) raw results
and its separate artifact/representation disclosure when examining that experiment.

Election/scheduling, operation identity and engine-owned application-worker choices
are recorded engine differences; existing features are preserved.

## Collection-pass rule (H2, 18 October 2026)

This changes the measuring instrument for VoteBoat's actor only; the OpenRaft
engine and the shared driver are unchanged. Results from before H2 are not
comparable with results after it: re-baseline main with H2 before gating any
VoteBoat change on it.

Before H2, a VoteBoat collection pass stopped taking ready client requests as
soon as any peer message was queued. Batch width therefore tracked follower reply
latency. A library change that made replication faster was charged extra
proposal rounds, and so extra Tokio and kernel wake/park work, at 16 server
workers. Researcher measurement on the quiet Mac (bulk 4096×4, 16 workers): the
V3b candidate cut library CPU per write by 22%, but proposal rounds rose from
about 7.7k to 17.3k (mean width 516 → 229), 94% of passes were cut by a queued
peer message (57% on main), system CPU rose from 30 to 50 ns/write and
whole-process CPU per write rose 14%. At one worker no pass was cut.

From H2, a pass takes only the requests already queued when it starts, within
the same `max_proposal_entries` ceiling and the same deferral rule. Peer messages
queued before a pass are still handled first by the biased loop; peer progress
that arrives during a pass is handled before the next pass, so it waits at most
one bounded pass. This also matches SCHEMA.md's "already-ready original
proposals" wording more literally. Raft safety, persistence, per-original reply
order and admission bounds are unchanged.

Researcher measurement of H2 alone on main (quiet Mac): bulk-16 +0.4%, bulk-1
+0.9%, singletons within ±1% CPU per write, p99 unchanged. Builder measurement
(five pairs, `validation/performance/h2/`): CPU per write ×1.008 bulk-16, ×0.991
bulk-1, ×0.993 single-16, ×1.009 single-1. Bulk-16 rounds and width are
unchanged. At one worker passes are about 18% narrower (rounds +22%), because
main's one-worker passes, never cut by a peer message, also took requests that
arrived during the pass. Singleton attribution parity is exact (leader persists
264,195, width 1).
