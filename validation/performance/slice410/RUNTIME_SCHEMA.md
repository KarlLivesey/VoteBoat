# Ready collection runtime schema

Own src/runtime.rs + ready_batch.rs + timed.rs only. Root owns EffectOwner/ClientRouter.

Shape: existing public singleton stepping unchanged. New crate-internal next_ready_effect_reservation(visit, now) and step_ready_checked(visit, now, check: FnMut(&Raft,&Event,&[Proposal])) opt into ready collection. Stepped retains first singular admission/operation/position/result plus batch_members: Vec<SteppedProposal> for later original invocations (admission,operation,proposed,error).

Selection: exact existing next_queued priority choice. Batch only tracked Data Propose without timer, no Control/Background queue, ordinary owner/no activation profile, leader/voter without active transfer. Consecutive ready proposals only; stop at first non-Propose/untracked/timer or any item/byte/Core owned staging/retained/index budget. Max64 and remaining visit capacity. No arrival wait, no dequeue across another event.

Reservation: same bounded pure queue prefix inspected before execution, conservatively reserve whole input via Core effect_reservation, at least existing max_event_bytes singleton envelope. Failed application candidate remains queued. Check each candidate with previously accepted original Proposal prefix; never clone retained state. First failing check uses old singleton consume/error semantics. Only a checked prefix of two or more uses Core propose_batch; prefix of one uses legacy step.

Ownership: dequeue only after candidate approval; move original bytes into one bounded Proposal Vec. Charge each original q.cost to visit.used and preserve all tickets. One original Persist suffix provides ordinal positions for all original invocations; duplicates cannot alias by OperationId. Entire Core failure returns per-original errors for consumed prefix. Visit/used credits remain retained until existing completion/finish/stop.

Failure: no Core state change while only reservation/checking; invalid next candidate stays in queue. Shared guard rejection follows existing result ownership; no admission or receipt invented. Timed physical wrapper refreshes once after actual step; logical authorization always uses original singleton. Root must map every SteppedProposal into its existing Pending/Admission/position/error ownership.

Checks: four tracked proposals one persistence suffix/four exact admissions/positions; failing middle candidate consumes only successful prefix and leaves candidate/frontier queued; visit count and bytes stop prefix; protected/nonproposal/timer/untracked queues disable or end collection; prefix callback sees actual earlier bytes; original singleton small-limit/spare-capacity behavior preserved; samegroup one visit and used credits survive pending completion untilfinish. No performance runs by this agent.

Final API refinement: preview and step both accept max_items, capping ORIGINAL output count (not merely visits). Prefix type is &[ReadyProposal { admission:AdmissionTicket, command:Proposal }], crate-internal, enabling exact admission-index lookups without scanning all pending requests. Collector moves original bytes; no retained-state or payload clone is introduced. Parent selects max_items=1 before preview for apps that do not opt in.

Reservation additionally charges checked selected ReadyProposal/original tuple/later SteppedProposal metadata, expanded OwnerStep output metadata with explicit Vec growth slack, and four Vec headers. The Core input reservation uses complete owned Proposal input, at least the legacy singleton envelope; no reliance on vague fanout slack.

Focused final core-only checks: 9 tests passed. These cover all four original positions/tickets/heldcredits; later refusal and first callbackonce; maxoriginals/visit items; nonproposal/untracked/protected/timer boundary; legacy spare-capacity/smallbatch limits; aggregate input+metadata reservation; visit byte budget and hard64cap; repeatedOperationId distinct admission/position. Root handles broader physical service/ClientRouter/app opt-in acceptance.
