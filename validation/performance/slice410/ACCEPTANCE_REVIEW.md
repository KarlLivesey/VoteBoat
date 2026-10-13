# Ready batching acceptance review (read-only source/design)

Reviewed existing Shard stepping, EffectOwner reservation/installation,
ClientRouter original-request mapping, ProposalAdmission and direct Core batch.
No source changes, builds or measurements by this reviewer. Runtime schema:
RUNTIME_SCHEMA.md. Findings below are implementation acceptance conditions, not
claims that the pending implementation passed them.

## Contracts that must stay exact

- Existing public raw stepping/EffectOwner singleton modes retain their behavior.
  New authorized collector is crate-internal and selects the same next_queued
  traffic class before considering any batch.
- Application capability defaults false. Counter may opt in: its consensus-context
  check is the default pure check, pending dedup capacity counts unique IDs, and
  actual ordered outcomes remain application results. Composite/custom apps do
  not inherit Counter's permission merely by embedding it. A custom context gate
  can depend on last_index and must wait for the prior original transition.
- Batch only adjacent tracked ordinary Data Propose, without timer, competing
  queued Control/Background work, logical activation/control authority or another
  event between commands. Do not scan past Read/Receive/untracked/nonproposal.
- Original queue costs remain charged until visit finish/stop, including all
  consumed members. An observer's cancel_wait publishes Unknown but does not
  withdraw admitted work or free its capacity (`clients.rs:724+`).
- Member mapping uses admission + ordered position, never OperationId alone.
  Two requests can intentionally share an operation ID; they still have distinct
  admissions, log positions and individual application receipts.
- One batch Persist result and dependency belong to the one original visit.
  Expand observation steps without duplicating effects, storage submissions or
  durability evidence. ClientRouter's existing ticket/position/outcome routing
  must continue to consume every exact original admission.

## Blocker found and design remedy

The old next_effect_reservation uses one max_event_bytes input envelope. Visit
bytes or Core staging limits can exceed that, so a prefix cannot inherit the
singleton input allowance without adjustment. Runtime schema now proposes
next_ready_effect_reservation over the same bounded candidate prefix. This
resolves the design blocker if it uses the complete owned Proposal input cost
(Vec header, actual allocated element capacity, every payload capacity), at least
old singleton max_event_bytes, and preserves both group/control budgets.

The inspector and executor must share exact caps. Actual execution can shorten
an inspected prefix on application failure, but cannot expand beyond its reserved
input. Deliberate Vec::with_capacity(exact_count) is simpler than allowing Vec
growth slack that differs between reservation and actual Core staging validation.

Do not switch a surviving one-command prefix to Core::propose_batch: its new
allocation/staging ceiling is not part of the old singleton contract. If first
command alone is too large for new batching, original singleton execution is
still allowed through its original limits. If later candidate exceeds aggregate
limits, leave it queued and execute only the fitting prefix.

## Failure, ordering and cancellation review

- First application check failure consumes just that original request and reports
  its original NotProposed error, as singleton stepping does. Later callback failure
  stays at queue head for later original execution; it is not prematurely dropped
  or rejected using context that will change after prefix application.
- Avoid validating the first callback twice when falling back to singleton after
  collection. Reuse the exact first verdict; the input/Core cannot change during
  this serialized decision. Duplicate checks add unnecessary work and can change
  behavior for a stateful FnMut host validator.
- Core batch refusal after a selected approved prefix must assign the same refusal
  to every consumed original. No positions are claimed unless the one staged suffix
  contains each exact ordered original command. Failed plans leave Core unchanged.
- Higher term/removal/leadership transfer remains its ordered event boundary. A
  queued control event disables collection. No term/config changes can interleave
  between prefix checks and the one Core call on a serialized owner.
- A canceled request remains eligible work and its original pending record remains
  mapped. Actual receipt may resolve internal terminal ownership while the already
  published Unknown output stays unchanged, following existing ClientRouter rules.
- Timed wrapper refreshes timer state once after the real Core step. Stale queued
  timer behavior stays on the original singleton path.

## Bounds/fairness that need explicit checks

- Selected count <=64, remaining visit_items, group retained-entry budget and checked
  remaining indexes; owned byte costs <= remaining visit_bytes and Core staging
  budget. No hidden arrival wait or repeated clock polling.
- Group's class cursor advances once for one chosen Data turn; every member counts
  toward the visit item budget. Any queued background/control must force normal
  cursor behavior. Other ready groups get their scheduled turns after the hot visit
  reaches its already configured bounds, rather than another unbounded drain loop.
- Expanded OwnerStep cardinality is bounded by sum of selected originals across
  bounded visits, at most visits * min(64, visit_items), further bounded by admitted
  requests. This is larger than one step per visit; document it for ClientRouter's
  batching mode. Preserve singleton output cardinality. Account/check growth of
  transient SteppedProposal and expanded step vectors; these are observations, not
  free effects covered solely by Persist's payload accounting.
- App validate_proposal already sees all retained queued/in-flight requests under
  ClientRouter's contract. EarlierSelected must supplement missing lower-level
  inputs, not accidentally double replay already represented requests for opted-in
  apps. Counter's unique-ID set is insensitive to duplicates, but generic bounded
  pending validators can count duplicates and replay state. Defaultfalse currently
  protects those providers; any future opt-in must specify prefix/pending semantics.

## Focused acceptance cases

1. Four queued original Counter requests -> one Persist suffix, four exact tracked
   steps/positions under one visit; no successful client result before original
   durable quorum and individual applied outcomes.
2. Repeated operation IDs, same and conflicting payloads -> distinct admissions and
   positions, deterministic original duplicate/conflict outcomes, no map overwrite.
3. First callback failure vs middle failure vs Core role/transfer refusal -> exact
   consumed/retained frontier, no dropped tickets and no invented positions.
4. Capability-defaultfalse custom app with last_index-sensitive context -> first
   singleton and later original recheck after previous transition, preserving old
   behavior. Composite defaultfalse application likewise remains singleton.
5. Small legacy singleton limit/spare payload capacity -> singleton still works;
   additional batch ceiling blocks joining, not that first request.
6. Visit item1/item2, byte boundary, Core count/retained/index/owned capacity boundary
   -> exact prefix and charges. Reservation equality should use actual capacities.
7. Queued Control, Background, Data Read/Receive, timer, untracked event and logical
   activation authority -> singleton/no-cross-boundary collection.
8. Two hot/cold groups plus pending persistence -> bounded scheduled visits, no hot
   monopolization, no early queue-credit release while dependency is pending.
9. Cancel wait before collection and after proposed position -> original work still
   admitted, Unknown output remains, eventual ownership cleanup exact.
10. Worker refusal/failure and owner stop after dequeue -> original consumed members
    become unknown/fenced through existing paths; still-queued tickets remain in
    Stopped.admissions and retryable events exactly once.
11. Expanded observation-output count and capacity stay within documented visit and
    admitted-input bounds; a single effect is installed once for the batch.

No separate capability/security feature is needed for these controls. They verify
that the performance optimization preserves existing application, queue, deadline,
receipt and lifetime contracts.
