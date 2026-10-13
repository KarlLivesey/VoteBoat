# Isolated bounded matched-host peer lookahead (420)

Base is published355cfa6, exported into ignored target/p9/lookahead420/source.
No production root or generic runtime contract changes. No response aggregation,
FIFO-result change, dependency, artificial delay or benchmark execution.

At a selected Data turn the actor may move at most64 ready peer messages into an
owned FIFO. A complete inspected prefix consisting solely of current-term
Appended messages permits one count/owned-input-bounded Core batch (maximum4096,
Common64 remains64). Role Leader, local voter, no transfer and no pending Core
dependency are prerequisites. No peer message becomes trusted or acknowledged by
inspection. Any protected message, count/byte stop or remaining uninspected tail
vetoes additional Data; the already-selected first request keeps legacy handling.
New arrivals after the decision remain next-turn ingress. They do not become
quorum evidence before normal Core validation.

Routine lookahead stops at1MiB active message charge and64 items. The first
protected message is retained as a conservative singleton boundary, even if its
payload exceeds the routine soft limit; existing actor4GiB input/effect ceiling
bounds total ownership. Account allocated FIFO slots/header plus every retained
payload using public message_cost; malformed/overflowing ownership fails the run
before proposal execution. Include held peer bytes in every effect reservation.
Report staged item/byte peaks and include staged items in total peer queue peak;
extra bounded staging is disclosed, never hidden as absent queue occupancy.

The next ordinary actor turns process staged peers FIFO before later raw peer
messages and before further Data. Keep original biased stop/peer/Data/timer select
priority and per-turn maintenance; bounded lookahead adds no separate timer delay
or draining loop. A capacity-deferred Data request must also yield to staged peers.
Stop/failure drops owned staging and waits through existing cluster joins. Canceled
client waits retain existing failure/accepted-write semantics, with no fabricated
receipts. Operation IDs, duplicate admission map, individual oneshots, per-original
result checks, snapshots, retained suffix and frontier verification are unchanged.

Checks: prior eight semantic controls and attribution off/on; current-term replies
permit a deterministic wide batch; stale/higher-term reply, election/snapshot and
protected tail veto; unread overflow and soft-byte stop veto; exact64 capacity,
owned-byte retention/release and FIFO processing; role/transfer guards; stop and
maintenance between staged turns; no partial result when canceled waiter fails.
All fmt/strict lint diagnostics must be zero. Return hashes/patch only; apply to
main and timed comparison require root coordination after418 is stable.

Timer courtesy is a reviewed exception to original biased ordering: at a normal
turn decision, a live local leader with no pending dependency/transfer, a fully
owned current-term routine prefix and no raw unread tail gives the existing
interval one ready opportunity before one staged peer. Stop remains first.
At most one tick executes in that turn; a nonready interval never delays ready
peer work. Protected/stale/higher-term/uninspected inputs retain original priority.
Continuous uninspected peer streams can still postpone timers; no arbitrary-stream
fairness claim. Per-turn maintenance remains unchanged and is actually exercised.

Actor-level controls drive the same production loop on pre-elected Core fixtures.
A4094-original first wave produces an actual deferred4-original request; staged
protected and raw higher-term inputs precede it, preserving singleton fallback
and preventing extra Data append. A three-voter controlled-response fixture then
commits/applies6096 original commands through emitted request contexts, owns64
routine unmatched-scope replies, observes an actual timer turn and a real public
checkpoint at5097 while processing that prefix, and stops through original watch
ownership. This is host-loop control coverage, not distributed fault/performance
qualification. Existing actual three-voter runtime/snapshot controls still pass.

The pure single-voter test fixture now retains its peer inbox sender in the peers
vector. Core never emits to itself; routing/consensus therefore stays unchanged.
This prevents an artificial closed inbox when lookahead polls it, matching the
production cluster's live sender ownership. Initial field/closed-inbox/scope
fixture failures and the interrupted unbounded observer are retained. The fixed
observer terminates on actor failure and has a test-only timeout; no production
batching/timer delay or protocol deadline was added.
