# Retained original deadline cleanup — separate298 schema

Purpose/macro: complete the observed P5/P6 parent shutdown fixture acceptance.
297 is independently frozen/published first. Do not modify production, deadlines,
poll budgets, lint levels or the general request-progress15s helper.

Captured actual state (target/slice297/linux-original-checkpoint.log): at
MonoTime57874 node3 alone is Draining; clients/reads/results/owner/outbound and
replica work are empty. Peer1 original generation150000/store session15 remains
Draining with no accepted ticket, send/completion/frame/input resources; session
Closing, flushedtrue, close_startedtrue, protocol_drainedfalse. Its own deadline
is MonoTime60926, 3052ms after the fixture's fixed15s cutoff. This occurrence is
not unsent hidden output or stuck accepted work; the actual protocol timer has
not become due. The older285/288 lacked this predicate, so retain their exact
historical uncertainty rather than retroactively proving their state.

Data/API shape: fixture-only ClosedProtocolWindow collects bounded original
(local node, peer, SessionBinding, MonoTime deadline) records via existing public
drain inspection. No new provider/authority seam. All Node local/replica and
connector/driver work must already be empty; enumerate tracked roster peers,
not just current voter IDs. Missing optional inspection is a refusal.

Transitions: the existing close stage polls under its unchanged15s deadline.
If it completes, reclaim exactly as before. Only on its cutoff, classify all
remaining nodes/connections as pure settled protocol close. Pin exact original
bindings/deadlines once. A separate terminal cleanup stage retains those same
owners and existing per-visit budgets until the greatest pinned provider timer
becomes due. Never recompute/extend the cap from subsequent polls. Missing
connections mean actual retirement; fresh connections, original identity
changes, accepted work, retained input or altered/missing timers are immediate
refusal. Deadline equality is meaningful; the owner may get one bounded visit
at/after its due timestamp, and any remaining retained owner then fails.

Ownership/failure: no is_drained override, no inferred Sent, no synthetic socket
completion or virtual clock jump. Existing public Node.poll performs actual
protocol timeout/retirement and releases original resources. Return parts only
when all original Nodes genuinely report drained. General request failures,
unsupported inspections and local work keep the original15s failure semantics.

Acceptance: causal classifier regression uses typed equivalent captured297
state: strict future original timer is eligible, cutoff+3052 bound retained;
now>=deadline at initial classification, missing timer/detail, accepted ticket,
framed input/completion, non-close state, changed binding or extended deadline
are refused. Real existing long-RTT QUIC292 remains evidence that original close
can exceed15s; an actual original delegated checkpoint history after fixture
correction must terminate through true drain on both hosts. No unchanged passing
probe. Full relevant lint/test qualification uses separate298 frozen inputs.

## Implemented and actual evidence

Fixture-only298 is implemented in tests/routed/native.rs (unchanged owner poll
body extracted to poll_nodes; generic drive still15s), new routed/shutdown.rs
(conditional pure-protocol stage and frozen window) and shutdown/contract.rs
(captured297 cause and refusal controls). Production remains unchanged.

Eligibility checks exact original binding/peer identity, closed authenticated
roster, no attempts/accepted original tickets, transport Draining with no frame,
completion or decoded input and existing close_started, session Closing/flushed
with close_started/protocol_drainedfalse and supported original deadline.
Node clients/reads/results/owner/persistence/outbound/replica/checkpoints/snapshots
and connector/driver/ingress must already be drained. All tracked peers are
inspected. Only original records may remain; timer extension/identity changes,
fresh work and any original timer already due after its owner visit are refused.
The maximum original deadline is pinned once; no duration/slack/loop renewal is
added. Actual Node.is_drained is still required before original into_parts and
worker reclamation.

- Captured-cause/refusal tests:4/0 (`retained-contract-final.log`). They exercise
  the exact297 timer57874->60926, deadline equality/late refusal, fixed bound,
  changed deadline/binding/new owner, another owner retiring, original accepted
  ticket preservation, unsupported timer/protocol detail, frame/completion,
  fresh attempt, unread input and non-close/identity states.
- Actual original checkpoint history:1/0 in59.22s (`retained-original.log`).
  This run did NOT print conditional-cleanup activation: it drained inside its
  ordinary cutoff. Preserve this coverage limitation; do not claim the real
  extended cleanup path was exercised solely from this selected pass.
- Formatting and strict all-target/all-feature Clippy passed before the fourth
  negative test; final repeat recorded in retained-fmt-final.log and
  retained-clippy-final.log. Parent performs integrated four-profile and Mac
  frozen-source qualification; no broad current acceptance claim.
- Initial test compile imported OutboundGeneration and PeerIdentity through
  private reexports; focused fix imports from their public identity/secure
  definitions. No lint or assertion weakened.

### Actual native long-RTT supplement

A fixture-only controlled actual NativeQuicSession/NativePeerTransport regression
now lives in tests/routed/shutdown/long_rtt.rs. It delays actual first-server
processing in virtualMonoTime3500, checks exact encrypted UDP readiness without
consuming packets, completes authenticated hello/flush and drops the original
peer. The original framed close timer exceeds start+15000; actual public transport
inspection at that cutoff enters the same ProtocolClose/ProtocolWindow, remains
Draining at its original deadline-1 and closes/releases the real session only
after bounded poll at the exact original deadline. There are no outstanding
original queue credits or synthesized send/receive completions. Roster flags
are explicitly fixture metadata; all transport/session/timer fields come from
actual native public drain inspection.

Five focused shutdown tests pass5/0 (`retained-shutdown-native.log`), including
this actual timer-driven regression and the four captured-cause/refusal controls.
The semantic cap is the pinned original provider deadline; this is not a hard
wall-clock scheduling guarantee. A bounded owner visit sampled at/after due time
can cross it due to OS scheduling; actual drained state is still mandatory.
The selected original wall-clock run remains separate1/0 in59.22s, without
conditional activation in that execution.
