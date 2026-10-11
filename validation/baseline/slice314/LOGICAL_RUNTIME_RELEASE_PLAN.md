# Proposed315 release/identity seam — read-only review

Plan only. No compilation input changes after freezing314. Source basis is
`target/slice314/PLANNED_NATIVE_SCHEMA.md` and actual runtime/router/transport
contracts. Keep one parent ordered log and one child-backed persistence path.
This is the necessary release path for a usable fixed logical cohort, not an
optional generic framework, performance project or security review.

## Smallest linked path

1. Implement the deterministic backing application and one explicitly pumped,
   host-owned bounded child bridge. Claim/mutation/checkpoint/capsule work retains
   exact mapping, fence, expected parent revision/term/configuration, original
   child ticket/operation/content and caller owner/effect/request. Existing child
   Node Applied or fresh quorum original-outcome consumption alone emits parent
   worker/snapshot events or a release permit. Child admission/write/local sync
   never authorizes parent release. Native and host providers use the same ports.
   Synchronous ports only move owned bounded work; the caller pumps actual child
   Nodes separately. This is needed before any parent campaign can complete.

2. Add one mandatory logical release coordinator to the selected parent runtime,
   alongside existing ReplicaDriver. Select logical authority immutably at
   construction, bind it to the recovered logical mapping/core/worker identity,
   and validate that the assembly has its release coordinator. Physical-mode
   direct release remains valid only for physical authority. A replaceable
   worker/outbound adapter must not turn off logical validation. Exact release
   permits are opaque protocol-owned capabilities bound to original owner,
   effect/result ticket, parent revision/context and child Applied outcome.
   A boolean provider-ready callback or mutable optional bypass is insufficient.

3. Extend the existing physical framed transport with an explicit logical frame
   payload/routing contract. The authenticated outer scope remains the actual
   physical TLS/QUIC peer/store/session. Inner Message keeps stable logical
   from/to/store and exact parent request/configuration; the capsule binds both.
   Validate committed mapping, original capsule and destination before inner
   core ingress. Reuse existing bounded frame/connection ownership and generic
   provider injection. Ordinary Message decoding under a rewritten physical
   WireScope, or treating a logical voter ID as a physical representative, is
   invalid. A named logical parent assembly can compose existing EffectOwner/
   ReplicaDriver with this routing seam while actual backing Nodes retain their
   ordinary physical PeerDriver; it need not immediately broaden every Node
   convenience constructor.

## Exact release transitions and ownership

Send: generated original EffectLease -> owned gate slot/reservation -> actual
child capsule command -> consumed exact Applied or fresh original outcome ->
logical outbound envelope admission -> original transferred-send release. A
refused queue returns the same owned envelope/permit; no second response fact is
manufactured. Snapshot packet expiry may abandon an unadmitted send as existing
packet-loss semantics, but cannot erase accepted child capsule work. Historical
committed capsules may arrive late only for their original request; a new gate
submission after fence/configuration change must validate current authority.

Client: apply original Committed batch through the existing application router;
retain its exact opaque ApplicationResults and original position/operation/content
while the child validates an exact client-response capsule. Only after permit
validation call ClientRouter::deliver/publish Applied. Application progress may
advance locally before observation, but public success and result credits remain
owned. No replacement receipt construction and no early completion release.

Read: reserve and capture the original query/result through existing live
ReadReady authority, then retain the exact private ReadResults/barrier and
request before successful public publish. Commit a capsule tied to captured
query/result/application boundary plus parent context. Separate execution from
publication in ReadRequests, preserving original cancellation and cleanup.
Reading application state after authorization without retaining its original
result/context would produce a different response. Revalidate local barrier/
owner at the selected transitions; changed authority yields unavailable/unknown,
not an old read success. Legitimate historical original observations remain
allowed after later changes; no instantaneous remote-fence assumption.

Close/cancel retains accepted gate/child work until terminal consumption. Refusal
returns original ownership; provider error fences further phase production and
transfers real outstanding work to explicit recovery/drain. Node/replica drain
must include gate slots/results/accepted child observation ownership. Reserve
gate result bytes before child admission so an acknowledged capsule cannot strand
unbounded outputs. Parent effects hold their existing byte credits, with separately
bounded gate metadata and encoded query/result bytes.

## Concrete touchpoints and objections

- `src/runtime/replica.rs:608` retry_send currently submits raw Message directly
  to OutboundQueue; gate before that transfer. `:689` retry_committed currently
  polls ApplicationResults and immediately calls ClientRouter::deliver; this
  client path must also be intercepted. `:728` retry_read calls synchronous
  ReadRequests::execute. `retry_effects`, usage/drain and failure ownership must
  include gated effects/results instead of duplicating them.
- `src/runtime/clients.rs:505–591` validates and consumes opaque application
  outputs then publishes Applied; preserve these checks at delayed delivery.
  `src/runtime/read_requests.rs:513–594` consumes private ReadResults and publishes
  Read synchronously; split bounded capture from permitted publication here.
  `src/runtime/applications.rs` / `reads.rs` already have opaque owned result
  types and byte credits; reuse those rather than expose fabricated receipts.
  `src/runtime/node.rs:540–555,927–1032` public polling/drain then sees only gated
  success, with actual recovery ownership on failure. Wrapping poll_client/read
  alone is too late and leaves direct router users outside the guarantee.
- Existing BoundedReadableStateMachine supplies size bounds, not canonical
  query/result encodings. Exact generic local-result capsules need a small
  bounded application-response encoding contract or explicitly supported
  application codec. Counter/transfer can implement it; Debug formatting or
  an arbitrary result-size/hash assertion cannot substitute. Keep the application
  provider's existing trusted deterministic-result contract; do not invent a
  second business state machine just to validate response encoding.
- `src/wire.rs:21–29,128–178` and current transport batch types are Message-bound;
  add an explicit supported envelope/frame type and codec capacity negotiation.
  `transport/membership.rs:39–99` requires core local NodeId/store to equal the
  physical LocalIdentity, and derives peer assignments from those core IDs.
  Ordinary parent Node PeerDriver assembly therefore cannot represent the new
  distinction as it stands. Physical connection assignments must derive from
  authoritative logical mappings, not addresses/cached representative hints.
  `runtime/ingress.rs:375–428` must route/validate the logical envelope before
  owner.admit of its inner Event::Receive. Existing physical PeerRoster transport
  authentication and exact connection-ticket checks remain mandatory.
- Keep named verified recovered-image startup and compacted log/application/
  checkpoint validation. Each bridge event uses separate parent/child indices;
  accepted child policy gates new representative work, while replay of original
  committed commands follows its historical ordered context. The native whole-
  child restart must reconstruct authority through real child recovery, not314's
  surviving-runtime assumption. No duplicate parent WAL or empty fallback.

## Bounded acceptance order

First compile the application/bridge/mandatory release coordinator through native
and host-injected public providers, including actual opaque wrong-ticket/content
and bounded cancellation/drain controls. Then exercise one parent write/read/
original retry across three logical leaves each backed by three actual physical
child voters over TCP/TLS and QUIC. The original worker-only stale ReadAck trace
must refuse through the same gate used by real sends; held client/read success
must also stay unpublished after lost authority/inner quorum. Then representative
replacement, old checkpoint plus suffix, whole-child recovery and exact historical
capsule/stale generation cleanup qualify this fixed usable315 assembly. Existing
316 nesting/layer-membership remains separate. No native/default promotion or
general safety claim follows from these planned seams alone.
