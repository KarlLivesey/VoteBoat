# Slice464: distinct acknowledged commit and append frontiers

Before-edit schema. Parent85eaf3e; full P9 scope remains active.

Purpose: actual463 TCP/QUIC controls had zero joined transitions. Headless
same-ELF debugger diagnostics found successful exact-primary replies ending
before the newer local durable tip. Three captured replies also exceeded the
paired commit index. These are scheduling-perturbed candidates, not proven
eligible joins, an opportunity frequency, or a performance result.

Data/API: private ready-reply plan owns the prospective peer progress map and
one commit frontier. Public APIs, provider capabilities, batch limits, runtime
selection/reservations and persisted/wire formats stay unchanged. A successful
primary reply must match its exact outstanding range end, context, current
term/configuration and authenticated source; its end cannot exceed the durable
tip. Existing empty-alias and dependency refusals stay.

Transitions: update peer progress with max(previous, acknowledged end). Compute
the ordinary policy frontier capped at the durable tip. The first reply must
advance the current commit in the current term. Every subsequent selected reply
must leave that frontier unchanged: otherwise fall back to the original events,
preserving distinct application commit batches and their failure boundaries.
Append proposals strictly after the old durable tip, retaining the uncommitted
middle. Install each selected peer's next index from its own progress plus one,
never from the append base. Retire only selected exact primary requests.

Ownership/failure: plan and validate all updates before Core mutation; refusal
leaves Core and original queued inputs unchanged. Keep the existing conservative
two-revision/context budget. One pending exact ticket owns the combined update.
Written gains no local durable vote, commit, application or client success.
The existing full-old-tip quorum proof for early replication is unchanged;
partial-prefix joins emit no early tail. Exact Durable exposes only the older
committed prefix and creates catch-up/commit notifications under existing rules.
The new tail still requires quorum. Failed publication fences; restart recovers
only the existing native manifest boundary, with stale tickets rejected.

Acceptance: differential three/five/weighted serialized receipts, retained middle
and peer catch-up indices; Written silence and exact Durable; unchanged-first-
frontier multi-reply success and increasing-frontier/old-commit refusals with no
mutation; existing stale/alias/auth/config/term/budget controls; native modeled
loss/publication/restart. Run affected all/core tests, real TCP/QUIC recovery,
matched tests and full enabled strict hook. Preserve failures. Publish checked
source, build a fresh asserted artifact, and demonstrate real activation in
short disabled/enabled controls before repeated paired timing. No ready/window,
maintenance/checkpoint-frequency or durability changes. Sustained both-host
floors, headroom/fairness/placement/P8 acceptance remain open.
