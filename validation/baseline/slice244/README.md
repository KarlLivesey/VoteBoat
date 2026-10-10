# Slice244a — response window after durable self-vote

Base ebc7fae (production code unchanged from1e60e8a). A campaign originally armed
its next election before its self-vote finished persistence. If completion was
late, an already queued expiration remained valid when the original Vote requests
escaped. The candidate could immediately supersede those requests with a new term.
Granted follower votes already reset at durability completion; self-votes did not.
The core now advances the existing volatile reset sequence at After::Campaign,
only after the original exact self-vote ticket passes completion validation.
TimedShard's existing refresh fences the queued expiration and starts the normal
configured response window. No clock in core, timing-threshold/caller-budget
increase, weaker synchronization/quorum, new public seam or stored/wire change.

The actual public TimedShard/downstream store regression fails the old code0/1.
It delays the self-vote beyond expiry, queues the original timer, refuses a foreign
store-session completion without term/timer change, then synchronizes the exact
ticket. Same-term Vote messages escape, the old expiration is discarded and a
matching ballot elects that original candidate. Leader-noop synchronization still
precedes announcements. Another case confirms a fresh timeout without ballots
can persist a new term and release new requests. Initial post-fix tests correctly
hit DependencyPending because the test tried to finish unadmitted noop/new-vote
updates; their logs remain. A shared public-barrier helper completes those exact
dependencies before visit release. The core reset itself is unchanged by that
focused fixture correction.

Final Linux: Raft41/runtime33, core-only29/25; full all-feature counter197/directory22/
transfer33 and sequential default counter138 pass. Formatting/four strict profiles
are zero. Inventory108/conformance metadata9 partial reviews/68 operations unchanged.
Mac source is the isolated immutable1e60e8a checkout plus the recorded exact source
patch, verified against source.sha256. Installed stable Rust1.98.0, arm64/macOS27.0,
default12-test concurrency; host settings, provider choices and deadlines unchanged.
Mac Raft41/runtime33 pass. Its original broad operator schedule remains failing:
counter121/75, directory20/2, transfer17/16 (passed/failed), versus prior65/131,
9/13 and14/19. Those are single finite runs, not statistical performance attribution
or whole-platform acceptance. Remaining diagnostics include original write deadline,
leader unavailability, handoff uncertainty, canceled preparing configuration,
drain observation budget and transfer initialization uncertainty. Keep them.
Mac formatting/four strict profiles are zero. Its sequential default counter
run remains99/38, versus prior61/76; reduced failures are not zero acceptance.

Mac public Rust embedding creates7, cold-retries duplicate7 and cold-applies
fresh10; all three runs join workers. This selected path does not clear the broad
operator failures.

Next: inspect actual completion/response timing and compose explicit native
operator profiles through existing startup contracts, using the architecture
pack's declared throughput/edge proposals as input. Proposed numbers are not
measured guarantees; changing election settings is distinct from caller bounds,
durability or quorum. Status-wait cleanup and required operator integration follow.
Full P0–P7 remains active; functional features/Linux/macOS first, tuning/security
later, Windows/P8 deferred.
