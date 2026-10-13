# Startup batch limits in the matched harness

Purpose: expose the production startup-only BatchLimits seam in the primary
framework comparison, without reducing the competitor's published defaults.

Data/API: configure every recovered VoteBoat Core before actors start using
`BatchLimits { max_proposal_entries, max_replication_entries }`. Primary
UpstreamDefault selects 4096/1024; optional Common64 selects 64/64. Record the
effective per-Core values after configuration; actor collection uses that Core's
proposal bound. OpenRaft configuration remains unchanged and reports its validated
effective capacities. The four-original client API and all per-original receipts
remain unchanged.

Ownership/state: immutable startup settings -> validate all three Cores -> spawn
actors -> greedily collect only already-ready complete client requests within the
effective original-entry bound -> one public Core proposal -> existing ticket,
quorum, application and result handling. No waiting to fill, splitting results,
extra concurrency, retention changes or bypass of effect reservations. A request
that would cross the bound remains the existing deferred request.

Diagnostics: a bounded 0..4096 original-entry width histogram per actor counts
successfully admitted proposal batches, including singleton writes and warmup.
Each bucket represents the number of original commands, not the number of client
requests. Derive total batches, total originals and maximum width after the run.
Followers with no local proposal admission report an empty histogram. Existing
queue, checkpoint, retained-log and persistence measurements remain intact.

Failure/cleanup: startup configuration failure occurs before actors spawn. Invalid
request shape, receipt loss, Core rejection, queue overflow or unavailable actor
still invalidates the run and uses the existing stop/join cleanup. No change to
the immutable baseline artifacts or source exports.

Acceptance: existing exact singleton/four-original tests, 12000-entry actual
maintenance and paused-follower verified snapshot recovery remain required.
Add a controlled preloaded ready queue to exercise a batch wider than 64 through
real Core admission and all original replies, then check Common64 stays bounded.
Reports must reflect actual configured limits and the width histogram must sum
to admitted original commands. This test-only queue gate changes no release
scheduling. Run standalone fmt, all-target strict Clippy and scoped tests only
after the production API is available and root permits builds. No paired timing
or performance acceptance is implied by functional qualification.
