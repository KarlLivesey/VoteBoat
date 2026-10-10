# Slice254 — owned manifest read-source handoff

NativeManifestLookup::replace_source returns the old owned source on success and
returns the replacement intact on refusal. The caller selects sources and drives
both; replacement does not campaign, infer read authority or discard work.
Pending/cancelled driver reads and unresolved source reads prohibit replacement.
Closed, backwards-clock and invalid/same-binding requests leave the driver intact.

Handoff invalidates old hints, clears negative retry state and resets only the
source-specific sequence domain. Bounded manifest/version/barrier floors and
monotonic observation IDs survive; a checked volatile source generation prevents
cross-domain duplicate recognition. An absent read advances the known barrier
floor. New hints still require actual bound quorum outcomes. No persistent/wire
format, consensus rule, timer, deadline, quota or dependency changes.

Independent host tests exercise source ownership, cancel/drain, clock/binding,
sequence/observation identity and retained floors. Real TCP/QUIC histories force
the original source to lose leadership before local and remote discovery, use
another ready voter through the public seam, and retain returned Nodes for normal
polling and joined cleanup. The provisioned remote responder identity stays fixed.
Stopped logs are compared with their actual store IDs. Original manifests,
queries,10s/15s deadlines, cached child independence, cold retries and fencing
remain. Positive fixture recovery distinguishes exact known NotLeader refusal
from accepted LeadershipChanged uncertainty and preserves original IDs/content;
a duplicate first receipt requires the latter witness.

| Selected checks | Linux | Mac |
| --- | --- | --- |
| Affected local/remote native histories |15 passed|15 passed|
| Host lookup, mapping, routing, remote contracts |31 passed|31 passed|
| Core-only public source contract |1 passed|1 passed|
| Native-only host lookup |9 passed|9 passed|
| Default host lookup |9 passed|9 passed|
| Default native histories |8 passed|Not run|
| Additional original parent-independence histories |2 passed|0 passed,2 failed|
| Formatting/default/all/core/native strict Clippy |Zero diagnostics|Zero diagnostics|
| Final build inputs |652 verify|652 verify|

The additional Mac parent histories fail on raw positive proposal
Unknown(LeadershipChanged). They remain the next focused recovery deliverable;
this is selected handoff acceptance, not full routing/platform acceptance.
The original251 Mac full suite stays on its unchanged separate source and is
still running with failures. Original251 Linux is terminal1826/0 on ba67c8c;
it does not certify this source or later added tests.

Retained negative checks: forced local source loss, exact write/read NotLeader
admission, missing-read floor regression, and forced remote source loss. The last
has Timeout, no pending read, original store1 Follower and store2 Leader. A prior
intermittent Mac remote timeout is retained; its diagnostic rerun passes and does
not establish its cause. Rejected campaign/latch/positive candidates and failed
fixture preparation remain in the logs. The controlled handoff checks do not
claim the old uncontrolled timing schedule always succeeds.

Commands use installed stable toolchains, locked/offline dependencies, all-feature
routed filters and full related test targets. Mac arm64/macOS27.0 uses stable1.98.0
and a child-shell4096 descriptor limit; Linux x86_64 uses stable1.98.1. Feature
configurations that run executables are sequential per host. Failed test Drop is
not proof of worker joins; successful histories assert explicit drain/reclaim.
Final-handoff-source.sha256 binds652 build inputs; earlier manifests bind retained
candidates. Raw.sha256 records original log digests; published logs normalize task
paths/trailing whitespace. Final.patch records the implementation/test diff from
17d7411 with zero context (git apply --unidiff-zero). Inventory108 and13 partial
reviews/81 operation groups remain metadata, not certification. Full P0–P7 stays
active; broader faults, P7 tuning, Daybreak security and P8/Windows stay separate.
