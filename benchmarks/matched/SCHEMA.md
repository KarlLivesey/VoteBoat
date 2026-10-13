# Concurrent framework schema and qualification

Macro: P9.0 reproducible matched-framework evidence, P9.1 ordered-group capacity,
and the selected P9.3 maintenance path. This is a benchmark embedding; it adds no
production runtime adapter or dependency.

Data/API: one real three-voter group; clients1/64/256/512/1024/4096; original entries
per request1 or4; requested total rounded down to equal complete client batches;
1024 singleton warmups. Empty business requests/responses. Original VoteBoat IDs
remain required metadata. Each original has an applied completion; a batch client
waits for all four. Common client runtime has1 worker; separate server runtime has16.

Primary OpenRaft configuration matches pinned minimal bench: election200–2000ms,
purge1024, payload1024; default API4096/append4096, snapshot5000/keep1000, zero
append/flush/network delays, ordinary tick/heartbeat/election flags and runtime
statistics feature. Shared dependency requirements follow its pinned manifest;
this standalone lock preserves exact resolved versions. VoteBoat configures its
startup-only Core limits to 4096 proposal/1024 replication entries and reads back
the actual limits before actors start; optional Common64 is 64/64 attribution only. RetainAll is an
explicit secondary profile, never silently substituted for default maintenance.

Ownership: three Core actors each own one LogStore, snapshot store and no-op
application. Client -> bounded admission -> already-ready original proposals
combined within the configured proposal bound -> exact volatile persistence ticket/barrier -> real owned peer
messages -> validated quorum commit -> exact local completion -> contiguous no-op
application -> per-original reply. No shared Core lock, group aggregation, opaque
four-result command or wait to fill a batch. Core effect reservations and owned
input/receipt/mailbox bounds remain enforced. Peer progress precedes more data.

Failure/cleanup: overflow, unexpected effect, missing/duplicate/wrong result,
noncontiguous application, unavailable actor or timeout invalidates a run. Each
client has at most one live request. Setup/warmup precede measurement; joins of all
original results end measurement. Postmeasurement verifies every follower's full
applied prefix. Watched stop joins actors; dropped handles abort owned tasks.
Fallible replica construction completes before any actor starts.

Original batch widths: each actor has a bounded 0..4096 histogram. A bucket counts
the original commands in a successfully admitted Core proposal, not request
batches. Post-run diagnostics derive total admitted originals, batch count and
maximum width; warmup is included. Non-proposing followers expose empty histograms.

Maintenance: volatile SnapshotRetention validates stage/chunk/seal/publication,
checks image integrity and retains at most two exact log anchors. Public checkpoint,
compact, supply, stage and install helpers perform actual transitions. After5000
newly applied entries, historical no-op state at applied−1000 is exactly known;
compact only with at least1024 newly reclaimable entries. No hidden reset/truncation
or bypass of the authoritative log validator. Core/store generations and original
context/application gates remain intact.

Remaining model differences: VoteBoat snapshot image frontier is also its logical
base, while OpenRaft keeps1000 older entries behind a latest-frontier image. Their
healthy retained suffix policy is selected alike, but image frontiers and catch-up
mechanics differ. VoteBoat inline application and fixed2000ms host campaign guard
also differ from OpenRaft's worker separation/randomized200–2000ms elections.
VoteBoat uses owned message delivery; upstream uses function-call RPC into its
actors. Engine metadata/store/queue differences are measured, not erased. The
original upstream driver, including its stats-print task, remains separately
reproducible. Framework volatile callbacks establish no device durability claim.

Frozen baseline evidence: Linux and macOS six tests pass under the final independent lock;
strict all-target Clippy and fmt exit0 without a Cargo-home override. Controls
cover rounding, partial-prefix refusal, exact historical restore, both-engine
single/four-original outcomes,12000-entry real maintenance, and a paused follower
that catches up through verified snapshot installation. Two actual1/16-runtime
CLI controls at4096/batch4 complete16384 originals plus1024 warmups with all three
frontiers17409. Raw compiler failure/final checks and CLI controls are preserved in
validation/. Tiny debug rates establish no minimum-floor or sustained-capacity win.

The short primary pilots subsequently passed all 18 exact cases and source/artifact
guards per host. They expose a high-concurrency gap and remain diagnostic. Their
64/64 VoteBoat baseline source and executables are immutable; the new startup
batch-limits harness slice has separate qualification evidence.

Startup-limit candidate evidence: eight Linux tests, standalone fmt and strict
all-target Clippy pass. Four separate 1-client/16-server-runtime controls complete
16384 originals in4096 requests per engine/profile, with all frontiers17409.
VoteBoat's observed original proposal width reaches4096 in the primary profile
and64 in Common64; histogram totals include exactly17408 originals with warmup.
Frozen414 Mac checks and short paired diagnostics now pass; full sustained
capacity and current415 Mac qualification remain pending. Raw checks are separate
under validation/performance/slice414-harness.

Fixture correction evidence: Mac's initial wide-ready assertion failed because
live peer-priority scheduling does not guarantee a minimum width. A per-leader
observed-pause acknowledgement makes preloading explicit, but the live3-voter
test promises exact receipts/frontiers and the configured upper bound. A separate
fully preloaded single-voter actor/Core/store test guarantees exact4096/64 widths
without concurrent peer delivery. Both failed Mac attempts are preserved. Final
Linux8-test/fmt/strict controls use immutable306b26b production plus the explicit
cfg(test) overlay; candidate release behavior remains unchanged.

Linked mini plan: current—qualify effective startup batch limits and original-width
diagnostics (depends on public Core startup API; every original result, wide-ready
batch, maintenance/follower tests, fmt/strict0).
Next—independent Linux/macOS candidate asserted release artifacts (depends on frozen
source/toolchain/flags; original outcomes/maintenance/snapshot controls and hashes).
Following—quiet complete primary curves on both hosts (depends on qualified
artifacts/policy review; seven cells, at least three interleaved trials, exact
counts/p99/errors/raw reports, source/lock/artifact/hardware identities). The
comparison gate remains pending; no performance acceptance closes from these
functional controls alone.
# Elapsed diagnostic attribution

The optional `--attribution true` VoteBoat mode uses the same const-generic actor
and MemoryStore code as the default untimed mode. Fixed local counters record
calls, failures, items, elapsed totals and maxima; checked overflow fails the run.
There are no per-entry traces. Actor reports become available only after owned
shutdown/join, including campaign, warmup, follower drain and shutdown. Default
reports omit attribution; unavailable upstream phases are explicitly rejected.

Direct Core intervals exclude helper continuation. Persistence helpers include
their store append/barrier and Core continuation. Maintenance ticks include their
effects and any snapshot helper intervals. Store and snapshot subintervals remain
labeled as nested. Intervals are monotonic elapsed wall time; actor overlap and
descheduling prevent CPU or critical-path inference. No phase totals should be
summed as exclusive work. Existing original counts, request latencies and all
applied-frontier checks retain their independent meaning.
