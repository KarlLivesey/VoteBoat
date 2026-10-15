#476 opt-in two-sync native publication: checked Linux slice

WitnessLogIo implements the existing JournalIo contract plus its new compound
publish_barrier operation. The default compound method keeps sync_log followed
by publish_manifest; NativeLogStore creation, recovery and barrier dispatch to
it. FileLogIo source/default/format remain unchanged. The separate provider is
explicitly selected for fresh stores and their own reopen/reclamation, with no
implicit conversion. Codec frames, original tickets/outcomes, authentication,
quorum, ordering, fencing and snapshot dependencies remain authoritative.

Normal candidate publication writes fixed A in the WAL header, synchronizes WAL
plus data, writes fixed independent B, then synchronizes B before any Durable
result. B remains mandatory when A is a newer interrupted candidate; never choose
the maximum or an older historical marker. Sole-survivor repair, initialization
and parity reclamation are explicit cold paths. The before-edit parent SCHEMA
contains fields, transitions, ownership, failure/restart and acceptance.

Actual root core original21247 ends0 with exact1380 root library/integration
inputs: journal_barrier4, log_reclaim11, log_store19, provider_conformance42,
raft79, snapshot27 and witness_log_store15 (197 integration checks); selected
native log-store library35 (includes witness10) also passes. Public native-only
witness15 and focused strict pass independently. Internal witness controls cover
byte cuts of candidate headers/data/second witness, repair/restart, initialization
and reclamation/parity. Ordinal model48 ordinary/90 composed damage/136 repeated
restart states plus8 invalid pairs and explicit unsafe counterexamples is finite;
these are not physical device power-loss certification.

Root checked3234 ends0: formatter, full all-target/all-feature strict Clippy,
debug CLI build and metadata43contracts/411operations plus negative controls.
Root strict60487 ends0: default/core/native-only strict and separate matched
format/strict, reusing exact all-feature source proof. All1491 selected inputs
retain exact bytes/modes before/after. Five focused publication/observer controls,
strict example Clippy and JS timing5/profile2/offered11/storage9 pass. Later root
metadata references stronger public witness controls as partial evidence only.

Actual network2326 ends0: four atomic/witness x TCP/TLS+QUIC histories each
apply512/512 offers, recover576 including64 warmups, verify retries, join workers,
complete1checkpoint/3reclaims, and have zero refusals/unknown/pending. Six capacity
and six actual provider readbacks per case cover Create and Recover. Current
frozen validators accept storage and primitive publication data. Observed maximum
inflight atomicTCP130/QUIC64, witnessTCP53/QUIC60 is functional backlog evidence,
not achieved4096-wide proposals or a performance comparison. Seven original CLI
refusals precede resources. Original invalid-6 used a malformed lanes argument
order and rejected as unsupported protocol; a separate corrected --lanes-first
invocation verifies the intended branch. Both raw attempts remain; no successful
workload was repeated. These debug runs do not establish native release gains.

Two review-discovered reporting defects are fixed with actual-store controls:
explicit providers count original append-attempt encoded bytes; native snapshots
retain upper-layer and primitive error observations by saturating addition.
The storage_error_scope metadata explicitly declares overlap, not unique errors.
Explicit atomic/witness arms use identical native diagnostics and directly forward
the compound barrier. Primitive publication counters describe ordinary barriers;
cold repair, initialization selector and reclamation namespace work is excluded,
matching the existing FileLogIo replacement scope. Zero ordinary namespace calls
must not be presented as zero whole-process namespace work or summed CPU time.

Keep failures: initial provider ancestor-path compilation exit101 is preserved as
verbatim original tool-transcript copies; original disk stream and exact failed
source checkpoint were unavailable and are not claimed. A legacy-I/O-handle
assertion failed8/9 before correction to logical NativeLogStore recovery; its raw
log remains. Initial example strict103/100 failed, followed by a focused storage-
writer extraction without threshold changes. Root core probes initially used a
stale generated root library whose dep-info watched frozen matched454 rather
than current main. Original E0407 logs/dependency metadata remain. Targeted cargo
clean --package voteboat removed only generated root package outputs, preserving
all p9 artifacts/evidence/dependencies; new CargoJSON/vv and dep-info prove the
current root library fresh:false and current witness sources. Unchanged stale
cache hits are not reused as476 proof. All corrections and original diagnostics
are retained rather than silently relabeled passes.

Root and independent source reviews find no concrete safety/selection blocker;
finite tests are not arbitrary-provider certification. Binaries, build trees,
duplicate full source trees and generated WAL states remain under target. PACKING
and SHA256SUMS preserve original raw selected bytes losslessly. The enabled full
pre-push hook remains the publication gate; see the subsequent publication receipt.
Fresh published native artifacts and actual Mac qualification precede paired
performance. Btrfs/APFS proceed while real ext4/XFS scratch remains pending; no
default promotion/universal gain before all three environments pass. Full P9
seven-cell/both-host sustained framework, bounded memory/debt, durable headroom/
fairness/skew, placement/shared barriers and P8 performance remain incomplete.
