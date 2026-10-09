# Retiring transferred owners

`retirement::RetirementGuard<S>` wraps either `TransferSource` or `TransferTarget`
from initial construction. It implements the ordinary application, admission,
bounded read/receipt and checkpoint contracts, so it uses the same authoritative
group log, snapshot providers and native Node. Its sealed `RetirableOwner` gate
has only the core source/target implementations. Host applications remain
replaceable through the public scope/application interfaces beneath those guards.

Before retirement, ordinary commands and queries delegate to the live owner.
The host obtains authenticated quorum observations of the committed metadata
publication and every required target activation. It also explicitly releases
external backup/application recovery promises with a `RetentionRelease` naming
the exact source, lifecycle operation and freeze boundary F plus a release ID.
The guard does not discover external pins or infer their release from elapsed
time. A general retention registry remains future work.

Construct `RetirementProof` from that decision/configuration, ordered
`TargetActivationEvidence` values and release. `retirement_command(proof, limit)`
checks complete target coverage, exact stage/import commitments and activation
publication/configuration references against the actual local frozen source.
Foreign observation provenance is trusted authenticated host input; serialized
facts are not cryptographic quorum certificates.

Propose this command using the original source freeze operation ID. Only ordered
apply after matching durable quorum commitment at R can retire the owner.
Successful apply retains the fence, intent, source export commitments, complete
retirement decision and release, then drops the actual provider and import
payload buffers. A later-source target also retains its original activation
command, decision and first activation boundary as lineage. The immutable initial
configuration/template remains, so providers should put live data in applied or
imported state rather than in that initial configuration.

After retirement, `owner()` returns `None`, `export_target` refuses, and owner/data
queries return `RetirementRead::Retired`. Quorum reads of `Status` and `Freeze`
retain recovery facts. Exact command/operation retries return the original R;
other operations refuse admission, and already queued commands apply a fenced
outcome. No-op and refused-entry progress advances the contiguous wrapper prefix
without restoring service. Retired owners cannot thaw.

The guard's schema-1 `VBRET001` checkpoint binds its initial configuration digest,
applied prefix and either the live inner checkpoint or retired command/lineage.
`VBRETC01` commands and `VBRETP01` proofs are versioned and bounded; proof bytes
are capped at 128 KiB and lineage at the existing activation-command bound plus
12 bytes. Readiness includes the larger control command/checkpoint requirements.
Check command and snapshot capacity during assembly, before fencing a source.
Restore validates the initial profile, source manifest, phase boundaries and
lineage before replacing candidate state. Retired restore requires no old data.
There is no silent migration from an existing unwrapped deployment: installing
the wrapper changes the application checkpoint contract and needs explicit
migration. This does not establish mixed-version deployment support.

Application retirement releases memory, not durable replay. Publish and verify a
retired checkpoint, pin it, install its reference in the authoritative durable
log, and reconcile old snapshot pins before reclaiming the physical log. A sealed
snapshot or failed publication cannot authorize advancing that log boundary.
Recovery follows its durable pinned reference and replays the retained retirement
entry even when a newer snapshot publication lost its completion. Existing
snapshot/log checks prevent an old live checkpoint from independently reviving
an owner. Reclamation does not promise secure erasure of inactive files/backups.

`tests/retirement.rs` covers original and later sources, exact retries, refusal
and atomic failed batches, codec/checkpoint truncations, original activation
lineage and retained new-owner operations/outbox. Its injected host provider
checks actual live-resource release. Native-file tests for both source kinds
interrupt before/after manifest publication or abandon a sealed image, recover
from the old pinned snapshot plus retirement tail, then publish, reclaim and
reopen a retired checkpoint with no retained log commands.

Four `tests/routed/retirement.rs` histories use actual three-replica metadata,
source and two target groups over TCP/TLS or QUIC. All targets activate before
retirement; lost client completion is recovered through quorum status. WAL and
checkpoint/reclamation reopen preserve fences, and a target serves original
retries with the old source offline. This native network coverage retires the
original split source; later activated-source retirement currently has
deterministic and native-file evidence, extended by the assigned nested-source
network histories below. These finite Linux tests are not machine
power-loss, arbitrary-fault, macOS or separate-host proof. Recursive lifecycle,
general retention and broader P7 validation remain outstanding.

The nested insertion fixture now selects raw or guarded grandchild targets before
their first bootstrap. Four guarded network histories cover TCP/TLS and QUIC,
WAL and checkpoint recovery, ten unread lifecycle cuts and two unread activation
cuts per history. They retain actual creation bindings, imported operation IDs,
outbox state and original activation while serving with ancestors offline. This
is evidence for the live guard profile through nested insertion and recovery.
Those insertion histories do not retire targets; the later path below supplies
the ownership movement and exact retirement proof.

Four `tests/routed/nested_retirement.rs` histories now split actual assigned
grandchild31 into41/42, then retire31 using the complete quorum-observed
publication/activation facts and exact host retention release. Guards are selected
before bootstrap throughout this path. Missing-target and wrong-freeze proofs
refuse. The retirement completion stays unread through native owner abort/join;
reopen preserves the exact freeze, retirement result and original activation
lineage. Owner queries, exports and data admission refuse; exact retries recover R.

TCP/TLS and QUIC each exercise WAL replay and replay from an older live checkpoint
with retirement in its retained tail. The latter then publishes a verified retired
checkpoint, reclaims through exact Node request completions with reduced WAL bytes,
and reopens with no retained application commands. New owners41/42 and sibling32
serve imported retries/new writes and retain outbox state while metadata, ancestors
and retired31 are offline with unchanged durable files. Actual assigned31 creation
bindings remain identical. These selected Linux histories do not establish
cleanup of both later merge sources, general retention registry, mixed-version
migration, macOS, separate-host or physical power-loss behavior.
