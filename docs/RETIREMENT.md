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

`TransferOperation::retirement_proof(&observations, source, release_id)` now
builds that proof from the complete original observation set. It requires all
original activations, selects the source's recorded fence and binds the explicit
release ID to it. The function performs no I/O and releases no data itself.
The [transfer executable](TRANSFER_SERVICE.md#retiring-the-original-source)
uses this same contract for its explicit fresh-store retirement profile.

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
are capped at 128 KiB. Legacy lineage uses the original activation/manifest
bound; selected partial owners declare their larger, finite history envelope
through `RetirableOwner::retirement_lineage_bound`. Readiness includes the larger control command/checkpoint requirements.
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
general retention registry, mixed-version
migration, macOS, separate-host or physical power-loss behavior.

Four `tests/routed/nested_merge_retirement.rs` histories extend that guarded
ancestry through501 split and601 merge into43. Each41/42 proof has its own exact
freeze/release; swapping sources refuses. The first unread retirement recovers,
then the whole fixture restarts with only41 retired and42 still fenced. Fresh
quorum metadata/43 activation observations authorize42 retirement. The merged
owner serves imported retries and a new write between retirements. Both original
activation lineages, freezes and exact retirement retries survive independently.

TCP/TLS and QUIC WAL/checkpoint paths cover unread retirement-tail replay for both
sources. Checkpoint cases physically reclaim both through verified retired bases
and exact tickets, then reopen without retained application commands. With all
ancestors/old sources offline,43 and sibling32 serve further writes with unchanged
stopped files and preserved imported operation IDs/outbox. Final source reopen
remains retired. This is the selected41-before42 order and application profile;
broader recursive lifecycle/fault coverage and external retention remain open.


Partial imported owners now retire after their complete remaining-data handoff.
Their VBTPRTL1 lineage retains the original activation and ordered completed
retained publications plus parent/slot observations. Recovery checks each exact
before/after grant, original scoped freeze ordering, command digest, control and
creation IDs, configured lifetime counts and the final full-transfer grant.
Application/import/export payloads are omitted and released with the live owner.
The underlying scope provider remains replaceable through the same contracts.

The guard uses the construction-time lineage bound for readiness, admission and
restore. Old owner defaults and formats are unchanged. Tests cover actual two-
partial-transfer then remaining relocation/split, explicit release, exact retired
retry, rejected altered histories and native-file interrupted snapshot publication,
retirement-tail replay, WAL reclamation and final retired reopen. Parent/slot
lineage replay uses supplied observations. TCP/QUIC composition of this new partial
retirement path remains155b3; wider fault/platform/retention coverage remains open.

Slice155b3 runs the partial-import retirement path over native TCP/TLS and QUIC,
with WAL and checkpoint recovery. Imported21 delegates to30/31, transfers its
remaining range to40, then retires from the actual final publication and target
activation observations with an explicit release. An unread retirement first
recovers through the previous live image and WAL tail. Checkpoint cases then
reclaim the retired WAL and reopen without application commands. Exact lineage,
freeze, refusal and retirement retry remain. Both children and40 serve their
imported data/retries after metadata and old owners stop. These are selected
Linux relocation histories, not arbitrary fault, platform or retention coverage.
