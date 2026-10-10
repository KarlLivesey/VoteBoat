// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super as support;
use std::{cell::Cell, io, path::Path, rc::Rc};
use voteboat::{
    application::*,
    contracts::*,
    log::*,
    native::{log_store::*, snapshot_store::*},
    retirement::*,
    snapshot::*,
};
fn entry(index: u64, id: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: voteboat::identity::OperationId::new(id).unwrap(),
            bytes,
        },
    }
}
fn noop(index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Noop,
    }
}
// Inject only at the actual native manifest publication boundary. These are
// interrupted publication/lost-completion checks, not machine power-loss tests.
struct InterruptedPublish {
    inner: FileSnapshotIo,
    fault: Rc<Cell<u8>>,
}
impl SnapshotIo for InterruptedPublish {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.inner.read_manifest()
    }
    fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
        self.inner.read_slot(slot, limit)
    }
    fn begin_slot(&mut self, slot: u8, prefix: &[u8]) -> io::Result<()> {
        self.inner.begin_slot(slot, prefix)
    }
    fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
        self.inner.append_slot(slot, bytes)
    }
    fn sync_slot(&mut self, slot: u8) -> io::Result<()> {
        self.inner.sync_slot(slot)
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let fault = self.fault.replace(0);
        if fault == 1 {
            return Err(io::Error::other("before retirement snapshot publication"));
        }
        self.inner.publish_manifest(bytes)?;
        if fault == 2 {
            return Err(io::Error::other(
                "lost retirement snapshot publication completion",
            ));
        }
        Ok(())
    }
}
fn install<L: LogStore, S: SnapshotRetention>(
    logs: &mut L,
    snapshots: &mut S,
    reference: SnapshotRef,
) {
    snapshots.pin_for_log(reference).unwrap();
    let state = logs.state(reference.group).unwrap();
    support::append(
        logs,
        vec![LogMutation::Update(LogUpdate {
            group: reference.group,
            expected_revision: state.revision,
            hard_state: state.hard_state,
            commit_index: state.commit_index,
            suffix: None,
            snapshot: Some(reference),
            snapshot_membership: None,
        })],
    );
    snapshots.reconcile_log(Some(reference)).unwrap();
}
struct PreparedRetirement {
    live: SnapshotRef,
    frozen: voteboat::transfer_source::SourceFreezeStatus,
    expected: RetirementStatus,
    lineage: Vec<u8>,
    retire_index: u64,
    metadata: SnapshotMetadata,
    bytes: Vec<u8>,
}

fn prepare_retirement<S: RetirableOwner>(
    fresh: &impl Fn() -> RetirementGuard<S>,
    prefix: &[LogEntry],
    proof: &RetirementProof,
    logs: &mut NativeLogStore<FileLogIo>,
    snapshots: &mut NativeSnapshotStore<InterruptedPublish>,
) -> PreparedRetirement
where
    S::Receipt: ApplicationReceipt,
{
    let group = proof.release.source;
    support::append(
        logs,
        vec![LogMutation::Create(support::bootstrap(group.id.get(), 1))],
    );
    let state = logs.state(group).unwrap();
    let freeze_index = prefix.last().unwrap().index;
    support::append(
        logs,
        vec![support::update(
            &state,
            1,
            freeze_index,
            Some(Suffix {
                from: 1,
                entries: prefix.to_vec(),
            }),
        )],
    );
    let mut owner = fresh();
    let (raft, replay) =
        recover_replica(support::node(1), group, logs, snapshots, &mut owner).unwrap();
    assert_eq!(replay.checkpoint_index, 0);
    let frozen = owner.freeze_status().unwrap().unwrap();
    let live = checkpoint_application(&raft, &owner, snapshots)
        .unwrap()
        .reference();
    install(logs, snapshots, live);
    let command = owner
        .retirement_command(proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    let retire_index = freeze_index + 1;
    let retirement = entry(retire_index, proof.release.operation.get(), command);
    let state = logs.state(group).unwrap();
    support::append(
        logs,
        vec![support::update(
            &state,
            1,
            retire_index,
            Some(Suffix {
                from: retire_index,
                entries: vec![retirement.clone()],
            }),
        )],
    );
    owner.apply_batch(&[retirement]).unwrap();
    let expected = owner.status().unwrap();
    let lineage = owner.retired_lineage().unwrap().to_vec();
    let bytes = owner.checkpoint(500000).unwrap();
    let mut restored = fresh();
    let (raft, _) =
        recover_replica(support::node(1), group, logs, snapshots, &mut restored).unwrap();
    let metadata = SnapshotMetadata {
        bootstrap: raft.state().bootstrap.clone(),
        membership: raft.state().checkpoint_membership(retire_index).unwrap(),
        index: retire_index,
        term: 1,
        application_schema: RETIREMENT_GUARD_SCHEMA,
    };
    PreparedRetirement {
        live,
        frozen,
        expected,
        lineage,
        retire_index,
        metadata,
        bytes,
    }
}

fn seal_retirement(
    snapshots: &mut NativeSnapshotStore<InterruptedPublish>,
    metadata: SnapshotMetadata,
    bytes: &[u8],
) -> SealedSnapshot {
    let ticket = snapshots.begin(metadata, bytes.len()).unwrap();
    for (chunk, data) in bytes.chunks(snapshots.limits().max_chunk_bytes).enumerate() {
        snapshots
            .write_chunk(ticket, chunk * snapshots.limits().max_chunk_bytes, data)
            .unwrap();
    }
    snapshots.seal(ticket).unwrap()
}

fn interrupt_publication(
    snapshots: &mut NativeSnapshotStore<InterruptedPublish>,
    sealed: SealedSnapshot,
    fault: &Cell<u8>,
    fault_mode: u8,
) {
    if fault_mode != 0 {
        fault.set(fault_mode);
        assert!(matches!(
            snapshots.publish(sealed),
            Err(StorageError::Uncertain(_))
        ));
        assert_eq!(snapshots.load(), Err(StorageError::Fenced));
    } // mode 0 drops a synchronized but unpublished snapshot.
}

fn verify_reclaimed<S: RetirableOwner>(
    path: &Path,
    identity: SnapshotIdentity,
    fresh: &impl Fn() -> RetirementGuard<S>,
    prepared: &PreparedRetirement,
    index: u64,
    proof: &RetirementProof,
) where
    S::Receipt: ApplicationReceipt,
{
    let group = proof.release.source;
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(path.join("snapshots")).unwrap(),
        identity,
        SnapshotLimits::default(),
    )
    .unwrap();
    let logs = NativeLogStore::recover(
        FileLogIo::open(path.join("logs")).unwrap(),
        support::identity(1),
        LogLimits::default(),
    )
    .unwrap();
    let mut owner = fresh();
    let (_, replay) =
        recover_replica(support::node(1), group, &logs, &mut snapshots, &mut owner).unwrap();
    assert_eq!(replay.checkpoint_index, index);
    assert!(replay.replay_receipts.is_empty());
    assert_eq!(owner.status(), Some(prepared.expected));
    assert_eq!(
        owner.freeze_status().unwrap(),
        Some(prepared.frozen.clone())
    );
    assert_eq!(owner.retired_lineage(), Some(prepared.lineage.as_slice()));
    assert!(owner.owner().is_none());
    assert!(owner
        .export_target(proof.decision.publication.targets()[0].group, 65536)
        .is_err());
    drop(logs);
    drop(snapshots);
}

pub fn history<S: RetirableOwner>(
    name: &str,
    fresh: impl Fn() -> RetirementGuard<S>,
    prefix: Vec<LogEntry>,
    proof: RetirementProof,
) where
    S::Receipt: ApplicationReceipt,
{
    for fault_mode in 0..=2 {
        let path = std::env::temp_dir().join(format!(
            "voteboat-retirement-{name}-{fault_mode}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let group = proof.release.source;
        let identity = SnapshotIdentity {
            store: support::identity(1),
            group,
        };
        let fault = Rc::new(Cell::new(0));
        let mut snapshots = NativeSnapshotStore::create(
            InterruptedPublish {
                inner: FileSnapshotIo::create(path.join("snapshots")).unwrap(),
                fault: fault.clone(),
            },
            identity,
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut logs = NativeLogStore::create(
            FileLogIo::create(path.join("logs")).unwrap(),
            support::identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let prepared = prepare_retirement(&fresh, &prefix, &proof, &mut logs, &mut snapshots);
        let PreparedRetirement {
            live,
            expected,
            frozen,
            lineage,
            retire_index,
            ..
        } = &prepared;
        let live = *live;
        let expected = *expected;
        let frozen = frozen.clone();
        let retire_index = *retire_index;
        let freeze_index = prefix.last().unwrap().index;
        let sealed = seal_retirement(&mut snapshots, prepared.metadata.clone(), &prepared.bytes);
        interrupt_publication(&mut snapshots, sealed, &fault, fault_mode);
        assert_eq!(logs.state(group).unwrap().snapshot, Some(live));
        drop(snapshots);
        drop(logs);
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            identity,
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut logs = NativeLogStore::recover(
            FileLogIo::open(path.join("logs")).unwrap(),
            support::identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let mut owner = fresh();
        let (_, replay) =
            recover_replica(support::node(1), group, &logs, &mut snapshots, &mut owner).unwrap();
        assert_eq!(replay.checkpoint_index, freeze_index);
        assert_eq!(replay.replay_receipts.len(), 1);
        assert_eq!(owner.status(), Some(expected));
        assert_eq!(owner.freeze_status().unwrap(), Some(frozen.clone()));
        assert!(owner.owner().is_none());
        assert_eq!(owner.retired_lineage(), Some(lineage.as_slice()));
        // Move beyond either possible root after an uncertain publication.
        let index = retire_index + 1;
        let state = logs.state(group).unwrap();
        support::append(
            &mut logs,
            vec![support::update(
                &state,
                1,
                index,
                Some(Suffix {
                    from: index,
                    entries: vec![noop(index)],
                }),
            )],
        );
        let mut owner = fresh();
        let (raft, _) =
            recover_replica(support::node(1), group, &logs, &mut snapshots, &mut owner).unwrap();
        let retired = checkpoint_application(&raft, &owner, &mut snapshots)
            .unwrap()
            .reference();
        install(&mut logs, &mut snapshots, retired);
        let report = logs.reclaim(logs.limits().max_wal_bytes).unwrap();
        assert!(report.after_bytes < report.before_bytes);
        assert!(logs.state(group).unwrap().entries.is_empty());
        assert!(snapshots.load_pinned(live).is_err());
        drop(logs);
        drop(snapshots);
        verify_reclaimed(&path, identity, &fresh, &prepared, index, &proof);
        std::fs::remove_dir_all(path).unwrap();
    }
}
