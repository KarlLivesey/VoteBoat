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
mod support;
use support::{snapshot::HostSnapshots, *};
use voteboat::contracts::StorageError;
use voteboat::{application::*, identity::*, log::*, membership::*, raft::*, snapshot::*};

struct FaultSnapshots {
    inner: HostSnapshots,
    fail: &'static str,
}
impl FaultSnapshots {
    fn check(&self, stage: &str) -> Result<(), StorageError> {
        if self.fail == stage {
            Err(StorageError::Uncertain(stage.into()))
        } else {
            Ok(())
        }
    }
}
impl SnapshotStore for FaultSnapshots {
    fn identity(&self) -> SnapshotIdentity {
        self.inner.identity()
    }
    fn binding(&self) -> StoreBinding {
        self.inner.binding()
    }
    fn limits(&self) -> SnapshotLimits {
        self.inner.limits()
    }
    fn begin(&mut self, m: SnapshotMetadata, n: usize) -> Result<SnapshotTicket, StorageError> {
        self.check("begin")?;
        self.inner.begin(m, n)
    }
    fn write_chunk(&mut self, t: SnapshotTicket, o: usize, b: &[u8]) -> Result<(), StorageError> {
        self.check("chunk")?;
        self.inner.write_chunk(t, o, b)
    }
    fn seal(&mut self, t: SnapshotTicket) -> Result<SealedSnapshot, StorageError> {
        self.check("seal")?;
        self.inner.seal(t)
    }
    fn publish(&mut self, t: SealedSnapshot) -> Result<SnapshotReceipt, StorageError> {
        self.check("publish")?;
        let r = self.inner.publish(t)?;
        self.check("published")?;
        Ok(r)
    }
    fn abort(&mut self, t: SnapshotTicket) -> Result<(), StorageError> {
        self.inner.abort(t)
    }
    fn load(&mut self) -> Result<Option<Snapshot>, StorageError> {
        self.inner.load()
    }
}
impl SnapshotRetention for FaultSnapshots {
    fn latest_reference(&self) -> Result<Option<SnapshotRef>, StorageError> {
        self.inner.latest_reference()
    }
    fn pin_for_log(&mut self, r: SnapshotRef) -> Result<(), StorageError> {
        self.check("pin")?;
        self.inner.pin_for_log(r)?;
        self.check("pinned")
    }
    fn load_pinned(&mut self, r: SnapshotRef) -> Result<Snapshot, StorageError> {
        self.inner.load_pinned(r)
    }
    fn reconcile_log(&mut self, r: Option<SnapshotRef>) -> Result<(), StorageError> {
        self.inner.reconcile_log(r)
    }
}

fn incoming() -> Snapshot {
    let bootstrap = bootstrap(1, 3);
    let assignment = LogEntry {
        index: 1,
        term: 2,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(90).unwrap(),
            expected: bootstrap.configuration,
            change: ConfigurationChange::Learners(
                Configuration::new(
                    ConfigurationId::new(2).unwrap(),
                    bootstrap.policy.clone(),
                    bootstrap.voter_stores.clone(),
                    [(node(4), identity(4))].into(),
                )
                .unwrap(),
            ),
        })),
    };
    let command = LogEntry {
        index: 2,
        term: 2,
        payload: EntryPayload::Command {
            operation: OperationId::new(91).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        },
    };
    let entries = vec![assignment, command];
    let mut app = Counter::new(100).unwrap();
    app.apply_batch(&entries).unwrap();
    Snapshot {
        metadata: SnapshotMetadata {
            membership: Some(Box::new(
                Membership::replay(&bootstrap, &entries, 2).unwrap(),
            )),
            bootstrap,
            index: 2,
            term: 2,
            application_schema: 1,
        },
        application: app.checkpoint(4096).unwrap(),
    }
}
fn snapshots() -> HostSnapshots {
    let mut snapshots = HostSnapshots::new();
    snapshots.identity.store = identity(4);
    snapshots.binding.identity = identity(4);
    snapshots
}
fn conformance<L: LogStore>(log: &mut L) {
    append(log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let mut snapshots = snapshots();
    let incoming = incoming();
    let mut app = Counter::new(100).unwrap();
    let mut core =
        enroll_learner_snapshot(node(4), log, &mut snapshots, &mut app, &incoming).unwrap();
    assert!(!core.local_voter());
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
    assert_eq!(app.read_applied(2).unwrap(), 7);
    let before = log.state(group(1)).unwrap();
    let mut retry = Counter::new(100).unwrap();
    enroll_learner_snapshot(node(4), log, &mut snapshots, &mut retry, &incoming).unwrap();
    assert_eq!(before, log.state(group(1)).unwrap());
    retry
        .apply_batch(&[LogEntry {
            index: 3,
            term: 2,
            payload: EntryPayload::Command {
                operation: OperationId::new(91).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        }])
        .unwrap();
    assert_eq!(retry.read_applied(3).unwrap(), 7);
    let mut different = incoming.clone();
    different.application[20] ^= 1;
    assert!(enroll_learner_snapshot(
        node(4),
        log,
        &mut snapshots,
        &mut Counter::new(100).unwrap(),
        &different
    )
    .is_err());
    assert_eq!(before, log.state(group(1)).unwrap());
    // An import retry cannot reset a replica that has begun protocol work.
    append(log, vec![update(&before, 3, 2, None)]);
    let progressed = log.state(group(1)).unwrap();
    assert!(enroll_learner_snapshot(
        node(4),
        log,
        &mut snapshots,
        &mut Counter::new(100).unwrap(),
        &incoming
    )
    .is_err());
    assert_eq!(progressed, log.state(group(1)).unwrap());
}
#[test]
fn host_contract_imports_exact_learner_and_retries_without_losing_deduplication() {
    conformance(&mut HostLogStore::new(4));
}
#[test]
fn snapshot_stage_publication_and_pin_failures_do_not_assign_the_local_replica() {
    for fail in [
        "begin",
        "chunk",
        "seal",
        "publish",
        "published",
        "pin",
        "pinned",
    ] {
        let mut log = HostLogStore::new(4);
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let before = log.state(group(1)).unwrap();
        let mut selected = FaultSnapshots {
            inner: snapshots(),
            fail,
        };
        let mut app = Counter::new(100).unwrap();
        assert!(
            enroll_learner_snapshot(node(4), &mut log, &mut selected, &mut app, &incoming())
                .is_err()
        );
        assert_eq!(app.applied_index(), 0);
        assert_eq!(log.state(group(1)).unwrap(), before);
        // Model recovery discards unpublished staging, preserving published
        // images and durable pins; actual native snapshot crash tests cover files.
        let mut recovered = if selected.inner.current.is_some() {
            selected.inner
        } else {
            snapshots()
        };
        enroll_learner_snapshot(
            node(4),
            &mut log,
            &mut recovered,
            &mut Counter::new(100).unwrap(),
            &incoming(),
        )
        .unwrap();
    }
}
#[test]
fn invalid_identity_schema_data_and_budget_do_not_mutate_storage() {
    for variant in 0..5 {
        let mut log = HostLogStore::new(4);
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let before = log.state(group(1)).unwrap();
        let mut snapshots = snapshots();
        let mut image = incoming();
        let mut local = node(4);
        match variant {
            0 => local = node(1),
            1 => snapshots.identity.store = identity(5),
            2 => image.metadata.application_schema = 2,
            3 => image.application.clear(),
            4 => snapshots.limits.max_application_bytes = 32,
            _ => unreachable!(),
        }
        assert!(enroll_learner_snapshot(
            local,
            &mut log,
            &mut snapshots,
            &mut Counter::new(100).unwrap(),
            &image
        )
        .is_err());
        assert_eq!(before, log.state(group(1)).unwrap());
        assert!(snapshots.current.is_none());
        assert!(snapshots.pins.is_empty());
    }
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::log_store::*;
    #[test]
    fn native_log_uses_the_same_import_contract() {
        let mut log =
            NativeLogStore::create(ModelIo::default(), identity(4), LogLimits::default()).unwrap();
        conformance(&mut log);
    }
    #[test]
    fn failed_log_transition_never_exposes_unpinned_or_partially_enrolled_state() {
        for fault in [
            Fault::Append(0),
            Fault::Append(64),
            Fault::Sync,
            Fault::PublishBefore,
            Fault::PublishAfter,
        ] {
            let io = ModelIo::default();
            let mut log =
                NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
            append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
            let mut snapshots = snapshots();
            io.0.borrow_mut().fault = fault;
            assert!(enroll_learner_snapshot(
                node(4),
                &mut log,
                &mut snapshots,
                &mut Counter::new(100).unwrap(),
                &incoming()
            )
            .is_err());
            drop(log);
            io.0.borrow_mut().power_loss();
            let mut log = NativeLogStore::recover(io, identity(4), LogLimits::default()).unwrap();
            let state = log.state(group(1)).unwrap();
            if state.snapshot.is_some() {
                assert_eq!(state.commit_index, 2);
                assert!(recover_member_replica(
                    node(4),
                    group(1),
                    &log,
                    &mut snapshots,
                    &mut Counter::new(100).unwrap()
                )
                .is_ok());
            } else {
                assert_eq!(state.commit_index, 0);
                assert!(Raft::recover_member(node(4), log.binding(), state, log.limits()).is_err());
            }
            enroll_learner_snapshot(
                node(4),
                &mut log,
                &mut snapshots,
                &mut Counter::new(100).unwrap(),
                &incoming(),
            )
            .unwrap();
        }
    }
}
