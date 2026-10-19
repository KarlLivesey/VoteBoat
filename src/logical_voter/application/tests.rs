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
use super::*;
use crate::{
    contracts::HardState,
    quorum::{Limits as PolicyLimits, Policy, Tree},
    runtime::*,
    secure::PeerIdentity,
    wire::WireLimits,
    worker::*,
};
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn bootstrap(group: GroupIdentity, node: u64, store: StoreIdentity) -> Bootstrap {
    let node = NodeId::new(node).unwrap();
    Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), PolicyLimits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, store)]),
    }
}
fn app() -> LogicalApplication {
    app_with_limits(LogicalLimits::default())
}
fn app_with_limits(limits: LogicalLimits) -> LogicalApplication {
    LogicalApplication::new(
        LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2)).unwrap(),
        bootstrap(group(2), 2, store(2)),
        bootstrap(group(1), 1, store(1)),
        limits,
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
    )
    .unwrap()
}
fn representative(epoch: u64) -> RepresentativeBinding {
    RepresentativeBinding {
        epoch: RepresentativeEpoch::new(epoch).unwrap(),
        physical: PeerIdentity {
            node: NodeId::new(2).unwrap(),
            store: store(2),
        },
        physical_session: StoreSession::new(epoch).unwrap(),
        logical_session: StoreSession::new(epoch).unwrap(),
    }
}
/// A command at `id` within its partition: claim bodies use claim space
/// (`id` itself); every other body is sequence `id` of session `epoch` (the
/// test representative's logical session), acknowledging nothing.
fn command(id: u128, epoch: u64, body: CommandBody) -> Command {
    acknowledging(id, epoch, 0, body)
}
/// As `command`, acknowledging `consumed_through` earlier session outcomes.
fn acknowledging(id: u128, epoch: u64, consumed_through: u64, body: CommandBody) -> Command {
    let claim = matches!(
        body,
        CommandBody::Claim { .. } | CommandBody::EnrollmentClaim(_)
    );
    let operation = if claim {
        id
    } else {
        command::session_operation(epoch, u64::try_from(id).unwrap())
    };
    Command {
        operation: OperationId::new(operation).unwrap(),
        origin_configuration: ConfigurationId::new(1).unwrap(),
        representative: representative(epoch),
        consumed_through,
        body,
    }
}
fn entry(app: &LogicalApplication, c: &Command, index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: c.operation,
            bytes: app
                .encode_command(c, app.limits.max_command_bytes)
                .unwrap()
                .into(),
        },
    }
}
fn apply(app: &mut LogicalApplication, c: &Command) -> LogicalReceipt {
    let e = entry(app, c, app.applied_index() + 1);
    let before = app.checkpoint(usize::MAX).unwrap();
    let receipts = app.apply_batch(std::slice::from_ref(&e)).unwrap();
    formats::assert_restore_equivalent(app);
    formats::assert_step_replays(&before, &e, &receipts, app);
    receipts[0]
}
fn claim(app: &mut LogicalApplication) {
    assert_eq!(
        apply(
            app,
            &command(
                1,
                1,
                CommandBody::Claim {
                    expected_epoch: None
                }
            )
        )
        .outcome,
        LogicalOutcome::Claimed
    );
}
fn owner() -> RuntimeOwner {
    RuntimeOwner {
        store: StoreBinding {
            identity: store(1),
            session: StoreSession::new(1).unwrap(),
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    }
}
fn visit() -> VisitTicket {
    VisitTicket {
        owner: owner(),
        group: group(1),
        sequence: 1,
    }
}
fn origin() -> PersistOrigin {
    PersistOrigin {
        request: WorkerTicket {
            binding: WorkerBinding {
                store: owner().store,
                generation: StorageWorkerGeneration::new(1).unwrap(),
            },
            sequence: 1,
        },
        visit: visit(),
    }
}
fn update(app: &LogicalApplication, commit: u64, suffix: Option<Suffix>) -> LogUpdate {
    let p = app.state.parent.as_ref().unwrap();
    LogUpdate {
        group: group(1),
        expected_revision: p.revision,
        hard_state: HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: commit,
        suffix,
        snapshot: None,
        snapshot_membership: None,
    }
}
fn mutate(app: &LogicalApplication, id: u128, update: LogUpdate) -> Command {
    command(
        id,
        1,
        CommandBody::Mutate {
            expected_generation: app.state.parent.as_ref().unwrap().generation,
            update,
            origin: origin(),
        },
    )
}
#[test]
fn exact_retry_keeps_original_outcome_after_replacement() {
    let mut a = app();
    claim(&mut a);
    let old = command(
        1,
        1,
        CommandBody::Claim {
            expected_epoch: None,
        },
    );
    assert_eq!(
        apply(
            &mut a,
            &command(
                2,
                2,
                CommandBody::Claim {
                    expected_epoch: Some(representative(1).epoch)
                }
            )
        )
        .outcome,
        LogicalOutcome::Claimed
    );
    let r = apply(&mut a, &old);
    assert!(r.duplicate);
    assert_eq!(r.first_index, 1);
    assert_eq!(r.index, 3);
    assert_eq!(r.epoch, Some(representative(1).epoch));
    assert_eq!(a.state.representative, Some(representative(2)));
    // The claim closed session 1: an old-session command is `Expired`
    // (never executed, never recorded) instead of a recorded refusal.
    let old_mutation = mutate(&a, 3, update(&a, 0, None));
    let before = a.state.clone();
    let r = apply(&mut a, &old_mutation);
    assert_eq!(r.outcome, LogicalOutcome::Refused(LogicalRefusal::Expired));
    assert!(r.duplicate);
    assert_eq!(
        a.state,
        State {
            applied: before.applied + 1,
            ..before
        }
    );
    // A different binding inside the current session is still refused, and
    // recorded, as stale.
    let mut forged = command(
        3,
        2,
        CommandBody::Mutate {
            expected_generation: a.state.parent.as_ref().unwrap().generation,
            update: update(&a, 0, None),
            origin: origin(),
        },
    );
    forged.representative.physical_session = StoreSession::new(9).unwrap();
    assert_eq!(
        apply(&mut a, &forged).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    assert!(a.state.records.contains_key(&forged.operation));
}
#[test]
fn batch_conflict_and_malformed_suffix_are_atomic() {
    let mut a = app();
    claim(&mut a);
    let before = a.checkpoint(usize::MAX).unwrap();
    let good = entry(&a, &mutate(&a, 2, update(&a, 0, None)), 2);
    let bad = LogEntry {
        index: 3,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(3).unwrap(),
            bytes: vec![0].into(),
        },
    };
    assert_eq!(
        a.apply_batch(&[good, bad]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    let conflict = entry(
        &a,
        &command(
            1,
            2,
            CommandBody::Claim {
                expected_epoch: Some(representative(1).epoch),
            },
        ),
        2,
    );
    assert_eq!(
        a.apply_batch(&[conflict]),
        Err(ApplicationError::OperationConflict)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
}
#[test]
fn durable_term_vote_and_prefix_survive_checkpoint() {
    let mut a = app();
    claim(&mut a);
    let update = LogUpdate {
        hard_state: HardState {
            term: 1,
            voted_for: Some(NodeId::new(1).unwrap()),
        },
        ..update(
            &a,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Noop,
                }],
            }),
        )
    };
    let c = mutate(&a, 2, update);
    assert_eq!(apply(&mut a, &c).outcome, LogicalOutcome::Mutated);
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app();
    restored
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 2, &bytes)
        .unwrap();
    assert_eq!(restored.state, a.state);
    assert!(apply(&mut restored, &c).duplicate);
    assert_eq!(
        restored.state.parent.as_ref().unwrap().hard_state.voted_for,
        Some(NodeId::new(1).unwrap())
    );
    for cut in 0..bytes.len() {
        let before = restored.state.clone();
        assert!(restored
            .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 2, &bytes[..cut])
            .is_err());
        assert_eq!(restored.state, before);
    }
}
fn pending_conflict(a: &LogicalApplication, c: &Command, b: &[u8]) {
    let mut conflict = c.clone();
    conflict.origin_configuration = ConfigurationId::new(2).unwrap();
    let other = a.encode_command(&conflict, usize::MAX).unwrap();
    assert_eq!(
        a.validate_proposal(
            c.operation,
            b,
            std::iter::once((c.operation, other.as_slice()))
        ),
        Err(ApplicationError::OperationConflict)
    );
}
/// A retry of a retired action is answered `Expired`, never re-executed, and
/// reserves nothing however many copies are pending.
fn assert_retired_retry(a: &mut LogicalApplication, c: &Command, b: &[u8]) {
    let retry = apply(a, c);
    assert!(retry.duplicate);
    assert_eq!(
        retry.outcome,
        LogicalOutcome::Refused(LogicalRefusal::Expired)
    );
    assert!(a
        .validate_proposal(c.operation, b, std::iter::repeat_n((c.operation, b), 8))
        .is_ok());
}
/// Claims refused at the current epoch (2) stay retained: their copies
/// could not yet differ. They fill the table so the total count bounds
/// control actions too.
fn refuse_claims_at_current_epoch(a: &mut LogicalApplication, ids: std::ops::Range<u128>) {
    for id in ids {
        let refused = command(
            id,
            4,
            CommandBody::Claim {
                expected_epoch: Some(representative(2).epoch),
            },
        );
        assert_eq!(
            apply(a, &refused).outcome,
            LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
        );
    }
}
#[test]
fn control_reserve_and_pending_content_are_enforced() {
    let mut a = app_with_limits(LogicalLimits {
        max_actions: 4,
        control_actions: 3,
        ..LogicalLimits::default()
    });
    claim(&mut a);
    let c = mutate(
        &a,
        2,
        update(
            &a,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(101).unwrap(),
                        bytes: vec![1].into(),
                    },
                }],
            }),
        ),
    );
    let b = a.encode_command(&c, usize::MAX).unwrap();
    assert!(a
        .validate_proposal(c.operation, &b, std::iter::empty())
        .is_ok());
    pending_conflict(&a, &c, &b);
    assert_eq!(apply(&mut a, &c).outcome, LogicalOutcome::Mutated);
    let c2 = mutate(
        &a,
        3,
        update(
            &a,
            0,
            Some(Suffix {
                from: 2,
                entries: vec![LogEntry {
                    index: 2,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(102).unwrap(),
                        bytes: vec![2].into(),
                    },
                }],
            }),
        ),
    );
    let b = a.encode_command(&c2, usize::MAX).unwrap();
    assert_eq!(
        a.validate_proposal(c2.operation, &b, std::iter::empty()),
        Err(ApplicationError::DedupCapacity)
    );
    let control = mutate(&a, 4, update(&a, 1, None));
    let b = a.encode_command(&control, usize::MAX).unwrap();
    assert!(a
        .validate_proposal(control.operation, &b, std::iter::empty())
        .is_ok());
    assert_eq!(apply(&mut a, &control).outcome, LogicalOutcome::Mutated);
    assert_eq!(
        apply(
            &mut a,
            &command(
                5,
                2,
                CommandBody::Claim {
                    expected_epoch: Some(representative(1).epoch)
                }
            )
        )
        .outcome,
        LogicalOutcome::Claimed
    );
    // The new claim closed session 1: its records are retired.
    assert_eq!(a.state.records.len(), 1);
    assert_retired_retry(&mut a, &control, &b);
    refuse_claims_at_current_epoch(&mut a, 7..10);
    assert_eq!(a.state.records.len(), 4);
    let full = command(
        6,
        3,
        CommandBody::Claim {
            expected_epoch: Some(representative(2).epoch),
        },
    );
    let bytes = a.encode_command(&full, usize::MAX).unwrap();
    assert_eq!(
        a.validate_proposal(full.operation, &bytes, std::iter::empty()),
        Err(ApplicationError::DedupCapacity)
    );
}
#[test]
fn stale_read_capsule_cannot_release_after_new_write() {
    let mut a = app();
    claim(&mut a);
    let p = a.state.parent.as_ref().unwrap();
    let capsule = LogicalCapsule {
        expected_revision: p.revision,
        expected_generation: p.generation,
        local_configuration: ConfigurationId::new(1).unwrap(),
        origin: LogicalCapsuleOrigin::Protocol(EffectTicket {
            visit: visit(),
            sequence: 1,
        }),
        body: LogicalCapsuleBody::Protocol(crate::raft::Message {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            from: NodeId::new(1).unwrap(),
            sender: owner().store,
            to: NodeId::new(3).unwrap(),
            term: 1,
            context: crate::raft::RequestContext {
                origin: owner().store,
                sequence: 1,
            },
            rpc: crate::raft::Rpc::ReadAck,
        }),
    };
    let c = mutate(&a, 2, update(&a, 0, None));
    apply(&mut a, &c);
    assert_eq!(
        apply(
            &mut a,
            &command(3, 1, CommandBody::Authorize(capsule.clone()))
        )
        .outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleImage)
    );
    let p = a.state.parent.as_ref().unwrap();
    let mut fresh = capsule;
    fresh.expected_revision = p.revision;
    fresh.expected_generation = p.generation;
    assert_eq!(
        apply(&mut a, &command(4, 1, CommandBody::Authorize(fresh))).outcome,
        LogicalOutcome::CapsuleAuthorized
    );
}

mod authority;
mod control_profile;
mod eligibility;
mod enrollment;
mod formats;
mod reads;
mod retirement;
mod snapshots;

#[test]
fn applied_boundary_terms_are_monotonic_and_restored_exactly() {
    let mut a = app();
    a.apply_batch(&[LogEntry {
        index: 1,
        term: 3,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app();
    restored
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 1, &bytes)
        .unwrap();
    assert_eq!(restored.state.applied_term, 3);
    let c = command(
        1,
        1,
        CommandBody::Claim {
            expected_epoch: None,
        },
    );
    let e = entry(&restored, &c, 2);
    assert_eq!(
        restored.apply_batch(&[e]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), bytes);
    let mut e = entry(&restored, &c, 2);
    e.term = 3;
    assert_eq!(
        restored.apply_batch(&[e]).unwrap()[0].outcome,
        LogicalOutcome::Claimed
    );
    let good = restored.checkpoint(usize::MAX).unwrap();
    let mut bad = good.clone();
    let offset = 8 + 8 + restored.configuration.len() + 8;
    bad[offset..offset + 8].copy_from_slice(&2u64.to_le_bytes());
    assert_eq!(
        restored.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 2, &bad),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), good);
}
