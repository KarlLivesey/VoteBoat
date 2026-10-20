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
//! DR1 round 7 (Sol, round 6): positive readiness receipts for replicas that
//! then disagree on an admitted entry. A family whose bootstrap entry binds
//! inputs outside the identity gets no receipt until that entry has applied
//! (`BootstrapPending`); a transaction participant's opening image is part of
//! its plan, so another image is refused (`Capability`). Each test first
//! shows the divergence, through the real learner verifier and leader.
use super::*;
use transfer_fixtures::{grant_for, Policy};
use voteboat::{
    directory::*,
    program_transaction::*,
    routed::*,
    routing::*,
    transaction::{
        AccountId, AccountRef, PhaseIds, PrepareOutcome, PrepareRefusal, TransactionId,
        TransferApplication, TransferLimits, TransferOutcome, TransferProfile, TransferSpec,
    },
};

fn op(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
/// Group 1's ordinary fixture grant at ownership epoch `epoch`.
fn grant_at(local: GroupIdentity, epoch: u64) -> ResponsibilityManifest {
    ResponsibilityManifest::new(ManifestInput {
        epoch: OwnershipEpoch::new(epoch).unwrap(),
        ..grant_for(local).into_input()
    })
    .unwrap()
}
fn routed_counter(epoch: u64) -> RoutedApplication<Counter, Policy> {
    RoutedApplication::new(
        group(1),
        grant_at(group(1), epoch),
        Counter::new(4).unwrap(),
        Policy,
        RoutedLimits {
            operations: 4,
            semantic_bytes: 4096,
            payload_bytes: 128,
            inner_checkpoint_bytes: 4096,
        },
    )
    .unwrap_or_else(|r| panic!("{:?}", r.error))
}
fn directory_at(epoch: u64) -> Directory {
    Directory::new(
        DirectoryPlan::new(group(1), vec![grant_at(group(20), epoch)]).unwrap(),
        DirectoryLimits {
            operations: 8,
            history_bytes: 16 * 1024,
        },
    )
    .unwrap()
}

/// A host log whose command limit covers a bare Directory's control
/// envelope (64 KiB + 128; the fixture default is 64 KiB). Everything else is
/// the ordinary host store.
struct Wide(HostLogStore);
impl LogStore for Wide {
    fn binding(&self) -> StoreBinding {
        self.0.binding()
    }
    fn limits(&self) -> LogLimits {
        LogLimits {
            max_command_bytes: 128 * 1024,
            ..self.0.limits()
        }
    }
    fn state(&self, group: GroupIdentity) -> Result<GroupLog, voteboat::contracts::StorageError> {
        self.0.state(group)
    }
    fn append_batch(
        &mut self,
        mutations: Vec<LogMutation>,
    ) -> Result<Vec<LogTicket>, voteboat::contracts::StorageError> {
        self.0.append_batch(mutations)
    }
    fn barrier(
        &mut self,
        dependencies: &[LogTicket],
    ) -> Result<DurableLog, voteboat::contracts::StorageError> {
        self.0.barrier(dependencies)
    }
    fn fetch_range(
        &self,
        group: GroupIdentity,
        generation: LogGeneration,
        from: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>, voteboat::contracts::StorageError> {
        self.0
            .fetch_range(group, generation, from, max_entries, max_bytes)
    }
}

/// Sol's round-6 sequence for two replicas with equal requirements whose
/// bootstrap bindings differ: both caught up on the configuration/Noop
/// prefix, both refused (`BootstrapPending`) before any bootstrap; `two`'s
/// admitted bootstrap commits at index 3 by actual replication; `one` fails
/// it and `two` gets a receipt the leader accepts and promotion rechecks.
/// Returns `two`'s receipts for the bootstrap entry.
fn bootstrap_receipt_gap<A: CheckpointStateMachine>(
    mut one: A,
    mut two: A,
    bootstrap: Vec<u8>,
) -> Vec<A::Receipt> {
    let declared = two.deployment_requirements().unwrap();
    assert_eq!(one.deployment_requirements().unwrap(), declared);
    let (mut log, mut leader_log) = (Wide(HostLogStore::new(4)), Wide(HostLogStore::new(1)));
    let (mut leader, mut learner, _) = readiness_cluster_with_stores(&mut log, &mut leader_log);
    let mut snapshots = snapshots();
    for app in [&mut one, &mut two] {
        app.apply_batch(learner.replay_committed()).unwrap();
        assert_eq!(app.applied_index(), 2);
        assert!(!app.deployment_bound());
    }
    // a02f5734 gave both a receipt here; the leader accepted it.
    let request = leader
        .begin_learner_readiness(learner_peer(), log.binding().session, declared)
        .unwrap();
    for app in [&one, &two] {
        assert_eq!(
            verify_learner_readiness(
                &learner,
                app,
                &log,
                &mut snapshots,
                request,
                leader.storage_binding()
            ),
            Err(ReadinessError::BootstrapPending)
        );
    }
    leader.cancel_learner_readiness();

    let index = commit_command(
        &mut leader,
        &mut leader_log,
        &mut learner,
        &mut log,
        op(991),
        bootstrap,
    );
    assert_eq!(index, 3);
    let entry = &learner.replay_committed()[2..];
    assert_eq!(entry.len(), 1);
    let receipts = two.apply_batch(entry).unwrap();
    assert_eq!(
        one.apply_batch(entry).map(|_| ()),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!((one.applied_index(), two.applied_index()), (2, 3));
    assert!(two.deployment_bound());
    assert!(!one.deployment_bound());

    let request = leader
        .begin_learner_readiness(learner_peer(), log.binding().session, declared)
        .unwrap();
    assert_eq!(request.index, 3);
    let receipt = verify_learner_readiness(
        &learner,
        &two,
        &log,
        &mut snapshots,
        request,
        leader.storage_binding(),
    )
    .unwrap();
    let ready = leader
        .accept_learner_readiness(receipt, log.binding())
        .unwrap();
    assert_eq!(
        leader.check_learner_readiness(&ready, log.binding()),
        Ok(())
    );
    // The replica that failed the entry never covers the prefix.
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &one,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::NotCaughtUp)
    );
    receipts
}

#[test]
fn rr6_routed_bootstrap_receipt_gap() {
    // Sol, round 6: grant epochs 1 and 2, otherwise identical; the whole
    // requirements compare equal (the grant is the bootstrap's).
    let (one, two) = (routed_counter(1), routed_counter(2));
    let bootstrap = two.bootstrap_command(65536).unwrap();
    let receipts = bootstrap_receipt_gap(one, two, bootstrap);
    assert!(matches!(
        receipts.as_slice(),
        [RoutedReceipt {
            index: 3,
            outcome: RoutedOutcome::Bootstrapped,
            ..
        }]
    ));
}

#[test]
fn rr6_directory_bootstrap_receipt_gap() {
    // The Directory analogue: plans whose one manifest differs in its epoch.
    let (one, two) = (directory_at(1), directory_at(2));
    let bootstrap = two.bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES).unwrap();
    let receipts = bootstrap_receipt_gap(one, two, bootstrap);
    assert!(matches!(
        receipts.as_slice(),
        [DirectoryReceipt {
            index: 3,
            outcome: DirectoryOutcome::Initialized,
            ..
        }]
    ));
}

#[test]
fn bootstrap_binding_is_propagated_through_wrappers() {
    use voteboat::maintenance::Maintenance;
    // A wrapper is bound only when its inner Routed layer is.
    let wrapped = Maintenance::new(group(1), 3, 4, routed_counter(1)).unwrap();
    assert!(!wrapped.deployment_bound());
    let mut bound = wrapped.clone();
    let bytes = Maintenance::<RoutedApplication<Counter, Policy>>::data(
        &routed_counter(1).bootstrap_command(65536).unwrap(),
        65536,
    )
    .unwrap();
    bound
        .apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: op(991),
                bytes: bytes.into(),
            },
        }])
        .unwrap();
    assert!(bound.deployment_bound());
    // Applications with no binding entry are bound from construction.
    assert!(Counter::new(4).unwrap().deployment_bound());
}

fn participant_profile() -> TransferProfile {
    TransferProfile::new(group(3), [group(1), group(2)]).unwrap()
}
fn participant(openings: &[(u64, i64)]) -> TransferApplication {
    let accounts: Vec<_> = openings
        .iter()
        .map(|&(id, opening)| (AccountId::new(id).unwrap(), opening))
        .collect();
    TransferApplication::participant(
        group(1),
        participant_profile(),
        &accounts,
        TransferLimits::new(2, accounts.len().max(2)).unwrap(),
    )
    .unwrap()
}
/// The canonical Prepare for transaction 1 at group 1, from the public
/// `start` encoding with the format's step byte (offset 8) set to Prepare.
fn prepare(index: u64) -> LogEntry {
    let phases = PhaseIds::new(
        [op(101), op(102), op(103), op(104)],
        [[op(101), op(102), op(103)]; 2],
    )
    .unwrap();
    let spec = TransferSpec::new(
        TransactionId::new(1).unwrap(),
        participant_profile(),
        AccountRef::new(group(1), AccountId::new(1).unwrap()),
        AccountRef::new(group(2), AccountId::new(2).unwrap()),
        5,
        phases,
    )
    .unwrap();
    let mut bytes = TransferApplication::start(spec).unwrap();
    assert_eq!(bytes[8], 0);
    bytes[8] = 1;
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: op(101),
            bytes: bytes.into(),
        },
    }
}

#[test]
fn rr6_participant_initial_image_receipt_gap() {
    // Sol, round 6: account 1 opening 4 vs 10, otherwise identical; the
    // declarations compared equal and the same admitted Prepare committed
    // different outcomes on both.
    let (poor, rich) = (|| participant(&[(1, 4)]), || participant(&[(1, 10)]));
    let (mut on_poor, mut on_rich) = (participant(&[(1, 4)]), participant(&[(1, 10)]));
    let mut log = HostLogStore::new(4);
    let (_, learner, _, _) = readiness_cluster(&mut log);
    for app in [&mut on_poor, &mut on_rich] {
        app.apply_batch(learner.replay_committed()).unwrap();
    }
    let entry = prepare(3);
    assert_eq!(
        on_poor.apply_batch(std::slice::from_ref(&entry)).unwrap()[0].outcome,
        TransferOutcome::Prepare(PrepareOutcome::Refused(PrepareRefusal::InsufficientFunds))
    );
    assert_eq!(
        on_rich.apply_batch(std::slice::from_ref(&entry)).unwrap()[0].outcome,
        TransferOutcome::Prepare(PrepareOutcome::Prepared {
            before: 10,
            after: 5
        })
    );
    assert_eq!((on_poor.applied_index(), on_rich.applied_index()), (3, 3));

    // The opening image is now the plan's tail: account id, opening.
    let (dp, dr) = (
        poor().deployment_requirements().unwrap(),
        rich().deployment_requirements().unwrap(),
    );
    assert_eq!(
        (dp.identity, dp.command_bytes),
        (dr.identity, dr.command_bytes)
    );
    assert_eq!(&dp.inner_profile.plan().segment(0).unwrap()[12..], &[1, 4]);
    assert_eq!(&dr.inner_profile.plan().segment(0).unwrap()[12..], &[1, 10]);
    // Refused both ways through the real verifier; each own image accepted.
    assert_learners_refuse_each_other(poor, rich, |r| r);
}

#[test]
fn participant_images_within_the_plan_bound_get_receipts_and_larger_ones_are_refused() {
    // 1 header + 12 group words + 2 per account: 89 accounts fill 191 words.
    let image =
        |n: u64| move || participant(&(1..=n).map(|id| (id, id as i64)).collect::<Vec<_>>());
    let full = image(89)().deployment_requirements().unwrap();
    assert_eq!(full.inner_profile.plan().words().len(), 1 + 12 + 2 * 89);
    assert!(learner_verdict(full, image(89)).is_ok());
    // A one-account image: within budget, accepted.
    let one = image(1)().deployment_requirements().unwrap();
    assert!(learner_verdict(one, image(1)).is_ok());
    // 90 accounts do not fit: the profile overflows and readiness refuses
    // even the replica's own declaration (never truncated).
    let over = image(90)().deployment_requirements().unwrap();
    assert_eq!(over.inner_profile.layers(), None);
    assert_eq!(
        image(90)().validate_deployment_requirements(over),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(
        learner_verdict(over, image(90)),
        Err(ReadinessError::Capability)
    );
    // The largest image the limits allow overflows the same way.
    let largest = image(4096)().deployment_requirements().unwrap();
    assert_eq!(largest.inner_profile.layers(), None);
    // A coordinator has no image; its plan is the 12 group words.
    let coordinator = TransferApplication::coordinator(
        group(3),
        participant_profile(),
        TransferLimits::new(2, 2).unwrap(),
    )
    .unwrap();
    assert_eq!(
        coordinator
            .deployment_requirements()
            .unwrap()
            .inner_profile
            .plan()
            .segment(0)
            .unwrap()
            .len(),
        12
    );
}

#[test]
fn program_participant_opening_keys_are_compared_exactly() {
    // The Program analogue: key 1 opening 4 vs 10, otherwise identical.
    let profile = || ProgramProfile::new(1, vec![group(2)], vec![group(1), group(3)]).unwrap();
    let with = |opening: i64| {
        move || {
            ProgramApplication::participant(
                group(1),
                profile(),
                &[
                    (KeyId::new(1).unwrap(), opening),
                    (KeyId::new(2).unwrap(), 20),
                ],
                ProgramLimits::new(2, 2).unwrap(),
            )
            .unwrap()
        }
    };
    let (dp, dr) = (
        with(4)().deployment_requirements().unwrap(),
        with(10)().deployment_requirements().unwrap(),
    );
    // Group 3, version and counts 3, three catalog groups 9; then the keys.
    let tail =
        |d: &ReadinessRequirements| d.inner_profile.plan().segment(0).unwrap()[15..].to_vec();
    assert_eq!(tail(&dp), [1, 4, 2, 20]);
    assert_eq!(tail(&dr), [1, 10, 2, 20]);
    assert_learners_refuse_each_other(with(4), with(10), |r| r);
}
