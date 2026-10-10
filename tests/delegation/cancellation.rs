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

fn cfg() -> ConfigurationId {
    ConfigurationId::new(1).unwrap()
}
fn prepared(
    parent_operations: usize,
) -> (
    LifecycleDirectory,
    LifecycleDirectory,
    DelegationReservationStatus,
    TransferIntent,
) {
    let mut p = ready_directory(true, parent_operations);
    command(&mut p, 400, plan().encode(65536).unwrap());
    let s = reservation(&p);
    let intent = s.child_intent(cfg()).unwrap();
    (p, ready_directory(false, 10), s, intent)
}
fn restore(d: &LifecycleDirectory) -> LifecycleDirectory {
    let bytes = d.checkpoint(1000000).unwrap();
    let mut fresh = LifecycleDirectory::new(
        Directory::new(d.directory().plan().clone(), d.directory().limits()).unwrap(),
    );
    fresh
        .restore_checkpoint(d.schema_version(), d.applied_index(), &bytes)
        .unwrap();
    assert_eq!(fresh.checkpoint(1000000).unwrap(), bytes);
    fresh
}
fn declined(c: &LifecycleDirectory, operation: u128) -> Option<DelegationDeclineStatus> {
    let DirectoryRead::DelegationDecline(s) = c
        .read_at(
            c.applied_index(),
            DirectoryQuery::DelegationDecline(op(operation)),
        )
        .unwrap()
    else {
        panic!("decline query")
    };
    s
}
fn decline(c: &mut LifecycleDirectory, intent: &TransferIntent) -> DelegationDeclineStatus {
    let bytes = DelegationDecline::new(intent.clone())
        .unwrap()
        .encode(100000)
        .unwrap();
    assert_eq!(
        command(c, 500, bytes.clone()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    *c = restore(c);
    let s = declined(c, intent.delegation().unwrap().child_operation.get()).unwrap();
    assert!(command(c, 500, bytes).duplicate);
    assert_eq!(declined(c, 200).unwrap(), s);
    s
}
fn cancellation(
    s: &DelegationReservationStatus,
    decline: DelegationDeclineStatus,
) -> DelegationCancellation {
    DelegationCancellation {
        reservation: s.operation,
        reservation_index: s.index,
        parent_configuration: cfg(),
        child_configuration: cfg(),
        decline,
    }
}

fn verify_replanned_targets(
    next: &TransferIntent,
    targets: &mut [Target],
    decision: voteboat::transfer_publication::TransferPublicationStatus,
) {
    let activation = TargetActivation {
        metadata_configuration: cfg(),
        decision,
    };
    let cache = Cache(BTreeMap::from([(id(10), next.after().clone())]));
    for (t, (key, operation, value)) in targets.iter_mut().zip([(1, 1, 7), (200, 2, 11)]) {
        let bytes = t.activation_command(&activation, 65536).unwrap();
        command(t, 202, bytes);
        let hint = resolve(&cache, &base::Policy, id(10), &[key], 1).unwrap();
        let bytes = encode_routed(
            hint,
            &[key],
            &encode_add(&[key], value, b"effect", 1024).unwrap(),
            4096,
        )
        .unwrap();
        let TargetOutcome::Applied(r) = command(t, operation, bytes).outcome else {
            panic!("original retry")
        };
        assert!(r.duplicate);
        assert_eq!(r.outcome, BucketOutcome::Value(value));
    }
    let hint = resolve(&cache, &base::Policy, id(10), &[1], 1).unwrap();
    let bytes = encode_routed(
        hint,
        &[1],
        &encode_add(&[1], 2, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    let TargetOutcome::Applied(r) = command(&mut targets[0], 3, bytes).outcome else {
        panic!("new write")
    };
    assert!(!r.duplicate);
    assert_eq!(r.outcome, BucketOutcome::Value(9));
    let bytes = targets[0].checkpoint(300000).unwrap();
    let mut recovered = fresh_target_for(21, next, 202);
    recovered
        .restore_checkpoint(
            targets[0].schema_version(),
            targets[0].applied_index(),
            &bytes,
        )
        .unwrap();
    assert_eq!(
        recovered
            .read_at(
                recovered.applied_index(),
                TargetQuery::Data(RoutedQuery {
                    hint,
                    key: vec![1],
                    query: vec![1]
                })
            )
            .unwrap(),
        TargetRead::Data(9)
    );
}
#[test]
fn durable_decline_cancels_parent_and_fresh_transfer_preserves_real_data() {
    let (mut p, mut c, s, intent) = prepared(10);
    let refused = decline(&mut c, &intent);
    assert_eq!(
        command(&mut c, 200, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    assert!(matches!(
        c.read_at(c.applied_index(), DirectoryQuery::Transfer(op(200)))
            .unwrap(),
        DirectoryRead::Transfer(None)
    ));
    c = restore(&c);
    p = restore(&p);
    assert_eq!(
        p.directory().reserved_publication_bytes(),
        MAX_DIRECTORY_CONTROL_BYTES
    );
    let cancel = cancellation(&s, refused.clone());
    let bytes = cancel.encode(100000).unwrap();
    assert_eq!(
        command(&mut p, 401, bytes.clone()).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    let original = p
        .directory()
        .delegation_cancellation_at(p.applied_index(), op(400))
        .unwrap()
        .unwrap();
    p = restore(&p);
    assert!(command(&mut p, 401, bytes).duplicate);
    assert_eq!(
        p.directory()
            .delegation_cancellation_at(p.applied_index(), op(400))
            .unwrap()
            .unwrap(),
        original
    );
    assert_eq!(p.directory().manifest(id(500)), Some(&parent()));
    assert_eq!(p.directory().reserved_publication_bytes(), 0);

    let fresh_plan = DelegationPlan::new(parent(), before(), after(), op(202)).unwrap();
    assert_eq!(
        command(&mut p, 402, fresh_plan.encode(65536).unwrap()).outcome,
        DirectoryOutcome::DelegationReserved
    );
    let s = p
        .directory()
        .delegation_reservation_at(p.applied_index(), op(402))
        .unwrap()
        .unwrap();
    let next = s.child_intent(cfg()).unwrap();
    let (source, mut targets, decision) = completed_child_for(&next, &mut c, 202);
    let completion = DelegationCompletion {
        reservation: s.operation,
        reservation_index: s.index,
        parent_configuration: cfg(),
        child_configuration: cfg(),
        decision: decision.clone(),
    };
    assert_eq!(
        command(&mut p, 403, completion.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationPublished(RouteGeneration::new(2).unwrap())
    );
    verify_replanned_targets(&next, &mut targets, decision);
    assert!(matches!(
        source
            .read_at(
                source.applied_index(),
                voteboat::transfer_source::SourceQuery::Freeze
            )
            .unwrap(),
        voteboat::transfer_source::SourceRead::Freeze(Some(_))
    ));
    c = restore(&c);
    assert_eq!(declined(&c, 200).unwrap(), refused);
    assert_eq!(
        command(&mut c, 200, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    p = restore(&p);
    assert_eq!(
        p.directory()
            .delegation_cancellation_at(p.applied_index(), op(400))
            .unwrap()
            .unwrap(),
        original
    );
}

#[test]
fn ordered_child_intent_and_decline_races_never_revoke_a_successful_intent() {
    for intent_first in [true, false] {
        let (_, mut c, _, intent) = prepared(10);
        let decline = DelegationDecline::new(intent.clone())
            .unwrap()
            .encode(100000)
            .unwrap();
        let intent_bytes = intent.encode(65536).unwrap();
        let index = c.applied_index();
        let entries = if intent_first {
            vec![
                entry(index + 1, 200, intent_bytes.clone()),
                entry(index + 2, 500, decline.clone()),
            ]
        } else {
            vec![
                entry(index + 1, 500, decline.clone()),
                entry(index + 2, 200, intent_bytes.clone()),
            ]
        };
        let receipts = c.apply_batch(&entries).unwrap();
        c = restore(&c);
        if intent_first {
            assert_eq!(
                receipts[0].outcome,
                DirectoryOutcome::TransferIntentRecorded
            );
            assert_eq!(receipts[1].outcome, DirectoryOutcome::LifecycleBusy);
            assert!(declined(&c, 200).is_none());
            let (_, _, _) = completed_child(&intent, &mut c);
            assert_eq!(
                command(&mut c, 501, decline).outcome,
                DirectoryOutcome::LifecycleBusy
            );
            c = restore(&c);
            assert!(declined(&c, 200).is_none());
            assert!(matches!(
                c.read_at(c.applied_index(), DirectoryQuery::Transfer(op(200)))
                    .unwrap(),
                DirectoryRead::Transfer(Some(_))
            ));
        } else {
            assert_eq!(receipts[0].outcome, DirectoryOutcome::DelegationDeclined);
            assert_eq!(receipts[1].outcome, DirectoryOutcome::DelegationDeclined);
            assert!(declined(&c, 200).is_some());
            assert_eq!(
                command(&mut c, 200, intent_bytes).outcome,
                DirectoryOutcome::DelegationDeclined
            );
        }
    }
}

#[test]
fn parent_cancellation_keeps_reserved_credit_and_queries_account_nested_capacity() {
    let (mut p, mut c, s, intent) = prepared(3);
    let declined = decline(&mut c, &intent);
    let bytes = cancellation(&s, declined).encode(100000).unwrap();
    assert_eq!(p.directory().remaining_operations(), 0);
    p.validate_proposal(op(401), &bytes, std::iter::empty())
        .unwrap();
    assert!(p
        .validate_proposal(op(402), &bytes, [(op(401), bytes.as_slice())].into_iter())
        .is_err());
    assert_eq!(
        command(&mut p, 401, bytes).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    assert_eq!(p.directory().remaining_operations(), 0);
    assert_eq!(p.directory().reserved_publication_bytes(), 0);
    p = restore(&p);
    c = restore(&c);
    for (d, q) in [
        (&p, DirectoryQuery::DelegationCancellation(op(400))),
        (&c, DirectoryQuery::DelegationDecline(op(200))),
    ] {
        let result = d.read_at(d.applied_index(), q).unwrap();
        let nested = d.read_result_bytes(&result, usize::MAX).unwrap();
        assert!(nested + std::mem::size_of_val(&result) <= d.read_result_bound(&q).unwrap());
        if nested > 0 {
            assert!(d.read_result_bytes(&result, nested - 1).is_err());
        }
        assert!(d.read_at(d.applied_index() + 1, q).is_err());
        let bytes = d.checkpoint(1000000).unwrap();
        let mut fresh = LifecycleDirectory::new(
            Directory::new(d.directory().plan().clone(), d.directory().limits()).unwrap(),
        );
        let pristine = fresh.checkpoint(1000000).unwrap();
        for end in 0..bytes.len() {
            assert!(fresh
                .restore_checkpoint(d.schema_version(), d.applied_index(), &bytes[..end])
                .is_err());
            assert_eq!(fresh.checkpoint(1000000).unwrap(), pristine);
        }
    }
}

#[test]
fn same_group_cancellation_requires_actual_local_decline_and_exact_context() {
    let mut b = before().into_input();
    b.authority = group(100);
    let b = ResponsibilityManifest::new(b).unwrap();
    let mut a = after().into_input();
    a.authority = group(100);
    let a = ResponsibilityManifest::new(a).unwrap();
    let mut p = parent().into_input();
    p.execution = ExecutionMode::Delegated(vec![RouteEntry {
        scope: b.input().scope,
        target: RouteTarget::Child(ChildAuthority {
            responsibility: b.input().responsibility,
            group: group(100),
            epoch: b.input().epoch,
        }),
    }]);
    let p = ResponsibilityManifest::new(p).unwrap();
    let plan = DelegationPlan::new(p.clone(), b.clone(), a, op(200)).unwrap();
    let mut d = LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(100), vec![p.clone(), b.clone()]).unwrap(),
            DirectoryLimits {
                operations: 24,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    );
    let boot = d.directory().bootstrap_command(65536).unwrap();
    command(&mut d, 1000, boot);
    for (operation, manifest) in [(1001, p.clone()), (1002, b)] {
        command(
            &mut d,
            operation,
            DirectoryCommand {
                expected: None,
                manifest,
            }
            .encode(65536)
            .unwrap(),
        );
    }
    command(&mut d, 400, plan.encode(65536).unwrap());
    let s = reservation(&d);
    let intent = s.child_intent(cfg()).unwrap();
    let fake = DelegationDeclineStatus {
        operation: op(500),
        index: d.applied_index() + 1,
        decline: DelegationDecline::new(intent.clone()).unwrap(),
    };
    assert_eq!(
        command(&mut d, 401, cancellation(&s, fake).encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let mut wrong = s.clone();
    wrong.index += 1;
    let bytes = DelegationDecline::new(wrong.child_intent(cfg()).unwrap())
        .unwrap()
        .encode(100000)
        .unwrap();
    assert_eq!(
        command(&mut d, 502, bytes).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    let actual = decline(&mut d, &intent);
    let valid = cancellation(&s, actual.clone());
    for mutation in 0..5 {
        let mut invalid = valid.clone();
        match mutation {
            0 => invalid.reservation_index += 1,
            1 => invalid.child_configuration = ConfigurationId::new(2).unwrap(),
            2 => invalid.decline.index += 1,
            3 => invalid.decline.operation = op(501),
            _ => invalid.parent_configuration = ConfigurationId::new(2).unwrap(),
        }
        assert_eq!(
            command(&mut d, 600 + mutation, invalid.encode(100000).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
        assert_eq!(
            d.directory().reserved_publication_bytes(),
            MAX_DIRECTORY_CONTROL_BYTES
        );
    }
    assert_eq!(
        command(&mut d, 501, actual.decline.encode(100000).unwrap()).outcome,
        DirectoryOutcome::TransferEvidenceMismatch
    );
    assert_eq!(declined(&d, 200).unwrap(), actual);
    assert_eq!(
        command(&mut d, 499, valid.encode(100000).unwrap()).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    d = restore(&d);
    assert_eq!(d.directory().manifest(id(500)), Some(&p));
    assert_eq!(
        command(&mut d, 200, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
}

#[test]
fn failed_old_intent_can_be_declined_but_child_history_exhaustion_never_infers_refusal() {
    let (mut p, mut c, s, intent) = prepared(10);
    let mut newer = before().into_input();
    newer.generation = RouteGeneration::new(2).unwrap();
    let newer = ResponsibilityManifest::new(newer).unwrap();
    assert_eq!(
        command(
            &mut c,
            390,
            DirectoryCommand {
                expected: Some(RouteGeneration::new(1).unwrap()),
                manifest: newer.clone()
            }
            .encode(65536)
            .unwrap()
        )
        .outcome,
        DirectoryOutcome::Published(RouteGeneration::new(2).unwrap())
    );
    assert_eq!(
        command(&mut c, 200, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    let actual = decline(&mut c, &intent);
    assert_eq!(
        command(
            &mut p,
            401,
            cancellation(&s, actual).encode(100000).unwrap()
        )
        .outcome,
        DirectoryOutcome::DelegationCancelled
    );
    c = restore(&c);
    p = restore(&p);
    // Existing failed-operation deduplication retains its original refusal.
    assert_eq!(
        command(&mut c, 200, intent.encode(65536).unwrap()).outcome,
        DirectoryOutcome::GenerationMismatch
    );
    assert_eq!(c.directory().manifest(id(10)), Some(&newer));
    assert_eq!(p.directory().manifest(id(500)), Some(&parent()));

    let full = ready_directory(false, 2);
    let before = full.checkpoint(1000000).unwrap();
    let bytes = DelegationDecline::new(intent)
        .unwrap()
        .encode(100000)
        .unwrap();
    assert!(full
        .validate_proposal(op(500), &bytes, std::iter::empty())
        .is_err());
    assert!(declined(&full, 200).is_none());
    assert_eq!(full.checkpoint(1000000).unwrap(), before);
}

fn verify_decline_status_codec(status: &DelegationDeclineStatus) {
    let bytes = status.encode(MAX_DELEGATION_DECLINE_STATUS_BYTES).unwrap();
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(DelegationDeclineStatus::decode(&bytes).unwrap(), *status);
    assert!(status.encode(bytes.len() - 1).is_err());
    for end in 0..bytes.len() {
        assert!(DelegationDeclineStatus::decode(&bytes[..end]).is_err());
    }
    for index in [0, u64::MAX] {
        let mut invalid = status.clone();
        invalid.index = index;
        assert!(invalid.encode(100000).is_err());
        let mut altered = bytes.clone();
        altered[24..32].copy_from_slice(&index.to_le_bytes());
        assert!(DelegationDeclineStatus::decode(&altered).is_err());
    }
    let mut invalid = status.clone();
    invalid.operation = op(200);
    assert!(invalid.encode(100000).is_err());
    let mut altered = bytes.clone();
    altered[8..24].copy_from_slice(&200u128.to_le_bytes());
    assert!(DelegationDeclineStatus::decode(&altered).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(DelegationDeclineStatus::decode(&trailing).is_err());
    let mut length = bytes;
    length[32..36].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(DelegationDeclineStatus::decode(&length).is_err());
}
#[test]
fn decline_and_cancellation_codecs_are_bounded_and_reject_incomplete_provenance() {
    let mut p = ready_directory(true, 10);
    command(&mut p, 400, plan().encode(65536).unwrap());
    let reservation = reservation(&p);
    let cfg = ConfigurationId::new(1).unwrap();
    let intent = reservation.child_intent(cfg).unwrap();
    assert!(DelegationDecline::new(base::intent()).is_err());
    let decline = DelegationDecline::new(intent).unwrap();
    let bytes = decline.encode(MAX_DELEGATION_DECLINE_BYTES).unwrap();
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(DelegationDecline::decode(&bytes).unwrap(), decline);
    assert!(decline.encode(bytes.len() - 1).is_err());
    for end in 0..bytes.len() {
        assert!(DelegationDecline::decode(&bytes[..end]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(DelegationDecline::decode(&trailing).is_err());
    let mut length = bytes;
    length[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(DelegationDecline::decode(&length).is_err());

    // Pure codec fixture: constructing this value does not establish commitment.
    let status = DelegationDeclineStatus {
        operation: op(500),
        index: 17,
        decline,
    };
    verify_decline_status_codec(&status);

    let cancellation = DelegationCancellation {
        reservation: reservation.operation,
        reservation_index: reservation.index,
        parent_configuration: cfg,
        child_configuration: cfg,
        decline: status,
    };
    let bytes = cancellation.encode(MAX_DIRECTORY_CONTROL_BYTES).unwrap();
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(
        DelegationCancellation::decode(&bytes).unwrap(),
        cancellation
    );
    assert!(cancellation.encode(bytes.len() - 1).is_err());
    for end in 0..bytes.len() {
        assert!(DelegationCancellation::decode(&bytes[..end]).is_err());
    }
    for index in [0, u64::MAX] {
        let mut invalid = cancellation.clone();
        invalid.reservation_index = index;
        assert!(invalid.encode(100000).is_err());
        let mut altered = bytes.clone();
        altered[24..32].copy_from_slice(&index.to_le_bytes());
        assert!(DelegationCancellation::decode(&altered).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(DelegationCancellation::decode(&trailing).is_err());
    let mut length = bytes;
    length[48..52].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(DelegationCancellation::decode(&length).is_err());
}
