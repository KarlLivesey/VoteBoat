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
fn op(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn config(nodes: &[Node<LifecycleDirectory>], g: u128) -> ConfigurationId {
    nodes[0]
        .local()
        .owner
        .core(group(g))
        .unwrap()
        .state()
        .bootstrap
        .configuration
}
fn decline_status(rig: &mut Delegated) -> Option<DelegationDeclineStatus> {
    let DirectoryRead::DelegationDecline(s) = observe(
        &mut rig.child,
        &rig.clock,
        1,
        DirectoryQuery::DelegationDecline(op(200)),
    ) else {
        panic!("decline")
    };
    s
}
fn cancellation_status(rig: &mut Delegated) -> Option<DelegationCancellationStatus> {
    let DirectoryRead::DelegationCancellation(s) = observe(
        &mut rig.parent,
        &rig.clock,
        100,
        DirectoryQuery::DelegationCancellation(op(400)),
    ) else {
        panic!("cancellation")
    };
    s
}
fn reject_old(rig: &mut Delegated, intent: &TransferIntent) {
    campaign(&mut rig.child, &rig.clock, 1);
    assert_eq!(
        propose_recovering(
            &mut rig.child,
            &rig.clock,
            1,
            200,
            intent.encode(65536).unwrap()
        )
        .outcome,
        DirectoryOutcome::DelegationDeclined
    );
    assert_eq!(
        observe(
            &mut rig.child,
            &rig.clock,
            1,
            DirectoryQuery::Transfer(op(200))
        ),
        DirectoryRead::Transfer(None)
    );
}
fn cancelled_then_transferred(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Delegated::new(protocol, checkpoint);
    assert_eq!(rig.resume_one(), Phase::Reserve);
    let reserved = rig.observed();
    rig.restart();
    assert_eq!(rig.observed(), reserved);
    let reservation = reserved.reservation.as_ref().unwrap();
    let intent = reservation.child_intent(config(&rig.parent, 100)).unwrap();
    let decline_bytes = DelegationDecline::new(intent.clone())
        .unwrap()
        .encode(65536)
        .unwrap();
    campaign(&mut rig.child, &rig.clock, 1);
    assert_eq!(
        propose_recovering(&mut rig.child, &rig.clock, 1, 500, decline_bytes.clone()).outcome,
        DirectoryOutcome::DelegationDeclined
    );
    let declined = decline_status(&mut rig).unwrap();
    assert_eq!(declined.operation, op(500));
    rig.parent_outage(false);
    assert_eq!(cancellation_status(&mut rig), None);
    rig.restart();
    assert_eq!(decline_status(&mut rig), Some(declined.clone()));
    assert_eq!(rig.observed(), reserved);
    campaign(&mut rig.child, &rig.clock, 1);
    assert!(propose_recovering(&mut rig.child, &rig.clock, 1, 500, decline_bytes).duplicate);
    reject_old(&mut rig, &intent);
    let cancellation = DelegationCancellation {
        reservation: reservation.operation,
        reservation_index: reservation.index,
        parent_configuration: config(&rig.parent, 100),
        child_configuration: config(&rig.child, 1),
        decline: declined.clone(),
    };
    let cancel_bytes = cancellation.encode(65536).unwrap();
    campaign(&mut rig.parent, &rig.clock, 100);
    assert_eq!(
        propose_recovering(&mut rig.parent, &rig.clock, 100, 401, cancel_bytes.clone()).outcome,
        DirectoryOutcome::DelegationCancelled
    );
    let cancelled = cancellation_status(&mut rig).unwrap();
    assert_eq!(cancelled.operation, op(401));
    rig.restart();
    assert_eq!(cancellation_status(&mut rig), Some(cancelled.clone()));
    assert_eq!(decline_status(&mut rig), Some(declined.clone()));
    assert_eq!(rig.observed(), reserved);
    rig.check_service(&reserved);
    campaign(&mut rig.parent, &rig.clock, 100);
    assert!(propose_recovering(&mut rig.parent, &rig.clock, 100, 401, cancel_bytes).duplicate);
    reject_old(&mut rig, &intent);

    // A new operation uses the unchanged compatible source grant. No old target
    // was staged: both fresh targets bind the new actual reservation and intent.
    rig.transfer_operation = 202;
    rig.reservation_operation = 402;
    for phase in [
        Phase::Reserve,
        Phase::Intent,
        Phase::Stage(21),
        Phase::Stage(22),
        Phase::Fence,
        Phase::Import(21),
        Phase::Import(22),
        Phase::ChildPublication,
        Phase::ParentPublication,
        Phase::Activate(21),
        Phase::Activate(22),
    ] {
        eprintln!("cancellation {protocol:?} checkpoint={checkpoint} phase={phase:?}");
        assert_eq!(rig.resume_one(), phase);
        let original = rig.observed();
        rig.check_service(&original);
        if phase == Phase::Intent {
            // Committed intent wins even before any source fence exists.
            assert!(original.source.is_none());
            let next = original.intent.as_ref().unwrap().intent.clone();
            campaign(&mut rig.child, &rig.clock, 1);
            assert_eq!(
                propose_recovering(
                    &mut rig.child,
                    &rig.clock,
                    1,
                    502,
                    DelegationDecline::new(next).unwrap().encode(65536).unwrap()
                )
                .outcome,
                DirectoryOutcome::LifecycleBusy
            );
            assert_eq!(
                observe(
                    &mut rig.child,
                    &rig.clock,
                    1,
                    DirectoryQuery::DelegationDecline(op(202))
                ),
                DirectoryRead::DelegationDecline(None)
            );
        }
        if phase == Phase::ChildPublication {
            rig.parent_outage(true);
        }
        rig.restart();
        assert_eq!(rig.observed(), original, "reopen after {phase:?}");
        assert_eq!(decline_status(&mut rig), Some(declined.clone()));
        assert_eq!(cancellation_status(&mut rig), Some(cancelled.clone()));
        rig.check_service(&original);
    }
    assert_eq!(rig.resume_one(), Phase::Done);
    reject_old(&mut rig, &intent);
    // Original operation IDs survive actual image import; fresh writes follow
    // the existing target-only path after cancellation and movement.
    for (i, key, operation, value) in [(0, 1, 1, 7), (1, 200, 2, 11)] {
        let g = 21 + i as u128;
        campaign(&mut rig.targets[i], &rig.clock, g);
        let TargetOutcome::Applied(retry) = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            operation,
            target_data(key, value),
        )
        .outcome
        else {
            panic!("retry")
        };
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, BucketOutcome::Value(value));
        let TargetOutcome::Applied(write) = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            operation + 100,
            target_data(key, 2),
        )
        .outcome
        else {
            panic!("write")
        };
        assert_eq!(write.outcome, BucketOutcome::Value(value + 2));
    }
    rig.restart();
    assert_eq!(rig.resume_one(), Phase::Done);
    assert_eq!(decline_status(&mut rig), Some(declined));
    assert_eq!(cancellation_status(&mut rig), Some(cancelled));
    reject_old(&mut rig, &intent);
    for (i, key, value) in [(0, 1, 9), (1, 200, 13)] {
        assert_eq!(
            observe(
                &mut rig.targets[i],
                &rig.clock,
                21 + i as u128,
                target_query(key)
            ),
            TargetRead::Data(value)
        );
    }
    rig.close_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_cancelled_delegation_replans_and_recovers_from_wal() {
    cancelled_then_transferred(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_cancelled_delegation_replans_and_recovers_from_checkpoints() {
    cancelled_then_transferred(NativePeerProtocol::Quic, true);
}
