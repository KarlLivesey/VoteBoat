// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn manifest(n: &Boat) -> LocalDrainRequest {
    LocalDrainRequest {
        operation: OperationId::new(100).unwrap(),
        groups: n
            .local()
            .owner
            .groups()
            .map(|g| DrainGroup {
                group: g,
                configuration: n.local().owner.core(g).unwrap().membership().id(),
            })
            .collect(),
    }
}
#[test]
fn exact_manifest_validation_has_no_partial_side_effects() {
    let mut n = boat(parts(1, false));
    let original = manifest(&n);
    let mut missing = original.clone();
    missing.groups.pop();
    let mut reordered = original.clone();
    reordered.groups.reverse();
    let mut duplicate = original.clone();
    duplicate.groups[1] = duplicate.groups[0];
    let mut wrong_incarnation = original.clone();
    wrong_incarnation.groups[1].group.incarnation = GroupIncarnation::new(99).unwrap();
    for invalid in [missing, reordered, duplicate, wrong_incarnation] {
        assert_eq!(
            n.begin_local_drain(invalid),
            Err(LocalDrainError::AssignmentMismatch)
        );
        assert!(n.local_drain_status().is_none());
        assert!(n.local().owner.is_drained());
    }
    let mut stale = original.clone();
    stale.groups[1].configuration = ConfigurationId::new(99).unwrap();
    assert_eq!(
        n.begin_local_drain(stale),
        Err(LocalDrainError::UnstableConfiguration)
    );
    let mut oversized = original.clone();
    oversized.groups = vec![original.groups[0]; MAX_LOCAL_DRAIN_GROUPS + 1];
    assert_eq!(
        n.begin_local_drain(oversized),
        Err(LocalDrainError::TooManyGroups)
    );
    n.control(group(1), NodeControl::Campaign).unwrap();
    settle(&mut n); // Rejected drain did not close admission.
    n.propose(request(1)).unwrap();
    settle(&mut n);
    let output = n.poll_client().unwrap();
    n.complete_client(output).unwrap();
    shutdown(&mut n);
}
#[test]
fn gate_is_idempotent_and_disables_every_assigned_follower() {
    let mut n = boat(parts(1, false));
    let request = manifest(&n);
    n.begin_local_drain(request.clone()).unwrap();
    n.begin_local_drain(request.clone()).unwrap();
    assert!(!n.local_drain_status().unwrap().locally_quiescent);
    let mut conflicting = request;
    conflicting.operation = OperationId::new(101).unwrap();
    assert_eq!(
        n.begin_local_drain(conflicting),
        Err(LocalDrainError::ConflictingRequest)
    );
    settle(&mut n);
    let status = n.local_drain_status().unwrap();
    assert!(status.locally_quiescent);
    assert_eq!(status.groups.len(), 2);
    assert!(status
        .groups
        .iter()
        .all(|(_, s)| *s == DrainGroupState::LocallyQuiescent));
    assert!(n.local().owner.groups().all(|g| !n
        .local()
        .owner
        .core(g)
        .unwrap()
        .campaigning_enabled()));
    assert_eq!(
        n.control(group(1), NodeControl::Campaign),
        Err(NodeError::Draining)
    );
    shutdown(&mut n);
    assert!(!n.local_drain_status().unwrap().locally_quiescent);
    assert_eq!(
        n.begin_local_drain(manifest(&n)),
        Err(LocalDrainError::Closed)
    );
}
#[test]
fn ordinary_requests_keep_their_ownership_and_maintenance_stays_available() {
    let mut n = elected();
    n.begin_local_drain(manifest(&n)).unwrap();
    let command = request(1);
    let pointer = command.bytes.as_ptr();
    let rejected = n.propose(command).unwrap_err();
    assert_eq!(rejected.reason, ClientError::Draining);
    assert_eq!(rejected.request.bytes.as_ptr(), pointer);
    assert_eq!(
        n.read(group(1), ()).unwrap_err().reason,
        ReadInvocationError::Draining
    );
    n.propose_maintenance(rejected.request).unwrap();
    settle(&mut n);
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    n.read_maintenance(group(1), ()).unwrap();
    settle(&mut n);
    let output = n.poll_read().unwrap();
    assert!(matches!(
        n.complete_read(output).unwrap(),
        ReadOutcome::Read { result: Ok(7), .. }
    ));
    let status = n.local_drain_status().unwrap();
    assert!(!status.locally_quiescent);
    assert_eq!(status.groups[0].1, DrainGroupState::Leader);
    n.control(group(1), NodeControl::Heartbeat).unwrap();
    settle(&mut n);
    shutdown(&mut n);
}
#[test]
fn accepted_work_is_not_lost_when_admission_closes() {
    let mut n = elected();
    let ticket = n.propose(request(1)).unwrap();
    n.begin_local_drain(manifest(&n)).unwrap();
    settle(&mut n);
    assert_eq!(n.local_drain_status().unwrap().outstanding_requests, 1);
    let output = n.poll_client().unwrap();
    assert_eq!(output.ticket(), ticket);
    // Taking a completion does not release its resources; consuming it does.
    assert_eq!(n.local_drain_status().unwrap().outstanding_requests, 1);
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    assert_eq!(n.local_drain_status().unwrap().outstanding_requests, 0);
    shutdown(&mut n);
}

#[test]
fn accepted_read_is_retained_until_its_completion_is_consumed() {
    let mut n = elected();
    let ticket = n.read(group(1), ()).unwrap();
    n.begin_local_drain(manifest(&n)).unwrap();
    settle(&mut n);
    assert_eq!(n.local_drain_status().unwrap().outstanding_requests, 1);
    let output = n.poll_read().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert_eq!(n.local_drain_status().unwrap().outstanding_requests, 1);
    assert!(matches!(
        n.complete_read(output).unwrap(),
        ReadOutcome::Read { result: Ok(0), .. }
    ));
    assert_eq!(n.local_drain_status().unwrap().outstanding_requests, 0);
    shutdown(&mut n);
}

#[test]
fn queue_pressure_retries_suppression_without_failing_the_node() {
    let mut n = boat(parts(1, false));
    let mut full = false;
    for _ in 0..4096 {
        if let Err(reason) = n.control(group(1), NodeControl::Heartbeat) {
            assert_eq!(
                reason,
                NodeError::Owner(EffectOwnerError::Runtime(RuntimeError::Overloaded))
            );
            full = true;
            break;
        }
    }
    assert!(full);
    n.begin_local_drain(manifest(&n)).unwrap();
    settle(&mut n);
    assert!(n.local_drain_status().unwrap().locally_quiescent);
    assert_eq!(n.state(), NodeState::Running);
    shutdown(&mut n);
}

#[test]
fn membership_change_never_reuses_old_drain_configuration() {
    let mut n = elected();
    n.begin_local_drain(manifest(&n)).unwrap();
    n.configure(admin_request(1, 100, false)).unwrap();
    admin_settle(&mut n);
    let status = n.local_drain_status().unwrap();
    assert!(!status.locally_quiescent);
    assert_eq!(status.groups[0].1, DrainGroupState::ConfigurationChanged);
    n.poll_configuration().unwrap();
    shutdown(&mut n);
}
