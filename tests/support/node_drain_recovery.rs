// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{drain::*, secure::PeerIdentity};
struct Journal {
    owner: PeerIdentity,
    latest: Option<DrainRecord>,
}
impl DrainJournal for Journal {
    fn owner(&self) -> PeerIdentity {
        self.owner
    }
    fn latest(&self) -> Result<Option<DrainRecord>, DrainJournalError> {
        Ok(self.latest.clone())
    }
    fn publish(&mut self, record: DrainRecord) -> Result<(), DrainJournalError> {
        record.validate()?;
        if record.owner != self.owner {
            return Err(DrainJournalError::WrongOwner);
        }
        if self.latest.is_none() && record.phase != DrainPhase::Active {
            return Err(DrainJournalError::Conflict);
        }
        if self.latest.as_ref().is_some_and(|r| !record.follows(r)) {
            return Err(DrainJournalError::Conflict);
        }
        self.latest = Some(record);
        Ok(())
    }
}
fn journal(n: &Boat) -> Journal {
    let owner = PeerIdentity {
        node: node(1),
        store: n.local().owner.identity().store.identity,
    };
    Journal {
        owner,
        latest: Some(DrainRecord {
            owner,
            sequence: 1,
            request: LocalDrainRequest {
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
            },
            phase: DrainPhase::Active,
            plan: None,
        }),
    }
}

fn planned(n: &Boat, all_groups: bool) -> MembershipDrainPlan {
    use voteboat::{membership::*, placement::*, quorum::*};
    let entries = n
        .local()
        .owner
        .groups()
        .take(if all_groups { usize::MAX } else { 1 })
        .map(|group| {
            let original = n
                .local()
                .owner
                .core(group)
                .unwrap()
                .membership()
                .stable()
                .clone();
            let target = Configuration::new(
                ConfigurationId::new(3).unwrap(),
                Policy::new(
                    Tree::Majority(vec![Tree::Voter(node(2)), Tree::Voter(node(3))]),
                    Limits::default(),
                )
                .unwrap(),
                [(node(2), identity(2)), (node(3), identity(3))].into(),
                [(node(1), identity(1))].into(),
            )
            .unwrap();
            DrainMembershipGroup {
                group,
                original,
                handoff: PeerIdentity {
                    node: node(2),
                    store: identity(2),
                },
                change: PlannedVoterChange {
                    joint: ConfigurationRecord {
                        operation: OperationId::new(200).unwrap(),
                        expected: ConfigurationId::new(1).unwrap(),
                        change: ConfigurationChange::Joint {
                            id: ConfigurationId::new(2).unwrap(),
                            next: target,
                        },
                    },
                    finalize: ConfigurationRecord {
                        operation: OperationId::new(200).unwrap(),
                        expected: ConfigurationId::new(2).unwrap(),
                        change: ConfigurationChange::Final {
                            id: ConfigurationId::new(3).unwrap(),
                        },
                    },
                },
            }
        })
        .collect();
    MembershipDrainPlan::new(journal(n).owner, OperationId::new(201).unwrap(), entries).unwrap()
}

#[test]
fn planned_membership_drain_requires_restored_complete_assignments_and_final_commits() {
    let mut n = boat(parts(3, true));
    let plan = planned(&n, true);
    let mut j = Journal {
        owner: journal(&n).owner,
        latest: Some(plan.record(1).unwrap()),
    };
    assert_eq!(
        n.membership_drain_ready(&plan, &j),
        Err(DrainPlanError::Unrestored)
    );
    n.restore_drain(&j).unwrap();
    settle(&mut n);
    assert!(!n.membership_drain_ready(&plan, &j).unwrap());
    assert!(n.local().owner.groups().all(|g| !n
        .local()
        .owner
        .core(g)
        .unwrap()
        .campaigning_enabled()));
    let partial = planned(&n, false);
    assert_eq!(
        n.membership_drain_ready(&partial, &j),
        Err(DrainPlanError::WrongIntent)
    );
    let mut other = boat(parts(3, true));
    let partial_journal = Journal {
        owner: j.owner,
        latest: Some(partial.record(1).unwrap()),
    };
    other.restore_drain(&partial_journal).unwrap();
    settle(&mut other);
    assert!(!other
        .membership_drain_ready(&partial, &partial_journal)
        .unwrap());
    j.latest.as_mut().unwrap().phase = DrainPhase::Cancelled;
    assert_eq!(
        n.membership_drain_ready(&plan, &j),
        Err(DrainPlanError::WrongIntent)
    );
    shutdown(&mut n);
    shutdown(&mut other);
}
#[test]
fn recovered_stale_manifest_still_gates_every_actual_assignment() {
    let mut n = boat(parts(1, false));
    let mut j = journal(&n);
    j.latest.as_mut().unwrap().request.groups.pop();
    n.restore_drain(&j).unwrap();
    assert_eq!(
        n.propose(request(1)).unwrap_err().reason,
        ClientError::Draining
    );
    settle(&mut n);
    let s = n.local_drain_status().unwrap();
    assert!(!s.assignments_match);
    assert!(!s.locally_quiescent);
    assert!(n.local().owner.groups().all(|g| !n
        .local()
        .owner
        .core(g)
        .unwrap()
        .campaigning_enabled()));
    let mut recovered = boat(parts(1, false));
    recovered.restore_drain(&j).unwrap();
    assert_eq!(
        recovered.read(group(1), ()).unwrap_err().reason,
        ReadInvocationError::Draining
    );
    shutdown(&mut recovered);
    shutdown(&mut n);
}
#[test]
fn cancellation_before_disable_executes_waits_for_exact_enable_admissions() {
    let mut n = boat(parts(1, false));
    let mut j = journal(&n);
    n.restore_drain(&j).unwrap();
    let budget = NodePollBudget {
        replica: ReplicaPollBudget {
            steps: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    n.poll(MonoTime(0), budget).unwrap(); // Only one of two disable events has run.
    let mut cancelled = j.latest.clone().unwrap();
    cancelled.phase = DrainPhase::Cancelled;
    j.publish(cancelled).unwrap();
    n.restore_drain(&j).unwrap();
    assert!(n.local_drain_status().unwrap().resuming);
    n.poll(MonoTime(0), budget).unwrap(); // Old disable must not count as enable.
    assert!(n.local_drain_status().is_some());
    assert_eq!(
        n.propose(request(1)).unwrap_err().reason,
        ClientError::Draining
    );
    settle(&mut n);
    assert!(n.local_drain_status().is_none());
    assert!(n.local().owner.groups().all(|g| n
        .local()
        .owner
        .core(g)
        .unwrap()
        .campaigning_enabled()));
    n.restore_drain(&j).unwrap(); // Exact retry cannot restart/resubmit cancellation.
    assert!(n.local().owner.is_drained());
    n.control(group(1), NodeControl::Campaign).unwrap();
    settle(&mut n);
    n.propose(request(1)).unwrap();
    settle(&mut n);
    let output = n.poll_client().unwrap();
    n.complete_client(output).unwrap();
    shutdown(&mut n);
}

#[test]
fn cancellation_waits_behind_an_accepted_persistence_dependency() {
    let mut n = elected();
    let mut j = journal(&n);
    n.propose(request(1)).unwrap();
    let budget = NodePollBudget {
        replica: ReplicaPollBudget {
            steps: 1,
            worker_events: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    n.poll(MonoTime(0), budget).unwrap();
    assert!(n
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .has_pending_dependency());
    n.restore_drain(&j).unwrap();
    let mut cancelled = j.latest.clone().unwrap();
    cancelled.phase = DrainPhase::Cancelled;
    j.publish(cancelled).unwrap();
    n.restore_drain(&j).unwrap();
    n.poll(MonoTime(0), budget).unwrap();
    assert!(n
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .has_pending_dependency());
    assert_eq!(
        n.propose(request(2)).unwrap_err().reason,
        ClientError::Draining
    );
    settle(&mut n);
    assert!(n.local_drain_status().is_none());
    let output = n.poll_client().unwrap();
    assert!(matches!(
        n.complete_client(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    shutdown(&mut n);
}
#[test]
fn old_owner_and_superseded_journal_views_do_not_reopen_or_replace_a_gate() {
    let mut n = boat(parts(1, false));
    let mut j = journal(&n);
    let original = j.latest.clone().unwrap();
    n.restore_drain(&j).unwrap();
    j.owner.store.incarnation = StoreIncarnation::new(99).unwrap();
    assert_eq!(n.restore_drain(&j), Err(DrainRestoreError::WrongOwner));
    j.owner = original.owner;
    let mut cancelled = original.clone();
    cancelled.phase = DrainPhase::Cancelled;
    j.latest = Some(cancelled);
    n.restore_drain(&j).unwrap();
    settle(&mut n);
    j.latest = Some(original);
    assert_eq!(n.restore_drain(&j), Err(DrainRestoreError::Conflict));
    j.latest = None;
    assert_eq!(n.restore_drain(&j), Err(DrainRestoreError::Conflict));
    shutdown(&mut n);
}

#[test]
fn journal_failure_preserves_active_gate_and_old_cancel_cannot_release_new_local_intent() {
    struct FailedJournal(PeerIdentity);
    impl DrainJournal for FailedJournal {
        fn owner(&self) -> PeerIdentity {
            self.0
        }
        fn latest(&self) -> Result<Option<DrainRecord>, DrainJournalError> {
            Err(DrainJournalError::Fenced)
        }
        fn publish(&mut self, _: DrainRecord) -> Result<(), DrainJournalError> {
            Err(DrainJournalError::Fenced)
        }
    }
    let mut n = boat(parts(1, false));
    let j = journal(&n);
    n.restore_drain(&j).unwrap();
    assert_eq!(
        n.restore_drain(&FailedJournal(j.owner)),
        Err(DrainRestoreError::Journal(DrainJournalError::Fenced))
    );
    assert_eq!(
        n.propose(request(1)).unwrap_err().reason,
        ClientError::Draining
    );
    let mut other = boat(parts(1, false));
    let mut intent = j.latest.clone().unwrap().request;
    intent.operation = OperationId::new(200).unwrap();
    other.begin_local_drain(intent).unwrap();
    let mut old = j;
    old.latest.as_mut().unwrap().phase = DrainPhase::Cancelled;
    assert_eq!(other.restore_drain(&old), Err(DrainRestoreError::Conflict));
    assert_eq!(
        other.propose(request(1)).unwrap_err().reason,
        ClientError::Draining
    );
    shutdown(&mut other);
    shutdown(&mut n);
}
