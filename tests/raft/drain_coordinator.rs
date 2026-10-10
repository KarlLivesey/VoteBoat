// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{drain::*, membership::*, placement::*, secure::PeerIdentity};

struct Journal(DrainRecord);
impl DrainJournal for Journal {
    fn owner(&self) -> PeerIdentity {
        self.0.owner
    }
    fn latest(&self) -> Result<Option<DrainRecord>, DrainJournalError> {
        Ok(Some(self.0.clone()))
    }
    fn publish(&mut self, record: DrainRecord) -> Result<(), DrainJournalError> {
        if !record.follows(&self.0) {
            return Err(DrainJournalError::Conflict);
        }
        self.0 = record;
        Ok(())
    }
}
fn setup() -> (Vec<Cluster<HostLogStore>>, MembershipDrainPlan, Journal) {
    let mut clusters = Vec::new();
    let mut entries = Vec::new();
    for g in 1..=3 {
        let mut b = bootstrap(g, 3);
        b.group = group(g);
        let original = Configuration::new(
            b.configuration,
            b.policy.clone(),
            b.voter_stores.clone(),
            Default::default(),
        )
        .unwrap();
        let mut c = Cluster::new(b, |id| HostLogStore::new(id as u128));
        c.act(1, Event::Campaign);
        c.pump();
        assert_eq!(c.replicas[&1].core.role(), Role::Leader);
        let op = OperationId::new(100 + g).unwrap();
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
        entries.push(DrainMembershipGroup {
            group: group(g),
            original,
            handoff: PeerIdentity {
                node: node(2),
                store: identity(2),
            },
            change: PlannedVoterChange {
                joint: ConfigurationRecord {
                    operation: op,
                    expected: ConfigurationId::new(1).unwrap(),
                    change: ConfigurationChange::Joint {
                        id: ConfigurationId::new(2).unwrap(),
                        next: target,
                    },
                },
                finalize: ConfigurationRecord {
                    operation: op,
                    expected: ConfigurationId::new(2).unwrap(),
                    change: ConfigurationChange::Final {
                        id: ConfigurationId::new(3).unwrap(),
                    },
                },
            },
        });
        clusters.push(c);
    }
    let plan = MembershipDrainPlan::new(
        PeerIdentity {
            node: node(1),
            store: identity(1),
        },
        OperationId::new(99).unwrap(),
        entries,
    )
    .unwrap();
    let journal = Journal(plan.record(1).unwrap());
    (clusters, plan, journal)
}
#[test]
fn bounded_fair_dispatch_rejects_stale_completion_and_rechecks_original_intent() {
    let (clusters, plan, mut journal) = setup();
    assert!(matches!(
        MembershipDrainCoordinator::new(plan.clone(), 0),
        Err(DrainCoordinatorError::InvalidLimit)
    ));
    let mut coordinator = MembershipDrainCoordinator::new(plan.clone(), 2).unwrap();
    let observe = |g: GroupIdentity| Some(&clusters[g.id.get() as usize - 1].replicas[&1].core);
    assert!(matches!(
        coordinator.poll(&journal, 0, observe),
        Err(DrainCoordinatorError::InvalidLimit)
    ));
    let first = coordinator.poll(&journal, 1, observe).unwrap();
    assert_eq!(first.scanned, 1);
    assert_eq!(first.requests[0].ticket.group(), group(1));
    let second = coordinator.poll(&journal, 1, observe).unwrap();
    assert_eq!(second.requests[0].ticket.group(), group(2));
    assert!(coordinator
        .poll(&journal, 1, observe)
        .unwrap()
        .requests
        .is_empty());
    assert_eq!(coordinator.in_flight(), 2);
    coordinator.finish(&first.requests[0].ticket).unwrap();
    assert_eq!(
        coordinator.finish(&first.requests[0].ticket),
        Err(DrainCoordinatorError::StaleTicket)
    );
    // Cursor continues rather than favoring an already released first group.
    let next = coordinator.poll(&journal, 3, observe).unwrap();
    assert_eq!(next.requests.len(), 1);
    assert_eq!(next.requests[0].ticket.group(), group(3));
    let mut restarted = MembershipDrainCoordinator::new(plan, 2).unwrap();
    assert_eq!(
        restarted.finish(&second.requests[0].ticket),
        Err(DrainCoordinatorError::StaleTicket)
    );
    journal.0.phase = DrainPhase::Cancelled;
    assert!(matches!(
        coordinator.poll(&journal, 3, observe),
        Err(DrainCoordinatorError::Plan(DrainPlanError::WrongIntent))
    ));
}
#[test]
fn missing_and_wrong_group_views_do_not_discard_independent_dispatches() {
    let (clusters, plan, journal) = setup();
    let mut coordinator = MembershipDrainCoordinator::new(plan, 3).unwrap();
    let batch = coordinator
        .poll(&journal, 3, |g| match g.id.get() {
            1 => Some(&clusters[0].replicas[&1].core),
            2 => Some(&clusters[0].replicas[&1].core),
            _ => None,
        })
        .unwrap();
    assert_eq!(batch.scanned, 3);
    assert_eq!(batch.requests.len(), 1);
    assert_eq!(batch.errors, vec![(group(2), DrainPlanError::UnknownGroup)]);
    assert_eq!(batch.unavailable, 1);
    coordinator.finish(&batch.requests[0].ticket).unwrap();
    assert_eq!(coordinator.in_flight(), 0);
}
