// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
mod support;
use support::*;
use voteboat::{
    drain::*, identity::*, log::*, membership::*, placement::*, quorum::*, raft::*,
    secure::PeerIdentity,
};
#[path = "drain_membership/retained.rs"]
mod retained;
#[derive(Clone)]
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
fn owner() -> PeerIdentity {
    PeerIdentity {
        node: node(1),
        store: identity(1),
    }
}
fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voters
            .iter()
            .map(|n| (node(*n), identity((*n).into())))
            .collect(),
        learners
            .iter()
            .map(|n| (node(*n), identity((*n).into())))
            .collect(),
    )
    .unwrap()
}
fn entry(g: u128) -> DrainMembershipGroup {
    DrainMembershipGroup {
        group: group(g),
        original: configuration(1, &[1, 2, 3], &[]),
        handoff: PeerIdentity {
            node: node(2),
            store: identity(2),
        },
        change: PlannedVoterChange {
            joint: ConfigurationRecord {
                operation: OperationId::new(100).unwrap(),
                expected: ConfigurationId::new(1).unwrap(),
                change: ConfigurationChange::Joint {
                    id: ConfigurationId::new(2).unwrap(),
                    next: configuration(3, &[2, 3], &[1]),
                },
            },
            finalize: ConfigurationRecord {
                operation: OperationId::new(100).unwrap(),
                expected: ConfigurationId::new(2).unwrap(),
                change: ConfigurationChange::Final {
                    id: ConfigurationId::new(3).unwrap(),
                },
            },
        },
    }
}
fn plan(groups: Vec<DrainMembershipGroup>) -> MembershipDrainPlan {
    MembershipDrainPlan::new(owner(), OperationId::new(101).unwrap(), groups).unwrap()
}
#[test]
fn exact_multigroup_plan_binds_original_fields_and_rejects_replacement_on_recovery() {
    let original = plan(vec![entry(1), entry(2)]);
    let mut journal = Journal(original.record(7).unwrap());
    assert_eq!(original.verify(&journal).unwrap().sequence, 7);
    assert_eq!(original.clone().digest(), original.digest());
    let mut changed = entry(2);
    changed.handoff = PeerIdentity {
        node: node(3),
        store: identity(3),
    };
    let replacement = plan(vec![entry(1), changed]);
    assert_ne!(original.digest(), replacement.digest());
    assert_eq!(
        replacement.verify(&journal),
        Err(DrainPlanError::WrongIntent)
    );
    let mut substituted = journal.0.clone();
    substituted.plan = Some(replacement.digest());
    substituted.phase = DrainPhase::Cancelled;
    assert_eq!(
        journal.publish(substituted),
        Err(DrainJournalError::Conflict)
    );
    journal.0.phase = DrainPhase::Cancelled;
    assert_eq!(original.verify(&journal), Err(DrainPlanError::WrongIntent));
}
#[test]
fn invalid_targets_and_joint_links_are_refused_before_journal_creation() {
    for mutation in 0..7 {
        let mut value = entry(1);
        match mutation {
            0 => value.handoff = owner(),
            1 => value.handoff.store = identity(99),
            2 => value.change.finalize.operation = OperationId::new(999).unwrap(),
            3 => value.change.finalize.expected = ConfigurationId::new(3).unwrap(),
            4 => value.change.joint.expected = ConfigurationId::new(2).unwrap(),
            5 => {
                value.change.joint.change = ConfigurationChange::Joint {
                    id: ConfigurationId::new(2).unwrap(),
                    next: configuration(3, &[1, 2, 3], &[]),
                }
            }
            _ => {
                value.change.joint.change = ConfigurationChange::Joint {
                    id: ConfigurationId::new(2).unwrap(),
                    next: configuration(3, &[2, 4], &[1]),
                }
            }
        }
        assert_eq!(
            MembershipDrainPlan::new(owner(), OperationId::new(101).unwrap(), vec![value]),
            Err(DrainPlanError::InvalidPlan)
        );
    }
}
#[test]
fn only_prepared_exact_learners_can_be_replacement_voters() {
    let mut value = entry(1);
    value.original = configuration(1, &[1, 2, 3], &[4]);
    value.change.joint.change = ConfigurationChange::Joint {
        id: ConfigurationId::new(2).unwrap(),
        next: configuration(3, &[2, 3, 4], &[1]),
    };
    let planned = plan(vec![value.clone()]);
    assert_eq!(planned.groups(), &[value]);
    // Readiness is deliberately not synthesized by plan construction.
}
#[test]
fn bounded_sorted_plan_inventory_and_retained_byte_limit_are_enforced() {
    let op = OperationId::new(101).unwrap();
    for groups in [vec![], vec![entry(1), entry(1)], vec![entry(2), entry(1)]] {
        assert_eq!(
            MembershipDrainPlan::new(owner(), op, groups),
            Err(DrainPlanError::InvalidPlan)
        );
    }
    let mut groups =
        Vec::with_capacity(MAX_DRAIN_PLAN_BYTES / std::mem::size_of::<DrainMembershipGroup>() + 1);
    groups.push(entry(1));
    assert_eq!(
        MembershipDrainPlan::new(owner(), op, groups),
        Err(DrainPlanError::TooLarge)
    );
}
fn recovered(
    value: &DrainMembershipGroup,
    records: &[ConfigurationRecord],
    committed: u64,
) -> Raft {
    let mut store = HostLogStore::new(1);
    append(
        &mut store,
        vec![LogMutation::Create(Bootstrap {
            group: value.group,
            configuration: value.original.id(),
            policy: value.original.policy().clone(),
            voter_stores: value.original.voter_stores().clone(),
        })],
    );
    let state = store.state(value.group).unwrap();
    let entries = records
        .iter()
        .enumerate()
        .map(|(i, r)| LogEntry {
            index: i as u64 + 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(r.clone())),
        })
        .collect();
    append(
        &mut store,
        vec![update(
            &state,
            1,
            committed,
            Some(Suffix { from: 1, entries }),
        )],
    );
    Raft::recover_member(
        node(1),
        store.binding(),
        store.state(value.group).unwrap(),
        store.limits(),
    )
    .unwrap()
}
#[test]
fn accepted_final_and_rolled_back_suffix_are_not_completed_evacuation() {
    let value = entry(1);
    let plan = plan(vec![value.clone()]);
    let journal = Journal(plan.record(1).unwrap());
    let records = [value.change.joint.clone(), value.change.finalize.clone()];
    for (records, commit) in [(&records[..1], 0), (&records[..1], 1), (&records[..], 1)] {
        let core = recovered(&value, records, commit);
        assert_eq!(plan.next(&journal, &core), Ok(MembershipDrainAction::Wait));
    }
    let committed = recovered(&value, &records, 2);
    assert_eq!(
        plan.next(&journal, &committed),
        Ok(MembershipDrainAction::Completed)
    );
    let rolled_back = recovered(&value, &records[..1], 1);
    assert_eq!(
        plan.next(&journal, &rolled_back),
        Ok(MembershipDrainAction::Wait)
    );
}

#[test]
fn digest_binds_recursive_branch_structure_and_fixed_weights() {
    let original = plan(vec![entry(1)]);
    let journal = Journal(original.record(1).unwrap());
    let mut value = entry(1);
    let policy = Policy::new(
        Tree::Weighted(vec![
            WeightedChild {
                weight: 3,
                node: Tree::Majority(vec![Tree::Voter(node(1)), Tree::Voter(node(2))]),
            },
            WeightedChild {
                weight: 1,
                node: Tree::Voter(node(3)),
            },
        ]),
        Limits::default(),
    )
    .unwrap();
    value.original = Configuration::new(
        ConfigurationId::new(1).unwrap(),
        policy,
        value.original.voter_stores().clone(),
        Default::default(),
    )
    .unwrap();
    let nested = plan(vec![value.clone()]);
    assert_ne!(nested.digest(), original.digest());
    assert_eq!(nested.verify(&journal), Err(DrainPlanError::WrongIntent));
    let mut tree = value.original.policy().tree().clone();
    let Tree::Weighted(children) = &mut tree else {
        unreachable!()
    };
    children[0].weight = 4;
    value.original = Configuration::new(
        ConfigurationId::new(1).unwrap(),
        Policy::new(tree, Limits::default()).unwrap(),
        value.original.voter_stores().clone(),
        Default::default(),
    )
    .unwrap();
    assert_ne!(plan(vec![value]).digest(), nested.digest());
}
