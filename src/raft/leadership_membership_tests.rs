// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn request(c: &Raft, target: u64) -> Event {
    Event::TransferLeadership(LeadershipTransferRequest {
        operation: operation(800),
        target: PeerIdentity {
            node: node(target),
            store: store(target),
        },
        configuration: c.membership().id(),
    })
}
#[test]
fn real_learner_and_joint_views_refuse_transfer_without_changing_membership() {
    for (mut c, target) in [(staged(), 4), (joint(false), 2), (joint(true), 2)] {
        let before = c.state().clone();
        let event = request(&c, target);
        assert_eq!(c.step(event), Err(RaftError::WrongIdentity));
        assert_eq!(c.state(), &before);
        assert!(c.leadership_transfer().is_none());
    }
}
#[test]
fn uncommitted_configuration_and_in_flight_configuration_changes_cannot_overlap_transfer() {
    let mut c = core();
    let effects = accept(
        &mut c,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4])),
    );
    durable(&mut c, effects);
    let event = request(&c, 2);
    assert_eq!(c.step(event), Err(RaftError::WrongIdentity));
    committed_fixture(&mut c, 2);
    let event = request(&c, 2);
    c.step(event).unwrap();
    let proposal = ConfigurationProposal {
        record: ConfigurationRecord {
            operation: operation(101),
            expected: cid(2),
            change: ConfigurationChange::Learners(configuration(3, &[1, 2, 3], &[4, 5])),
        },
        readiness: Vec::new(),
        requirements: ReadinessRequirements {
            application_schema: 1,
            command_bytes: 1024,
            snapshot_bytes: 4096,
        },
    };
    assert_eq!(
        c.step(Event::Configure(Box::new(proposal))),
        Err(RaftError::Busy)
    );
    c.storage_failed();
    assert!(c.leadership_transfer().is_none());
}
