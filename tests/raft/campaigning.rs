// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn cluster() -> Cluster<HostLogStore> {
    Cluster::new(bootstrap(1, 3), |id| HostLogStore::new(id.into()))
}
fn pause(c: &mut Cluster<HostLogStore>, id: u64) {
    c.act(id, Event::SetCampaigning { enabled: false });
}
#[test]
fn pause_blocks_explicit_campaign_without_changing_durable_vote() {
    let mut c = cluster();
    let before = c.replicas[&1].core.state().clone();
    pause(&mut c, 1);
    let r = &mut c.replicas.get_mut(&1).unwrap().core;
    assert!(!r.campaigning_enabled());
    assert_eq!(r.step(Event::Campaign), Err(RaftError::Busy));
    assert_eq!(r.state(), &before);
    c.act(1, Event::SetCampaigning { enabled: true });
    c.act(1, Event::Campaign);
    c.pump();
    assert_eq!(c.replicas[&1].core.role(), Role::Leader);
}
#[test]
fn paused_candidate_ignores_its_late_ballots_and_still_votes_and_replicates() {
    let mut c = cluster();
    c.act(1, Event::Campaign);
    assert_eq!(c.replicas[&1].core.role(), Role::Candidate);
    pause(&mut c, 1);
    c.pump();
    assert_eq!(c.replicas[&1].core.role(), Role::Follower);
    // A later term can still elect another candidate with this paused voter.
    c.partition(&[1, 2]); // The paused voter is necessary for this quorum.
    c.act(2, Event::Campaign);
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    c.propose(2, 1, 7);
    assert_eq!(c.commands(1), vec![vec![7]]);
    assert!(!c.replicas[&1].core.campaigning_enabled());
}
#[test]
fn pending_self_vote_finishes_before_pause_discards_candidacy() {
    let mut c = cluster();
    let r = &mut c.replicas.get_mut(&1).unwrap().core;
    let effects = r.step(Event::Campaign).unwrap();
    assert_eq!(
        r.step(Event::SetCampaigning { enabled: false }),
        Err(RaftError::Busy)
    );
    c.effects(1, effects);
    pause(&mut c, 1);
    c.pump();
    assert_eq!(c.replicas[&1].core.role(), Role::Follower);
    assert_eq!(
        c.replicas[&1].core.state().hard_state.voted_for,
        Some(node(1))
    );
}
#[test]
fn paused_leader_can_handoff_and_cannot_campaign_again() {
    let mut c = leadership::elected();
    pause(&mut c, 1);
    assert_eq!(c.replicas[&1].core.role(), Role::Leader);
    c.act(1, Event::TransferLeadership(leadership::request(2)));
    c.pump();
    assert_eq!(c.replicas[&2].core.role(), Role::Leader);
    assert_eq!(c.replicas[&1].core.role(), Role::Follower);
    assert_eq!(
        c.replicas.get_mut(&1).unwrap().core.step(Event::Campaign),
        Err(RaftError::Busy)
    );
    c.propose(2, 2, 8);
    assert_eq!(c.commands(1), vec![vec![7], vec![8]]);
}
#[test]
fn pause_also_blocks_a_valid_timeout_now_invitation() {
    let mut c = leadership::elected();
    pause(&mut c, 2);
    c.act(1, Event::TransferLeadership(leadership::request(2)));
    let message = leadership::signal(&mut c);
    let r = &mut c.replicas.get_mut(&2).unwrap().core;
    let before = r.state().clone();
    assert_eq!(r.step(Event::Receive(message)), Err(RaftError::Busy));
    assert_eq!(r.state(), &before);
}
#[test]
fn recovery_requires_the_host_to_reapply_volatile_pause() {
    let mut c = cluster();
    pause(&mut c, 1);
    let r = c.replicas.get_mut(&1).unwrap();
    r.core = Raft::recover(
        node(1),
        r.store.binding(),
        r.store.state(group(1)).unwrap(),
        r.store.limits(),
    )
    .unwrap();
    assert!(r.core.campaigning_enabled());
    pause(&mut c, 1);
    assert_eq!(
        c.replicas.get_mut(&1).unwrap().core.step(Event::Campaign),
        Err(RaftError::Busy)
    );
}
