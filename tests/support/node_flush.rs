// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Same-poll send flush (N1): a node poll writes and completes the sends it
//! made itself, after their durability gate, instead of in two later polls.
use super::super::peer_driver::PeerNetwork;
use super::*;
use std::collections::VecDeque;

/// Node 1 of three voters, both peer transports attached, no leader yet.
fn mesh() -> (Boat, PeerNetwork) {
    let p = parts(3, true);
    let peers = p.peers.as_ref().unwrap();
    let network = peers.factory.network(&peers.connector);
    let mut n = boat(p);
    for _ in 0..10 {
        n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        if network.binding(node(2)).is_some() && network.binding(node(3)).is_some() {
            return (n, network);
        }
    }
    panic!("peers not attached");
}

/// Peers 2 and 3 as granting, caught-up voters: one reply per request.
struct Peers {
    network: PeerNetwork,
    replies: BTreeMap<u64, VecDeque<Message>>,
    votes: usize,
    appends: usize,
    entries: usize,
}
impl Peers {
    fn new(network: PeerNetwork) -> Self {
        Self {
            network,
            replies: BTreeMap::new(),
            votes: 0,
            appends: 0,
            entries: 0,
        }
    }
    /// Collect what node 1 wrote during the last poll and queue the answers.
    /// Returns the number of messages written.
    fn answer(&mut self) -> usize {
        let mut written = 0;
        for peer in [2, 3] {
            let sender = self.network.binding(node(peer)).unwrap().peer.store;
            for request in self.network.take_sent(node(peer)) {
                written += 1;
                let rpc = match &request.rpc {
                    Rpc::Vote { .. } => {
                        self.votes += 1;
                        Rpc::Voted { granted: true }
                    }
                    Rpc::Append {
                        previous_index,
                        entries,
                        ..
                    } => {
                        self.appends += 1;
                        self.entries += entries.len();
                        Rpc::Appended {
                            success: true,
                            matching_index: entries.last().map_or(*previous_index, |e| e.index),
                        }
                    }
                    other => panic!("unexpected request {other:?}"),
                };
                self.replies.entry(peer).or_default().push_back(Message {
                    group: request.group,
                    configuration: request.configuration,
                    from: node(peer),
                    sender,
                    to: node(1),
                    term: request.term,
                    context: request.context,
                    rpc,
                });
            }
            if self.network.can_deliver(node(peer)) {
                let queued = self.replies.entry(peer).or_default();
                if !queued.is_empty() {
                    self.network
                        .deliver_batch(node(peer), queued.drain(..).collect());
                }
            }
        }
        written
    }
}

/// One node poll, then the invariant this slice adds: every peer transport
/// accepts bytes, so nothing the poll queued is left in the outbound queue.
fn poll(n: &mut Boat) {
    n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
    assert!(
        n.local().outbound.is_drained(),
        "a send made by this poll was left queued: {:?}",
        n.local().outbound.usage()
    );
}

#[test]
fn a_poll_writes_and_completes_its_own_sends_after_their_durability_gate() {
    let (mut n, network) = mesh();
    let mut peers = Peers::new(network);
    assert_eq!(peers.answer(), 0);
    n.control(group(1), NodeControl::Campaign).unwrap();
    // The vote requests wait for the durable term and self-vote; once that
    // persist completes they are on the wire in the same poll.
    let (mut polls, mut first_vote, mut elected) = (0, None, None);
    while elected.is_none() {
        polls += 1;
        assert!(polls < 50, "not elected");
        poll(&mut n);
        let durable = n.local().persistence.store.durable[&group(1)].hard_state;
        let sent = peers.network.peek_sent();
        for request in sent.iter().filter(|m| matches!(m.rpc, Rpc::Vote { .. })) {
            assert!(
                durable.term == request.term && durable.voted_for == Some(node(1)),
                "a vote request left before its persist was durable"
            );
            first_vote.get_or_insert(polls);
        }
        peers.answer();
        if n.local().owner.core(group(1)).unwrap().role() == Role::Leader {
            elected = Some(polls);
        }
    }
    assert_eq!(peers.votes, 2);
    // Commit the term's first entry so a write is served at once.
    for _ in 0..50 {
        poll(&mut n);
        peers.answer();
    }
    let before = (peers.appends, peers.entries);
    let ticket = n.propose(request(1)).unwrap();
    let mut append_polls = 0;
    while peers.entries == before.1 {
        append_polls += 1;
        assert!(append_polls < 20, "no append");
        poll(&mut n);
        peers.answer();
    }
    let mut commit_polls = 0;
    let completion = loop {
        commit_polls += 1;
        assert!(commit_polls < 20, "no completion");
        poll(&mut n);
        peers.answer();
        if let Some(c) = n.poll_client() {
            break c;
        }
    };
    assert_eq!(completion.ticket(), ticket);
    // Exact poll counts. The parent (main 2d798a19, no flush) took 5 polls
    // from Campaign to the first vote request on the wire, 6 to leadership,
    // 4 from propose to the Append on the wire and 3 more to the client
    // completion (7 in all). With the flush a send leaves in the poll that
    // made it: 3, 5, 2 and 4 (6 in all). The commit leg is now bounded by
    // the leader's own three-poll persist (submit, written, durable), which
    // the earlier Append no longer waits behind, and the second vote by the
    // fixture's one-item replica visits; the peers here answer at once.
    assert_eq!(
        (first_vote, elected, append_polls, commit_polls),
        (Some(3), Some(5), 2, 4)
    );
    assert!(matches!(
        n.complete_client(completion).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    shutdown(&mut n);
}
