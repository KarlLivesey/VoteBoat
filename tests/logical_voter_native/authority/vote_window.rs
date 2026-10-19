// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real child authority plus trusted local parent clock/peer controls, not a
//! complete authenticated parent network or a claim about transport latency.
use super::super::{outbound::Outbound, parent::Parent};
use super::*;
use std::time::{Duration, Instant};
use voteboat::{outbound::*, raft::*};
#[derive(Default)]
struct Gate {
    rejected: usize,
    closed: bool,
}
struct Selected<'a> {
    queue: &'a mut Outbound,
    gate: &'a mut Gate,
}
impl LogicalOutboundQueue for Selected<'_> {
    fn submit_authorized(
        &mut self,
        send: AuthorizedLogicalSend,
    ) -> Result<SendTicket, LogicalOutboundRejected> {
        if self.gate.closed {
            self.gate.rejected += 1;
            Err(LogicalOutboundRejected {
                reason: OutboundError::Overloaded,
                send: Box::new(send),
            })
        } else {
            self.queue.submit_authorized(send)
        }
    }
}
impl OutboundQueue for Selected<'_> {
    fn binding(&self) -> OutboundBinding {
        self.queue.binding()
    }
    fn limits(&self) -> OutboundLimits {
        self.queue.limits()
    }
    fn usage(&self) -> OutboundUsage {
        self.queue.usage()
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        self.queue.peer_usage(peer)
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        self.queue.submit(messages)
    }
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch> {
        self.queue.poll(limit)
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        self.queue.complete(batch, result)
    }
    fn close(&mut self) {
        self.queue.close();
    }
}
fn poll(
    parent: &mut Parent,
    now: MonoTime,
    gate: &mut Gate,
) -> Result<ReplicaProgress, ReplicaError> {
    parent.driver.poll(
        &mut ReplicaParts {
            owner: &mut parent.owner,
            persistence: &mut parent.worker,
            applications: &mut parent.apps,
            results: &mut parent.routers.results,
            clients: &mut parent.routers.clients,
            reads: &mut parent.routers.reads,
            outbound: &mut Selected {
                queue: &mut parent.outbound,
                gate,
            },
            snapshots: None,
        },
        now,
        ReplicaPollBudget::default(),
    )
}
fn pump(
    bridge: &mut super::super::aliases::Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    completions: usize,
    now: MonoTime,
    gate: &mut Gate,
) -> Result<ReplicaProgress, ReplicaError> {
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    bridge
        .advance(
            MonoTime(clock.elapsed().as_millis() as u64),
            LogicalBridgeBudget {
                node_polls: 3,
                completions,
                transitions: 1,
                node: NodePollBudget::default(),
            },
        )
        .unwrap();
    bridge.pump_persistence(&mut parent.port).unwrap();
    bridge.pump_release(parent.driver.releases()).unwrap();
    poll(parent, now, gate)
}
fn assert_candidate(parent: &Parent, term: u64) {
    let core = parent.owner.core(setup::mapping().parent).unwrap();
    assert_eq!(core.role(), Role::Candidate);
    assert_eq!(core.state().hard_state.term, term);
    assert_eq!(
        core.state().hard_state.voted_for,
        Some(setup::mapping().voter)
    );
}
fn wait_for_capture(
    bridge: &mut super::super::aliases::Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    gate: &mut Gate,
    end: Instant,
) {
    while parent.driver.release_usage().pending == 0 {
        pump(bridge, parent, clock, 4, MonoTime(0), gate).unwrap();
        assert!(Instant::now() < end, "original Campaign Vote capture");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn settle(
    bridge: &mut super::super::aliases::Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    now: MonoTime,
    gate: &mut Gate,
    end: Instant,
) -> Vec<Message> {
    let mut messages = Vec::new();
    while !bridge.is_drained() || !parent.is_drained() {
        let progress = pump(bridge, parent, clock, 4, now, gate).unwrap();
        assert!(progress.steps.iter().all(|step| step.error.is_none()));
        messages.extend(parent.take_sends());
        assert!(Instant::now() < end, "original ownership drains");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    messages
}
#[test]
fn original_accepted_votes_refresh_and_invalidate_queued_election_after_refusal() {
    let _serial = serial();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(),
        super::super::support::node(1),
    )
    .unwrap();
    let image = super::super::drive::claim_in(&mut bridge, clock, 7030, false);
    let mut parent = Parent::new(&image);
    let mut gate = Gate {
        closed: true,
        ..Gate::default()
    };
    let end = Instant::now() + Duration::from_secs(15);
    parent
        .owner
        .admit(setup::mapping().parent, Event::Campaign)
        .unwrap();
    poll(&mut parent, MonoTime(0), &mut gate).unwrap();
    wait_for_capture(&mut bridge, &mut parent, clock, &mut gate, end);
    let (now, old, reset) = refuse_original(&mut bridge, &mut parent, clock, &mut gate, end);
    let fresh = accept_originals(
        &mut bridge,
        &mut parent,
        clock,
        &mut gate,
        end,
        (now, old, reset),
    );
    let future = MonoTime(fresh.deadline.0);
    let progress = poll(&mut parent, future, &mut gate).unwrap();
    assert_eq!(
        progress.persistence_batches, 1,
        "fresh election still campaigns"
    );
    let later = settle(&mut bridge, &mut parent, clock, future, &mut gate, end);
    assert!(later
        .iter()
        .all(|m| matches!(m.rpc, Rpc::Vote { .. }) && m.term == 2));
    assert_eq!(later.len(), 2);
    assert_candidate(&parent, 2);
    bridge.close();
    drop(bridge);
    parent.close();
    cluster.finish();
}

fn refuse_original(
    bridge: &mut super::super::aliases::Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    gate: &mut Gate,
    end: Instant,
) -> (MonoTime, TimerToken, u64) {
    let group = setup::mapping().parent;
    assert!(!parent.owner.core(group).unwrap().has_pending_dependency());
    let old = parent.owner.deadline(group).unwrap();
    let reset = parent.owner.core(group).unwrap().election_reset_sequence();
    let now = MonoTime(old.deadline.0 + 1);
    assert!(parent.owner.advance(now, 0).unwrap().is_empty());
    for _ in 0..8 {
        pump(bridge, parent, clock, 0, now, gate).unwrap();
        assert_eq!(parent.owner.deadline(group), Some(old));
        assert_eq!(
            parent.owner.core(group).unwrap().election_reset_sequence(),
            reset
        );
        assert!(parent.take_sends().is_empty());
    }
    while gate.rejected == 0 {
        pump(bridge, parent, clock, 4, now, gate).unwrap();
        assert!(
            Instant::now() < end,
            "selected queue refuses original authorized Vote"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(parent.owner.deadline(group), Some(old));
    assert_eq!(
        parent.owner.core(group).unwrap().election_reset_sequence(),
        reset
    );
    assert_eq!(parent.driver.release_usage().pending, 1);
    (now, old, reset)
}
fn accept_originals(
    bridge: &mut super::super::aliases::Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
    gate: &mut Gate,
    end: Instant,
    held: (MonoTime, TimerToken, u64),
) -> TimerToken {
    let (now, old, reset) = held;
    let group = setup::mapping().parent;
    gate.closed = false;
    let first = pump(bridge, parent, clock, 4, now, gate).unwrap();
    assert!(first.steps.iter().all(|step| step.error.is_none()));
    assert_candidate(parent, 1);
    let first_token = parent.owner.deadline(group).unwrap();
    assert!(first_token.sequence > old.sequence);
    assert!(first_token.deadline > now);
    assert_eq!(
        parent.owner.core(group).unwrap().election_reset_sequence(),
        reset + 1
    );
    let mut messages = parent.take_sends();
    messages.extend(settle(bridge, parent, clock, now, gate, end));
    assert_eq!(messages.len(), 2);
    assert!(messages
        .iter()
        .all(|m| matches!(m.rpc, Rpc::Vote { .. }) && m.term == 1));
    assert_ne!(messages[0].to, messages[1].to);
    assert_candidate(parent, 1);
    assert_eq!(
        parent.owner.core(group).unwrap().election_reset_sequence(),
        reset + 2
    );
    let fresh = parent.owner.deadline(group).unwrap();
    assert!(fresh.sequence > first_token.sequence);
    fresh
}

#[test]
fn postaccept_timer_exhaustion_fences_but_keeps_original_queue_proof() {
    use voteboat::worker::PersistenceWorker;
    let _serial = serial();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(),
        super::super::support::node(1),
    )
    .unwrap();
    let image = super::super::drive::claim_in(&mut bridge, clock, 7031, false);
    let mut parent = Parent::new(&image);
    let mut gate = Gate {
        closed: true,
        ..Gate::default()
    };
    let end = Instant::now() + Duration::from_secs(15);
    parent
        .owner
        .admit(setup::mapping().parent, Event::Campaign)
        .unwrap();
    poll(&mut parent, MonoTime(0), &mut gate).unwrap();
    wait_for_capture(&mut bridge, &mut parent, clock, &mut gate, end);
    refuse_original(&mut bridge, &mut parent, clock, &mut gate, end);
    gate.closed = false;
    let error = poll(&mut parent, MonoTime(u64::MAX), &mut gate).unwrap_err();
    assert_eq!(
        error,
        ReplicaError::Logical(LogicalReleaseError::Owner(EffectOwnerError::Runtime(
            RuntimeError::Exhausted
        ),))
    );
    assert!(parent.owner.is_failed());
    assert_eq!(parent.outbound.usage().batches, 1);
    assert_eq!(parent.driver.release_usage().pending, 0);
    assert!(parent.worker.is_drained());
    assert!(parent.port.is_drained());
    assert!(bridge.is_drained());
    super::discard(&mut parent).unwrap();
    assert_eq!(
        parent.outbound.usage().batches,
        1,
        "accepted queue owns original proof after parent cleanup"
    );
    let batches = parent.outbound.poll(8);
    assert_eq!(batches.len(), 1);
    let batch = batches.into_iter().next().unwrap();
    assert!(matches!(
        batch.messages.as_slice(),
        [Message {
            rpc: Rpc::Vote { .. },
            term: 1,
            ..
        }]
    ));
    let proof = parent.outbound.proof(batch.ticket);
    assert_eq!(proof.receipt().outcome, LogicalOutcome::CapsuleAuthorized);
    assert_eq!(proof.mapping(), image.mapping());
    assert!(!proof.command_bytes().is_empty());
    parent
        .outbound
        .complete(batch, LocalSendResult::Cancelled)
        .unwrap();
    assert!(parent.outbound.is_drained());
    assert!(parent.owner.is_failed());
    assert_eq!(parent.owner.usage(), EffectOwnerUsage::default());
    assert!(parent.driver.is_drained());
    parent.worker.close();
    parent.outbound.close();
    bridge.close();
    drop(bridge);
    drop(parent);
    cluster.finish();
}

#[test]
fn original_granted_reply_acceptance_preserves_full_follower_window() {
    let _serial = serial();
    let mut cluster = Cluster::new(NativePeerProtocol::TcpTls);
    let clock = cluster.clock;
    let mut bridge = LogicalChildBridge::new(
        &mut cluster.nodes,
        setup::mapping(),
        super::super::support::node(1),
    )
    .unwrap();
    let image = super::super::drive::claim_in(&mut bridge, clock, 7032, false);
    let mut parent = Parent::new(&image);
    let mut gate = Gate {
        closed: true,
        ..Gate::default()
    };
    let end = Instant::now() + Duration::from_secs(15);
    super::tracked_input(
        &mut parent,
        Rpc::Vote {
            last_index: 0,
            last_term: 0,
        },
        99,
    );
    poll(&mut parent, MonoTime(0), &mut gate).unwrap();
    wait_for_capture(&mut bridge, &mut parent, clock, &mut gate, end);
    let group = setup::mapping().parent;
    assert_follower(&parent, 7);
    let (now, old, reset) = refuse_original(&mut bridge, &mut parent, clock, &mut gate, end);
    assert_follower(&parent, 7);
    gate.closed = false;
    let progress = pump(&mut bridge, &mut parent, clock, 4, now, &mut gate).unwrap();
    assert!(progress.steps.iter().all(|step| step.error.is_none()));
    assert_eq!(
        progress.persistence_batches, 0,
        "queued old follower timer must be stale"
    );
    assert_follower(&parent, 7);
    let fresh = parent.owner.deadline(group).unwrap();
    assert!(fresh.sequence > old.sequence && fresh.deadline > now);
    assert_eq!(
        parent.owner.core(group).unwrap().election_reset_sequence(),
        reset + 1
    );
    let messages = parent.take_sends();
    assert_eq!(messages.len(), 1);
    let message = &messages[0];
    assert!(matches!(message.rpc, Rpc::Voted { granted: true }));
    assert_eq!(message.term, 7);
    assert_eq!(message.to, super::super::support::node(12));
    assert_eq!(message.context.sequence, 99);
    assert_eq!(
        message.context.origin.identity,
        super::super::support::identity(102)
    );
    assert!(settle(&mut bridge, &mut parent, clock, now, &mut gate, end).is_empty());
    let future = MonoTime(fresh.deadline.0);
    assert_eq!(
        poll(&mut parent, future, &mut gate)
            .unwrap()
            .persistence_batches,
        1
    );
    let later = settle(&mut bridge, &mut parent, clock, future, &mut gate, end);
    assert_eq!(later.len(), 2);
    assert!(later
        .iter()
        .all(|m| matches!(m.rpc, Rpc::Vote { .. }) && m.term == 8));
    assert_candidate(&parent, 8);
    bridge.close();
    drop(bridge);
    parent.close();
    cluster.finish();
}
fn assert_follower(parent: &Parent, term: u64) {
    let core = parent.owner.core(setup::mapping().parent).unwrap();
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.state().hard_state.term, term);
    assert_eq!(
        core.state().hard_state.voted_for,
        Some(super::super::support::node(12))
    );
    assert_eq!(
        core.state().ballot_origin.unwrap().candidate_store,
        super::super::support::identity(102)
    );
}
