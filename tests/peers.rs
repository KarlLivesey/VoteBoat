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
mod support;
use std::sync::{Arc, Mutex};
use support::*;
use voteboat::{identity::*, outbound::*, raft::*, runtime::MonoTime, secure::*, transport::*};

#[derive(Debug)]
struct Control {
    binding: SessionBinding,
    security: SessionSecurity,
    state: TransportState,
    blocked: bool,
    fail: bool,
    wrong_completion: bool,
    polls: usize,
    incoming: Option<ReceivedBatch>,
    limits: TransportLimits,
    reported_bytes: usize,
}
/// Trusted-host attestation, used for ownership/fault scheduling, not crypto.
#[derive(Debug)]
struct Host {
    control: Arc<Mutex<Control>>,
    sending: Option<OutboundBatch>,
    completed: Option<TransportSend>,
}
impl Host {
    fn new(ticket: ConnectTicket, session: u64) -> Self {
        Self {
            control: Arc::new(Mutex::new(Control {
                binding: SessionBinding {
                    local: ticket.local,
                    peer: LocalIdentity {
                        node: ticket.peer.node,
                        store: StoreBinding {
                            identity: ticket.peer.store,
                            session: StoreSession::new(session).unwrap(),
                        },
                    },
                    generation: ticket.generation,
                    wire_version: 1,
                },
                security: SessionSecurity::Authenticated,
                state: TransportState::Open,
                blocked: false,
                fail: false,
                wrong_completion: false,
                polls: 0,
                incoming: None,
                limits: TransportLimits::default(),
                reported_bytes: 0,
            })),
            sending: None,
            completed: None,
        }
    }
}
impl PeerTransport for Host {
    fn security(&self) -> SessionSecurity {
        self.control.lock().unwrap().security
    }
    fn binding(&self) -> SessionBinding {
        self.control.lock().unwrap().binding
    }
    fn state(&self) -> TransportState {
        self.control.lock().unwrap().state
    }
    fn limits(&self) -> TransportLimits {
        self.control.lock().unwrap().limits
    }
    fn usage(&self) -> TransportUsage {
        TransportUsage {
            send_frame_bytes: self.control.lock().unwrap().reported_bytes,
            sending: self.sending.is_some(),
            completion: self.completed.is_some(),
            ..TransportUsage::default()
        }
    }
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
        if self.state() != TransportState::Open
            || self.sending.is_some()
            || self.completed.is_some()
        {
            return Err(TransportRejected {
                reason: TransportError::Overloaded,
                batch: Box::new(batch),
            });
        }
        self.sending = Some(batch);
        Ok(())
    }
    fn poll(
        &mut self,
        _: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError> {
        budget.validate()?;
        let mut c = self.control.lock().unwrap();
        c.polls += 1;
        if c.fail {
            c.state = TransportState::Failed;
            return Err(TransportError::Truncated);
        }
        let mut sent = false;
        if !c.blocked {
            if let Some(batch) = self.sending.take() {
                self.completed = Some(TransportSend {
                    connection: c.binding,
                    batch,
                    result: LocalSendResult::Sent,
                });
                if c.wrong_completion {
                    self.completed.as_mut().unwrap().connection.generation =
                        SecureSessionGeneration::new(999).unwrap();
                }
                sent = true;
            }
            if c.state == TransportState::Draining {
                c.state = TransportState::Closed;
            }
        }
        Ok(TransportProgress {
            sent,
            ..TransportProgress::default()
        })
    }
    fn take_send(&mut self) -> Option<TransportSend> {
        self.completed.take()
    }
    fn take_received(&mut self) -> Option<ReceivedBatch> {
        self.control.lock().unwrap().incoming.take()
    }
    fn close(&mut self) {
        self.control.lock().unwrap().state = TransportState::Draining;
    }
    fn abort(&mut self) {
        let mut c = self.control.lock().unwrap();
        c.state = TransportState::Failed;
        if let Some(batch) = self.sending.take() {
            self.completed = Some(TransportSend {
                connection: c.binding,
                batch,
                result: LocalSendResult::Failed,
            });
        }
    }
}
fn local() -> LocalIdentity {
    LocalIdentity {
        node: node(1),
        store: StoreBinding {
            identity: identity(1),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn outbound() -> OutboundBinding {
    OutboundBinding {
        node: local().node,
        store: local().store,
        generation: OutboundGeneration::new(1).unwrap(),
    }
}
fn limits() -> PeerRosterLimits {
    PeerRosterLimits {
        peers: 4,
        connecting: 2,
        connect_timeout_ms: 10,
        retry_min_ms: 5,
        retry_max_ms: 20,
        ..PeerRosterLimits::default()
    }
}
fn roster(l: PeerRosterLimits) -> PeerRoster<Host> {
    PeerRoster::new(
        config(l),
        [(node(2), identity(2)), (node(3), identity(3))].into(),
        MonoTime(0),
    )
    .unwrap()
}
fn config(limits: PeerRosterLimits) -> PeerRosterConfig {
    PeerRosterConfig {
        local: local(),
        outbound: outbound(),
        first_generation: SecureSessionGeneration::new(1).unwrap(),
        last_generation: SecureSessionGeneration::new(u64::MAX).unwrap(),
        wire_version: 1,
        limits,
        transport_limits: TransportLimits::default(),
    }
}
fn message(from: LocalIdentity, to: NodeId) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: from.node,
        sender: from.store,
        to,
        term: 1,
        context: RequestContext {
            origin: from.store,
            sequence: 1,
        },
        rpc: Rpc::ReadProbe,
    }
}
fn queue() -> support::outbound::HostOutbound {
    support::outbound::HostOutbound::new(outbound(), OutboundLimits::default()).unwrap()
}
fn send(q: &mut impl OutboundQueue, peer: NodeId) -> OutboundBatch {
    q.submit(vec![message(local(), peer)]).unwrap();
    q.poll(1).pop().unwrap()
}
#[test]
fn bounded_attempts_timeout_backoff_and_stale_ready_connections() {
    let mut r = roster(PeerRosterLimits {
        connecting: 1,
        ..limits()
    });
    assert_eq!(r.next_deadline(), Some(MonoTime(0)));
    let first = r.due_connections(MonoTime(0), 2).unwrap()[0];
    assert_eq!(r.usage().connecting, 1);
    assert_eq!(r.next_deadline(), Some(MonoTime(10)));
    assert!(r.due_connections(MonoTime(0), 2).unwrap().is_empty());
    assert_eq!(
        r.poll(MonoTime(10), 2, TransportPollBudget::default())
            .unwrap(),
        vec![PeerPoll::ConnectExpired(first)]
    );
    assert_eq!(r.usage().reserved_bytes, 0);
    assert_eq!(
        r.attach(first, Host::new(first, 1), MonoTime(10))
            .unwrap_err()
            .reason,
        PeerRosterError::StaleConnection
    );
    assert_eq!(
        r.connect_failed(first, MonoTime(10)),
        Err(PeerRosterError::StaleConnection)
    );
    let other = r.due_connections(MonoTime(10), 2).unwrap()[0];
    assert_ne!(first.peer, other.peer);
    r.connect_failed(other, MonoTime(10)).unwrap();
    assert!(r.due_connections(MonoTime(14), 2).unwrap().is_empty());
    let second = r.due_connections(MonoTime(15), 2).unwrap()[0];
    assert!(second.generation > first.generation);
    r.connect_failed(second, MonoTime(15)).unwrap();
    // A second failed attempt doubles this peer's retry delay.
    let due = r.due_connections(MonoTime(20), 2).unwrap();
    assert!(due.iter().all(|t| t.peer != second.peer));
    for t in due {
        r.connect_failed(t, MonoTime(20)).unwrap();
    }
    assert!(r
        .due_connections(MonoTime(25), 2)
        .unwrap()
        .iter()
        .any(|t| t.peer == second.peer));
}
#[test]
fn authentication_scope_limits_session_rollback_and_duplicate_attach_are_rejected() {
    let mut r = roster(limits());
    let t = r.due_connections(MonoTime(0), 1).unwrap()[0];
    for invalid in 0..9 {
        let host = Host::new(t, 4);
        {
            let mut c = host.control.lock().unwrap();
            match invalid {
                0 => c.security = SessionSecurity::SimulatorOnly,
                1 => c.binding.peer.store.identity = identity(9),
                2 => c.binding.generation = SecureSessionGeneration::new(99).unwrap(),
                3 => c.binding.local.store.session = StoreSession::new(9).unwrap(),
                4 => c.binding.wire_version = 9,
                5 => c.state = TransportState::Closed,
                6 => c.limits.send_frame_bytes *= 2,
                7 => c.limits.decoded_bytes *= 2,
                _ => c.reported_bytes = c.limits.send_frame_bytes + 1,
            }
        }
        let rejected = r.attach(t, host, MonoTime(0)).unwrap_err();
        assert_eq!(rejected.reason, PeerRosterError::WrongBinding);
        assert_eq!(rejected.transport.control.lock().unwrap().polls, 0);
        assert_eq!(r.usage().connecting, 1);
    }
    let preallocated = Host::new(t, 4);
    preallocated.control.lock().unwrap().reported_bytes = 1024;
    r.attach(t, preallocated, MonoTime(0)).unwrap();
    assert_eq!(
        r.attach(t, Host::new(t, 4), MonoTime(0))
            .unwrap_err()
            .reason,
        PeerRosterError::StaleConnection
    );
    r.disconnect(t.peer.node, MonoTime(0)).unwrap();
    let retry = r
        .due_connections(MonoTime(5), 2)
        .unwrap()
        .into_iter()
        .find(|v| v.peer == t.peer)
        .unwrap();
    assert_eq!(
        r.attach(retry, Host::new(retry, 3), MonoTime(5))
            .unwrap_err()
            .reason,
        PeerRosterError::WrongBinding
    );
    r.attach(retry, Host::new(retry, 5), MonoTime(5)).unwrap();
    assert_eq!(r.binding(t.peer.node).unwrap().peer.store.session.get(), 5);
}
#[test]
fn failed_connection_holds_original_send_and_capacity_until_exact_completion() {
    let mut r = roster(limits());
    let t = r.due_connections(MonoTime(0), 1).unwrap()[0];
    let host = Host::new(t, 1);
    let control = host.control.clone();
    control.lock().unwrap().blocked = true;
    r.attach(t, host, MonoTime(0)).unwrap();
    let mut q = queue();
    let batch = send(&mut q, t.peer.node);
    let ticket = batch.ticket;
    r.submit(batch).unwrap();
    let next = send(&mut q, t.peer.node);
    let rejected = r.submit(next).unwrap_err();
    assert_eq!(rejected.reason, PeerRosterError::Overloaded);
    assert_eq!(q.usage().batches, 2);
    control.lock().unwrap().fail = true;
    assert!(matches!(
        r.poll(MonoTime(1), 1, TransportPollBudget::default())
            .unwrap()[0],
        PeerPoll::Disconnected { .. }
    ));
    assert!(r.binding(t.peer.node).is_none());
    assert_eq!(r.usage().connections, 1);
    assert!(r
        .due_connections(MonoTime(6), 2)
        .unwrap()
        .iter()
        .all(|v| v.peer != t.peer));
    let done = r.take_send(t.peer.node).unwrap().unwrap();
    assert_eq!(done.batch.ticket, ticket);
    assert_eq!(done.result, LocalSendResult::Failed);
    q.complete(done.batch, done.result).unwrap();
    assert_eq!(q.usage().batches, 1);
    assert!(r.take_send(t.peer.node).unwrap().is_none());
    let retry = r
        .due_connections(MonoTime(6), 2)
        .unwrap()
        .into_iter()
        .find(|v| v.peer == t.peer)
        .unwrap();
    r.attach(retry, Host::new(retry, 1), MonoTime(6)).unwrap();
    r.submit(*rejected.batch).unwrap();
    r.poll(MonoTime(6), 2, TransportPollBudget::default())
        .unwrap();
    let done = r.take_send(t.peer.node).unwrap().unwrap();
    q.complete(done.batch, done.result).unwrap();
    assert!(q.is_drained());
}
#[test]
fn fair_visits_and_aggregate_capacity_bound_connection_resources() {
    let per_connection = 6 * 1024 * 1024;
    let mut r = roster(PeerRosterLimits {
        connection_bytes: per_connection,
        ..limits()
    });
    let t = r.due_connections(MonoTime(0), 2).unwrap();
    assert_eq!(t.len(), 1);
    let host = Host::new(t[0], 1);
    let c = host.control.clone();
    r.attach(t[0], host, MonoTime(0)).unwrap();
    assert!(r.due_connections(MonoTime(0), 2).unwrap().is_empty());
    assert_eq!(r.usage().reserved_bytes, per_connection);
    for _ in 0..4 {
        r.poll(MonoTime(0), 1, TransportPollBudget::default())
            .unwrap();
    }
    assert_eq!(c.lock().unwrap().polls, 2);
    assert_eq!(
        r.poll(MonoTime(0), 0, TransportPollBudget::default())
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        r.poll(
            MonoTime(0),
            2,
            TransportPollBudget {
                plaintext_calls: 5000,
                ..TransportPollBudget::default()
            }
        ),
        Err(PeerRosterError::Transport(TransportError::InvalidLimits))
    );
    assert_eq!(c.lock().unwrap().polls, 2);
}
#[test]
fn receives_are_scoped_and_obsolete_input_is_discarded_before_reconnect() {
    let mut r = roster(limits());
    let t = r.due_connections(MonoTime(0), 1).unwrap()[0];
    let host = Host::new(t, 1);
    let control = host.control.clone();
    let b = host.binding();
    r.attach(t, host, MonoTime(0)).unwrap();
    control.lock().unwrap().incoming = Some(ReceivedBatch {
        connection: b,
        messages: vec![message(b.peer, b.local.node)],
    });
    assert_eq!(r.take_received(t.peer.node).unwrap().unwrap().connection, b);
    control.lock().unwrap().incoming = Some(ReceivedBatch {
        connection: b,
        messages: vec![message(b.peer, b.local.node)],
    });
    r.disconnect(t.peer.node, MonoTime(0)).unwrap();
    assert!(control.lock().unwrap().incoming.is_none());
    let retry = r
        .due_connections(MonoTime(5), 2)
        .unwrap()
        .into_iter()
        .find(|v| v.peer == t.peer)
        .unwrap();
    let host = Host::new(retry, 2);
    let control = host.control.clone();
    let b = host.binding();
    r.attach(retry, host, MonoTime(5)).unwrap();
    // A current connection cannot deliver a prior connection's tagged batch.
    let mut old = b;
    old.generation = t.generation;
    control.lock().unwrap().incoming = Some(ReceivedBatch {
        connection: old,
        messages: vec![message(b.peer, b.local.node)],
    });
    assert_eq!(
        r.take_received(t.peer.node).unwrap_err().reason,
        PeerRosterError::ProviderViolation
    );
    assert!(r.is_fenced());
    assert_eq!(
        r.due_connections(MonoTime(5), 2),
        Err(PeerRosterError::Fenced)
    );
}
#[test]
fn malformed_peer_messages_and_capacity_cannot_enter_ingress() {
    for invalid in 0..4 {
        let mut r = roster(limits());
        let t = r.due_connections(MonoTime(0), 1).unwrap()[0];
        let host = Host::new(t, 1);
        let control = host.control.clone();
        let b = host.binding();
        r.attach(t, host, MonoTime(0)).unwrap();
        let mut messages = vec![message(b.peer, b.local.node)];
        match invalid {
            0 => messages[0].from = node(3),
            1 => messages[0].sender.session = StoreSession::new(99).unwrap(),
            2 => messages[0].to = node(3),
            _ => messages.reserve(100_000),
        }
        control.lock().unwrap().incoming = Some(ReceivedBatch {
            connection: b,
            messages,
        });
        let rejected = r.take_received(t.peer.node).unwrap_err();
        assert_eq!(rejected.reason, PeerRosterError::ProviderViolation);
        assert_eq!(rejected.batch.connection, b);
        assert!(r.is_fenced());
    }
}
#[test]
fn shutdown_drains_accepted_send_and_invalidates_pending_connection_ticket() {
    let mut r = roster(limits());
    let tickets = r.due_connections(MonoTime(0), 2).unwrap();
    r.attach(tickets[0], Host::new(tickets[0], 1), MonoTime(0))
        .unwrap();
    let mut q = queue();
    r.submit(send(&mut q, tickets[0].peer.node)).unwrap();
    r.close();
    assert_eq!(r.next_deadline(), None);
    assert!(!r.is_drained());
    assert_eq!(
        r.attach(tickets[1], Host::new(tickets[1], 1), MonoTime(0))
            .unwrap_err()
            .reason,
        PeerRosterError::Closed
    );
    r.poll(MonoTime(0), 2, TransportPollBudget::default())
        .unwrap();
    assert!(!r.is_drained());
    let done = r.take_send(tickets[0].peer.node).unwrap().unwrap();
    q.complete(done.batch, done.result).unwrap();
    assert!(r.is_drained());
    assert!(q.is_drained());
    assert_eq!(
        r.due_connections(MonoTime(0), 2),
        Err(PeerRosterError::Closed)
    );
    assert_eq!(
        r.poll(MonoTime(1), 2, TransportPollBudget::default())
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        r.poll(MonoTime(0), 2, TransportPollBudget::default()),
        Err(PeerRosterError::TimeWentBack)
    );
}

#[test]
fn invalid_send_completion_fences_without_releasing_queue_credits() {
    let mut r = roster(limits());
    let t = r.due_connections(MonoTime(0), 1).unwrap()[0];
    let host = Host::new(t, 1);
    host.control.lock().unwrap().wrong_completion = true;
    r.attach(t, host, MonoTime(0)).unwrap();
    let mut q = queue();
    let batch = send(&mut q, t.peer.node);
    let ticket = batch.ticket;
    r.submit(batch).unwrap();
    assert_eq!(
        r.discard_failed_send(ticket),
        Err(PeerRosterError::ProviderViolation)
    );
    r.poll(MonoTime(0), 2, TransportPollBudget::default())
        .unwrap();
    let rejected = r.take_send(t.peer.node).unwrap_err();
    assert_eq!(rejected.reason, PeerRosterError::ProviderViolation);
    assert!(r.is_fenced());
    assert_eq!(q.usage().batches, 1);
    assert_eq!(r.usage().connections, 1);
    // The host explicitly resolves uncertain delivery as Failed and abandons
    // only this fenced roster's correlation. It cannot resume the same owner.
    q.complete(rejected.send.batch, LocalSendResult::Failed)
        .unwrap();
    r.discard_failed_send(ticket).unwrap();
    assert_eq!(r.usage().connections, 0);
    assert_eq!(
        r.due_connections(MonoTime(0), 2),
        Err(PeerRosterError::Fenced)
    );
    r.abort();
    assert!(r.is_drained());
}

#[test]
fn construction_exhaustion_and_rejection_preserve_resources() {
    let invalid = PeerRoster::<Host>::new(
        config(limits()),
        [(node(1), identity(1))].into(),
        MonoTime(0),
    );
    assert!(matches!(invalid, Err(PeerRosterError::InvalidLimits)));
    let mut r = roster(limits());
    let mut q = queue();
    let batch = send(&mut q, node(3));
    let ticket = batch.ticket;
    let rejected = r.submit(batch).unwrap_err();
    assert_eq!(rejected.reason, PeerRosterError::Overloaded);
    assert_eq!(rejected.batch.ticket, ticket);
    q.complete(*rejected.batch, LocalSendResult::Failed)
        .unwrap();
    assert!(q.is_drained());
    assert_eq!(
        r.due_connections(MonoTime(u64::MAX), 2),
        Err(PeerRosterError::Exhausted)
    );
    assert_eq!(r.usage(), PeerRosterUsage::default());
    assert!(r.due_connections(MonoTime(u64::MAX), 0).unwrap().is_empty());
}

#[test]
fn outbound_priority_reordering_does_not_make_an_older_dispatch_stale() {
    let mut r = roster(limits());
    let t = r.due_connections(MonoTime(0), 1).unwrap()[0];
    r.attach(t, Host::new(t, 1), MonoTime(0)).unwrap();
    let mut q = queue();
    let older = send(&mut q, t.peer.node);
    let newer = send(&mut q, t.peer.node);
    assert!(newer.ticket.sequence > older.ticket.sequence);
    // A queue or host may prioritize a later control batch over older bulk.
    for batch in [newer, older] {
        r.submit(batch).unwrap();
        r.poll(MonoTime(0), 2, TransportPollBudget::default())
            .unwrap();
        let done = r.take_send(t.peer.node).unwrap().unwrap();
        q.complete(done.batch, done.result).unwrap();
    }
    assert!(q.is_drained());
}

#[test]
fn changed_provider_limits_are_retired_before_further_external_polling() {
    let mut r = roster(limits());
    let ticket = r.due_connections(MonoTime(0), 1).unwrap()[0];
    let host = Host::new(ticket, 1);
    let control = host.control.clone();
    r.attach(ticket, host, MonoTime(0)).unwrap();
    control.lock().unwrap().limits.receive_frame_bytes *= 2;
    let events = r
        .poll(MonoTime(0), 2, TransportPollBudget::default())
        .unwrap();
    assert_eq!(
        events,
        vec![PeerPoll::Disconnected {
            peer: ticket.peer.node,
            error: Some(TransportError::ProviderViolation)
        }]
    );
    assert_eq!(control.lock().unwrap().polls, 0);
    assert_eq!(r.usage().reserved_bytes, 0);
}

#[test]
fn drained_roster_hands_off_fresh_generations_and_rejects_old_ready_work() {
    let mut old = roster(limits());
    let ticket = old.due_connections(MonoTime(0), 1).unwrap()[0];
    assert_eq!(old.reclaim_generation(), Err(PeerRosterError::Overloaded));
    old.abort();
    let first_generation = old.reclaim_generation().unwrap();
    assert!(first_generation > ticket.generation);
    let mut new = PeerRoster::<Host>::new(
        PeerRosterConfig {
            first_generation,
            ..config(limits())
        },
        [(ticket.peer.node, ticket.peer.store)].into(),
        MonoTime(0),
    )
    .unwrap();
    let current = new.due_connections(MonoTime(0), 1).unwrap()[0];
    assert_eq!(current.generation, first_generation);
    assert_eq!(
        new.attach(ticket, Host::new(ticket, 1), MonoTime(0))
            .unwrap_err()
            .reason,
        PeerRosterError::StaleConnection
    );
    assert_eq!(new.usage().connecting, 1);
    new.attach(current, Host::new(current, 1), MonoTime(0))
        .unwrap();
}

#[test]
fn generation_range_exhaustion_never_hides_partially_issued_tickets() {
    let last = SecureSessionGeneration::new(u64::MAX).unwrap();
    let mut r = PeerRoster::<Host>::new(
        PeerRosterConfig {
            first_generation: last,
            last_generation: last,
            ..config(limits())
        },
        [(node(2), identity(2)), (node(3), identity(3))].into(),
        MonoTime(0),
    )
    .unwrap();
    let issued = r.due_connections(MonoTime(0), 2).unwrap();
    assert_eq!(issued.len(), 1);
    assert_eq!(r.usage().connecting, 1);
    let ticket = issued[0];
    assert_eq!(ticket.generation, last);
    r.abort();
    assert_eq!(r.reclaim_generation(), Err(PeerRosterError::Exhausted));
}
