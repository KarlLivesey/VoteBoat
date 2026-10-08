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
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};
use support::outbound::HostOutbound;
use voteboat::{connect::*, outbound::*, secure::*, transport::*};
fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: node(id),
        store: StoreBinding {
            identity: identity(id as u128),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
struct Session {
    binding: SessionBinding,
    drops: Rc<Cell<usize>>,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}
impl SecureSession for Session {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        SessionState::Ready
    }
    fn binding(&self) -> Option<SessionBinding> {
        Some(self.binding)
    }
    fn limits(&self) -> SessionLimits {
        SessionLimits::default()
    }
    fn poll(&mut self, _: MonoTime, b: SessionPollBudget) -> Result<SessionProgress, SessionError> {
        b.validate()?;
        Ok(SessionProgress::default())
    }
    fn read_plaintext(&mut self, _: &mut [u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn write_plaintext(&mut self, _: &[u8]) -> Result<usize, SessionError> {
        Err(SessionError::WouldBlock)
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {}
    fn revoke(&mut self) {}
}
#[derive(Default)]
struct ConnectControl {
    pending: Vec<ConnectRequest<()>>,
    submitted: Vec<ConnectTicket>,
    cancelled: Vec<ConnectTicket>,
    ready: bool,
    closed: bool,
    wrong: bool,
    polls: usize,
    drops: Rc<Cell<usize>>,
}
struct Connector(Rc<RefCell<ConnectControl>>);
impl PeerConnector for Connector {
    type Endpoint = ();
    type Session = Session;
    fn local(&self) -> LocalIdentity {
        local(1)
    }
    fn limits(&self) -> ConnectLimits {
        ConnectLimits {
            requests: 2,
            anonymous: 0,
            timeout_ms: 100,
        }
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: self.0.borrow().pending.len(),
            ..Default::default()
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        None
    } // Host wakes explicit completion availability.
    fn submit(&mut self, r: ConnectRequest<()>, _: MonoTime) -> Result<(), ConnectRejected<()>> {
        let mut c = self.0.borrow_mut();
        if c.closed || c.pending.len() == 2 {
            return Err(ConnectRejected {
                reason: if c.closed {
                    ConnectError::Closed
                } else {
                    ConnectError::Overloaded
                },
                request: Box::new(r),
            });
        }
        c.submitted.push(r.ticket);
        c.pending.push(r);
        Ok(())
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        let mut c = self.0.borrow_mut();
        if c.pending.iter().any(|r| r.ticket == ticket) {
            c.cancelled.push(ticket);
            true
        } else {
            false
        }
    }
    fn poll(
        &mut self,
        _: MonoTime,
        b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Session>>, ConnectError> {
        b.validate()?;
        let mut c = self.0.borrow_mut();
        c.polls += 1;
        if !c.ready {
            return Ok(vec![]);
        }
        let count = c.pending.len().min(b.completions);
        let requests = c.pending.drain(..count).collect::<Vec<_>>();
        Ok(requests
            .into_iter()
            .map(|r| {
                let mut ticket = r.ticket;
                if c.wrong {
                    ticket.generation = SecureSessionGeneration::new(999).unwrap();
                }
                ConnectCompletion {
                    ticket,
                    result: if c.closed {
                        Err(ConnectError::Cancelled)
                    } else {
                        Ok(Session {
                            binding: SessionBinding {
                                local: ticket.local,
                                peer: local(ticket.peer.node.get()),
                                generation: ticket.generation,
                                wire_version: 1,
                            },
                            drops: c.drops.clone(),
                        })
                    },
                }
            })
            .collect())
    }
    fn close(&mut self) {
        self.0.borrow_mut().closed = true;
    }
}
struct TransportControl {
    binding: SessionBinding,
    state: TransportState,
    blocked: bool,
    wrong: bool,
    incoming: Option<ReceivedBatch>,
}
struct Transport {
    session: Session,
    control: Rc<RefCell<TransportControl>>,
    sending: Option<OutboundBatch>,
    done: Option<TransportSend>,
}
impl PeerTransport for Transport {
    fn security(&self) -> SessionSecurity {
        self.session.security()
    }
    fn binding(&self) -> SessionBinding {
        self.control.borrow().binding
    }
    fn state(&self) -> TransportState {
        self.control.borrow().state
    }
    fn limits(&self) -> TransportLimits {
        TransportLimits::default()
    }
    fn usage(&self) -> TransportUsage {
        TransportUsage {
            sending: self.sending.is_some(),
            completion: self.done.is_some(),
            decoded_bytes: self.received_info().map_or(0, |i| i.bytes),
            ..Default::default()
        }
    }
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
        if self.sending.is_some() || self.done.is_some() || self.state() != TransportState::Open {
            Err(TransportRejected {
                reason: TransportError::Overloaded,
                batch: Box::new(batch),
            })
        } else {
            self.sending = Some(batch);
            Ok(())
        }
    }
    fn poll(
        &mut self,
        _: MonoTime,
        b: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError> {
        b.validate()?;
        let mut c = self.control.borrow_mut();
        if c.blocked {
            return Ok(TransportProgress::default());
        }
        if let Some(batch) = self.sending.take() {
            let mut connection = c.binding;
            if c.wrong {
                connection.generation = SecureSessionGeneration::new(999).unwrap();
            }
            self.done = Some(TransportSend {
                connection,
                batch,
                result: LocalSendResult::Sent,
            });
        }
        if c.state == TransportState::Draining {
            c.state = TransportState::Closed;
        }
        Ok(TransportProgress::default())
    }
    fn take_send(&mut self) -> Option<TransportSend> {
        self.done.take()
    }
    fn received_info(&self) -> Option<ReceiveInfo> {
        self.control
            .borrow()
            .incoming
            .as_ref()
            .map(|b| b.info(self.limits().decoded_bytes).unwrap())
    }
    fn take_received(&mut self) -> Option<ReceivedBatch> {
        self.control.borrow_mut().incoming.take()
    }
    fn close(&mut self) {
        self.control.borrow_mut().state = TransportState::Draining;
    }
    fn abort(&mut self) {
        self.control.borrow_mut().state = TransportState::Failed;
        if let Some(batch) = self.sending.take() {
            self.done = Some(TransportSend {
                connection: self.binding(),
                batch,
                result: LocalSendResult::Failed,
            });
        }
    }
}
type Controls = Rc<RefCell<BTreeMap<NodeId, Rc<RefCell<TransportControl>>>>>;
struct Factory {
    controls: Controls,
    bad: bool,
}
impl PeerTransportFactory<Session> for Factory {
    type Transport = Transport;
    fn build<O: OutboundQueue>(
        &mut self,
        session: Session,
        outbound: &O,
    ) -> Result<Transport, TransportError> {
        assert_eq!(session.binding.local.store, outbound.binding().store);
        let mut binding = session.binding;
        if self.bad {
            binding.generation = SecureSessionGeneration::new(888).unwrap();
        }
        let control = Rc::new(RefCell::new(TransportControl {
            binding,
            state: TransportState::Open,
            blocked: false,
            wrong: false,
            incoming: None,
        }));
        self.controls
            .borrow_mut()
            .insert(binding.peer.node, control.clone());
        Ok(Transport {
            session,
            control,
            sending: None,
            done: None,
        })
    }
}
type Driver = PeerDriver<Connector, Factory>;
struct Fixture {
    owner: Owner,
    outbound: HostOutbound,
    driver: Option<Driver>,
    connect: Rc<RefCell<ConnectControl>>,
    transports: Controls,
}
impl Fixture {
    fn parts(&self) -> PeerParts<Connector, Factory> {
        PeerParts {
            connector: Connector(self.connect.clone()),
            factory: Factory {
                controls: self.transports.clone(),
                bad: false,
            },
            roster: PeerRoster::new(
                PeerRosterConfig {
                    local: local(1),
                    outbound: self.outbound.binding(),
                    first_generation: SecureSessionGeneration::new(1).unwrap(),
                    last_generation: SecureSessionGeneration::new(100).unwrap(),
                    wire_version: 1,
                    limits: PeerRosterLimits {
                        peers: 2,
                        connecting: 2,
                        connect_timeout_ms: 10,
                        retry_min_ms: 1,
                        retry_max_ms: 4,
                        ..Default::default()
                    },
                    transport_limits: TransportLimits::default(),
                },
                [(node(2), identity(2)), (node(3), identity(3))].into(),
                MonoTime(0),
            )
            .unwrap(),
            ingress: IngressRouter::new(
                IngressBinding {
                    owner: self.owner.identity(),
                    local: local(1),
                    generation: IngressGeneration::new(1).unwrap(),
                },
                IngressLimits::default(),
            )
            .unwrap(),
            routes: [
                (node(2), ConnectDirection::Accept),
                (node(3), ConnectDirection::Dial(())),
            ]
            .into(),
        }
    }
    fn new() -> Self {
        let (owner, _) = single(1);
        let mut limits = OutboundLimits::default();
        limits.node.max.batches = 8;
        limits.node.control.batches = 1;
        limits.node.background.batches = 1;
        limits.peer.max.batches = 4;
        limits.peer.control.batches = 1;
        limits.peer.background.batches = 1;
        let outbound = HostOutbound::new(
            OutboundBinding {
                node: node(1),
                store: owner.identity().store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            limits,
        )
        .unwrap();
        let mut f = Self {
            owner,
            outbound,
            driver: None,
            connect: Default::default(),
            transports: Default::default(),
        };
        f.driver = Some(
            PeerDriver::new(
                f.parts(),
                f.owner.identity(),
                &f.outbound,
                PeerDriverLimits {
                    staged_batches: 8,
                    ..Default::default()
                },
                MonoTime(0),
            )
            .unwrap_or_else(|r| panic!("{:?}", r.reason)),
        );
        f
    }
    fn poll(&mut self, now: u64) -> Result<PeerDriverProgress, PeerDriverError> {
        self.driver.as_mut().unwrap().poll(
            &mut self.owner,
            &mut self.outbound,
            MonoTime(now),
            PeerDriverBudget::default(),
        )
    }
    fn attach(&mut self) {
        self.connect.borrow_mut().ready = true;
        self.poll(0).unwrap();
        assert_eq!(self.poll(0).unwrap().connections, 2);
    }
    fn enqueue(&mut self, peer: u64) -> SendTicket {
        self.outbound.submit(vec![message(1, peer)]).unwrap()
    }
    fn close(&mut self, now: u64) {
        self.driver.as_mut().unwrap().close();
        for _ in 0..10 {
            self.poll(now).unwrap();
            if self.driver.as_ref().unwrap().is_drained() {
                return;
            }
        }
        panic!("not drained");
    }
}
fn message(from: u64, to: u64) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(from),
        sender: local(from).store,
        to: node(to),
        term: 1,
        context: RequestContext {
            origin: local(from).store,
            sequence: 1,
        },
        rpc: Rpc::ReadProbe,
    }
}
#[test]
fn public_host_reactor_attaches_and_keeps_blocked_peer_credits_while_other_peer_and_ingress_progress(
) {
    let mut f = Fixture::new();
    f.attach();
    f.transports.borrow()[&node(2)].borrow_mut().blocked = true;
    f.enqueue(2);
    f.enqueue(2);
    let other = f.enqueue(3);
    let p = f.poll(0).unwrap();
    assert_eq!(p.sends, 2);
    assert_eq!(f.outbound.usage().batches, 3);
    let control = f.transports.borrow()[&node(3)].clone();
    let binding = control.borrow().binding;
    control.borrow_mut().incoming = Some(ReceivedBatch {
        connection: binding,
        messages: vec![message(3, 1)],
    });
    let p = f.poll(0).unwrap();
    assert!(p
        .completions
        .iter()
        .any(|c| c.ticket == other && c.result == LocalSendResult::Sent));
    assert_eq!(p.received, 1);
    assert_eq!(p.ingress.admitted, 1);
    assert_eq!(f.owner.core(group(1)).unwrap().state().commit_index, 0);
    assert_eq!(f.outbound.usage().batches, 2);
    f.transports.borrow()[&node(2)].borrow_mut().blocked = false;
    for _ in 0..4 {
        f.poll(0).unwrap();
    }
    assert!(f.outbound.is_drained());
    f.close(0);
}
#[test]
fn expired_attempt_retains_provider_slot_and_late_session_cannot_attach_or_spend_replacement_generation(
) {
    let mut f = Fixture::new();
    f.poll(0).unwrap();
    let originals = f.connect.borrow().submitted.clone();
    f.poll(11).unwrap();
    assert_eq!(f.connect.borrow().cancelled.len(), 2);
    for _ in 0..5 {
        f.poll(20).unwrap();
    }
    assert_eq!(f.connect.borrow().submitted, originals);
    assert_eq!(f.driver.as_ref().unwrap().usage().attempts, 2);
    assert_eq!(f.driver.as_ref().unwrap().next_deadline(), None);
    f.connect.borrow_mut().ready = true;
    let p = f.poll(20).unwrap();
    assert_eq!(p.obsolete_connections, 2);
    assert_eq!(p.connections, 0);
    assert_eq!(f.connect.borrow().drops.get(), 2);
    assert_eq!(f.poll(20).unwrap().connections, 2);
    for t in originals {
        assert!(
            f.driver
                .as_ref()
                .unwrap()
                .roster()
                .binding(t.peer.node)
                .unwrap()
                .generation
                > t.generation
        );
    }
    f.close(20);
}
#[test]
fn explicit_disconnect_cancels_exact_attempt_and_other_peer_can_attach() {
    let mut f = Fixture::new();
    f.poll(0).unwrap();
    let old = f.connect.borrow().submitted[0];
    f.driver
        .as_mut()
        .unwrap()
        .disconnect(old.peer.node, MonoTime(1))
        .unwrap();
    assert_eq!(f.connect.borrow().cancelled, [old]);
    f.poll(2).unwrap();
    assert_eq!(f.connect.borrow().submitted.len(), 2);
    f.connect.borrow_mut().ready = true;
    let p = f.poll(2).unwrap();
    assert_eq!(p.connections, 1);
    assert_eq!(p.obsolete_connections, 1);
    assert_eq!(f.poll(2).unwrap().connections, 1);
    assert!(
        f.driver
            .as_ref()
            .unwrap()
            .roster()
            .binding(old.peer.node)
            .unwrap()
            .generation
            > old.generation
    );
    f.close(2);
}
#[test]
fn construction_returns_selected_parts_on_route_or_budget_failure_without_closing_host_resources() {
    let f = Fixture::new();
    let mut parts = f.parts();
    parts.routes.remove(&node(2));
    let mut rejected = match PeerDriver::new(
        parts,
        f.owner.identity(),
        &f.outbound,
        PeerDriverLimits::default(),
        MonoTime(0),
    ) {
        Err(r) => r,
        Ok(_) => panic!("accepted missing route"),
    };
    assert_eq!(rejected.reason, PeerDriverError::WrongBinding);
    assert!(!f.connect.borrow().closed);
    rejected
        .parts
        .routes
        .insert(node(2), ConnectDirection::Accept);
    let rejected = match PeerDriver::new(
        *rejected.parts,
        f.owner.identity(),
        &f.outbound,
        PeerDriverLimits {
            metadata_bytes: 0,
            ..Default::default()
        },
        MonoTime(0),
    ) {
        Err(r) => r,
        Ok(_) => panic!("accepted zero budget"),
    };
    assert_eq!(rejected.reason, PeerDriverError::InvalidLimits);
    assert!(!f.connect.borrow().closed);
    let _driver = PeerDriver::new(
        *rejected.parts,
        f.owner.identity(),
        &f.outbound,
        PeerDriverLimits::default(),
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
}
#[test]
fn binding_time_and_budget_errors_do_not_poll_provider_or_fence_owner() {
    let mut f = Fixture::new();
    f.poll(5).unwrap();
    let polls = f.connect.borrow().polls;
    assert_eq!(f.poll(4).unwrap_err(), PeerDriverError::TimeWentBack);
    let mut wrong = HostOutbound::new(
        OutboundBinding {
            generation: OutboundGeneration::new(2).unwrap(),
            ..f.outbound.binding()
        },
        OutboundLimits::default(),
    )
    .unwrap();
    assert_eq!(
        f.driver
            .as_mut()
            .unwrap()
            .poll(
                &mut f.owner,
                &mut wrong,
                MonoTime(5),
                PeerDriverBudget::default()
            )
            .unwrap_err(),
        PeerDriverError::WrongBinding
    );
    assert_eq!(
        f.driver
            .as_mut()
            .unwrap()
            .poll(
                &mut f.owner,
                &mut f.outbound,
                MonoTime(5),
                PeerDriverBudget {
                    peer_visits: 4097,
                    ..Default::default()
                }
            )
            .unwrap_err(),
        PeerDriverError::InvalidLimits
    );
    assert_eq!(f.connect.borrow().polls, polls);
    assert!(!f.owner.is_failed());
    f.connect.borrow_mut().ready = true;
    f.poll(5).unwrap();
    f.close(5);
}
#[test]
fn mis_scoped_connector_receipt_fences_and_closes_returned_session_for_recovery() {
    let mut f = Fixture::new();
    f.poll(0).unwrap();
    f.connect.borrow_mut().ready = true;
    f.connect.borrow_mut().wrong = true;
    assert_eq!(f.poll(0).unwrap_err(), PeerDriverError::ProviderViolation);
    assert!(f.owner.is_failed());
    assert!(f.connect.borrow().closed);
    assert_eq!(f.connect.borrow().drops.get(), 2);
    let recovery = f
        .driver
        .take()
        .unwrap()
        .into_recovery()
        .unwrap_or_else(|_| panic!("not failed"));
    assert_eq!(recovery.attempts.len(), 2);
    assert!(recovery.staged.is_empty());
}
#[test]
fn mis_scoped_transport_completion_keeps_exact_queue_payload_until_explicit_recovery() {
    let mut f = Fixture::new();
    f.attach();
    let ticket = f.enqueue(2);
    f.poll(0).unwrap();
    f.transports.borrow()[&node(2)].borrow_mut().wrong = true;
    assert!(matches!(
        f.poll(0),
        Err(PeerDriverError::Roster(PeerRosterError::ProviderViolation))
    ));
    assert!(f.owner.is_failed());
    assert_eq!(f.outbound.usage().batches, 1);
    let mut recovery = f
        .driver
        .take()
        .unwrap()
        .into_recovery()
        .unwrap_or_else(|_| panic!("not failed"));
    let send = recovery.failed_send.take().unwrap();
    assert_eq!(send.batch.ticket, ticket);
    f.outbound
        .complete(send.batch, LocalSendResult::Failed)
        .unwrap();
    recovery.parts.roster.discard_failed_send(ticket).unwrap();
    assert!(f.outbound.is_drained());
    assert!(recovery.parts.roster.is_drained());
}
#[test]
fn close_resolves_staged_work_but_waits_for_accepted_send_and_preserves_other_instance() {
    let mut f = Fixture::new();
    let mut other = Fixture::new();
    f.attach();
    other.attach();
    f.transports.borrow()[&node(2)].borrow_mut().blocked = true;
    let accepted = f.enqueue(2);
    let staged = f.enqueue(2);
    f.poll(0).unwrap();
    f.driver.as_mut().unwrap().close();
    let p = f.poll(0).unwrap();
    assert!(p
        .completions
        .iter()
        .any(|c| c.ticket == staged && c.result == LocalSendResult::Failed));
    assert!(!f.driver.as_ref().unwrap().is_drained());
    assert_eq!(f.outbound.usage().batches, 1);
    let ticket = other.enqueue(3);
    other.poll(0).unwrap();
    assert!(other
        .poll(0)
        .unwrap()
        .completions
        .iter()
        .any(|c| c.ticket == ticket));
    assert!(!other.connect.borrow().closed);
    f.transports.borrow()[&node(2)].borrow_mut().blocked = false;
    assert!(f
        .poll(0)
        .unwrap()
        .completions
        .iter()
        .any(|c| c.ticket == accepted && c.result == LocalSendResult::Sent));
    assert!(f.driver.as_ref().unwrap().is_drained());
    assert!(f.outbound.is_drained());
    let parts = f
        .driver
        .take()
        .unwrap()
        .into_parts()
        .unwrap_or_else(|_| panic!("not drained"));
    assert!(parts.connector.is_drained());
    assert!(!f.owner.is_failed());
    other.close(0);
}
#[test]
fn bounded_staging_covers_whole_outbound_queue_so_one_stalled_peer_cannot_pin_other_peer_admission()
{
    let mut f = Fixture::new();
    let parts = f.parts();
    assert!(matches!(
        PeerDriver::new(
            parts,
            f.owner.identity(),
            &f.outbound,
            PeerDriverLimits {
                staged_batches: 4,
                ..Default::default()
            },
            MonoTime(0)
        ),
        Err(PeerDriverRejected {
            reason: PeerDriverError::InvalidLimits,
            ..
        })
    ));
    f.attach();
    f.transports.borrow()[&node(2)].borrow_mut().blocked = true;
    for _ in 0..4 {
        f.enqueue(2);
    }
    f.poll(0).unwrap();
    for _ in 0..8 {
        let ticket = f.enqueue(3);
        f.poll(0).unwrap();
        let p = f.poll(0).unwrap();
        assert!(p.completions.iter().any(|c| c.ticket == ticket));
        assert_eq!(f.outbound.usage().batches, 4);
    }
    f.transports.borrow()[&node(2)].borrow_mut().blocked = false;
    for _ in 0..8 {
        f.poll(0).unwrap();
    }
    assert!(f.outbound.is_drained());
    f.close(0);
}
#[test]
fn invalid_factory_transport_is_aborted_and_fences_before_any_raft_or_send_progress() {
    let mut f = Fixture::new();
    let mut parts = f.parts();
    parts.factory.bad = true;
    f.driver = Some(
        PeerDriver::new(
            parts,
            f.owner.identity(),
            &f.outbound,
            PeerDriverLimits::default(),
            MonoTime(0),
        )
        .unwrap_or_else(|r| panic!("{:?}", r.reason)),
    );
    f.connect.borrow_mut().ready = true;
    f.poll(0).unwrap();
    assert_eq!(
        f.poll(0).unwrap_err(),
        PeerDriverError::Roster(PeerRosterError::WrongBinding)
    );
    assert!(f.owner.is_failed());
    assert!(f.outbound.is_drained());
    assert_eq!(f.connect.borrow().drops.get(), 2);
    assert_eq!(f.owner.core(group(1)).unwrap().state().commit_index, 0);
}

struct FaultQueue {
    inner: HostOutbound,
    oversize: bool,
    wrong_complete: bool,
    reject_complete: bool,
}
impl OutboundQueue for FaultQueue {
    fn binding(&self) -> OutboundBinding {
        self.inner.binding()
    }
    fn limits(&self) -> OutboundLimits {
        self.inner.limits()
    }
    fn usage(&self) -> OutboundUsage {
        self.inner.usage()
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        self.inner.peer_usage(peer)
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        self.inner.submit(messages)
    }
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch> {
        self.inner.poll(if self.oversize { 2 } else { limit })
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        if self.reject_complete {
            return Err(CompletionRejected {
                reason: OutboundError::NotDispatched,
                batch: Box::new(batch),
            });
        }
        self.inner.complete(batch, result).map(|mut done| {
            if self.wrong_complete {
                done.ticket.sequence += 100;
            }
            done
        })
    }
    fn close(&mut self) {
        self.inner.close();
    }
}
#[test]
fn oversized_outbound_poll_is_quarantined_whole_with_credits_until_explicit_recovery() {
    let mut f = Fixture::new();
    f.attach();
    let a = f.enqueue(2);
    let b = f.enqueue(3);
    let mut queue = FaultQueue {
        inner: f.outbound,
        oversize: true,
        wrong_complete: false,
        reject_complete: false,
    };
    assert_eq!(
        f.driver
            .as_mut()
            .unwrap()
            .poll(
                &mut f.owner,
                &mut queue,
                MonoTime(0),
                PeerDriverBudget::default()
            )
            .unwrap_err(),
        PeerDriverError::ProviderViolation
    );
    assert!(f.owner.is_failed());
    assert_eq!(queue.usage().batches, 2);
    let recovery = f
        .driver
        .take()
        .unwrap()
        .into_recovery()
        .unwrap_or_else(|_| panic!("not failed"));
    assert_eq!(
        recovery
            .quarantined
            .iter()
            .map(|b| b.ticket)
            .collect::<Vec<_>>(),
        [a, b]
    );
    for batch in recovery.quarantined {
        queue.complete(batch, LocalSendResult::Failed).unwrap();
    }
    assert!(queue.is_drained());
}
#[test]
fn wrong_outbound_terminal_ticket_fences_instead_of_publishing_unscoped_local_completion() {
    let mut f = Fixture::new();
    f.attach();
    f.enqueue(2);
    f.poll(0).unwrap();
    let mut queue = FaultQueue {
        inner: f.outbound,
        oversize: false,
        wrong_complete: true,
        reject_complete: false,
    };
    assert_eq!(
        f.driver
            .as_mut()
            .unwrap()
            .poll(
                &mut f.owner,
                &mut queue,
                MonoTime(0),
                PeerDriverBudget::default()
            )
            .unwrap_err(),
        PeerDriverError::ProviderViolation
    );
    assert!(f.owner.is_failed());
    assert!(queue.is_drained());
    assert_eq!(f.owner.core(group(1)).unwrap().state().commit_index, 0);
}
#[test]
fn rejected_queue_completion_returns_original_payload_and_holds_credit_for_recovery() {
    let mut f = Fixture::new();
    f.attach();
    let ticket = f.enqueue(2);
    f.poll(0).unwrap();
    let mut queue = FaultQueue {
        inner: f.outbound,
        oversize: false,
        wrong_complete: false,
        reject_complete: true,
    };
    assert_eq!(
        f.driver
            .as_mut()
            .unwrap()
            .poll(
                &mut f.owner,
                &mut queue,
                MonoTime(0),
                PeerDriverBudget::default()
            )
            .unwrap_err(),
        PeerDriverError::Outbound(OutboundError::NotDispatched)
    );
    assert!(f.owner.is_failed());
    assert_eq!(queue.usage().batches, 1);
    let mut recovery = f
        .driver
        .take()
        .unwrap()
        .into_recovery()
        .unwrap_or_else(|_| panic!("not failed"));
    let send = recovery.failed_send.take().unwrap();
    assert_eq!(send.batch.ticket, ticket);
    queue.reject_complete = false;
    queue.complete(send.batch, LocalSendResult::Failed).unwrap();
    assert!(queue.is_drained());
}
#[test]
fn closing_connecting_driver_waits_for_actual_provider_receipts_before_reclaim() {
    let mut f = Fixture::new();
    f.poll(0).unwrap();
    f.driver.as_mut().unwrap().close();
    f.poll(1).unwrap();
    assert_eq!(f.driver.as_ref().unwrap().usage().attempts, 2);
    assert!(!f.driver.as_ref().unwrap().is_drained());
    let driver = f.driver.take().unwrap();
    f.driver = Some(*driver.into_parts().err().expect("premature reclaim"));
    f.connect.borrow_mut().ready = true;
    assert_eq!(f.poll(1).unwrap().obsolete_connections, 2);
    assert!(f.driver.as_ref().unwrap().is_drained());
    assert_eq!(f.connect.borrow().submitted.len(), 2);
    assert_eq!(f.connect.borrow().drops.get(), 0);
}
#[test]
fn closing_network_resolves_queued_outbound_on_poll_without_closing_host_queue() {
    let mut f = Fixture::new();
    let ticket = f.enqueue(2);
    f.driver.as_mut().unwrap().close();
    assert_eq!(f.outbound.usage().batches, 1);
    let p = f.poll(0).unwrap();
    assert!(p
        .completions
        .iter()
        .any(|c| c.ticket == ticket && c.result == LocalSendResult::Failed));
    assert!(f.outbound.is_drained());
    assert!(f.driver.as_ref().unwrap().is_drained());
    // The selected queue belongs to the host; shutdown does not close it.
    f.enqueue(3);
    assert_eq!(f.outbound.usage().batches, 1);
    f.poll(0).unwrap();
    assert!(f.outbound.is_drained());
}
#[test]
fn failed_local_owner_can_drain_network_and_cancel_held_ingress_without_core_access() {
    let mut f = Fixture::new();
    f.attach();
    let control = f.transports.borrow()[&node(3)].clone();
    let binding = control.borrow().binding;
    control.borrow_mut().incoming = Some(ReceivedBatch {
        connection: binding,
        messages: vec![message(3, 1)],
    });
    f.driver
        .as_mut()
        .unwrap()
        .poll(
            &mut f.owner,
            &mut f.outbound,
            MonoTime(0),
            PeerDriverBudget {
                ingress: 0,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(f.driver.as_ref().unwrap().ingress().usage().messages, 1);
    let ticket = f.enqueue(2);
    f.poll_with_no_ingress();
    let id = f.owner.identity();
    let execution = ReadRouter::new(
        ReadRouterBinding {
            owner: id,
            generation: ReadRouterGeneration::new(1).unwrap(),
        },
        ReadRouterLimits::default(),
    )
    .unwrap();
    let mut reads = ReadRequests::<(), i64>::new(
        ReadInvocationBinding {
            owner: id,
            generation: ReadInvocationGeneration::new(1).unwrap(),
        },
        ReadInvocationLimits::default(),
        execution,
    )
    .unwrap();
    reads.abort(&mut f.owner).unwrap();
    assert_eq!(f.poll(0).unwrap_err(), PeerDriverError::Fenced);
    assert!(!f.driver.as_ref().unwrap().is_failed());
    f.driver.as_mut().unwrap().close();
    let p = f
        .driver
        .as_mut()
        .unwrap()
        .drain(&mut f.outbound, MonoTime(0), PeerDriverBudget::default())
        .unwrap();
    assert!(p.completions.iter().any(|c| c.ticket == ticket));
    assert_eq!(p.ingress.admitted, 0);
    assert_eq!(p.ingress.discarded, 1);
    assert_eq!(p.ingress.completed[0].end, IngressEnd::Cancelled);
    assert!(f.driver.as_ref().unwrap().is_drained());
    assert!(f.outbound.is_drained());
    assert!(f.owner.is_failed());
}
impl Fixture {
    fn poll_with_no_ingress(&mut self) {
        self.driver
            .as_mut()
            .unwrap()
            .poll(
                &mut self.owner,
                &mut self.outbound,
                MonoTime(0),
                PeerDriverBudget {
                    ingress: 0,
                    ..Default::default()
                },
            )
            .unwrap();
    }
}
