// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual TLS close_notify preserves accepted capture; identity changes do not.
use super::{channels::*, support::*};
use std::{
    cell::Cell,
    collections::BTreeMap,
    rc::Rc,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    native::{connect::NativePeerProtocol, relay::*},
    outbound::LocalSendResult,
    runtime::MonoTime,
    secure::*,
};

struct Observed {
    actual: Box<dyn SecureSession>,
    state: Rc<Cell<SessionState>>,
    changed: Rc<Cell<bool>>,
}
impl SecureSession for Observed {
    fn security(&self) -> SessionSecurity {
        self.actual.security()
    }
    fn state(&self) -> SessionState {
        self.actual.state()
    }
    fn binding(&self) -> Option<SessionBinding> {
        self.actual.binding().map(|mut binding| {
            if self.changed.get() {
                binding.generation = SecureSessionGeneration::new(2).unwrap();
            }
            binding
        })
    }
    fn limits(&self) -> SessionLimits {
        self.actual.limits()
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<SessionProgress, SessionError> {
        let result = self.actual.poll(now, budget);
        self.state.set(self.actual.state());
        result
    }
    fn read_plaintext(&mut self, b: &mut [u8]) -> Result<usize, SessionError> {
        self.actual.read_plaintext(b)
    }
    fn write_plaintext(&mut self, b: &[u8]) -> Result<usize, SessionError> {
        self.actual.write_plaintext(b)
    }
    fn is_flushed(&self) -> bool {
        self.actual.is_flushed()
    }
    fn drain_status(&mut self) -> SessionDrainStatus {
        self.actual.drain_status()
    }
    fn close(&mut self) {
        self.actual.close();
    }
    fn revoke(&mut self) {
        self.actual.revoke();
    }
}
fn local(n: u64) -> LocalIdentity {
    LocalIdentity {
        node: node(n),
        store: StoreBinding {
            identity: identity(u128::from(n)),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn helper_plan() -> RelayHelperPlan {
    RelayHelperPlan::new(
        local(9),
        BTreeMap::from([(node(1), identity(1)), (node(2), identity(2))]),
        8,
    )
    .unwrap()
}
fn voter_plan() -> NativeRelayPlan {
    NativeRelayPlan::new(
        local(2),
        PeerIdentity {
            node: node(9),
            store: identity(9),
        },
        BTreeMap::from([(node(1), identity(1)), (node(2), identity(2))]),
        8,
    )
    .unwrap()
}
fn original(clock: Instant) -> RelayFrame {
    let (mut leaf, helper, _) = pair(NativePeerProtocol::TcpTls, local(1), local(9), 1, clock);
    let mut hop = RelayHop::for_helper(helper, helper_plan(), Default::default())
        .unwrap_or_else(|e| panic!("original {:?}", e.reason));
    let frame = hop.source_opened().unwrap();
    hop.abort();
    assert!(hop.into_session().is_ok());
    leaf.revoke();
    frame
}
fn now(clock: Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
fn deadline(end: Instant) {
    assert!(Instant::now() < end, "actual TLS closure did not progress");
    std::thread::park_timeout(Duration::from_millis(1));
}
#[test]
fn actual_tls_buffered_original_survives_clean_close_without_new_admission() {
    let clock = Instant::now();
    let frame = original(clock);
    let capture = frame.source_capture().unwrap();
    let (leaf, helper, _) = pair(NativePeerProtocol::TcpTls, local(2), local(9), 1, clock);
    let state = Rc::new(Cell::new(SessionState::Ready));
    let observed = Observed {
        actual: leaf,
        state: state.clone(),
        changed: Rc::new(Cell::new(false)),
    };
    let mut receiver = RelayHop::for_voter(observed, voter_plan(), Default::default())
        .unwrap_or_else(|e| panic!("receiver {:?}", e.reason));
    let mut sender = RelayHop::for_helper(helper, helper_plan(), Default::default())
        .unwrap_or_else(|e| panic!("sender {:?}", e.reason));
    let ticket = sender
        .submit(frame)
        .unwrap_or_else(|e| panic!("submit {:?}", e.reason));
    let end = Instant::now() + Duration::from_secs(5);
    let original = loop {
        sender.poll(now(clock), Default::default()).unwrap();
        if let Some(done) = sender.take_send() {
            break done;
        }
        deadline(end);
    };
    assert_eq!(original.ticket, ticket);
    assert_eq!(original.result, LocalSendResult::Sent);
    sender.close();
    while !sender.is_drained() {
        sender.poll(now(clock), Default::default()).unwrap();
        deadline(end);
    }
    let mut session = sender
        .into_session()
        .unwrap_or_else(|_| panic!("sender not drained"));
    while receiver.received().is_none() {
        receiver.poll(now(clock), Default::default()).unwrap();
        deadline(end);
    }
    assert!(matches!(
        state.get(),
        SessionState::Closing | SessionState::Closed
    ));
    assert!(receiver.authenticated_binding().is_none());
    assert!(!receiver.can_submit());
    let received = receiver.take_received().unwrap();
    assert_eq!(received.frame().source_capture(), Some(capture));
    assert_eq!(received.ticket().connection, receiver.binding());
    assert_eq!(received.ticket().sequence, 1);
    assert_eq!(received.source_view().unwrap().source(), local(1));
    assert_eq!(
        receiver.submit(original.frame).unwrap_err().reason,
        RelayNetworkError::Closed
    );
    assert_eq!(
        receiver.poll(now(clock), Default::default()),
        Err(RelayNetworkError::Truncated)
    );
    assert!(receiver.is_drained());
    assert!(receiver.into_session().is_ok());
    session.as_mut().unwrap().revoke();
}
#[test]
fn changed_actual_tls_binding_refuses_and_returns_the_original_owned_frame() {
    let clock = Instant::now();
    let frame = original(clock);
    let capture = frame.source_capture();
    let (mut leaf, helper, _) = pair(NativePeerProtocol::TcpTls, local(2), local(9), 1, clock);
    let changed = Rc::new(Cell::new(false));
    // Trusted host replacement contract violation over a genuine ready TLS
    // channel: no fake cryptography, catalog-derived origin or accepted relabel.
    let observed = Observed {
        actual: helper.actual,
        state: Rc::new(Cell::new(SessionState::Ready)),
        changed: changed.clone(),
    };
    let mut hop = RelayHop::for_helper(observed, helper_plan(), Default::default())
        .unwrap_or_else(|e| panic!("original helper {:?}", e.reason));
    let binding = hop.binding();
    let ticket = hop
        .submit(frame)
        .unwrap_or_else(|e| panic!("original submit {:?}", e.reason));
    changed.set(true);
    assert_eq!(
        hop.poll(now(clock), Default::default()),
        Err(RelayNetworkError::WrongBinding)
    );
    assert_eq!(hop.binding(), binding);
    assert!(hop.authenticated_binding().is_none());
    assert!(hop.received().is_none());
    let done = hop.take_send().unwrap();
    assert_eq!(done.ticket, ticket);
    assert_eq!(done.ticket.connection, binding);
    assert_eq!(done.result, LocalSendResult::Failed);
    assert_eq!(done.frame.source_capture(), capture);
    assert!(hop.is_drained());
    assert!(hop.into_session().is_ok());
    leaf.revoke();
}
