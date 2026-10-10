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
use std::{cell::RefCell, net::SocketAddr, rc::Rc};
use voteboat::{
    connect::*, discovery::*, identity::*, runtime::MonoTime, secure::*, transport::ConnectTicket,
};
#[path = "discovery/driven.rs"]
mod driven;
fn peer(n: u64) -> PeerIdentity {
    PeerIdentity {
        node: NodeId::new(n).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(n as u128).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    }
}
fn local() -> LocalIdentity {
    LocalIdentity {
        node: peer(1).node,
        store: StoreBinding {
            identity: peer(1).store,
            session: StoreSession::new(1).unwrap(),
        },
    }
}
fn hint(n: u64, gen: u64) -> PeerEndpointHint {
    PeerEndpointHint {
        peer: peer(n),
        generation: HintGeneration::new(gen).unwrap(),
        endpoint: "127.0.0.1:1234".parse().unwrap(),
        expires_at: MonoTime(100),
    }
}
fn request(gen: u64) -> ConnectRequest<SocketAddr> {
    ConnectRequest {
        ticket: ConnectTicket {
            local: local(),
            peer: peer(2),
            generation: SecureSessionGeneration::new(gen).unwrap(),
        },
        direction: ConnectDirection::Dial("127.0.0.1:9999".parse().unwrap()),
        deadline: MonoTime(50),
    }
}
#[derive(Clone)]
struct HostDiscovery {
    hint: PeerEndpointHint,
    closed: bool,
    invalidated: Rc<RefCell<Vec<HintGeneration>>>,
}
impl PeerDiscovery for HostDiscovery {
    fn resolve(
        &mut self,
        _: PeerIdentity,
        _: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        if self.closed {
            Err(DiscoveryError::Closed)
        } else {
            Ok(self.hint)
        }
    }
    fn invalidate(&mut self, _: PeerIdentity, g: HintGeneration) -> bool {
        self.invalidated.borrow_mut().push(g);
        g == self.hint.generation
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
#[derive(Default)]
struct Control {
    pending: Option<ConnectRequest<SocketAddr>>,
    reject: bool,
    duplicate: bool,
    polls: usize,
}
struct HostConnector(Rc<RefCell<Control>>);
impl PeerConnector for HostConnector {
    type Endpoint = SocketAddr;
    type Session = Box<dyn SecureSession>;
    fn local(&self) -> LocalIdentity {
        local()
    }
    fn supports_peer(&self, p: PeerIdentity) -> bool {
        p == peer(2)
    }
    fn limits(&self) -> ConnectLimits {
        ConnectLimits {
            requests: 1,
            anonymous: 0,
            timeout_ms: 100,
        }
    }
    fn usage(&self) -> ConnectUsage {
        ConnectUsage {
            requests: usize::from(self.0.borrow().pending.is_some()),
            ..Default::default()
        }
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        None
    }
    fn submit(
        &mut self,
        r: ConnectRequest<SocketAddr>,
        _: MonoTime,
    ) -> Result<(), ConnectRejected<SocketAddr>> {
        let mut c = self.0.borrow_mut();
        if c.reject {
            return Err(ConnectRejected {
                reason: ConnectError::Overloaded,
                request: Box::new(r),
            });
        }
        c.pending = Some(r);
        Ok(())
    }
    fn cancel(&mut self, t: ConnectTicket) -> bool {
        self.0
            .borrow()
            .pending
            .as_ref()
            .is_some_and(|r| r.ticket == t)
    }
    fn poll(
        &mut self,
        _: MonoTime,
        b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        b.validate()?;
        let mut c = self.0.borrow_mut();
        c.polls += 1;
        if b.completions == 0 {
            return Ok(vec![]);
        }
        let mut out = vec![];
        if let Some(r) = c.pending.take() {
            out.push(ConnectCompletion {
                ticket: r.ticket,
                result: Err(ConnectError::Timeout),
            });
            if c.duplicate {
                out.push(ConnectCompletion {
                    ticket: r.ticket,
                    result: Err(ConnectError::Timeout),
                });
            }
        }
        Ok(out)
    }
    fn close(&mut self) {}
}
fn fixture() -> (
    DiscoveryConnector<HostConnector, HostDiscovery>,
    Rc<RefCell<Control>>,
) {
    let c = Rc::new(RefCell::new(Control::default()));
    let resolver = HostDiscovery {
        hint: hint(2, 1),
        closed: false,
        invalidated: Default::default(),
    };
    (
        DiscoveryConnector::new(HostConnector(c.clone()), resolver, MonoTime(0))
            .unwrap_or_else(|_| panic!("construct")),
        c,
    )
}
#[test]
fn downstream_discovery_substitutes_only_address_and_preserves_rejection_and_attempt_ownership() {
    let (mut wrapper, c) = fixture();
    c.borrow_mut().reject = true;
    let rejected = wrapper.submit(request(1), MonoTime(0)).unwrap_err();
    assert_eq!(rejected.reason, ConnectError::Overloaded);
    assert!(matches!(rejected.request.direction,ConnectDirection::Dial(a) if a.port()==9999));
    c.borrow_mut().reject = false;
    wrapper.submit(*rejected.request, MonoTime(0)).unwrap();
    assert!(
        matches!(c.borrow().pending.as_ref().unwrap().direction,ConnectDirection::Dial(a) if a.port()==1234)
    );
    assert_eq!(
        wrapper.submit(request(2), MonoTime(0)).unwrap_err().reason,
        ConnectError::Overloaded
    );
    // A replacement while old work is pending must not be invalidated by its failure.
    wrapper.discovery_mut().hint = hint(2, 2);
    assert_eq!(
        wrapper
            .poll(MonoTime(1), ConnectPollBudget::default())
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        *wrapper.discovery_mut().invalidated.borrow(),
        vec![HintGeneration::new(1).unwrap()]
    );
    wrapper.submit(request(2), MonoTime(1)).unwrap();
    assert!(wrapper.cancel(request(2).ticket));
    wrapper
        .poll(MonoTime(2), ConnectPollBudget::default())
        .unwrap();
    assert_eq!(wrapper.discovery_mut().invalidated.borrow().len(), 1);
    wrapper.close();
    assert!(wrapper.is_drained());
    assert_eq!(
        wrapper.submit(request(3), MonoTime(2)).unwrap_err().reason,
        ConnectError::Closed
    );
    let (_, r) = wrapper.into_parts().unwrap_or_else(|_| panic!("drain"));
    assert!(r.closed);
}
#[test]
fn checked_discovery_refuses_wrong_identity_expiry_and_duplicate_completions() {
    let (mut wrapper, c) = fixture();
    wrapper.discovery_mut().hint = hint(3, 1);
    assert_eq!(
        wrapper.submit(request(1), MonoTime(0)).unwrap_err().reason,
        ConnectError::Discovery(DiscoveryError::WrongBinding)
    );
    assert!(c.borrow().pending.is_none());
    wrapper.discovery_mut().hint = PeerEndpointHint {
        expires_at: MonoTime(0),
        ..hint(2, 1)
    };
    assert_eq!(
        wrapper.submit(request(1), MonoTime(0)).unwrap_err().reason,
        ConnectError::Discovery(DiscoveryError::Expired)
    );
    wrapper.discovery_mut().hint = hint(2, 1);
    wrapper.submit(request(1), MonoTime(0)).unwrap();
    c.borrow_mut().duplicate = true;
    assert_eq!(
        wrapper
            .poll(MonoTime(1), ConnectPollBudget::default())
            .err(),
        Some(ConnectError::ProviderViolation)
    );
}
#[cfg(feature = "native")]
#[test]
fn native_cache_retains_generation_floors_and_bounds_even_after_expiry_and_close() {
    use voteboat::native::discovery::NativePeerDiscovery;
    let mut cache = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
    cache.publish(hint(2, 1), MonoTime(0)).unwrap();
    cache.publish(hint(2, 1), MonoTime(0)).unwrap();
    assert_eq!(
        cache
            .publish(
                PeerEndpointHint {
                    endpoint: "127.0.0.1:5678".parse().unwrap(),
                    ..hint(2, 1)
                },
                MonoTime(0)
            )
            .unwrap_err()
            .0,
        DiscoveryError::ConflictingGeneration
    );
    assert_eq!(
        cache.publish(hint(3, 1), MonoTime(0)).unwrap_err(),
        (DiscoveryError::Overloaded, hint(3, 1))
    );
    assert!(cache.invalidate(peer(2), HintGeneration::new(1).unwrap()));
    assert_eq!(
        cache.resolve(peer(2), MonoTime(1)),
        Err(DiscoveryError::Missing)
    );
    assert_eq!(
        cache.publish(hint(2, 1), MonoTime(1)).unwrap_err().0,
        DiscoveryError::StaleGeneration
    );
    cache.publish(hint(2, 2), MonoTime(1)).unwrap();
    assert!(!cache.invalidate(peer(2), HintGeneration::new(1).unwrap()));
    assert_eq!(cache.resolve(peer(2), MonoTime(2)).unwrap(), hint(2, 2));
    assert_eq!(
        cache.resolve(peer(2), MonoTime(100)),
        Err(DiscoveryError::Expired)
    );
    assert_eq!(
        cache.resolve(peer(2), MonoTime(99)),
        Err(DiscoveryError::TimeWentBack)
    );
    assert_eq!(cache.retained_peers(), 1);
    cache.close();
    assert_eq!(
        cache.resolve(peer(2), MonoTime(100)),
        Err(DiscoveryError::Closed)
    );
    let fresh = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
    assert_eq!(fresh.retained_peers(), 0);
}

#[test]
#[cfg(feature = "native")]
fn local_publication_cannot_extend_or_revive_an_unchanged_generation() {
    use voteboat::native::discovery::NativePeerDiscovery;
    let mut cache = NativePeerDiscovery::new(1, MonoTime(0)).unwrap();
    let original = hint(2, 1);
    cache.publish(original, MonoTime(0)).unwrap();
    let renewed = PeerEndpointHint {
        expires_at: MonoTime(original.expires_at.0 + 100),
        ..original
    };
    assert_eq!(
        cache.publish(renewed, MonoTime(1)),
        Err((DiscoveryError::ConflictingGeneration, renewed))
    );
    assert_eq!(cache.resolve(peer(2), MonoTime(1)), Ok(original));
    assert!(cache.invalidate(peer(2), original.generation));
    assert_eq!(
        cache.publish(original, MonoTime(1)),
        Err((DiscoveryError::StaleGeneration, original))
    );
    assert_eq!(
        cache.resolve(peer(2), MonoTime(1)),
        Err(DiscoveryError::Missing)
    );
}

#[test]
fn construction_returns_live_parts_and_shared_host_views_close_independently() {
    let control = Rc::new(RefCell::new(Control {
        pending: Some(request(1)),
        ..Default::default()
    }));
    let resolver = HostDiscovery {
        hint: hint(2, 1),
        closed: false,
        invalidated: Default::default(),
    };
    let shared = resolver.clone();
    let (error, connector, resolver) =
        match DiscoveryConnector::new(HostConnector(control.clone()), resolver, MonoTime(0)) {
            Err(parts) => parts,
            Ok(_) => panic!("accepted active connector"),
        };
    assert_eq!(error, ConnectError::InvalidRequest);
    assert!(!resolver.closed);
    assert!(control.borrow().pending.is_some());
    control.borrow_mut().pending = None;
    let mut one = DiscoveryConnector::new(connector, resolver, MonoTime(0))
        .unwrap_or_else(|_| panic!("reclaim"));
    let other_control = Rc::new(RefCell::new(Control::default()));
    let mut two = DiscoveryConnector::new(HostConnector(other_control), shared, MonoTime(0))
        .unwrap_or_else(|_| panic!("shared view"));
    one.close();
    assert_eq!(
        two.discovery_mut().resolve(peer(2), MonoTime(0)).unwrap(),
        hint(2, 1)
    );
    // Accept uses provisioned trust without an endpoint lookup, even if this
    // resolver would return an invalid identity for a dial.
    two.discovery_mut().hint = hint(3, 1);
    let mut accepted = request(1);
    accepted.direction = ConnectDirection::Accept;
    two.submit(accepted, MonoTime(0)).unwrap();
    two.close();
    two.poll(MonoTime(1), ConnectPollBudget::default()).unwrap();
    assert!(two.is_drained());
    assert!(one.discovery_mut().invalidated.borrow().is_empty());
}
