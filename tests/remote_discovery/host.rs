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
use std::{cell::RefCell, collections::VecDeque, rc::Rc};
type Bytes = Rc<RefCell<VecDeque<u8>>>;
pub(super) struct HostSession {
    binding: SessionBinding,
    input: Bytes,
    output: Bytes,
    closed: bool,
}
pub(super) fn pair() -> (HostSession, HostSession) {
    let a = Rc::new(RefCell::new(VecDeque::new()));
    let b = Rc::new(RefCell::new(VecDeque::new()));
    let binding = SessionBinding {
        local: local(1),
        peer: local(2),
        generation: voteboat::identity::SecureSessionGeneration::new(1).unwrap(),
        wire_version: 1,
    };
    (
        HostSession {
            binding,
            input: a.clone(),
            output: b.clone(),
            closed: false,
        },
        HostSession {
            binding: SessionBinding {
                local: local(2),
                peer: local(1),
                ..binding
            },
            input: b,
            output: a,
            closed: false,
        },
    )
}
impl SecureSession for HostSession {
    fn security(&self) -> SessionSecurity {
        SessionSecurity::Authenticated
    }
    fn state(&self) -> SessionState {
        if self.closed {
            SessionState::Closed
        } else {
            SessionState::Ready
        }
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
    fn read_plaintext(&mut self, b: &mut [u8]) -> Result<usize, SessionError> {
        let mut input = self.input.borrow_mut();
        if input.is_empty() {
            return Err(SessionError::WouldBlock);
        }
        let n = b.len().min(input.len()).min(5);
        for dst in &mut b[..n] {
            *dst = input.pop_front().unwrap();
        }
        Ok(n)
    }
    fn write_plaintext(&mut self, b: &[u8]) -> Result<usize, SessionError> {
        let n = b.len().min(7);
        self.output.borrow_mut().extend(&b[..n]);
        Ok(n)
    }
    fn is_flushed(&self) -> bool {
        true
    }
    fn close(&mut self) {
        self.closed = true;
    }
    fn revoke(&mut self) {
        self.closed = true;
    }
}
struct HostHints {
    hint: Option<PeerEndpointHint>,
    calls: usize,
    closed: bool,
}
impl PeerDiscovery for HostHints {
    fn resolve(
        &mut self,
        p: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.calls += 1;
        self.hint.ok_or(DiscoveryError::Missing)?.validate(p, now)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
fn client_server() -> (
    NativeRemotePeerDiscovery<HostSession>,
    NativeDiscoveryResponder<HostSession, HostHints>,
) {
    let (a, b) = pair();
    let config = RemoteDiscoveryConfig {
        retry_ms: 10,
        ..RemoteDiscoveryConfig::default()
    };
    let client = NativeRemotePeerDiscovery::new(a, peer(2), config, MonoTime(0))
        .ok()
        .unwrap();
    let server = NativeDiscoveryResponder::new(
        b,
        peer(1),
        HostHints {
            hint: Some(hint(3, 1, 50_000)),
            calls: 0,
            closed: false,
        },
        config,
        MonoTime(0),
    )
    .ok()
    .unwrap();
    (client, server)
}
#[test]
fn cancellation_retains_exact_slot_and_suppresses_late_positive() {
    let (mut client, mut server) = client_server();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let request = client.pending().unwrap();
    client
        .poll(MonoTime(0), SessionPollBudget::default())
        .unwrap(); // Partial request prefix.
    let mut wrong = request;
    wrong.sequence += 1;
    assert!(!client.cancel(wrong));
    assert!(client.cancel(request));
    assert_eq!(
        client.resolve(peer(4), MonoTime(0)),
        Err(DiscoveryError::Overloaded)
    );
    let (done, now) = scenario::drive(&mut client, &mut server, MonoTime(0));
    assert_eq!(done.request, request);
    assert_eq!(done.result, Err(RemoteDiscoveryError::Cancelled));
    assert_eq!(server.source_mut().calls, 1);
    assert!(client.pending().is_none());
    let later = MonoTime(now.0 + 10);
    assert_eq!(
        client.resolve(peer(3), later),
        Err(DiscoveryError::Unavailable)
    );
    assert!(client.pending().unwrap().sequence > request.sequence);
    let (done, _) = scenario::drive(&mut client, &mut server, later);
    assert!(done.result.is_ok());
}
#[test]
fn remote_negative_backoff_and_server_source_lifetime_are_bounded() {
    let (mut client, mut server) = client_server();
    server.source_mut().hint = None;
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut server, MonoTime(0));
    assert_eq!(done.result, Err(DiscoveryError::Missing.into()));
    assert_eq!(client.discovery_deadline(), Some(MonoTime(now.0 + 10)));
    for _ in 0..100 {
        assert_eq!(client.resolve(peer(3), now), Err(DiscoveryError::Missing));
    }
    assert!(client.pending().is_none());
    assert_eq!(server.source_mut().calls, 1);
    server.source_mut().hint = Some(hint(3, 2, 50_000));
    let later = MonoTime(now.0 + 10);
    client.resolve(peer(3), later).unwrap_err();
    assert!(scenario::drive(&mut client, &mut server, later)
        .0
        .result
        .is_ok());
    server.close();
    let (_, source) = server.into_parts();
    assert!(!source.closed);
}
#[test]
fn construction_refusal_returns_both_owners_without_closing_them() {
    let (a, b) = pair();
    let output = a.output.clone();
    let (error, a) =
        NativeRemotePeerDiscovery::new(a, peer(9), RemoteDiscoveryConfig::default(), MonoTime(0))
            .err()
            .unwrap();
    assert_eq!(error, DiscoveryError::WrongBinding.into());
    assert!(!a.closed);
    assert!(Rc::ptr_eq(&a.output, &output));
    let source = HostHints {
        hint: None,
        calls: 0,
        closed: false,
    };
    let (error, b, source) = NativeDiscoveryResponder::new(
        b,
        peer(1),
        source,
        RemoteDiscoveryConfig {
            cached_peers: 0,
            ..RemoteDiscoveryConfig::default()
        },
        MonoTime(0),
    )
    .err()
    .unwrap();
    assert_eq!(error, DiscoveryError::InvalidLimits.into());
    assert!(!b.closed);
    assert!(!source.closed);
}
#[test]
fn close_completes_pending_once_and_returns_original_session() {
    let (mut client, _) = client_server();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let request = client.pending().unwrap();
    client.close();
    assert!(client.into_session().is_err());
    let (mut client, _) = client_server();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    client.close();
    let done = client
        .poll(MonoTime(0), SessionPollBudget::default())
        .unwrap()
        .unwrap();
    assert_eq!(done.request, request);
    assert_eq!(done.result, Err(DiscoveryError::Closed.into()));
    assert_eq!(
        client
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap(),
        None
    );
    assert!(client.into_session().ok().unwrap().closed);
}

fn altered_reply(
    change: impl FnOnce(&mut VecDeque<u8>),
    server_now: u64,
    receive_at: u64,
) -> (RefreshCompletion, NativeRemotePeerDiscovery<HostSession>) {
    let (a, b) = pair();
    let input = a.input.clone();
    let config = RemoteDiscoveryConfig::default();
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, MonoTime(0))
        .ok()
        .unwrap();
    let source = HostHints {
        hint: Some(hint(3, 1, server_now + 50)),
        calls: 0,
        closed: false,
    };
    let mut server =
        NativeDiscoveryResponder::new(b, peer(1), source, config, MonoTime(server_now))
            .ok()
            .unwrap();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    for _ in 0..20 {
        assert!(client
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap()
            .is_none());
    }
    for _ in 0..50 {
        server
            .poll(MonoTime(server_now), SessionPollBudget::default())
            .unwrap();
    }
    assert_eq!(input.borrow().len(), 96);
    change(&mut input.borrow_mut());
    for _ in 0..50 {
        if let Some(done) = client
            .poll(MonoTime(receive_at), SessionPollBudget::default())
            .unwrap()
        {
            return (done, client);
        }
    }
    panic!("altered reply did not finish");
}
#[test]
fn malformed_or_mismatched_reply_closes_without_publishing() {
    for (offset, value) in [
        (4, 9),
        (6, 1),
        (15, 2),
        (23, 4),
        (55, 0),
        (74, 9),
        (90, 0),
        (95, 1),
    ] {
        let (done, mut client) = altered_reply(|b| b[offset] = value, 0, 0);
        assert_eq!(
            done.result,
            Err(RemoteDiscoveryError::Protocol),
            "offset {offset}"
        );
        assert_eq!(
            client.resolve(peer(3), MonoTime(0)),
            Err(DiscoveryError::Unavailable)
        );
        assert!(client.source_failed());
    }
    let (done, mut client) = altered_reply(
        |b| {
            for byte in b.iter_mut().take(91).skip(83) {
                *byte = 255;
            }
        },
        0,
        0,
    );
    assert_eq!(done.result, Err(RemoteDiscoveryError::Protocol));
    assert_eq!(
        client.resolve(peer(3), MonoTime(0)),
        Err(DiscoveryError::Unavailable)
    );
}
#[test]
fn clock_skew_and_delayed_delivery_cannot_extend_remote_lifetime() {
    let (done, mut client) = altered_reply(|_| {}, 100_000, 51);
    assert_eq!(done.result, Err(DiscoveryError::Expired.into()));
    assert!(client.resolve(peer(3), MonoTime(51)).is_err());
    assert!(client.pending().is_none());
    let (done, _) = altered_reply(|_| {}, 100_000, 20);
    assert_eq!(done.result.unwrap().expires_at, MonoTime(50));
}
#[test]
fn stale_generation_cannot_revive_an_invalidated_endpoint() {
    let (mut client, mut server) = client_server();
    server.source_mut().hint = Some(hint(3, 2, 50_000));
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut server, MonoTime(0));
    assert!(client.invalidate(peer(3), done.result.unwrap().generation));
    server.source_mut().hint = Some(hint(3, 1, 50_000));
    client.resolve(peer(3), now).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut server, now);
    assert_eq!(done.result, Err(DiscoveryError::StaleGeneration.into()));
    assert_eq!(
        client.resolve(peer(3), now),
        Err(DiscoveryError::StaleGeneration)
    );
}
#[test]
fn zero_budget_and_backwards_time_leave_accepted_request_unchanged() {
    let (mut client, _) = client_server();
    client.resolve(peer(3), MonoTime(1)).unwrap_err();
    let request = client.pending().unwrap();
    assert_eq!(
        client.poll(MonoTime(0), SessionPollBudget::default()),
        Err(DiscoveryError::TimeWentBack.into())
    );
    let budget = SessionPollBudget {
        io_calls: 0,
        read_bytes: 0,
        write_bytes: 0,
    };
    assert!(client.poll(MonoTime(1), budget).unwrap().is_none());
    assert_eq!(client.pending(), Some(request));
}

#[test]
fn reconnect_retains_floors_and_rejects_old_session_generations() {
    let (mut client, mut server) = client_server();
    server.source_mut().hint = Some(hint(3, 2, 50_000));
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut server, MonoTime(0));
    let known = done.result.unwrap();
    assert!(client.invalidate(peer(3), known.generation));
    client.resolve(peer(4), now).unwrap_err();
    let deadline = client.next_deadline().unwrap();
    assert_eq!(
        client
            .poll(deadline, SessionPollBudget::default())
            .unwrap()
            .unwrap()
            .result,
        Err(RemoteDiscoveryError::Timeout)
    );
    let (old, _) = pair();
    let (error, old) = client.replace_session(old).err().unwrap();
    assert_eq!(error, DiscoveryError::WrongBinding.into());
    assert!(!old.closed);
    let (mut a, mut b) = pair();
    a.binding.generation = voteboat::identity::SecureSessionGeneration::new(2).unwrap();
    b.binding.generation = voteboat::identity::SecureSessionGeneration::new(2).unwrap();
    let original = client.replace_session(a).ok().unwrap();
    assert!(original.closed);
    let mut replacement = NativeDiscoveryResponder::new(
        b,
        peer(1),
        HostHints {
            hint: Some(hint(3, 1, 50_000)),
            calls: 0,
            closed: false,
        },
        RemoteDiscoveryConfig::default(),
        deadline,
    )
    .ok()
    .unwrap();
    client.resolve(peer(3), deadline).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut replacement, deadline);
    assert_eq!(done.result, Err(DiscoveryError::StaleGeneration.into()));
    let later = MonoTime(now.0 + RemoteDiscoveryConfig::default().retry_ms);
    replacement.source_mut().hint = Some(PeerEndpointHint {
        expires_at: MonoTime(later.0 + 50_000),
        ..known
    });
    client.resolve(peer(3), later).unwrap_err();
    let (done, received) = scenario::drive(&mut client, &mut replacement, later);
    let renewed = done.result.unwrap();
    assert_eq!(renewed.generation, known.generation);
    assert_eq!(renewed.endpoint, known.endpoint);
    assert!(renewed.expires_at > known.expires_at);
    assert_eq!(client.resolve(peer(3), received), Ok(renewed));
}
#[test]
fn ipv6_scope_and_flow_round_trip_without_address_substitution() {
    let (mut client, mut server) = client_server();
    let endpoint = std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        1234,
        7,
        9,
    ));
    server.source_mut().hint = Some(PeerEndpointHint {
        endpoint,
        ..hint(3, 1, 50_000)
    });
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    assert_eq!(
        scenario::drive(&mut client, &mut server, MonoTime(0))
            .0
            .result
            .unwrap()
            .endpoint,
        endpoint
    );
}

#[test]
fn cache_capacity_includes_invalidated_generation_floors() {
    let (a, b) = pair();
    let config = RemoteDiscoveryConfig {
        cached_peers: 1,
        ..RemoteDiscoveryConfig::default()
    };
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, MonoTime(0))
        .ok()
        .unwrap();
    let mut server = NativeDiscoveryResponder::new(
        b,
        peer(1),
        HostHints {
            hint: Some(hint(3, 1, 50_000)),
            calls: 0,
            closed: false,
        },
        config,
        MonoTime(0),
    )
    .ok()
    .unwrap();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut server, MonoTime(0));
    assert!(client.invalidate(peer(3), done.result.unwrap().generation));
    server.source_mut().hint = Some(hint(4, 1, 50_000));
    client.resolve(peer(4), now).unwrap_err();
    let (done, now) = scenario::drive(&mut client, &mut server, now);
    assert_eq!(done.result, Err(DiscoveryError::Overloaded.into()));
    server.source_mut().hint = Some(hint(3, 2, 50_000));
    client.resolve(peer(3), now).unwrap_err();
    let (done, _) = scenario::drive(&mut client, &mut server, now);
    assert_eq!(
        done.result.unwrap().generation,
        HintGeneration::new(2).unwrap()
    );
}

#[test]
fn responder_rejects_replayed_request_before_calling_source_again() {
    let (a, b) = pair();
    let requests = b.input.clone();
    let config = RemoteDiscoveryConfig::default();
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, MonoTime(0))
        .ok()
        .unwrap();
    let mut server = NativeDiscoveryResponder::new(
        b,
        peer(1),
        HostHints {
            hint: Some(hint(3, 1, 50_000)),
            calls: 0,
            closed: false,
        },
        config,
        MonoTime(0),
    )
    .ok()
    .unwrap();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    for _ in 0..20 {
        client
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap();
    }
    let replay = requests.borrow().clone();
    assert_eq!(replay.len(), 96);
    for _ in 0..50 {
        server
            .poll(MonoTime(0), SessionPollBudget::default())
            .unwrap();
    }
    assert_eq!(server.source_mut().calls, 1);
    requests.borrow_mut().extend(replay);
    let error = (0..30).find_map(|_| server.poll(MonoTime(0), SessionPollBudget::default()).err());
    assert_eq!(error, Some(RemoteDiscoveryError::Protocol));
    assert_eq!(server.source_mut().calls, 1);
}

#[test]
fn replayed_positive_cannot_renew_an_invalidated_lease() {
    let (a, b) = pair();
    let replies = a.input.clone();
    let config = RemoteDiscoveryConfig::default();
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, MonoTime(0))
        .ok()
        .unwrap();
    let mut server = NativeDiscoveryResponder::new(
        b,
        peer(1),
        HostHints {
            hint: Some(hint(3, 1, 50_000)),
            calls: 0,
            closed: false,
        },
        config,
        MonoTime(0),
    )
    .ok()
    .unwrap();
    client.resolve(peer(3), MonoTime(0)).unwrap_err();
    buffer_reply(&mut client, &mut server, MonoTime(0));
    let replay = replies.borrow().clone();
    assert_eq!(replay.len(), 96);
    let (done, now) = scenario::drive(&mut client, &mut server, MonoTime(0));
    assert!(client.invalidate(peer(3), done.result.unwrap().generation));
    client.resolve(peer(3), now).unwrap_err();
    assert_eq!(client.pending().unwrap().sequence, 2);
    buffer_reply(&mut client, &mut server, now);
    assert_eq!(replies.borrow().len(), 96);
    *replies.borrow_mut() = replay;
    let (done, now) = scenario::drive(&mut client, &mut server, now);
    assert_eq!(done.result, Err(RemoteDiscoveryError::Protocol));
    assert!(client.source_failed());
    assert_eq!(
        client.resolve(peer(3), now),
        Err(DiscoveryError::Unavailable)
    );
}

fn buffer_reply(
    client: &mut NativeRemotePeerDiscovery<HostSession>,
    server: &mut NativeDiscoveryResponder<HostSession, HostHints>,
    now: MonoTime,
) {
    for _ in 0..20 {
        assert!(client
            .poll(now, SessionPollBudget::default())
            .unwrap()
            .is_none());
    }
    for _ in 0..50 {
        server.poll(now, SessionPollBudget::default()).unwrap();
    }
}
