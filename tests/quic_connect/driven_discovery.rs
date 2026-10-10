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
use voteboat::native::discovery::NativePeerDiscovery;
struct Fixture {
    dial: DiscoveryConnector<NativeQuicConnector, NativeRemotePeerDiscovery<NativeQuicSession>>,
    source: NativeDiscoveryResponder<NativeQuicSession, NativePeerDiscovery>,
    target: NativeQuicConnector,
    now: u64,
}
impl Fixture {
    fn new() -> Self {
        let mut connectors = connectors();
        let (mut sessions, now) = establish(&mut connectors, 0, 1);
        let client = sessions.remove(&(1, 2)).unwrap();
        let server = sessions.remove(&(2, 1)).unwrap();
        drop(sessions);
        let target = connectors.remove(2);
        let dial = connectors.remove(0);
        let config = RemoteDiscoveryConfig::default();
        let mut hints = NativePeerDiscovery::new(1, MonoTime(now)).unwrap();
        hints
            .publish(
                PeerEndpointHint {
                    peer: peer(3),
                    generation: HintGeneration::new(1).unwrap(),
                    endpoint: target.local_addr(),
                    expires_at: MonoTime(now + 10_000),
                },
                MonoTime(now),
            )
            .unwrap();
        Self {
            dial: DiscoveryConnector::new_driven(
                dial,
                NativeRemotePeerDiscovery::new(client, peer(2), config, MonoTime(now))
                    .ok()
                    .unwrap(),
                MonoTime(now),
            )
            .ok()
            .unwrap(),
            source: NativeDiscoveryResponder::new(server, peer(1), hints, config, MonoTime(now))
                .ok()
                .unwrap(),
            target,
            now,
        }
    }
    fn establish(&mut self) -> BTreeMap<(u64, u64), NativeQuicSession> {
        let mut retry = Some(ConnectRequest {
            ticket: ticket(1, 3, 2),
            direction: ConnectDirection::Dial("127.0.0.1:1".parse().unwrap()),
            deadline: MonoTime(self.now + 10_000),
        });
        request(
            &mut self.target,
            3,
            1,
            2,
            self.dial.connector().local_addr(),
            self.now,
        );
        let budget = ConnectPollBudget {
            visits: 1,
            ..Default::default()
        };
        let mut sessions = BTreeMap::new();
        for now in self.now..self.now + 9000 {
            self.source
                .poll(MonoTime(now), SessionPollBudget::default())
                .unwrap();
            for (id, events) in [
                (1, self.dial.poll(MonoTime(now), budget).unwrap()),
                (3, self.target.poll(MonoTime(now), budget).unwrap()),
            ] {
                for done in events {
                    assert_eq!(
                        done.ticket.generation,
                        SecureSessionGeneration::new(2).unwrap()
                    );
                    let session = done.result.unwrap();
                    let binding = require_authenticated(&session).unwrap();
                    assert_eq!(binding.peer.node, done.ticket.peer.node);
                    assert_eq!(binding.peer.store.identity, done.ticket.peer.store);
                    sessions.insert((id, done.ticket.peer.node.get()), session);
                }
            }
            if let Some(request) = retry.take() {
                if let Err(refused) = self.dial.submit(request, MonoTime(now)) {
                    assert_eq!(
                        refused.reason,
                        ConnectError::Discovery(DiscoveryError::Unavailable)
                    );
                    assert!(
                        matches!(refused.request.direction, ConnectDirection::Dial(a) if a.port() == 1)
                    );
                    retry = Some(*refused.request);
                }
            }
            for session in sessions.values_mut() {
                session
                    .poll(MonoTime(now), SessionPollBudget::default())
                    .unwrap();
            }
            if sessions.len() == 2 {
                self.now = now;
                return sessions;
            }
        }
        panic!("driven QUIC discovery stalled");
    }
}
#[test]
fn quic_connector_poll_drives_remote_lookup_and_authenticates_target() {
    let mut fixture = Fixture::new();
    let mut sessions = fixture.establish();
    exchange(&mut sessions, fixture.now);
    fixture.dial.close();
    fixture.target.close();
    fixture.source.close();
    let (_, remote) = fixture.dial.into_parts().ok().unwrap();
    assert!(remote.into_session().is_ok());
    assert!(sessions.values().all(|s| require_authenticated(s).is_ok()));
}
