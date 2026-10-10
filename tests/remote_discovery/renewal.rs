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

// Like the executable source: one stable endpoint generation, fresh bounded TTL.
struct RenewingSource {
    value: PeerEndpointHint,
    calls: usize,
}
impl PeerDiscovery for RenewingSource {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.calls += 1;
        PeerEndpointHint {
            expires_at: MonoTime(now.0 + 2_000),
            ..self.value
        }
        .validate(peer, now)
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {}
}

pub(super) fn exercise<S: SecureSession, T: SecureSession>(a: S, b: T, start: MonoTime) {
    let config = RemoteDiscoveryConfig {
        cached_peers: 1,
        ..RemoteDiscoveryConfig::default()
    };
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, start)
        .ok()
        .unwrap();
    let source = RenewingSource {
        value: hint(3, 1, 0),
        calls: 0,
    };
    let mut server = NativeDiscoveryResponder::new(b, peer(1), source, config, start)
        .ok()
        .unwrap();
    let (mut known, mut now) = fetch(&mut client, &mut server, start);
    for iteration in 0..4 {
        assert_eq!(client.resolve(peer(3), now), Ok(known));
        assert_eq!(server.source_mut().calls, iteration + 1);
        if iteration % 2 == 0 {
            now = MonoTime(known.expires_at.0 + 1);
        } else {
            assert!(client.invalidate(peer(3), known.generation));
            now = MonoTime(now.0 + 1);
        }
        let (renewed, received) = fetch(&mut client, &mut server, now);
        assert_eq!(renewed.peer, known.peer);
        assert_eq!(renewed.generation, known.generation);
        assert_eq!(renewed.endpoint, known.endpoint);
        assert!(renewed.expires_at > known.expires_at);
        known = renewed;
        now = received;
    }
    // Address changes need a new generation even on this authenticated channel.
    assert!(client.invalidate(peer(3), known.generation));
    server.source_mut().value = hint(3, 2, 0);
    let (moved, received) = fetch(&mut client, &mut server, now);
    assert_ne!(moved.endpoint, known.endpoint);
    assert!(!client.invalidate(peer(3), known.generation));
    refusals(&mut client, &mut server, moved, received);
    client.close();
    assert!(client.into_session().is_ok());
    server.close();
    let _owners = server.into_parts();
}

fn fetch<S: SecureSession, T: SecureSession>(
    client: &mut NativeRemotePeerDiscovery<S>,
    server: &mut NativeDiscoveryResponder<T, RenewingSource>,
    now: MonoTime,
) -> (PeerEndpointHint, MonoTime) {
    assert_eq!(
        client.resolve(peer(3), now),
        Err(DiscoveryError::Unavailable)
    );
    let request = client.pending().unwrap();
    let (done, received) = scenario::drive(client, server, now);
    assert_eq!(done.request, request);
    let value = done.result.unwrap();
    assert_eq!(value.expires_at, MonoTime(request.started_at.0 + 2_000));
    assert_eq!(client.resolve(peer(3), received), Ok(value));
    (value, received)
}

fn refusals<S: SecureSession, T: SecureSession>(
    client: &mut NativeRemotePeerDiscovery<S>,
    server: &mut NativeDiscoveryResponder<T, RenewingSource>,
    known: PeerEndpointHint,
    mut now: MonoTime,
) {
    assert!(client.invalidate(peer(3), known.generation));
    let conflict = PeerEndpointHint {
        endpoint: hint(3, 1, 0).endpoint,
        ..known
    };
    for (value, expected) in [
        (conflict, DiscoveryError::ConflictingGeneration),
        (hint(3, 1, 0), DiscoveryError::StaleGeneration),
    ] {
        server.source_mut().value = value;
        assert_eq!(
            client.resolve(peer(3), now),
            Err(DiscoveryError::Unavailable)
        );
        let (done, received) = scenario::drive(client, server, now);
        assert_eq!(done.result, Err(expected.into()));
        assert_eq!(client.resolve(peer(3), received), Err(expected));
        assert!(client.pending().is_none());
        now = MonoTime(received.0 + RemoteDiscoveryConfig::default().retry_ms);
    }
    server.source_mut().value = known;
    assert_eq!(
        client.resolve(peer(3), now),
        Err(DiscoveryError::Unavailable)
    );
    let request = client.pending().unwrap();
    assert!(client.cancel(request));
    let (done, received) = scenario::drive(client, server, now);
    assert_eq!(done.request, request);
    assert_eq!(done.result, Err(RemoteDiscoveryError::Cancelled));
    assert_eq!(
        client.resolve(peer(3), received),
        Err(DiscoveryError::Unavailable)
    );
    assert!(client.pending().is_none());
    let later = MonoTime(received.0 + RemoteDiscoveryConfig::default().retry_ms);
    let (renewed, _) = fetch(client, server, later);
    assert_eq!(renewed.generation, known.generation);
    assert_eq!(renewed.endpoint, known.endpoint);
}
