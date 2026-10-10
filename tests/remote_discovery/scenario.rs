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
pub(super) fn drive<S: SecureSession, T: SecureSession, R: PeerDiscovery>(
    client: &mut NativeRemotePeerDiscovery<S>,
    server: &mut NativeDiscoveryResponder<T, R>,
    start: MonoTime,
) -> (RefreshCompletion, MonoTime) {
    for step in 0..2000 {
        let now = MonoTime(start.0 + step);
        server.poll(now, SessionPollBudget::default()).unwrap();
        if let Some(c) = client.poll(now, SessionPollBudget::default()).unwrap() {
            return (c, now);
        }
    }
    panic!("remote refresh stalled");
}
pub(super) fn refresh<S: SecureSession, T: SecureSession>(a: S, b: T, start: MonoTime) {
    let config = RemoteDiscoveryConfig::default();
    let mut source = NativePeerDiscovery::new(3, start).unwrap();
    source.publish(hint(3, 1, start.0 + 2_000), start).unwrap();
    source.publish(hint(4, 1, start.0 + 60_000), start).unwrap();
    let mut client = NativeRemotePeerDiscovery::new(a, peer(2), config, start)
        .ok()
        .unwrap();
    let mut server = NativeDiscoveryResponder::new(b, peer(1), source, config, start)
        .ok()
        .unwrap();
    assert_eq!(
        client.resolve(peer(3), start),
        Err(DiscoveryError::Unavailable)
    );
    let request = client.pending().unwrap();
    assert_eq!(
        client.resolve(peer(3), start),
        Err(DiscoveryError::Unavailable)
    );
    assert_eq!(client.pending(), Some(request));
    assert_eq!(
        client.resolve(peer(4), start),
        Err(DiscoveryError::Overloaded)
    );
    let (done, now) = drive(&mut client, &mut server, start);
    assert_eq!(done.request, request);
    let first = done.result.unwrap();
    assert_eq!(first.generation, HintGeneration::new(1).unwrap());
    assert_eq!(client.resolve(peer(3), now), Ok(first));
    assert_eq!(
        client.resolve(peer(4), now),
        Err(DiscoveryError::Unavailable)
    );
    let (done, now) = drive(&mut client, &mut server, now);
    let independent = done.result.unwrap();
    let later = MonoTime(first.expires_at.0 + 1);
    assert!(later > now);
    server
        .source_mut()
        .publish(hint(3, 2, later.0 + 2_000), later)
        .unwrap();
    assert_eq!(
        client.resolve(peer(3), later),
        Err(DiscoveryError::Unavailable)
    );
    let (done, now) = drive(&mut client, &mut server, later);
    let renewed = done.result.unwrap();
    assert_ne!(first.endpoint, renewed.endpoint);
    assert!(renewed.generation > first.generation);
    assert!(!client.invalidate(peer(3), first.generation));
    assert_eq!(client.resolve(peer(3), now), Ok(renewed));
    source_outage(client, server, independent, now);
}

fn source_outage<S: SecureSession, T: SecureSession>(
    mut client: NativeRemotePeerDiscovery<S>,
    mut server: NativeDiscoveryResponder<T, NativePeerDiscovery>,
    independent: PeerEndpointHint,
    now: MonoTime,
) {
    assert_eq!(
        client.resolve(peer(5), now),
        Err(DiscoveryError::Unavailable)
    );
    // Do not poll the remote server: cached child/peer use does not contact it.
    assert_eq!(client.resolve(peer(4), now), Ok(independent));
    assert!(client
        .poll(now, SessionPollBudget::default())
        .unwrap()
        .is_none());
    assert_eq!(client.resolve(peer(4), now), Ok(independent));
    let deadline = client.next_deadline().unwrap();
    let request = client.pending().unwrap();
    let done = client
        .poll(deadline, SessionPollBudget::default())
        .unwrap()
        .unwrap();
    assert_eq!(done.request, request);
    assert_eq!(done.result, Err(RemoteDiscoveryError::Timeout));
    assert!(client.pending().is_none());
    assert!(client.source_failed());
    assert_eq!(client.resolve(peer(4), deadline), Ok(independent));
    assert_eq!(
        client.resolve(peer(5), deadline),
        Err(DiscoveryError::Unavailable)
    );
    let _session = client.into_session().ok().unwrap();
    server.close();
    let (_session, mut source) = server.into_parts();
    assert!(source.resolve(peer(4), deadline).is_ok());
}
