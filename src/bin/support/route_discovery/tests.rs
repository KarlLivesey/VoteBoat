// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{io::Read, net::TcpListener, path::Path};

pub(super) fn stalled_authority() -> (Discovery, Endpoint, TcpListener, ManifestLookup) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = Endpoint {
        node: 1,
        address: listener.local_addr().unwrap(),
        server_name: "node1.voteboat.test".into(),
    };
    let request = ManifestLookup {
        locator: AuthorityLocator {
            authority: GroupIdentity {
                id: GroupId::new(42).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(10).unwrap(),
                incarnation: ResponsibilityIncarnation::new(1).unwrap(),
            },
        },
        minimum_epoch: Some(OwnershipEpoch::new(2).unwrap()),
        minimum_generation: None,
    };
    let access = ClientAccess::load(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"),
        1,
        std::slice::from_ref(&endpoint),
    )
    .unwrap();
    let authorities = Authorities {
        groups: [(request.locator.authority, vec![endpoint.clone()])].into(),
        pins: vec![endpoint.clone()],
    };
    (
        Discovery::new(authorities, access, Instant::now()),
        endpoint,
        listener,
        request,
    )
}

fn assert_attempt_closed(listener: &TcpListener) {
    let (stream, _) = listener.accept().unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let mut bytes = Vec::new();
    stream.take(4096).read_to_end(&mut bytes).unwrap();
    assert!(bytes.starts_with(b"1\n"));
    assert!(bytes.len() > 2, "TLS client handshake was never sent");
}

#[test]
fn authority_authentication_timeout_preserves_remaining_lookup_budget() {
    let (mut discovery, _, listener, request) = stalled_authority();
    let deadline = discovery.deadline;
    assert!(matches!(
        discovery.lookup(request, discovery.now()),
        Err(ManifestDiscoveryError::Unavailable)
    ));
    let result = discovery.poll();
    assert_attempt_closed(&listener);
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(discovery.attempts, 1);
    assert_eq!(discovery.cursor, 1);
    assert_eq!(discovery.deadline, deadline);
    assert!(Instant::now() < deadline);
    assert!(discovery.active.is_none());
    assert!(discovery.observations.is_empty());
    assert!(matches!(
        discovery.lookup(request, discovery.now()),
        Err(ManifestDiscoveryError::Unavailable)
    ));
    discovery.close();
}

#[test]
fn authority_authentication_cannot_extend_the_lookup_deadline() {
    let (mut discovery, endpoint, listener, _) = stalled_authority();
    discovery.deadline = Instant::now() + Duration::from_millis(100);
    let result = discovery.channel(&endpoint, discovery.deadline);
    assert_attempt_closed(&listener);
    let error = result.err().expect("overall deadline must be terminal");
    assert!(error
        .to_string()
        .contains("recursive lookup deadline expired"));
    assert!(discovery.active.is_none());
    assert!(discovery.observations.is_empty());
    assert!(discovery
        .poll()
        .unwrap_err()
        .to_string()
        .contains("recursive lookup deadline expired"));
}
