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
fn source() -> Source {
    Source::parse("voteboat-discovery-peers-v2 10\n1 127.0.0.1:4001 node1.voteboat.test\n2 127.0.0.1:4002 node2.voteboat.test\n").unwrap()
}
fn hint(source: &mut Source, node: u64) -> PeerEndpointHint {
    source
        .resolve(service_access::transport_identity(node, false), MonoTime(5))
        .unwrap()
}
#[test]
fn existing_source_views_observe_checked_address_updates_and_close_independently() {
    let source = source();
    let mut view = source.clone();
    let mut another = source.clone();
    let before = hint(&mut view, 1);
    assert_eq!(
        source.update(&["10", "11", "1", "127.0.0.1:5001"]).unwrap(),
        "OK generation=11 duplicate=false durable=false"
    );
    assert_eq!(hint(&mut view, 1).endpoint.port(), 5001);
    assert_eq!(hint(&mut another, 1).generation.get(), 11);
    assert_eq!(hint(&mut another, 2).endpoint.port(), 4002);
    assert!(hint(&mut view, 1).generation > before.generation);
    assert_eq!(
        source.current.borrow().endpoints[0].server_name,
        "node1.voteboat.test"
    );
    view.close();
    assert_eq!(
        view.resolve(service_access::transport_identity(1, false), MonoTime(5)),
        Err(DiscoveryError::Closed)
    );
    assert_eq!(hint(&mut another, 1).endpoint.port(), 5001);
}
#[test]
fn invalid_updates_preserve_all_current_hints_and_generation() {
    let mut source = source();
    let status = source.status();
    let before = hint(&mut source, 1);
    for words in [
        ["9", "11", "1", "127.0.0.1:5001"],
        ["10", "10", "1", "127.0.0.1:5001"],
        ["10", "0", "1", "127.0.0.1:5001"],
        ["10", "11", "9", "127.0.0.1:5001"],
        ["10", "11", "1", "127.0.0.1:0"],
        ["10", "11", "1", "0.0.0.0:5001"],
        ["10", "11", "1", "224.0.0.1:5001"],
        ["10", "11", "1", "127.0.0.1:4002"],
        ["10", "11", "1", "invalid"],
    ] {
        assert!(source.update(&words).is_err(), "{words:?}");
        assert_eq!(source.status(), status);
        assert_eq!(hint(&mut source, 1), before);
    }
    assert!(source.update(&["10"]).is_err());
}
#[test]
fn only_the_exact_latest_update_can_be_retried() {
    let source = source();
    let first = ["10", "11", "1", "127.0.0.1:5001"];
    source.update(&first).unwrap();
    assert_eq!(
        source.update(&first).unwrap(),
        "OK generation=11 duplicate=true durable=false"
    );
    assert!(source.update(&["10", "11", "1", "127.0.0.1:5002"]).is_err());
    source.update(&["11", "12", "2", "127.0.0.1:5002"]).unwrap();
    assert!(source.update(&first).is_err());
    assert!(source.status().contains("generation=12"));
}
#[test]
fn explicit_generation_survives_binding_and_legacy_sources_keep_session_generation() {
    let mut explicit = source();
    explicit.bind_session(1000);
    assert_eq!(hint(&mut explicit, 1).generation.get(), 10);
    let mut legacy =
        Source::parse("voteboat-command-peers-v1\n1 127.0.0.1:4001 node1.voteboat.test\n").unwrap();
    legacy.bind_session(1000);
    assert_eq!(hint(&mut legacy, 1).generation.get(), 1000);
    assert!(legacy
        .update(&["1000", "1001", "1", "127.0.0.1:5001"])
        .is_err());
}
#[test]
fn malformed_versioned_sources_are_rejected() {
    for text in [
        "voteboat-discovery-peers-v2 0\n1 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-discovery-peers-v2 1 extra\n1 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-discovery-peers-v2 1\n",
        "voteboat-discovery-peers-v2 1",
    ] {
        assert!(Source::parse(text).is_err());
    }
}

fn raft_peer(node: u64, store: u128, incarnation: u64) -> PeerIdentity {
    use voteboat::identity::*;
    PeerIdentity {
        node: NodeId::new(node).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(store).unwrap(),
            incarnation: StoreIncarnation::new(incarnation).unwrap(),
        },
    }
}
#[test]
fn peer_source_preserves_exact_store_identity_through_updates_and_reconstruction() {
    let file = "voteboat-peer-discovery-v1 10\n1 401 7 127.0.0.1:4001 node1.voteboat.test\n2 402 9 127.0.0.1:4002 node2.voteboat.test\n";
    let mut source = Source::parse(file).unwrap();
    source.bind_session(500);
    let peer = raft_peer(1, 401, 7);
    let before = source.resolve(peer, MonoTime(5)).unwrap();
    assert_eq!(before.generation.get(), 10);
    for wrong in [
        raft_peer(1, 401, 8),
        raft_peer(1, 402, 7),
        raft_peer(2, 401, 7),
        service_access::transport_identity(1, false),
    ] {
        assert_eq!(
            source.resolve(wrong, MonoTime(5)),
            Err(DiscoveryError::Missing)
        );
    }
    source.update(&["10", "11", "1", "127.0.0.1:5001"]).unwrap();
    assert_eq!(
        source.resolve(peer, MonoTime(5)).unwrap().endpoint.port(),
        5001
    );
    assert_eq!(source.current.borrow().identities[&1], peer);
    assert_eq!(
        source.current.borrow().endpoints[0].server_name,
        "node1.voteboat.test"
    );
    let mut original = Source::parse(file).unwrap();
    assert_eq!(original.resolve(peer, MonoTime(5)).unwrap(), before);
    let mut saved =
        Source::parse(&file.replace("v1 10", "v1 11").replace(":4001", ":5001")).unwrap();
    assert_eq!(
        saved.resolve(peer, MonoTime(5)).unwrap(),
        source.resolve(peer, MonoTime(5)).unwrap()
    );
    assert_eq!(
        source
            .resolve(raft_peer(2, 402, 9), MonoTime(5))
            .unwrap()
            .endpoint
            .port(),
        4002
    );
}

#[test]
fn malformed_or_oversized_peer_sources_fail_before_publication() {
    for text in [
        "voteboat-peer-discovery-v1 0\n1 401 7 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1 extra\n1 401 7 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n",
        "voteboat-peer-discovery-v1 1\n0 401 7 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n4097 401 7 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 0 7 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 0 127.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 127.0.0.1:0 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 0.0.0.0:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 224.0.0.1:4001 node1.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 127.0.0.1:4001 invalid/name\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 127.0.0.1:4001 node1.voteboat.test\n1 402 7 127.0.0.1:4002 node2.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 127.0.0.1:4001 node1.voteboat.test\n2 401 7 127.0.0.1:4002 node2.voteboat.test\n",
        "voteboat-peer-discovery-v1 1\n1 401 7 127.0.0.1:4001 node1.voteboat.test\n2 402 7 127.0.0.1:4001 node2.voteboat.test\n",
    ] {
        assert!(Source::parse(text).is_err(), "{text}");
    }
    let mut text = String::from("voteboat-peer-discovery-v1 1\n");
    for node in 1..=64 {
        text.push_str(&format!(
            "{node} {node} 1 127.0.0.1:{} node{node}.voteboat.test\n",
            4000 + node
        ));
    }
    assert!(Source::parse(&text).is_ok());
    assert!(
        Source::parse(&(text.clone() + "65 65 1 127.0.0.1:4065 node65.voteboat.test\n")).is_err()
    );
    assert!(Source::parse(&(text + &" ".repeat(16 * 1024))).is_err());
}
