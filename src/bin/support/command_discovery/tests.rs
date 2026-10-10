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
