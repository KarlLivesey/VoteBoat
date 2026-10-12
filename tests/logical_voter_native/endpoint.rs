// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    cluster::{Cluster, HISTORY},
    setup, support,
};
use voteboat::{
    application::ApplicationError,
    log::Bootstrap,
    log_format::NativeLogCodec,
    logical_voter::*,
    native::connect::NativePeerProtocol,
    quorum::{Limits, Policy, Tree},
    runtime::{
        ClientRouterBinding, ClientUsage, ReadInvocationBinding, ReadInvocationUsage, RuntimeOwner,
    },
    secure::LocalIdentity,
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
};
fn configured(
    mapping: LogicalStoreMapping,
    parent: Bootstrap,
) -> Result<LogicalApplication, ApplicationError> {
    LogicalApplication::new(
        mapping,
        setup::child(),
        parent,
        setup::limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_leadership_transfer(voteboat::wire::WireLimits::default()).unwrap(),
    )
}
fn alias_app(voter_alias: bool) -> LogicalApplication {
    let mut mapping = setup::mapping();
    if voter_alias {
        mapping.voter = support::node(1);
    } else {
        mapping.store = support::identity(1);
    }
    let mut parent = setup::parent();
    parent.voter_stores.remove(&support::node(11));
    parent.voter_stores.insert(mapping.voter, mapping.store);
    parent.policy = Policy::new(
        Tree::Majority(
            parent
                .voter_stores
                .keys()
                .copied()
                .map(Tree::Voter)
                .collect(),
        ),
        Limits::default(),
    )
    .unwrap();
    configured(mapping, parent).unwrap()
}
#[derive(Debug, Eq, PartialEq)]
struct Resources {
    owner: RuntimeOwner,
    metadata: LogicalMetadata,
    clients: ClientRouterBinding,
    client_usage: ClientUsage,
    reads: ReadInvocationBinding,
    read_usage: ReadInvocationUsage,
}
fn original_resources(host: &Cluster) -> Vec<Resources> {
    host.nodes
        .iter()
        .map(|node| {
            let local = node.local();
            Resources {
                owner: local.owner.identity(),
                metadata: local.applications[&setup::mapping().child].metadata(),
                clients: local.clients.binding(),
                client_usage: local.clients.usage(),
                reads: local.reads.binding(),
                read_usage: local.reads.usage(),
            }
        })
        .collect()
}
fn refused_alias(protocol: NativePeerProtocol, voter_alias: bool) {
    let app = alias_app(voter_alias);
    let mapping = app.mapping();
    let mut host = Cluster::new_with_application(protocol, app);
    let before = original_resources(&host);
    let rejected = LogicalBackingBridge::new(&mut host.nodes, mapping, support::node(1));
    assert!(matches!(
        rejected,
        Err(LogicalBridgeError::Endpoint(
            LogicalEndpointError::DuplicateIdentity
        ))
    ));
    assert_eq!(original_resources(&host), before);
    assert!(host
        .nodes
        .iter()
        .all(|node| node.local().clients.is_drained() && node.local().reads.is_drained()));
    host.finish();
}
fn history(protocol: NativePeerProtocol) {
    let _serial = HISTORY.lock().unwrap();
    let mut host = Cluster::new(protocol);
    let leader = host.leader().unwrap();
    assert_eq!(
        host.nodes[leader]
            .local()
            .owner
            .core(setup::mapping().child)
            .unwrap()
            .local_node(),
        support::node(1)
    );
    let physical = LocalIdentity {
        node: support::node(3),
        store: host.nodes[2].local().owner.identity().store,
    };
    let zero = LogicalBackingChild::endpoint_chain(&host.nodes[2], setup::mapping().child).unwrap();
    assert_eq!(zero.depth(), 0);
    assert_eq!(zero.leaf(), physical);
    assert_eq!(
        LogicalBackingChild::endpoint_chain(&host.nodes[2], support::group(999)),
        Err(LogicalEndpointError::WrongBinding)
    );
    let before = original_resources(&host);
    let mut same = setup::mapping();
    same.parent = same.child;
    let mut parent = setup::parent();
    parent.group = same.parent;
    assert!(matches!(
        configured(same, parent),
        Err(ApplicationError::InvalidCommand)
    ));
    assert_eq!(original_resources(&host), before);
    let image = host.claim(3, 89101, true);
    let chain = image.endpoint_chain();
    assert_eq!(chain.depth(), 1);
    assert_eq!(chain.leaf_group(), setup::mapping().child);
    assert_eq!(chain.leaf(), physical);
    let hop = chain.hops()[0];
    assert_eq!(hop.mapping(), image.mapping());
    assert_eq!(hop.representative(), image.representative());
    assert_eq!(hop.claim(), image.claim());
    assert_eq!(hop.barrier(), image.child_barrier());
    assert_eq!(hop.child_applied(), image.image().child_applied);
    assert_eq!(
        hop.child_configuration(),
        image.image().child_membership.id()
    );
    let descriptor = LogicalPeerDescriptor::from_verified(&image, &setup::app()).unwrap();
    assert_eq!(descriptor.physical(), physical);
    assert_eq!(descriptor.endpoint_chain(), chain);
    assert_eq!(descriptor.representative().physical.node, support::node(3));
    host.finish();
    refused_alias(protocol, true);
    refused_alias(protocol, false);
}
#[test]
fn tcp_endpoint_uses_selected_original_representative_and_refuses_aliases() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_endpoint_uses_selected_original_representative_and_refuses_aliases() {
    history(NativePeerProtocol::Quic);
}
