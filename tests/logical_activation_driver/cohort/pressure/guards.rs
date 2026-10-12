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
//! Original refused inputs/outputs remain owned and charged under pressure.
use super::*;
pub fn ordinary(parent: &mut fixture::FacadeNode) {
    let app = fixture::app_with_mappings((0..3).map(groups::mapping).collect());
    let bytes = app.business(&1_i64.to_le_bytes()).unwrap();
    let pointer = bytes.as_ptr();
    let capacity = bytes.capacity();
    let clients = parent.local().clients.usage();
    let rejected = parent
        .propose(ClientRequest {
            group: groups::mapping(1).parent,
            operation: OperationId::new(335_090).unwrap(),
            bytes,
        })
        .unwrap_err();
    assert_eq!(rejected.reason, ClientError::Overloaded);
    assert_eq!(rejected.request.bytes.as_ptr(), pointer);
    assert_eq!(rejected.request.bytes.capacity(), capacity);
    assert_eq!(
        rejected.request.bytes,
        app.business(&1_i64.to_le_bytes()).unwrap()
    );
    assert_eq!(parent.local().clients.usage(), clients);
    let reads = parent.local().reads.usage();
    let rejected = parent
        .read(groups::mapping(1).parent, ActivationQuery::Business(()))
        .unwrap_err();
    assert_eq!(rejected.reason, ReadInvocationError::Overloaded);
    assert_eq!(rejected.group, groups::mapping(1).parent);
    assert_eq!(rejected.query, ActivationQuery::Business(()));
    assert_eq!(parent.local().reads.usage(), reads);
}
pub fn control(
    peers: &mut progress::Peers<'_>,
    parent: &mut fixture::FacadeNode,
    target: &mut aliases::Bridge<'_>,
) {
    let original = holding::read(peers, parent, target, ActivationQuery::Active);
    assert!(!parent
        .local()
        .reads
        .retains_only_exported_ordinary_outputs(groups::mapping(1).parent));
    ownership::refused(parent);
    assert!(matches!(
        parent.complete_read(original).unwrap(),
        ReadOutcome::Read {
            result: Ok(ActivationRead::Active(None)),
            ..
        }
    ));
    ownership::clear(parent);
}
pub fn stale(held: &holding::Held, parent: &mut fixture::FacadeNode) {
    let (mut client, mut read) = held.tickets();
    client.binding.generation =
        ClientRouterGeneration::new(client.binding.generation.get() + 1).unwrap();
    read.binding.generation =
        ReadInvocationGeneration::new(read.binding.generation.get() + 1).unwrap();
    assert_eq!(parent.cancel_client(client), Err(ClientError::StaleTicket));
    assert_eq!(
        parent.cancel_read(read),
        Err(ReadInvocationError::StaleTicket)
    );
    assert_eq!(
        parent.inspect_read(read),
        Err(ReadInvocationError::StaleTicket)
    );
    held.assert_retained(parent);
}
