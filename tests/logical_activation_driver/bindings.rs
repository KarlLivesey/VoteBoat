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
//! Original verified child profile and owned parent construction failures.
use super::*;
use voteboat::application::StateMachine;
#[path = "owner_intake.rs"]
mod owner_intake;
fn descriptors(image: &VerifiedLogicalImage) {
    let descriptor =
        LogicalPeerDescriptor::from_verified(image, &fixture::child_application()).unwrap();
    assert_eq!(descriptor.mapping(), image.mapping());
    assert_eq!(descriptor.representative(), image.representative());
    let selected = fixture::child_application();
    let ordinary = LogicalApplication::new(
        selected.mapping(),
        selected.child_bootstrap().clone(),
        selected.parent_bootstrap().clone(),
        selected.limits(),
        voteboat::log_format::NativeLogCodec,
        voteboat::snapshot_format::NativeSnapshotCodec,
        voteboat::wire_format::NativeWireCodec::with_leadership_transfer(
            voteboat::wire::WireLimits::default(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(LogicalPeerDescriptor::from_verified(image, &ordinary).is_err());
}
#[test]
fn original_verified_profile_rejects_changed_or_missing_parent_before_transfer() {
    let _serial = cluster::HISTORY.lock().unwrap();
    let mut cohort = cluster::Cluster::new_with_application(
        NativePeerProtocol::TcpTls,
        fixture::child_application(),
    );
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = drive::claim_in(&mut bridge, clock, 8000, false);
    assert_eq!(
        image.activation_control_profile(),
        Some(&fixture::app().control_profile())
    );
    descriptors(&image);
    owner_intake::short_input_reserves_preserve_original_runtime(&image);
    let mut parts = fixture::build(&image);
    router_reservations(&image, &parts.local.owner);
    client_reservations(&image, &parts.local.owner);
    let original = parts
        .local
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    let different = LogicalActivationApplication::new(
        setup::mapping().parent,
        vec![setup::mapping()],
        LogicalActivationLimits {
            intents: 3,
            ..fixture::app().limits()
        },
        application::App::new(),
    )
    .unwrap();
    parts
        .local
        .applications
        .insert(setup::mapping().parent, different);
    let rejected =
        match fixture::FacadeNode::from_parts(&image, parts, fixture::limits(), MonoTime(0)) {
            Ok(_) => panic!("changed original wrapper profile accepted"),
            Err(e) => e,
        };
    assert_eq!(rejected.reason, LogicalNodeError::WrongBinding);
    let mut parts = *rejected.parts;
    assert_eq!(
        parts
            .local
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state(),
        &original
    );
    assert!(parts.persistence.is_drained());
    assert_eq!(
        parts.local.applications[&setup::mapping().parent].applied_index(),
        0
    );
    parts.local.applications.remove(&setup::mapping().parent);
    let rejected =
        match fixture::FacadeNode::from_parts(&image, parts, fixture::limits(), MonoTime(0)) {
            Ok(_) => panic!("missing original parent application accepted"),
            Err(e) => e,
        };
    assert_eq!(rejected.reason, LogicalNodeError::WrongBinding);
    let mut parts = *rejected.parts;
    assert_eq!(
        parts
            .local
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state(),
        &original
    );
    parts
        .local
        .applications
        .insert(setup::mapping().parent, fixture::app());
    let parts = release_reservations(&image, parts);
    let mut node = fixture::FacadeNode::from_parts(&image, parts, fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("exact original profile {:?}", e.reason));
    node.close_intake().unwrap();
    progress::until(
        &mut bridge,
        &mut node,
        clock,
        fixture::FacadeNode::is_drained,
    );
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    let parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("original parent drain"));
    assert!(parts.persistence.is_drained());
    drop(parts);
    cohort.finish();
}
fn router_reservations(
    image: &VerifiedLogicalImage,
    owner: &EffectOwner<
        voteboat::native::runtime::FairScheduler,
        voteboat::native::runtime::DeadlineQueue,
        voteboat::native::runtime::JitterEntropy,
    >,
) {
    type Routers = LogicalRouterParts<
        ActivationReceipt<voteboat::application::CounterReceipt>,
        ActivationQuery<()>,
        ActivationRead<i64>,
    >;
    let original = owner.core(setup::mapping().parent).unwrap().state().clone();
    for field in 0..4 {
        let mut limits = fixture::router_limits();
        match field {
            0 => limits = LogicalRouterLimits::default(),
            1 => limits.applications.control_batches = 1,
            2 => limits.applications.batch_receipts -= 1,
            _ => limits.applications.receipts = 2 * limits.applications.batch_receipts,
        }
        let rejected: Result<Routers, _> = image.routers(owner, limits);
        assert!(matches!(
            rejected,
            Err(LogicalReleaseError::ApplicationRoute(
                ApplicationRouteError::InvalidLimits
            ))
        ));
        assert_eq!(
            owner.core(setup::mapping().parent).unwrap().state(),
            &original
        );
    }
    let exact: Routers = image.routers(owner, fixture::router_limits()).unwrap();
    assert_eq!(exact.results.usage(), ApplicationResultUsage::default());
    assert!(exact.results.is_drained());
}
fn release_reservations(image: &VerifiedLogicalImage, mut parts: fixture::Parts) -> fixture::Parts {
    let original = parts
        .local
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    let authorization = parts.local.owner.authorization();
    for field in 0..4 {
        let mut limits = fixture::limits();
        match field {
            0 => limits.releases.control_pending = 1,
            1 => limits.releases.control_bytes = 128 * 1024,
            2 => limits.releases.bytes = limits.releases.control_bytes + 1,
            _ => limits.releases = LogicalReleaseLimits::default(),
        }
        let rejected = match fixture::FacadeNode::from_parts(image, parts, limits, MonoTime(0)) {
            Ok(_) => panic!("incomplete selected release windows accepted"),
            Err(e) => e,
        };
        assert_eq!(
            rejected.reason,
            LogicalNodeError::Replica(ReplicaError::Logical(LogicalReleaseError::InvalidLimits))
        );
        parts = *rejected.parts;
        assert_eq!(parts.local.owner.authorization(), authorization);
        assert_eq!(
            parts
                .local
                .owner
                .core(setup::mapping().parent)
                .unwrap()
                .state(),
            &original
        );
        assert_eq!(
            parts.local.results.usage(),
            ApplicationResultUsage::default()
        );
        assert!(parts.persistence.is_drained());
    }
    parts
}
fn client_reservations(
    image: &VerifiedLogicalImage,
    owner: &EffectOwner<
        voteboat::native::runtime::FairScheduler,
        voteboat::native::runtime::DeadlineQueue,
        voteboat::native::runtime::JitterEntropy,
    >,
) {
    type Routers = LogicalRouterParts<
        ActivationReceipt<voteboat::application::CounterReceipt>,
        ActivationQuery<()>,
        ActivationRead<i64>,
    >;
    let original = owner.core(setup::mapping().parent).unwrap().state().clone();
    for field in 0..5 {
        let mut limits = fixture::router_limits();
        match field {
            0 => limits.clients.control_requests = 1,
            1 => limits.clients.group_control_requests = 1,
            2 => limits.clients.control_bytes = 1,
            3 => limits.clients.bytes = limits.clients.control_bytes + 1,
            _ => {
                limits.clients.command_bytes = image
                    .activation_control_profile()
                    .unwrap()
                    .limits()
                    .command_bytes
                    - 1;
            }
        }
        let rejected: Result<Routers, _> = image.routers(owner, limits);
        assert!(matches!(
            rejected,
            Err(LogicalReleaseError::Client(ClientError::InvalidLimits))
        ));
        assert_eq!(
            owner.core(setup::mapping().parent).unwrap().state(),
            &original
        );
    }
    let exact: Routers = image.routers(owner, fixture::router_limits()).unwrap();
    assert_eq!(exact.clients.usage(), ClientUsage::default());
    assert!(exact.clients.is_drained());
}
