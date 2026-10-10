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
//! Counter application declaration and offline enrollment.
pub use super::service_setup::{checked, configuration, group, join, Failure, PeerInput, MAX_NODE};
use std::path::Path;
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{connect::*, node::*, startup::*},
};
pub type Service = NativeNode<super::counter_application::Application, NativeServiceConnector>;
fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
pub fn application() -> Result<Counter, Failure> {
    let app = checked(Counter::new(10000))?;
    // Bind the service declaration to the same capacity enforced by admission,
    // application and restore, including future retry history growth.
    let requirements = voteboat::raft::ReadinessRequirements {
        application_schema: 1,
        command_bytes: 8,
        snapshot_bytes: 330032,
    };
    checked(app.validate_readiness_requirements(requirements))?;
    let wire = voteboat::wire::WireLimits::default();
    if requirements.command_bytes > wire.max_command_bytes
        || requirements.command_bytes > LogLimits::default().max_command_bytes
        || requirements.snapshot_bytes > wire.max_snapshot_bytes
        || requirements.snapshot_bytes > LogLimits::default().max_snapshot_bytes
        || requirements.snapshot_bytes
            > voteboat::snapshot::SnapshotLimits::default().max_application_bytes
    {
        return Err("counter application envelope exceeds selected native payload limits".into());
    }
    Ok(app)
}
/// Offline trusted handoff. Both stores must be stopped; recovery can advance
/// provider sessions even when enrollment subsequently fails.
pub fn enroll(
    mut config: NativeMemberStartup,
    source: &Path,
    source_node: u64,
    maintenance: bool,
) -> Result<(u64, u64), Failure> {
    use voteboat::{
        native::{log_store::*, snapshot_store::*},
        snapshot::*,
    };
    if !(1..=MAX_NODE).contains(&source_node) || node(source_node) == config.startup.node {
        return Err("expected a distinct provisioned source voter".into());
    }
    let source_path = source.canonicalize()?;
    if config.startup.directory.exists() && config.startup.directory.canonicalize()? == source_path
    {
        return Err("source and destination must be distinct stores".into());
    }
    let source_store = *config
        .provisioned_stores
        .get(&node(source_node))
        .ok_or("source node missing from deployment")?;
    config.startup.tls =
        checked(
            config
                .startup
                .tls
                .with_wire_version(if maintenance { 8 } else { 7 }),
        )?;
    let mut destination_app =
        super::counter_application::Application::new(application()?, maintenance)?;
    let mut source_app = super::counter_application::Application::new(application()?, maintenance)?;
    let log = checked(NativeLogStore::recover(
        FileLogIo::open(&source_path)?,
        source_store,
        LogLimits::default(),
    ))?;
    let state = checked(log.state(group()))?;
    if state.bootstrap != config.startup.bootstrap {
        return Err("source bootstrap does not match deployment".into());
    }
    let reference = state
        .snapshot
        .ok_or("source requires a pinned committed checkpoint")?;
    let mut snapshots = checked(NativeSnapshotStore::recover(
        FileSnapshotIo::open(source_path.join("snapshots"))?,
        SnapshotIdentity {
            store: source_store,
            group: group(),
        },
        SnapshotLimits::default(),
    ))?;
    // Verify authoritative boundary, exact source membership and application,
    // including any committed tail. A latest unpinned publication is insufficient.
    checked(recover_member_replica(
        node(source_node),
        group(),
        &log,
        &mut snapshots,
        &mut source_app,
    ))?;
    let image = checked(snapshots.load_pinned(reference))?;
    if image.metadata.membership != checked(state.checkpoint_membership(state.commit_index))? {
        return Err("source checkpoint membership is stale; checkpoint the committed view".into());
    }
    if !image
        .metadata
        .membership
        .as_ref()
        .is_some_and(|membership| {
            membership.joint().is_none()
                && membership.stable().voter_stores().get(&node(source_node)) == Some(&source_store)
        })
    {
        return Err("source checkpoint must contain a stable exact voter assignment".into());
    }
    config.enroll_snapshot(&image, &mut destination_app)?;
    Ok((image.metadata.index, image.metadata.term))
}
pub fn open(
    config: NativeMemberStartup,
    protocol: NativePeerProtocol,
    member: bool,
    maintenance: bool,
    rotation: Option<NativePeerRotationStartup>,
) -> Result<Service, Failure> {
    let mut config = config;
    if maintenance {
        config.startup.tls = checked(config.startup.tls.with_wire_version(8))?;
    }
    let app = super::counter_application::Application::new(application()?, maintenance)?;
    match rotation {
        Some(rotation) => super::service_setup::open_application_with_rotation(
            config,
            protocol,
            member,
            app,
            Some(rotation),
        ),
        None => super::service_setup::open_application(config, protocol, member, app),
    }
}
