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
//! Deterministic child-backed logical storage and consumed-observation bridge.
//! Serialized internal commands are trusted host-driver assertions, not certificates.
use crate::{
    application::*,
    identity::*,
    log::*,
    log_format::{LogCodec, NativeLogCodec},
    membership::Membership,
    snapshot::*,
    snapshot_format::{NativeSnapshotCodec, SnapshotCodec},
    wire::WireCodec,
    wire_format::NativeWireCodec,
};
use std::collections::BTreeMap;
mod activation;
pub use activation::*;
mod enrollment;
pub use enrollment::*;
mod types;
pub use types::*;
mod command;
mod control_profile;
pub(crate) use command::*;
mod codec;
pub use codec::LOGICAL_APPLICATION_SCHEMA;
mod admission;
mod application;
mod bridge;
mod checkpoint;
mod query;
mod response;
pub use response::{LOGICAL_ENROLLMENT_RESPONSE_SCHEMA, LOGICAL_RESPONSE_SCHEMA};
mod traits;
pub use bridge::*;
mod endpoint;
pub use endpoint::*;
mod startup;
pub use startup::*;
mod persistence;
pub use persistence::*;
mod snapshots;
pub use snapshots::*;
mod runtime;
pub use runtime::*;
mod network;
pub use network::*;
mod node;
pub use node::*;
#[derive(Clone)]
pub struct LogicalApplication<C = NativeLogCodec, S = NativeSnapshotCodec, W = NativeWireCodec> {
    mapping: LogicalStoreMapping,
    child_bootstrap: Bootstrap,
    parent_bootstrap: Bootstrap,
    limits: LogicalLimits,
    log_codec: C,
    snapshot_codec: S,
    wire_codec: W,
    configuration: Vec<u8>,
    /// Where the exact parent-bootstrap bytes (the binding a first claim
    /// carries) lie inside `configuration`; kept as a range, not a copy.
    parent_binding: std::ops::Range<usize>,
    control_profile: Option<LogicalActivationControlProfile>,
    state: application::State,
    initialization: LogicalInitialization,
    /// Test-only record of every entry applied since construction (`None`
    /// after a restore), so tests can replay the full history.
    #[cfg(test)]
    journal: Option<Vec<LogEntry>>,
}
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub fn new(
        mapping: LogicalStoreMapping,
        child_bootstrap: Bootstrap,
        parent_bootstrap: Bootstrap,
        limits: LogicalLimits,
        log_codec: C,
        snapshot_codec: S,
        wire_codec: W,
    ) -> Result<Self, ApplicationError> {
        Self::construct(
            mapping,
            child_bootstrap,
            parent_bootstrap,
            limits,
            (log_codec, snapshot_codec, wire_codec),
            LogicalInitialization::BootstrapVoter,
        )
    }
    fn construct(
        mapping: LogicalStoreMapping,
        child_bootstrap: Bootstrap,
        parent_bootstrap: Bootstrap,
        limits: LogicalLimits,
        codecs: (C, S, W),
        initialization: LogicalInitialization,
    ) -> Result<Self, ApplicationError> {
        let (log_codec, snapshot_codec, wire_codec) = codecs;
        let limits = limits.validate()?;
        if LogicalStoreMapping::new(mapping.parent, mapping.voter, mapping.store, mapping.child)?
            != mapping
            || child_bootstrap.group != mapping.child
            || parent_bootstrap.group != mapping.parent
            || !initialization.matches(mapping, &parent_bootstrap)
            || !log_codec.supports_checkpoint()
            || log_codec.format_version() == 0
            || snapshot_codec.format_version() == 0
            || wire_codec.format_version() == 0
        {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut groups = BTreeMap::new();
        apply_batch(
            &mut groups,
            &[LogMutation::Create(parent_bootstrap.clone())],
            limits.parent_log,
        )
        .map_err(|_| ApplicationError::InvalidCommand)?;
        let initial_parent = groups
            .remove(&mapping.parent)
            .ok_or(ApplicationError::InvalidCommand)?;
        groups.clear();
        apply_batch(
            &mut groups,
            &[LogMutation::Create(child_bootstrap.clone())],
            limits.parent_log,
        )
        .map_err(|_| ApplicationError::InvalidCommand)?;
        let state = application::State::new(&child_bootstrap)?;
        let mut app = Self {
            mapping,
            child_bootstrap,
            parent_bootstrap,
            limits,
            log_codec,
            snapshot_codec,
            wire_codec,
            configuration: Vec::new(),
            parent_binding: 0..0,
            control_profile: None,
            state,
            #[cfg(test)]
            journal: Some(Vec::new()),
            initialization,
        };
        app.set_configuration()?;
        app.validate_enrollment_limits()?;
        app.validate_claim_limits()?;
        app.validate_initial_parent(&initial_parent)?;
        app.validate_read_envelopes()?;
        app.encode_state(limits.max_child_checkpoint_bytes)?;
        Ok(app)
    }
    pub fn mapping(&self) -> LogicalStoreMapping {
        self.mapping
    }
    pub fn limits(&self) -> LogicalLimits {
        self.limits
    }
    pub fn child_bootstrap(&self) -> &Bootstrap {
        &self.child_bootstrap
    }
    pub fn parent_bootstrap(&self) -> &Bootstrap {
        &self.parent_bootstrap
    }
    pub fn same_configuration(&self, other: &Self) -> bool {
        self.configuration == other.configuration
    }
}
