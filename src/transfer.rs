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
//! Checked ownership-transfer intents. An intent does not fence or activate owners.
use crate::transfer_publication::{TransferPublication, TransferPublicationStatus};
use crate::{application::*, directory::*, identity::*, log::*, routing::codec::*, routing::*};
use std::{collections::BTreeSet, mem::size_of};

// One Single plus at most 256 concrete-group routes fits the existing envelope.
pub const MAX_TRANSFER_INTENT_BYTES: usize = MAX_DIRECTORY_PUBLICATION_BYTES;
pub const TRANSFER_INTENT_CONTRACT_VERSION: u32 = 1;

/// Exact before/after manifests for a conservative whole-responsibility split
/// or compatible merge. Sources and targets are distinct concrete groups.
/// Checked shape is not authorization, a source fence or target import evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferIntent {
    before: ResponsibilityManifest,
    after: ResponsibilityManifest,
}
impl TransferIntent {
    #[allow(clippy::result_large_err)]
    pub fn new(
        before: ResponsibilityManifest,
        after: ResponsibilityManifest,
    ) -> Result<Self, (RoutingError, ResponsibilityManifest, ResponsibilityManifest)> {
        if let Err(error) = Self::validate(&before, &after) {
            return Err((error, before, after));
        }
        Ok(Self { before, after })
    }
    fn validate(
        before: &ResponsibilityManifest,
        after: &ResponsibilityManifest,
    ) -> Result<(), RoutingError> {
        let b = before.input();
        let a = after.input();
        // Coordinating a delegated parent's child-epoch publication is a later
        // lifecycle step; do not admit an intent this initial journal cannot bind.
        if b.parent.is_some() {
            return Err(RoutingError::WrongParent);
        }
        if b.responsibility != a.responsibility
            || b.parent != a.parent
            || b.authority != a.authority
            || b.application != a.application
            || b.scheme != a.scheme
            || b.scope != a.scope
        {
            return Err(RoutingError::IdentityChange);
        }
        if b.state != ResponsibilityState::Active || a.state != ResponsibilityState::Active {
            return Err(RoutingError::Fenced);
        }
        if b.epoch.get().checked_add(1) != Some(a.epoch.get())
            || b.generation.get().checked_add(1) != Some(a.generation.get())
        {
            return Err(RoutingError::EpochMismatch);
        }
        let sources = Self::groups(before)?;
        let targets = Self::groups(after)?;
        // This initial shape moves the complete responsibility. Partial retained
        // source ownership and delegated-namespace moves need their own protocol.
        if !matches!(
            (&b.execution, &a.execution),
            (ExecutionMode::Single(_), ExecutionMode::Partitioned(_))
                | (ExecutionMode::Partitioned(_), ExecutionMode::Single(_))
        ) {
            return Err(RoutingError::InvalidRange);
        }
        if sources
            .iter()
            .chain(&targets)
            .any(|e| e.target == RouteTarget::Group(b.authority))
        {
            return Err(RoutingError::WrongOwner);
        }
        if sources
            .iter()
            .any(|source| targets.iter().any(|target| source.target == target.target))
        {
            return Err(RoutingError::WrongOwner);
        }
        if sources.len() == 1 && targets.len() == 1 {
            return Err(RoutingError::InvalidRange);
        }
        Ok(())
    }
    fn groups(manifest: &ResponsibilityManifest) -> Result<Vec<RouteEntry>, RoutingError> {
        let routes = match &manifest.input().execution {
            ExecutionMode::Single(group) => vec![RouteEntry {
                scope: manifest.input().scope,
                target: RouteTarget::Group(*group),
            }],
            ExecutionMode::Partitioned(routes) => routes.clone(),
            ExecutionMode::Delegated(_) => return Err(RoutingError::WrongChild),
        };
        let mut unique = BTreeSet::new();
        for route in &routes {
            let RouteTarget::Group(group) = route.target else {
                return Err(RoutingError::WrongChild);
            };
            if !unique.insert(group) {
                return Err(RoutingError::WrongOwner);
            }
        }
        Ok(routes)
    }
    pub fn before(&self) -> &ResponsibilityManifest {
        &self.before
    }
    pub fn after(&self) -> &ResponsibilityManifest {
        &self.after
    }
    pub fn sources(&self) -> Vec<RouteEntry> {
        Self::groups(&self.before).expect("checked intent")
    }
    pub fn targets(&self) -> Vec<RouteEntry> {
        Self::groups(&self.after).expect("checked intent")
    }
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        let len = 16 + manifest_len(&self.before) + manifest_len(&self.after);
        if len > max_bytes || len > MAX_TRANSFER_INTENT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut bytes = Vec::with_capacity(len);
        bytes.extend(b"VBTINT01");
        for m in [&self.before, &self.after] {
            bytes.extend((manifest_len(m) as u32).to_le_bytes());
            put_manifest(&mut bytes, m);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, ApplicationError> {
        if bytes.len() > MAX_TRANSFER_INTENT_BYTES {
            return Err(ApplicationError::InvalidCommand);
        }
        let mut r = Reader::new(bytes);
        if r.take(8)? != b"VBTINT01" {
            return Err(ApplicationError::InvalidCommand);
        }
        let len = r.u32()? as usize;
        let before = read_manifest(r.take(len)?)?;
        let len = r.u32()? as usize;
        let after = read_manifest(r.take(len)?)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCommand);
        }
        Self::new(before, after).map_err(|_| ApplicationError::InvalidCommand)
    }
    pub fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.before.retained_bytes() + self.after.retained_bytes()
            - 2 * size_of::<ResponsibilityManifest>()
    }
}

/// Local committed/applied intent status. A quorum-backed read establishes
/// authoritative observation; this value alone is not a transferable certificate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferIntentStatus {
    pub operation: OperationId,
    pub index: u64,
    pub intent: TransferIntent,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectoryQuery {
    Manifest(ResponsibilityIdentity),
    Transfer(OperationId),
    Publication(OperationId),
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)] // Fixed inline layout is charged in the result bound.
pub enum DirectoryRead {
    Manifest(Option<ResponsibilityManifest>),
    Transfer(Option<TransferIntentStatus>),
    Publication(Option<TransferPublicationStatus>),
}

/// Read view with lifecycle queries over the same directory state, log and
/// checkpoint. It introduces no second metadata owner or storage binding.
#[derive(Clone)]
pub struct LifecycleDirectory(Directory);
impl LifecycleDirectory {
    pub fn new(directory: Directory) -> Self {
        Self(directory)
    }
    pub fn directory(&self) -> &Directory {
        &self.0
    }
    pub fn into_directory(self) -> Directory {
        self.0
    }
}
impl StateMachine for LifecycleDirectory {
    type Receipt = DirectoryReceipt;
    fn validate_group(&self, group: GroupIdentity) -> Result<(), ApplicationError> {
        self.0.validate_group(group)
    }
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<DirectoryReceipt>, ApplicationError> {
        self.0.apply_batch(entries)
    }
}
impl BoundedStateMachine for LifecycleDirectory {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for LifecycleDirectory {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)
    }
}
impl CheckpointStateMachine for LifecycleDirectory {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, max_bytes: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(max_bytes)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(schema, applied, bytes)
    }
}
impl ReadableStateMachine for LifecycleDirectory {
    type Query = DirectoryQuery;
    type ReadResult = DirectoryRead;
    fn read_at(
        &self,
        required: u64,
        query: DirectoryQuery,
    ) -> Result<DirectoryRead, ApplicationError> {
        match query {
            DirectoryQuery::Manifest(id) => {
                self.0.read_at(required, id).map(DirectoryRead::Manifest)
            }
            DirectoryQuery::Transfer(operation) => self
                .0
                .transfer_intent_at(required, operation)
                .map(DirectoryRead::Transfer),
            DirectoryQuery::Publication(operation) => self
                .0
                .transfer_publication_at(required, operation)
                .map(DirectoryRead::Publication),
        }
    }
}
impl BoundedReadableStateMachine for LifecycleDirectory {
    fn query_bytes(&self, _: &DirectoryQuery, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
    fn read_result_bound(&self, query: &DirectoryQuery) -> Result<usize, ApplicationError> {
        Ok(size_of::<DirectoryRead>()
            + match query {
                DirectoryQuery::Manifest(id) => self.0.manifest(*id).map_or(0, |m| {
                    m.retained_bytes() - size_of::<ResponsibilityManifest>()
                }),
                DirectoryQuery::Transfer(operation) => self
                    .0
                    .transfer_intent_at(self.applied_index(), *operation)?
                    .map_or(0, |s| {
                        s.intent.retained_bytes() - size_of::<TransferIntent>()
                    }),
                DirectoryQuery::Publication(operation) => self
                    .0
                    .transfer_publication_at(self.applied_index(), *operation)?
                    .map_or(0, |s| {
                        s.publication.retained_bytes() - size_of::<TransferPublication>()
                    }),
            })
    }
    fn read_result_bytes(
        &self,
        result: &DirectoryRead,
        limit: usize,
    ) -> Result<usize, ApplicationError> {
        let bytes = match result {
            DirectoryRead::Manifest(m) => m.as_ref().map_or(0, |m| {
                m.retained_bytes() - size_of::<ResponsibilityManifest>()
            }),
            DirectoryRead::Transfer(s) => s.as_ref().map_or(0, |s| {
                s.intent.retained_bytes() - size_of::<TransferIntent>()
            }),
            DirectoryRead::Publication(s) => s.as_ref().map_or(0, |s| {
                s.publication.retained_bytes() - size_of::<TransferPublication>()
            }),
        };
        if bytes > limit {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentDigest(pub [u8; 32]);
impl ContentDigest {
    pub fn sha256(bytes: &[u8]) -> Self {
        Self(
            ring::digest::digest(&ring::digest::SHA256, bytes)
                .as_ref()
                .try_into()
                .expect("SHA-256 length"),
        )
    }
}
impl ContentDigest {
    /// Canonical domain-separated image commitment; not an authentication proof.
    pub fn scope_image(image: &crate::scope::ScopeImage) -> Self {
        let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
        hash.update(b"VBSIMAGE");
        hash.update(&image.schema().to_le_bytes());
        hash.update(&image.scheme().id.get().to_le_bytes());
        hash.update(&image.scheme().version.to_le_bytes());
        hash.update(&image.scope().start().to_le_bytes());
        hash.update(&image.scope().end().to_le_bytes());
        hash.update(&image.source_applied().to_le_bytes());
        hash.update(&(image.bytes().len() as u64).to_le_bytes());
        hash.update(image.bytes());
        Self(hash.finish().as_ref().try_into().expect("SHA-256 length"))
    }
}
