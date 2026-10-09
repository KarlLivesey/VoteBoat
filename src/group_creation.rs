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
//! Exact committed creation intent to durable assigned bootstrap. No serving lease.
use crate::{
    application::*, contracts::StorageError, directory::*, identity::*, log::*, raft::Raft,
    routing::ApplicationAdapter,
};

pub const MAX_CREATION_BINDING_BYTES: usize = MAX_GROUP_CREATION_BYTES + 68;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreationBootstrapError {
    Rejected(&'static str),
    Application(ApplicationError),
    Storage(StorageError),
}
impl From<StorageError> for CreationBootstrapError {
    fn from(e: StorageError) -> Self {
        Self::Storage(e)
    }
}
impl From<ApplicationError> for CreationBootstrapError {
    fn from(e: ApplicationError) -> Self {
        Self::Application(e)
    }
}
/// Verify exact source identity and historical commitment before local mutation.
/// Remote adapters require authenticated metadata authority; this is trusted
/// host verification under the non-Byzantine model, not a cryptographic proof.
pub trait CreationAuthority {
    fn verify(&self, status: &GroupCreationStatus) -> Result<(), CreationBootstrapError>;
}
pub struct LocalCreationAuthority<'a> {
    pub core: &'a Raft,
    pub directory: &'a Directory,
}
impl CreationAuthority for LocalCreationAuthority<'_> {
    fn verify(&self, status: &GroupCreationStatus) -> Result<(), CreationBootstrapError> {
        let state = self.core.state();
        self.directory.validate_group(state.bootstrap.group)?;
        if self.core.is_fenced()
            || status.index == 0
            || status.intent.authority != state.bootstrap.group
            || status.index > state.commit_index
            || self.directory.applied_index() > state.commit_index
            || self
                .directory
                .group_creation_at(status.index, status.intent.bootstrap.group)?
                .as_ref()
                != Some(status)
        {
            return Err(CreationBootstrapError::Rejected(
                "unverified committed creation",
            ));
        }
        Ok(())
    }
}
/// Optional bootstrap capability, using the same authoritative logical store.
/// Return typed durable absence, never turn a fenced/corrupt read into None.
/// Provisioning requires quiescent mutation ownership; implementations must
/// refuse outstanding accepted writes rather than guessing their result.
pub trait CreationLogStore: LogStore {
    fn creation_state(&self, group: GroupIdentity) -> Result<Option<GroupLog>, StorageError>;
}
/// One bounded immutable record. load returns published bytes only. publish_exact
/// must make identical bytes durable before success, refuse a differing existing
/// record and preserve uncertainty. No binding is a commitment certificate.
pub trait CreationBindings {
    fn load(&mut self) -> Result<Option<Vec<u8>>, StorageError>;
    fn publish_exact(&mut self, bytes: &[u8]) -> Result<(), StorageError>;
}
#[derive(Clone, Debug)]
pub struct VerifiedGroupCreation {
    status: GroupCreationStatus,
    node: NodeId,
    store: StoreIdentity,
    bytes: Vec<u8>,
}
impl VerifiedGroupCreation {
    pub fn verify(
        authority: &impl CreationAuthority,
        status: GroupCreationStatus,
        node: NodeId,
        store: StoreIdentity,
        application: ApplicationAdapter,
    ) -> Result<Self, CreationBootstrapError> {
        let intent = status.intent.encode(MAX_GROUP_CREATION_BYTES)?;
        if status.index == 0
            || status.intent.application != application
            || status.intent.bootstrap.voter_stores.get(&node) != Some(&store)
        {
            return Err(CreationBootstrapError::Rejected(
                "creation assignment or application",
            ));
        }
        authority.verify(&status)?;
        let mut bytes = Vec::with_capacity(68 + intent.len());
        bytes.extend(b"VBGCASN1");
        bytes.extend(node.get().to_le_bytes());
        bytes.extend(store.id.get().to_le_bytes());
        bytes.extend(store.incarnation.get().to_le_bytes());
        bytes.extend(status.operation.get().to_le_bytes());
        bytes.extend(status.index.to_le_bytes());
        bytes.extend((intent.len() as u32).to_le_bytes());
        bytes.extend(intent);
        Ok(Self {
            status,
            node,
            store,
            bytes,
        })
    }
    pub fn status(&self) -> &GroupCreationStatus {
        &self.status
    }
    pub fn node(&self) -> NodeId {
        self.node
    }
    pub fn store(&self) -> StoreIdentity {
        self.store
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreationBootstrapReceipt {
    binding: StoreBinding,
    group: GroupIdentity,
    operation: OperationId,
    metadata_index: u64,
}
impl CreationBootstrapReceipt {
    pub fn binding(&self) -> StoreBinding {
        self.binding
    }
    pub fn group(&self) -> GroupIdentity {
        self.group
    }
    pub fn operation(&self) -> OperationId {
        self.operation
    }
    pub fn metadata_index(&self) -> u64 {
        self.metadata_index
    }
}
/// Synchronous offline provisioning over selected host/native providers. Run
/// blocking providers outside the consensus owner. Error never means rollback.
pub fn establish_created_group(
    verified: &VerifiedGroupCreation,
    log: &mut impl CreationLogStore,
    bindings: &mut impl CreationBindings,
) -> Result<CreationBootstrapReceipt, CreationBootstrapError> {
    if log.binding().identity != verified.store {
        return Err(CreationBootstrapError::Rejected("wrong assigned store"));
    }
    let bootstrap = &verified.status.intent.bootstrap;
    let before = log.creation_state(bootstrap.group)?;
    let binding = bindings.load()?;
    if binding
        .as_ref()
        .is_some_and(|bytes| bytes != &verified.bytes)
        || before
            .as_ref()
            .is_some_and(|state| state.bootstrap != *bootstrap)
    {
        return Err(CreationBootstrapError::Rejected(
            "conflicting creation binding",
        ));
    }
    if before.is_some() && binding.is_none() {
        return Err(CreationBootstrapError::Rejected("existing unbound group"));
    }
    // Success must represent durability even when the published bytes match
    // after a prior uncertain publication or observation loss.
    bindings.publish_exact(&verified.bytes)?;
    if before.is_none() {
        let tickets = log.append_batch(vec![LogMutation::Create(bootstrap.clone())])?;
        if tickets.len() != 1
            || tickets[0].binding != log.binding()
            || tickets[0].group != bootstrap.group
        {
            return Err(CreationBootstrapError::Rejected(
                "bootstrap provider ticket",
            ));
        }
        let durable = log.barrier(&tickets)?;
        if durable.tickets != tickets {
            return Err(CreationBootstrapError::Rejected(
                "bootstrap provider completion",
            ));
        }
    }
    let state = log
        .creation_state(bootstrap.group)?
        .ok_or(CreationBootstrapError::Rejected("bootstrap is not durable"))?;
    if state.bootstrap != *bootstrap {
        return Err(CreationBootstrapError::Rejected("bootstrap provider state"));
    }
    Ok(CreationBootstrapReceipt {
        binding: log.binding(),
        group: bootstrap.group,
        operation: verified.status.operation,
        metadata_index: verified.status.index,
    })
}
