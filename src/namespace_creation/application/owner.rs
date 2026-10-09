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
//! Local committed readiness/activation guard over the existing routed application.
//! Sealed core owner composition; application and partition providers remain public.
use super::*;
use crate::scope::ScopeStateMachine;
mod sealed {
    use super::{RoutedApplication, TransferSource};
    pub trait Sealed {}
    impl<A, P> Sealed for RoutedApplication<A, P> {}
    impl<A, P> Sealed for TransferSource<A, P> {}
}
/// Only the existing core owner guards implement this contract. It cannot be
/// supplied by an application provider to replace activation/fencing invariants.
pub trait NamespaceOwner<A: StateMachine, P>:
    sealed::Sealed + CheckpointStateMachine<Receipt = RoutedReceipt<A::Receipt>> + BoundedStateMachine
where
    A::Receipt: ApplicationReceipt,
{
    const SCHEMA: u64;
    const CHECKPOINT_MAGIC: &'static [u8; 8];
    fn routed_owner(&self) -> &RoutedApplication<A, P>;
    fn owner_requirements(&self) -> ReadinessRequirements;
    fn owner_command(&self, bytes: &[u8]) -> Result<bool, ApplicationError>;
    fn application_payload<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<Option<&'a [u8]>, ApplicationError>;
}
impl<A, P> NamespaceOwner<A, P> for RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    const SCHEMA: u64 = CREATED_NAMESPACE_SCHEMA;
    const CHECKPOINT_MAGIC: &'static [u8; 8] = b"VBNCHK01";
    fn routed_owner(&self) -> &RoutedApplication<A, P> {
        self
    }
    fn owner_requirements(&self) -> ReadinessRequirements {
        self.readiness_requirements()
    }
    fn owner_command(&self, bytes: &[u8]) -> Result<bool, ApplicationError> {
        Ok(matches!(
            decode(bytes, self.limits().payload_bytes)?,
            Command::Data { .. }
        ))
    }
    fn application_payload<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<Option<&'a [u8]>, ApplicationError> {
        match decode(bytes, self.limits().payload_bytes)? {
            Command::Data { payload, .. } => Ok(Some(payload)),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
}
impl<A, P> NamespaceOwner<A, P> for TransferSource<A, P>
where
    A: ScopeStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    const SCHEMA: u64 = CREATED_NAMESPACE_SOURCE_SCHEMA;
    const CHECKPOINT_MAGIC: &'static [u8; 8] = b"VBNCHK02";
    fn routed_owner(&self) -> &RoutedApplication<A, P> {
        self.routed()
    }
    fn owner_requirements(&self) -> ReadinessRequirements {
        self.readiness_requirements()
    }
    fn owner_command(&self, bytes: &[u8]) -> Result<bool, ApplicationError> {
        self.application_payload(bytes).map(|_| true)
    }
    fn application_payload<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<Option<&'a [u8]>, ApplicationError> {
        if let Some(intent) = bytes.strip_prefix(b"VBSFREE1") {
            crate::transfer::TransferIntent::decode(intent)?;
            Ok(None)
        } else {
            match decode(bytes, self.routed().limits().payload_bytes)? {
                Command::Data { payload, .. } => Ok(Some(payload)),
                _ => Err(ApplicationError::InvalidCommand),
            }
        }
    }
}
