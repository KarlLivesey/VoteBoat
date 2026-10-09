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
//! Quorum-readable full fence over the original routed owner and persistence.
use super::*;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedControlQuery<Q> {
    Data(RoutedQuery<Q>),
    Fence,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedControlRead<R> {
    Data(RoutedRead<R>),
    Fence(Option<OwnershipFence>),
}
/// A read view, not another ownership guard or checkpoint format. Serving and
/// foreign use still require the original Node quorum read and host authentication.
#[derive(Clone)]
pub struct RoutedControlReads<A, P>(RoutedApplication<A, P>);
impl<A, P> RoutedControlReads<A, P> {
    pub fn new(owner: RoutedApplication<A, P>) -> Self {
        Self(owner)
    }
    pub fn routed(&self) -> &RoutedApplication<A, P> {
        &self.0
    }
    pub fn into_routed(self) -> RoutedApplication<A, P> {
        self.0
    }
}
impl<A, P> StateMachine for RoutedControlReads<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Receipt = <RoutedApplication<A, P> as StateMachine>::Receipt;
    fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        self.0.validate_group(g)
    }
    fn deployment_requirements(&self) -> Option<crate::raft::ReadinessRequirements> {
        self.0.deployment_requirements()
    }
    fn validate_deployment_requirements(
        &self,
        r: crate::raft::ReadinessRequirements,
    ) -> Result<(), ApplicationError> {
        self.0.validate_deployment_requirements(r)
    }
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(&mut self, e: &[LogEntry]) -> Result<Vec<Self::Receipt>, ApplicationError> {
        self.0.apply_batch(e)
    }
}
impl<A, P> BoundedStateMachine for RoutedControlReads<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn receipt_bytes_bound(&self, e: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(e)
    }
}
impl<A, P> ProposalAdmission for RoutedControlReads<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine + ProposalAdmission,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn validate_proposal<'a>(
        &self,
        op: OperationId,
        b: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(op, b, pending)
    }
}
impl<A, P> CheckpointStateMachine for RoutedControlReads<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(max)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        b: &[u8],
    ) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(schema, applied, b)
    }
}
impl<A, P> ReadableStateMachine for RoutedControlReads<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine + ReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    type Query = RoutedControlQuery<A::Query>;
    type ReadResult = RoutedControlRead<A::ReadResult>;
    fn read_at(&self, required: u64, q: Self::Query) -> Result<Self::ReadResult, ApplicationError> {
        if required > self.applied_index() {
            return Err(ApplicationError::NotApplied);
        }
        match q {
            RoutedControlQuery::Data(q) => self.0.read_at(required, q).map(RoutedControlRead::Data),
            RoutedControlQuery::Fence => Ok(RoutedControlRead::Fence(self.0.fence())),
        }
    }
}
impl<A, P> BoundedReadableStateMachine for RoutedControlReads<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine + BoundedReadableStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    fn query_bytes(&self, q: &Self::Query, max: usize) -> Result<usize, ApplicationError> {
        match q {
            RoutedControlQuery::Data(q) => self.0.query_bytes(q, max),
            RoutedControlQuery::Fence => Ok(0),
        }
    }
    fn read_result_bound(&self, q: &Self::Query) -> Result<usize, ApplicationError> {
        let nested = match q {
            RoutedControlQuery::Data(q) => self
                .0
                .read_result_bound(q)?
                .checked_sub(size_of::<RoutedRead<A::ReadResult>>())
                .ok_or(ApplicationError::ReceiptBudget)?,
            RoutedControlQuery::Fence => 0,
        };
        size_of::<Self::ReadResult>()
            .checked_add(nested)
            .ok_or(ApplicationError::ReceiptBudget)
    }
    fn read_result_bytes(
        &self,
        r: &Self::ReadResult,
        max: usize,
    ) -> Result<usize, ApplicationError> {
        match r {
            RoutedControlRead::Data(r) => self.0.read_result_bytes(r, max),
            RoutedControlRead::Fence(_) => Ok(0),
        }
    }
}
