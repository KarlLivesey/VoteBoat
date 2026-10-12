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
//! Recovery views minted only by consumed original child quorum observations.
use super::*;
use crate::{
    contracts::StorageError, raft::ReadBarrier, runtime::*, snapshot_worker::SnapshotWorkerBinding,
    worker::WorkerBinding,
};
use std::{collections::BTreeMap, sync::Arc};
mod enrollment;
mod views;
pub(super) use enrollment::{validate_enrollment, validate_initialization};
pub use views::{LogicalLogView, LogicalSnapshotView};

/// A public LogicalImage is inspection data. This value additionally owns the
/// consumed Claim and subsequent quorum-read provenance and complete pin bytes.
/// It cannot be constructed from an application getter or a caller certificate.
pub struct VerifiedLogicalImage {
    pub(super) image: LogicalImage,
    pub(super) claim: LogicalReceipt,
    pub(super) barrier: ReadBarrier,
    pub(super) snapshots: Vec<(SnapshotRef, Snapshot)>,
    pub(super) limits: LogicalLimits,
    pub(super) endpoint: LogicalEndpointChain,
    pub(super) control_profile: Option<LogicalActivationControlProfile>,
    pub(super) initialization: LogicalInitialization,
    pub(super) enrollment: Option<Box<ObservedLogicalEnrollment>>,
}
#[derive(Clone)]
pub(super) struct ObservedLogicalEnrollment {
    pub view: LogicalEnrollmentView,
    pub ticket: ReadInvocationTicket,
    pub owner: RuntimeOwner,
    pub barrier: ReadBarrier,
}
impl VerifiedLogicalImage {
    /// Immutable configuration inspection; the original consumed image retains
    /// all of its existing Claim and quorum-read provenance.
    pub fn activation_control_profile(&self) -> Option<&LogicalActivationControlProfile> {
        self.control_profile.as_ref()
    }
    /// Immutable route diagnostics cannot construct a new verified image.
    pub fn endpoint_chain(&self) -> &LogicalEndpointChain {
        &self.endpoint
    }
    pub fn mapping(&self) -> LogicalStoreMapping {
        self.image.mapping
    }
    pub fn representative(&self) -> RepresentativeBinding {
        self.image.representative
    }
    pub fn image(&self) -> &LogicalImage {
        &self.image
    }
    pub fn claim(&self) -> LogicalReceipt {
        self.claim
    }
    pub fn child_barrier(&self) -> ReadBarrier {
        self.barrier
    }
    pub fn binding(&self) -> StoreBinding {
        StoreBinding {
            identity: self.mapping().store,
            session: self.representative().logical_session,
        }
    }
    /// Recover this logical member from the consumed child-quorum image.
    /// Only the durable parent anchor restores application data. A newer
    /// publication is an orphan until a child-committed parent mutation selects
    /// it; recovery neither selects that orphan nor mutates retained pins.
    pub fn recover_parent<A: CheckpointStateMachine>(
        &self,
        application: &mut A,
    ) -> Result<(crate::raft::Raft, Restored<A::Receipt>), CheckpointError> {
        validate_initialization(
            &self.image,
            self.barrier,
            (self.initialization, self.enrollment.as_deref()),
        )?;
        let core = crate::raft::Raft::recover_member_verified(
            self.mapping().voter,
            self.binding(),
            self.image.parent.clone(),
            self.limits.parent_log,
        )?;
        let snapshot = self
            .image
            .parent
            .snapshot
            .map(|reference| {
                self.snapshots
                    .iter()
                    .find(|(r, _)| *r == reference)
                    .map(|(_, snapshot)| snapshot.clone())
                    .ok_or(StorageError::Corrupt("verified logical anchor missing"))
            })
            .transpose()?;
        let restored = crate::snapshot::restore_application_image(
            &core,
            application,
            snapshot,
            self.limits.parent_snapshots,
        )?;
        Ok((core, restored))
    }
    /// These immutable views supply recovery, not a second authoritative WAL.
    /// All new mutations must go through the asynchronous child-backed workers.
    pub fn recovery_views(&self) -> (LogicalLogView, LogicalSnapshotView) {
        let state = Arc::new(views::RecoveryImage {
            binding: self.binding(),
            image: self.image.clone(),
            snapshots: self.snapshots.clone(),
            limits: self.limits,
        });
        (
            LogicalLogView {
                state: state.clone(),
            },
            LogicalSnapshotView { state },
        )
    }
    /// Bind a quiescent recovered parent runtime to the verified child fence.
    /// Failed construction returns the original runtime; it performs no I/O.
    pub fn into_owner<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        mut runtime: TimedShard<Q, T, E>,
        worker: WorkerBinding,
        limits: EffectOwnerLimits,
    ) -> Result<EffectOwner<Q, T, E>, LogicalOwnerRejected<Q, T, E>> {
        let checked = (|| {
            EffectOwner::validate_logical_construction(&runtime, worker, limits)?;
            let core = runtime
                .core(self.mapping().parent)
                .ok_or(EffectOwnerError::WrongAuthorization)?;
            if core.state() != &self.image.parent || worker.store != self.binding() {
                return Err(EffectOwnerError::WrongAuthorization);
            }
            let authorization = RuntimeAuthorization::from_verified_image(runtime.owner(), self)?;
            runtime
                .bind_logical_authority(authorization, self.activation_control_profile())
                .map_err(EffectOwnerError::Runtime)
        })();
        if let Err(reason) = checked {
            return Err(LogicalOwnerRejected {
                reason,
                runtime: Box::new(runtime),
            });
        }
        Ok(EffectOwner::new_logical(runtime, worker, limits)
            .expect("unchanged preflighted logical owner"))
    }
    pub fn worker_binding(&self, generation: StorageWorkerGeneration) -> WorkerBinding {
        WorkerBinding {
            store: self.binding(),
            generation,
        }
    }
    pub fn snapshot_worker_binding(
        &self,
        generation: SnapshotWorkerGeneration,
    ) -> SnapshotWorkerBinding {
        SnapshotWorkerBinding {
            store: self.binding(),
            generation,
        }
    }
}
pub struct LogicalOwnerRejected<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy> {
    pub reason: EffectOwnerError,
    pub runtime: Box<TimedShard<Q, T, E>>,
}

pub(super) fn validate_parent_image<L: crate::log_format::LogCodec>(
    image: &LogicalImage,
    claim: LogicalReceipt,
    barrier: ReadBarrier,
    expected: RepresentativeBinding,
    limits: LogicalLimits,
    codec: &L,
    context: (LogicalInitialization, Option<&ObservedLogicalEnrollment>),
) -> Result<(), StorageError> {
    enrollment::validate_initialization(image, barrier, context)?;
    if image.representative != expected
        || claim.outcome != LogicalOutcome::Claimed
        || claim.epoch != Some(expected.epoch)
        || claim.first_index == 0
        || barrier.group() != image.mapping.child
        || barrier.index() < claim.first_index
        || image.child_applied < barrier.index()
        || image.parent.bootstrap.group != image.mapping.parent
        || image.pins.len() > 2
        || image
            .pins
            .iter()
            .enumerate()
            .any(|(i, r)| image.pins[..i].contains(r))
        || image
            .parent
            .snapshot
            .is_some_and(|r| !image.pins.contains(&r))
    {
        return Err(StorageError::Corrupt("logical quorum image scope"));
    }
    let state = BTreeMap::from([(image.mapping.parent, image.parent.clone())]);
    let sequence = image.parent.revision.get();
    let encoded = codec.encode_checkpoint(
        sequence,
        &state,
        limits.parent_log,
        limits.max_parent_checkpoint_bytes,
    )?;
    let (decoded_sequence, decoded, consumed) =
        codec.decode_checkpoint(&encoded, limits.parent_log)?;
    if decoded_sequence != sequence || consumed != encoded.len() || decoded != state {
        return Err(StorageError::Corrupt("logical parent codec round trip"));
    }
    Ok(())
}
