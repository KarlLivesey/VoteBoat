// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Host-loaded durable credential binding, checked before startup side effects.
use super::*;
use crate::{authorization::CredentialGeneration, credential_reload::*};

/// Opt-in peer revocation and restart binding. Load the journal under the same
/// exclusive local-store ownership used for startup; retain it after rotation.
/// A missing record means an explicitly trusted initial generation, not proof
/// that the store has never rotated. This selection performs no journal I/O.
pub struct NativePeerRotationStartup {
    pub protocol: NativePeerProtocol,
    pub generation: CredentialGeneration,
    pub latest: Option<CredentialReloadRecord>,
}
impl NativePeerRotationStartup {
    pub fn from_journal(
        protocol: NativePeerProtocol,
        generation: CredentialGeneration,
        journal: &impl CredentialJournal,
    ) -> Result<Self, CredentialJournalError> {
        Ok(Self {
            protocol,
            generation,
            latest: journal.latest()?,
        })
    }
    pub(super) fn validate(
        &self,
        config: &NativeStartup,
        stores: &BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<(), NativeStartupError> {
        let material = super::super::peer_credentials::NativePeerMaterial {
            tls: config.tls.clone(),
            peers: pins(config, stores),
        };
        if let Some(record) = self.latest {
            if record.owner
                != (PeerIdentity {
                    node: config.node,
                    store: config.store,
                })
                || record.request.sequence == 0
                || record.request.replacement <= record.request.expected
                || self.generation != record.request.replacement
                || material.digest() != record.digest
            {
                return Err(error(
                    "peer credentials",
                    "startup material conflicts with durable peer rollout",
                ));
            }
        }
        Ok(())
    }
}

pub(super) fn wrap(
    connector: NativeServiceConnector,
    rotation: Option<&NativePeerRotationStartup>,
    cleanup: &mut Cleanup,
) -> Result<NativeServiceConnector, NativeStartupError> {
    let Some(rotation) = rotation else {
        return Ok(connector);
    };
    connector
        .with_peer_rotation(rotation.generation)
        .map_err(|c| {
            c.reject(cleanup);
            error(
                "peer credentials",
                "startup connector must be fresh and drained",
            )
        })
}

impl NativeStartup {
    /// Prepare discovery composition with the same durable credential checks as open.
    pub fn prepare_discovery_with_peer_rotation<A>(
        self,
        rotation: NativePeerRotationStartup,
        timers: TimerConfig,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.prepare_with_protocol_as(
            rotation.protocol,
            app,
            wake,
            now,
            (StartupAuthorization::Static, timers, None, Some(rotation)),
            true,
        )
    }
    /// Start with guarded peer connections after checking the exact durable
    /// credential record. The host prepares and publishes later journal entries
    /// off-thread before calling Node::replace_peer_credentials.
    pub fn open_with_peer_rotation<A>(
        self,
        rotation: NativePeerRotationStartup,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_with_peer_rotation_and_timers(rotation, TimerConfig::default(), app, wake, now)
    }
    /// Explicit liveness timing with the same durable credential binding and cleanup.
    pub fn open_with_peer_rotation_and_timers<A>(
        self,
        rotation: NativePeerRotationStartup,
        timers: TimerConfig,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_with_protocol_as(
            rotation.protocol,
            app,
            wake,
            now,
            (StartupAuthorization::Static, timers, None, Some(rotation)),
        )
    }
}
impl NativeMemberStartup {
    /// Prepare recovered member parts without bypassing the recorded credential binding.
    pub fn prepare_discovery_with_peer_rotation<A>(
        self,
        rotation: NativePeerRotationStartup,
        timers: TimerConfig,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.startup.prepare_with_protocol_as(
            rotation.protocol,
            app,
            wake,
            now,
            (
                StartupAuthorization::Member(self.provisioned_stores),
                timers,
                None,
                Some(rotation),
            ),
            true,
        )
    }
    /// Member recovery with the same credential checks and revocation leases.
    /// Provisioned identities and credential records do not authorize membership.
    pub fn open_with_peer_rotation<A>(
        self,
        rotation: NativePeerRotationStartup,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_with_peer_rotation_and_timers(rotation, TimerConfig::default(), app, wake, now)
    }
    /// Explicit liveness timing with the same durable credential binding and cleanup.
    pub fn open_with_peer_rotation_and_timers<A>(
        self,
        rotation: NativePeerRotationStartup,
        timers: TimerConfig,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.startup.open_with_protocol_as(
            rotation.protocol,
            app,
            wake,
            now,
            (
                StartupAuthorization::Member(self.provisioned_stores),
                timers,
                None,
                Some(rotation),
            ),
        )
    }
}
