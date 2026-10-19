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
//! Explicit one-group native startup, generic over the host application.
use crate::{
    application::*,
    connect::*,
    dial::*,
    identity::*,
    log::*,
    native::{
        connect::*, dial::*, log_store::*, node::*, outbound::*, runtime::*, snapshot_store::*,
        snapshot_worker::*, tls::*, transport::*, wire::*, worker::*,
    },
    outbound::*,
    runtime::*,
    secure::*,
    snapshot::*,
    snapshot_worker::*,
    transport::*,
    worker::*,
};
use std::{collections::BTreeMap, net::SocketAddr, net::TcpListener, path::PathBuf, sync::Arc};
mod multi;
#[cfg(test)]
mod outbound_budget;
mod owner_limits;
mod peer_rotation;
#[cfg(test)]
mod ready_items;
mod timing_profile;
pub use multi::*;
pub use peer_rotation::NativePeerRotationStartup;
pub use timing_profile::NativeTimingProfile;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeOpenMode {
    Create,
    Recover,
}
/// Address and public certificate pin are routing/security inputs, not membership authority.
#[derive(Clone)]
pub struct NativeStartupPeer {
    pub address: SocketAddr,
    pub certificate: Vec<u8>,
    pub server_name: String,
}
/// One authoritative native WAL, one verified snapshot store, one group.
/// Calling open explicitly creates a listener and three bounded native workers.
/// Use Node::from_parts for different providers or shared multi-group assembly.
pub struct NativeStartup {
    pub directory: PathBuf,
    pub mode: NativeOpenMode,
    pub node: NodeId,
    pub store: StoreIdentity,
    pub bootstrap: Bootstrap,
    pub listen: SocketAddr,
    pub peers: BTreeMap<NodeId, NativeStartupPeer>,
    pub tls: NativeTlsConfig,
    pub entropy_seed: u64,
    pub limits: NodeLimits,
    /// Maximum original items in one runtime visit, including control and
    /// background turns. The native services select 256. Startup also sizes
    /// each core's proposal and replication width to carry one full visit
    /// (never below the 64-entry defaults) and admits up to four visits of
    /// client originals per group (never below 64, within the router-wide
    /// 4,096). Valid startup values are 1..=1024.
    pub ready_visit_items: usize,
}
/// Explicit trusted deployment input for reopening a durably assigned member.
/// Bootstrap remains the original group identity/history. Provisioned stores
/// and peer credentials are not membership authority and may include future peers.
/// Selects validated receive-side configuration replication after member,
/// membership-capable codec and exact route admission; static startup does not.
pub struct NativeMemberStartup {
    pub startup: NativeStartup,
    pub provisioned_stores: BTreeMap<NodeId, StoreIdentity>,
}
/// Explicit native file diagnostic selection; no service or durability authority.
pub struct NativeStartupTimings {
    pub protocol: NativePeerProtocol,
    pub timers: TimerConfig,
    pub journal: JournalTimings,
}
enum StartupAuthorization {
    Static,
    Member(BTreeMap<NodeId, StoreIdentity>),
}
#[derive(Default)]
struct StartupStorageOptions {
    log_limits: LogLimits,
    timings: Option<JournalTimings>,
}
struct StartupPreparation {
    authorization: StartupAuthorization,
    timers: TimerConfig,
    storage: StartupStorageOptions,
    rotation: Option<NativePeerRotationStartup>,
    discovered_dials: bool,
}
impl StartupAuthorization {
    fn stores<'a>(&'a self, config: &'a NativeStartup) -> &'a BTreeMap<NodeId, StoreIdentity> {
        match self {
            Self::Static => &config.bootstrap.voter_stores,
            Self::Member(stores) => stores,
        }
    }
    fn validate(&self, config: &NativeStartup) -> Result<(), NativeStartupError> {
        match self {
            Self::Static => config.validate(),
            Self::Member(stores) => config.validate_member_stores(stores),
        }
    }
}
impl NativeMemberStartup {
    /// Offline, explicitly host-authorized learner provisioning. Create selects
    /// fresh files; Recover retries existing bootstrap/snapshot stores. This does
    /// not open sockets or start workers. Source commitment is a host obligation.
    /// On error files may exist and must be inspected/recovered; never delete a
    /// possibly completed import merely because its receipt was lost.
    pub fn enroll_snapshot<A: CheckpointStateMachine>(
        &self,
        incoming: &Snapshot,
        application: &mut A,
    ) -> Result<(), NativeStartupError> {
        self.enroll_snapshot_with_log_limits(incoming, application, LogLimits::default())
    }
    /// Import through one explicitly selected WAL capability. The same limits
    /// must be selected when reopening the member; import grants no readiness.
    /// Invalid limits, and a batch no outbound budget can hold, are refused
    /// before opening files, including on retry.
    pub fn enroll_snapshot_with_log_limits<A: CheckpointStateMachine>(
        &self,
        incoming: &Snapshot,
        application: &mut A,
        log_limits: LogLimits,
    ) -> Result<(), NativeStartupError> {
        let log_limits = checked(log_limits.validate())?;
        // A member that every later start would refuse is not enrolled.
        startup_outbound_limits(log_limits)?;
        validate_bootstrap_selection(&self.startup, log_limits)?;
        self.startup.validate_stores(&self.provisioned_stores)?;
        if self.startup.tls.wire_version() < 2
            || incoming.metadata.bootstrap != self.startup.bootstrap
        {
            return Err(error("enrollment", "incompatible bootstrap or wire"));
        }
        // Validate application and exact learner assignment before file creation.
        application
            .validate_group(self.startup.bootstrap.group)
            .map_err(|_| error("application", "application rejects the configured group"))?;
        incoming.metadata.validate()?;
        let membership = incoming
            .metadata
            .membership
            .as_ref()
            .ok_or_else(|| error("enrollment", "missing learner assignment"))?;
        if membership.joint().is_some()
            || membership.stable().learners().get(&self.startup.node) != Some(&self.startup.store)
            || incoming.application.is_empty()
            || incoming.application.capacity() > SnapshotLimits::default().max_application_bytes
            || application.applied_index() != 0
            || membership.retained_bytes() > log_limits.max_batch_bytes
            || membership
                .stable()
                .voter_stores()
                .iter()
                .chain(membership.stable().learners().iter())
                .any(|(node, store)| self.provisioned_stores.get(node) != Some(store))
        {
            return Err(error(
                "enrollment",
                "invalid learner or application envelope",
            ));
        }
        let mut check = application.clone();
        checked(check.restore_checkpoint(
            incoming.metadata.application_schema,
            incoming.metadata.index,
            &incoming.application,
        ))?;
        if check.applied_index() != incoming.metadata.index {
            return Err(error("enrollment", "application restored wrong boundary"));
        }
        let create = self.startup.mode == NativeOpenMode::Create;
        let mut log = if create {
            let mut log = NativeLogStore::create(
                FileLogIo::create_witness(&self.startup.directory)?,
                self.startup.store,
                log_limits,
            )?;
            let tickets =
                log.append_batch(vec![LogMutation::Create(self.startup.bootstrap.clone())])?;
            log.barrier(&tickets)?;
            log
        } else {
            NativeLogStore::recover(
                FileLogIo::open(&self.startup.directory)?,
                self.startup.store,
                log_limits,
            )?
        };
        let identity = SnapshotIdentity {
            store: self.startup.store,
            group: self.startup.bootstrap.group,
        };
        let directory = self.startup.directory.join("snapshots");
        let mut snapshots = if create {
            NativeSnapshotStore::create(
                FileSnapshotIo::create(directory)?,
                identity,
                SnapshotLimits::default(),
            )?
        } else {
            NativeSnapshotStore::recover(
                FileSnapshotIo::open(directory)?,
                identity,
                SnapshotLimits::default(),
            )?
        };
        checked(enroll_learner_snapshot(
            self.startup.node,
            &mut log,
            &mut snapshots,
            application,
            incoming,
        ))?;
        Ok(())
    }
    /// Pure provisioning validation; durable assignment and checkpoint data are
    /// verified during open. Success here is not membership authority.
    pub fn validate(&self) -> Result<(), NativeStartupError> {
        self.startup
            .validate_member_stores(&self.provisioned_stores)
    }
    /// Recover existing native files with explicit member semantics. Failure may
    /// have advanced sessions/restored the returned application; poll cleanup
    /// before reopening resources. No implicit creation or enrollment occurs.
    pub fn open_with_protocol<A>(
        self,
        protocol: NativePeerProtocol,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_with_protocol_and_timers(
            protocol,
            NativeTimingProfile::default().timers(),
            app,
            wake,
            now,
        )
    }
    /// Recover a member with explicit host liveness timing; no membership authority is added.
    pub fn open_with_protocol_and_timers<A>(
        self,
        protocol: NativePeerProtocol,
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
            protocol,
            app,
            wake,
            now,
            (
                StartupAuthorization::Member(self.provisioned_stores),
                timers,
                None,
                None,
            ),
        )
    }
    /// Prepare an existing member's native parts for host discovery composition.
    /// Recover-only membership, exact store and startup cleanup checks are unchanged.
    /// The caller owns the returned parts and final Node::from_parts validation.
    pub fn prepare_for_discovery<A>(
        self,
        protocol: NativePeerProtocol,
        timers: TimerConfig,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.prepare_for_discovery_with_log_limits(
            protocol,
            timers,
            LogLimits::default(),
            app,
            wake,
            now,
        )
    }
    /// Prepare the member's actual WAL and recovered core with the same checked
    /// limits. Wire/application/router selection remains host composition over
    /// the returned original parts, followed by Node::from_parts validation.
    pub fn prepare_for_discovery_with_log_limits<A>(
        self,
        protocol: NativePeerProtocol,
        timers: TimerConfig,
        log_limits: LogLimits,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.startup.prepare_with_options(
            protocol,
            app,
            wake,
            now,
            StartupPreparation {
                authorization: StartupAuthorization::Member(self.provisioned_stores),
                timers,
                storage: StartupStorageOptions {
                    log_limits,
                    timings: None,
                },
                rotation: None,
                discovered_dials: true,
            },
        )
    }
}
#[derive(Debug)]
pub struct NativeStartupError {
    pub stage: &'static str,
    pub detail: String,
}
impl std::fmt::Display for NativeStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.stage, self.detail)
    }
}
impl std::error::Error for NativeStartupError {}
fn error(stage: &'static str, detail: impl std::fmt::Debug) -> NativeStartupError {
    NativeStartupError {
        stage,
        detail: format!("{detail:?}"),
    }
}
fn checked<T, E: std::fmt::Debug>(value: Result<T, E>) -> Result<T, NativeStartupError> {
    value.map_err(|e| error("native startup", e))
}
impl From<std::io::Error> for NativeStartupError {
    fn from(e: std::io::Error) -> Self {
        error("filesystem/listener", e.kind())
    }
}
impl From<crate::contracts::StorageError> for NativeStartupError {
    fn from(e: crate::contracts::StorageError) -> Self {
        error("storage", e)
    }
}
#[derive(Default)]
struct Cleanup {
    log: Option<NativeLogWorker<NativeLogStore<FileLogIo>>>,
    snapshots: Option<NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>>,
    connector: Option<NativePeerConnector>,
    dialer: Option<NativeTcpDialer>,
}
/// Failed startup has accepted no runtime requests. Files may already have been
/// created/recovered; cleanup is not rollback. Poll try_cleanup until true before
/// reopening them. The returned application may have been restored/replayed.
pub struct NativeStartupRejected<A> {
    pub reason: NativeStartupError,
    pub application: Option<A>,
    cleanup: Cleanup,
}
impl<A> std::fmt::Debug for NativeStartupRejected<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeStartupRejected")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl<A> NativeStartupRejected<A> {
    /// Nonblocking explicit join. No accepted runtime work or other host resource is stopped.
    pub fn try_cleanup(&mut self) -> Result<bool, NativeStartupError> {
        if let Some(mut connector) = self.cleanup.connector.take() {
            connector.close();
            match connector.into_dialer() {
                Ok(d) => self.cleanup.dialer = Some(d),
                Err(c) => {
                    self.cleanup.connector = Some(*c);
                    return Ok(false);
                }
            }
        }
        if let Some(d) = &mut self.cleanup.dialer {
            d.close();
            if checked(d.try_finish())? {
                self.cleanup.dialer = None;
            }
        }
        if let Some(w) = &mut self.cleanup.log {
            w.close();
            if w.try_reclaim()?.is_some() {
                self.cleanup.log = None;
            }
        }
        if let Some(w) = &mut self.cleanup.snapshots {
            w.close();
            if w.try_reclaim()?.is_some() {
                self.cleanup.snapshots = None;
            }
        }
        Ok(self.cleanup.connector.is_none()
            && self.cleanup.dialer.is_none()
            && self.cleanup.log.is_none()
            && self.cleanup.snapshots.is_none())
    }
}

trait StartupConnector: PeerConnector<Endpoint = SocketAddr> {
    fn reject(self, cleanup: &mut Cleanup);
}
impl StartupConnector for NativePeerConnector {
    fn reject(self, cleanup: &mut Cleanup) {
        cleanup.connector = Some(self);
    }
}
impl StartupConnector for NativeServiceConnector {
    fn reject(self, cleanup: &mut Cleanup) {
        match self {
            Self::Discovered(_) => unreachable!("startup has not attached host discovery"),
            Self::RotatingTcp(mut c) => {
                c.close();
                let c = c
                    .into_inner()
                    .unwrap_or_else(|_| unreachable!("unused startup connector"));
                cleanup.connector = Some(c);
            }
            #[cfg(feature = "quic")]
            Self::RotatingQuic(_) => (),
            Self::Tcp(c) => cleanup.connector = Some(*c),
            #[cfg(feature = "quic")]
            Self::Quic(_) => (), // No requests accepted during assembly; drop releases socket.
        }
    }
}
fn pins(
    config: &NativeStartup,
    stores: &BTreeMap<NodeId, StoreIdentity>,
) -> BTreeMap<NodeId, TlsPeer> {
    config
        .peers
        .iter()
        .map(|(n, p)| {
            (
                *n,
                TlsPeer {
                    identity: PeerIdentity {
                        node: *n,
                        store: stores[n],
                    },
                    certificate: p.certificate.clone(),
                    server_name: p.server_name.clone(),
                },
            )
        })
        .collect()
}
fn tcp_connector(
    config: &NativeStartup,
    stores: &BTreeMap<NodeId, StoreIdentity>,
    local: LocalIdentity,
    listener: TcpListener,
    wake: Arc<dyn WorkerWake>,
    cleanup: &mut Cleanup,
    now: MonoTime,
) -> Result<NativePeerConnector, NativeStartupError> {
    cleanup.dialer = Some(checked(NativeTcpDialer::spawn(
        local,
        stores
            .iter()
            .filter(|(n, _)| **n != config.node)
            .map(|(n, s)| (*n, *s))
            .collect(),
        DialLimits::default(),
        wake.clone(),
    ))?);
    match NativePeerConnector::new(
        NativeConnectConfig {
            local,
            limits: ConnectLimits::default(),
            session: SessionLimits::default(),
        },
        config.tls.clone(),
        pins(config, stores),
        cleanup.dialer.take().unwrap(),
        Some(listener),
        now,
    ) {
        Ok(c) => Ok(c),
        Err(r) => {
            cleanup.dialer = Some(r.dialer);
            Err(error("connector", r.reason))
        }
    }
}

impl NativeStartup {
    pub fn validate(&self) -> Result<(), NativeStartupError> {
        self.validate_stores(&self.bootstrap.voter_stores)
    }
    fn validate_member_stores(
        &self,
        stores: &BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<(), NativeStartupError> {
        if self.mode != NativeOpenMode::Recover || self.tls.wire_version() < 2 {
            return Err(error(
                "configuration",
                "member recovery requires existing files and membership wire",
            ));
        }
        self.validate_stores(stores)
    }
    fn validate_stores(
        &self,
        stores: &BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<(), NativeStartupError> {
        validate_ready_items(self.ready_visit_items)?;
        if stores.len() > 1024
            || self
                .bootstrap
                .voter_stores
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                != *self.bootstrap.policy.voters()
            || stores.get(&self.node) != Some(&self.store)
            || self.peers.len() + 1 != stores.len()
            || self.peers.contains_key(&self.node)
            || (self.listen.port() == 0 && !self.peers.is_empty())
        {
            return Err(error(
                "configuration",
                "exact bootstrap/local/peer identities required",
            ));
        }
        let mut retained = 0usize;
        for (id, peer) in &self.peers {
            if !stores.contains_key(id)
                || peer.address.port() == 0
                || peer.address.ip().is_unspecified()
                || peer.certificate.is_empty()
                || peer.certificate.len() > 65536
                || peer.server_name.is_empty()
                || peer.server_name.len() > 253
                || rustls::pki_types::ServerName::try_from(peer.server_name.as_str()).is_err()
            {
                return Err(error(
                    "configuration",
                    "invalid peer endpoint or TLS pin/name",
                ));
            }
            retained = retained
                .checked_add(peer.certificate.capacity())
                .and_then(|n| n.checked_add(peer.server_name.capacity()))
                .ok_or_else(|| error("configuration", "peer metadata overflow"))?;
        }
        if retained > 1024 * 1024 {
            return Err(error("configuration", "peer metadata limit"));
        }
        Ok(())
    }
    /// Native TCP startup using the same default timing profile as the service.
    /// Select host timer settings through `open_with_protocol_and_timers`.
    pub fn open<A>(
        self,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let mut cleanup = Cleanup::default();
        let mut application = Some(app);
        let result = (|| {
            self.validate()?;
            application
                .as_ref()
                .unwrap()
                .validate_group(self.bootstrap.group)
                .map_err(|_| error("application", "application rejects the configured group"))?;
            if application.as_ref().unwrap().applied_index() != 0 {
                return Err(error(
                    "application",
                    "provide a fresh application for verified restore/replay",
                ));
            }
            let listener = TcpListener::bind(self.listen)?;
            build(
                self,
                (
                    StartupAuthorization::Static,
                    NativeTimingProfile::default().timers(),
                    StartupStorageOptions::default(),
                ),
                &mut application,
                &mut cleanup,
                wake,
                now,
                |config, stores, local, wake, cleanup| {
                    tcp_connector(config, stores, local, listener, wake, cleanup, now)
                },
            )
        })();
        result.map_err(|reason| {
            Box::new(NativeStartupRejected {
                reason,
                application,
                cleanup,
            })
        })
    }
    /// Explicit TCP/TLS or optional QUIC startup, using the same storage/recovery path
    /// and default native timing profile as the runnable service.
    /// QUIC binds one UDP socket and starts only the two storage workers.
    pub fn open_with_protocol<A>(
        self,
        protocol: NativePeerProtocol,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_with_protocol_and_timers(
            protocol,
            NativeTimingProfile::default().timers(),
            app,
            wake,
            now,
        )
    }
    /// Explicit host liveness timing, validated before binding or opening files.
    /// Quorum, durable dependency ordering and authority gates are unchanged.
    pub fn open_with_protocol_and_timers<A>(
        self,
        protocol: NativePeerProtocol,
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
            protocol,
            app,
            wake,
            now,
            (StartupAuthorization::Static, timers, None, None),
        )
    }
    /// Open the same static native assembly with optional file-call timing.
    pub fn open_with_journal_timings<A>(
        self,
        options: NativeStartupTimings,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_with_protocol_as(
            options.protocol,
            app,
            wake,
            now,
            (
                StartupAuthorization::Static,
                options.timers,
                Some(options.journal),
                None,
            ),
        )
    }
    fn open_with_protocol_as<A>(
        self,
        protocol: NativePeerProtocol,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
        options: (
            StartupAuthorization,
            TimerConfig,
            Option<JournalTimings>,
            Option<NativePeerRotationStartup>,
        ),
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let limits = self.limits;
        let group = self.bootstrap.group;
        let parts = self.prepare_with_protocol_as(protocol, app, wake, now, options, false)?;
        NativeNode::from_parts(parts, limits, now).map_err(|rejected| {
            let mut cleanup = Cleanup::default();
            let application = reject_parts(*rejected.parts, &mut cleanup).remove(&group);
            Box::new(NativeStartupRejected {
                reason: error("node assembly", rejected.reason),
                application,
                cleanup,
            })
        })
    }

    /// Prepare validated native storage, workers and peer parts for discovery.
    /// QUIC permits discovered Dial addresses; peer pins and Accept addresses stay fixed.
    /// This opens resources and may advance the recovered store session, but accepts
    /// no Node work. Wrap the connector, then call Node::from_parts with the same
    /// limits and time. On abandonment the caller owns closing/draining all parts.
    /// Failures return the application and the ordinary explicit cleanup owner.
    pub fn prepare_for_discovery<A>(
        self,
        protocol: NativePeerProtocol,
        timers: TimerConfig,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.prepare_for_discovery_with_log_limits(
            protocol,
            timers,
            LogLimits::default(),
            app,
            wake,
            now,
        )
    }
    /// Select one WAL capability before opening resources. Recovered Raft cores
    /// inherit these exact limits from the same authoritative provider. Returned
    /// parts still require the host's final Node::from_parts validation.
    pub fn prepare_for_discovery_with_log_limits<A>(
        self,
        protocol: NativePeerProtocol,
        timers: TimerConfig,
        log_limits: LogLimits,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.prepare_with_options(
            protocol,
            app,
            wake,
            now,
            StartupPreparation {
                authorization: StartupAuthorization::Static,
                timers,
                storage: StartupStorageOptions {
                    log_limits,
                    timings: None,
                },
                rotation: None,
                discovered_dials: true,
            },
        )
    }
    fn prepare_with_protocol_as<A>(
        self,
        protocol: NativePeerProtocol,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
        options: (
            StartupAuthorization,
            TimerConfig,
            Option<JournalTimings>,
            Option<NativePeerRotationStartup>,
        ),
        discovered_dials: bool,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let (authorization, timers, timings, rotation) = options;
        self.prepare_with_options(
            protocol,
            app,
            wake,
            now,
            StartupPreparation {
                authorization,
                timers,
                storage: StartupStorageOptions {
                    log_limits: LogLimits::default(),
                    timings,
                },
                rotation,
                discovered_dials,
            },
        )
    }
    fn prepare_with_options<A>(
        self,
        protocol: NativePeerProtocol,
        app: A,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
        options: StartupPreparation,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let StartupPreparation {
            authorization,
            timers,
            storage,
            rotation,
            discovered_dials,
        } = options;
        #[cfg(not(feature = "quic"))]
        let _ = discovered_dials;
        let mut cleanup = Cleanup::default();
        let mut application = Some(app);
        let result = (|| {
            validate_storage_selection(&self, storage.log_limits)?;
            checked(timers.validate())?;
            if timers
                .election_min_ms
                .checked_add(timers.election_spread_ms - 1)
                .and_then(|wait| now.0.checked_add(wait))
                .is_none()
            {
                return Err(error("timers", "initial election deadline overflow"));
            }
            authorization.validate(&self)?;
            if let Some(rotation) = &rotation {
                rotation.validate(&self, authorization.stores(&self))?;
            }
            validate_fresh_application(application.as_ref().unwrap(), self.bootstrap.group)?;
            match protocol {
                NativePeerProtocol::TcpTls => {
                    let listener = TcpListener::bind(self.listen)?;
                    build_parts(
                        self,
                        (authorization, timers, storage),
                        &mut application,
                        &mut cleanup,
                        wake,
                        now,
                        |config, stores, local, wake, cleanup| {
                            tcp_connector(config, stores, local, listener, wake, cleanup, now)
                                .map(|c| NativeServiceConnector::Tcp(Box::new(c)))
                                .and_then(|c| peer_rotation::wrap(c, rotation.as_ref(), cleanup))
                        },
                    )
                }
                #[cfg(feature = "quic")]
                NativePeerProtocol::Quic => {
                    let mut addresses = std::collections::BTreeSet::new();
                    if self.peers.values().any(|p| {
                        p.address.ip().is_multicast()
                            || p.address == self.listen
                            || p.address.is_ipv4() != self.listen.is_ipv4()
                            || !addresses.insert(p.address)
                    }) {
                        return Err(error(
                            "configuration",
                            "distinct compatible QUIC peer addresses required",
                        ));
                    }
                    let socket = std::net::UdpSocket::bind(self.listen)?;
                    build_parts(
                        self,
                        (authorization, timers, storage),
                        &mut application,
                        &mut cleanup,
                        wake,
                        now,
                        |config, stores, local, _, cleanup| {
                            let peers = pins(config, stores)
                                .into_iter()
                                .map(|(n, p)| (n, (config.peers[&n].address, p)))
                                .collect();
                            let connect_config = NativeConnectConfig {
                                local,
                                limits: ConnectLimits::default(),
                                session: SessionLimits::default(),
                            };
                            let make = if discovered_dials {
                                super::quic_connect::NativeQuicConnector::new_with_discovered_dials
                            } else {
                                super::quic_connect::NativeQuicConnector::new
                            };
                            make(connect_config, config.tls.clone(), peers, socket, now)
                                .map(|c| NativeServiceConnector::Quic(Box::new(c)))
                                .map_err(|e| error("QUIC connector", e.reason))
                                .and_then(|c| peer_rotation::wrap(c, rotation.as_ref(), cleanup))
                        },
                    )
                }
            }
        })();
        result.map_err(|reason| {
            Box::new(NativeStartupRejected {
                reason,
                application,
                cleanup,
            })
        })
    }
}
fn validate_fresh_application<A: CheckpointStateMachine>(
    application: &A,
    group: GroupIdentity,
) -> Result<(), NativeStartupError> {
    application
        .validate_group(group)
        .map_err(|_| error("application", "application rejects the configured group"))?;
    if application.applied_index() != 0 {
        return Err(error(
            "application",
            "provide a fresh application for verified restore/replay",
        ));
    }
    Ok(())
}
fn validate_bootstrap_selection(
    config: &NativeStartup,
    limits: LogLimits,
) -> Result<(), NativeStartupError> {
    let mutations = [LogMutation::Create(config.bootstrap.clone())];
    checked(crate::log::apply_batch(
        &mut BTreeMap::new(),
        &mutations,
        limits,
    ))?;
    checked(NativeLogCodec.encode_batch(1, &mutations, limits))?;
    Ok(())
}
fn validate_storage_selection(
    config: &NativeStartup,
    limits: LogLimits,
) -> Result<(), NativeStartupError> {
    let limits = checked(limits.validate())?;
    let worker = startup_worker_limits(limits)?;
    startup_shard_limits(limits, 1, config.ready_visit_items)?;
    startup_outbound_limits(limits)?;
    if worker.batch_units > limits.max_batch_units || worker.batch_bytes > limits.max_batch_bytes {
        return Err(error(
            "storage limits",
            "native worker batch exceeds selected WAL limits",
        ));
    }
    validate_bootstrap_selection(config, limits)
}
fn startup_worker_limits(limits: LogLimits) -> Result<WorkerLimits, NativeStartupError> {
    let mut worker = WorkerLimits::default();
    worker.batch_bytes = worker.batch_bytes.max(limits.max_batch_bytes);
    let ordinary_and_control = worker
        .batch_bytes
        .checked_add(worker.control_bytes)
        .ok_or_else(|| error("storage limits", "worker byte budget overflow"))?;
    worker.max_bytes = worker.max_bytes.max(ordinary_and_control);
    checked(worker.validate())
}
/// Outbound limits that carry every Append the core selects under `limits`
/// that keeps hidden backing bytes: the batch budget is at least
/// `max_batch_bytes` (an Append keeps them only while its charge stays within
/// that), and the peer and node byte budgets each hold one such data batch
/// beside their control reserve. An Append that keeps none is a plain copy,
/// charged as on main; main's selector can pick a plain copy at the byte limit
/// whose charge exceeds `max_batch_bytes` (a pre-existing edge, not closed
/// here). Defaults are kept up to 1 MiB.
fn startup_outbound_limits(limits: LogLimits) -> Result<OutboundLimits, NativeStartupError> {
    let mut outbound = OutboundLimits::default();
    let batch = limits.max_batch_bytes;
    if batch > outbound.batch_bytes {
        outbound.batch_bytes = batch;
        for budget in [&mut outbound.peer, &mut outbound.node] {
            let data = budget
                .control
                .bytes
                .checked_add(batch)
                .ok_or_else(|| error("outbound limits", "byte budget overflow"))?;
            budget.max.bytes = budget.max.bytes.max(data);
        }
        outbound.node.max.bytes = outbound.node.max.bytes.max(outbound.peer.max.bytes);
    }
    outbound.validate().map_err(|e| error("outbound limits", e))
}
fn validate_ready_items(items: usize) -> Result<(), NativeStartupError> {
    if crate::raft::BatchLimits::for_ready_visit(items).is_err() {
        return Err(error(
            "runtime limits",
            "ready visit items outside 1..=1024",
        ));
    }
    Ok(())
}
fn startup_shard_limits(
    log: LogLimits,
    groups: usize,
    ready_visit_items: usize,
) -> Result<ShardLimits, NativeStartupError> {
    validate_ready_items(ready_visit_items)?;
    let mut limits = ShardLimits {
        max_groups: groups,
        visit_items: ready_visit_items,
        ..ShardLimits::default()
    };
    // A group's queue must hold every original its client router can admit
    // (at least one full visit) beside its control reserve.
    limits.group_items = limits
        .group_items
        .max(startup_client_limits(ready_visit_items).group_requests + limits.group_control_items);
    if log.max_batch_bytes > limits.max_event_bytes {
        limits.max_event_bytes = log
            .max_batch_bytes
            .checked_add(std::mem::size_of::<crate::raft::Event>())
            .ok_or_else(|| error("runtime limits", "event byte budget overflow"))?;
        limits.visit_bytes = limits.visit_bytes.max(limits.max_event_bytes);
        limits.group_bytes = limits.group_bytes.max(
            limits
                .max_event_bytes
                .checked_add(limits.group_control_bytes)
                .ok_or_else(|| error("runtime limits", "group byte budget overflow"))?,
        );
        limits.max_bytes = limits.max_bytes.max(
            limits
                .group_bytes
                .checked_add(limits.control_bytes)
                .ok_or_else(|| error("runtime limits", "shard byte budget overflow"))?,
        );
    }
    checked(limits.validate())
}
struct StartupStorage {
    store: NativeLogStore<FileLogIo>,
    snapshots: NativeSnapshotStore<FileSnapshotIo>,
    core: crate::raft::Raft,
}
fn recover_storage<A: CheckpointStateMachine>(
    config: &NativeStartup,
    authorization: &StartupAuthorization,
    storage: StartupStorageOptions,
    app: &mut A,
) -> Result<StartupStorage, NativeStartupError> {
    let StartupStorageOptions {
        log_limits,
        timings,
    } = storage;
    let create = config.mode == NativeOpenMode::Create;
    let io = if create {
        FileLogIo::create_witness(&config.directory)?
    } else {
        FileLogIo::open(&config.directory)?
    };
    let io = match timings {
        Some(timings) => io.with_timings(timings),
        None => io,
    };
    let store = if create {
        let mut store = checked(NativeLogStore::create(io, config.store, log_limits))?;
        let tickets =
            checked(store.append_batch(vec![LogMutation::Create(config.bootstrap.clone())]))?;
        checked(store.barrier(&tickets))?;
        store
    } else {
        NativeLogStore::recover(io, config.store, log_limits)?
    };
    if !store.matches_startup_groups(&[config.bootstrap.group])? {
        return Err(error(
            "recovery",
            "durable group inventory differs from selected startup",
        ));
    }
    let state = store.state(config.bootstrap.group)?;
    if state.bootstrap != config.bootstrap {
        return Err(error(
            "recovery",
            "recovered bootstrap differs from selected cluster",
        ));
    }
    let mut snapshots = if create {
        NativeSnapshotStore::create(
            FileSnapshotIo::create(config.directory.join("snapshots"))?,
            SnapshotIdentity {
                store: config.store,
                group: config.bootstrap.group,
            },
            SnapshotLimits::default(),
        )?
    } else {
        NativeSnapshotStore::recover(
            FileSnapshotIo::open(config.directory.join("snapshots"))?,
            SnapshotIdentity {
                store: config.store,
                group: config.bootstrap.group,
            },
            SnapshotLimits::default(),
        )?
    };
    let recover = if matches!(authorization, StartupAuthorization::Member(_)) {
        recover_member_replica
    } else {
        recover_replica
    };
    let core = checked(recover(
        config.node,
        config.bootstrap.group,
        &store,
        &mut snapshots,
        app,
    ))?
    .0;
    let core = configured_core(core, config.tls.wire_version(), authorization);
    Ok(StartupStorage {
        store,
        snapshots,
        core,
    })
}
fn configured_core(
    core: crate::raft::Raft,
    version: u16,
    authorization: &StartupAuthorization,
) -> crate::raft::Raft {
    let core = if version >= 7 {
        core.with_committed_snapshot_repair()
    } else if version >= 6 {
        core.with_snapshot_joint_repair()
    } else if version >= 5 {
        core.with_batched_joint_repair()
    } else {
        core
    };
    if matches!(authorization, StartupAuthorization::Member(_)) {
        core.with_configuration_replication()
    } else {
        core
    }
}
struct StartupRuntime<S: SecureSession> {
    owner_id: RuntimeOwner,
    local: LocalIdentity,
    remote: BTreeMap<NodeId, StoreIdentity>,
    timed: TimedShard<FairScheduler, DeadlineQueue, JitterEntropy>,
    owner_limits: EffectOwnerLimits,
    outbound: NativeOutbound,
    roster: PeerRoster<NativePeerTransport<S, NativeWireCodec>>,
    ingress: IngressRouter,
}
fn prepare_runtime<S: SecureSession>(
    config: &NativeStartup,
    authorization: &StartupAuthorization,
    cores: Vec<crate::raft::Raft>,
    binding: StoreBinding,
    log_limits: LogLimits,
    timers: TimerConfig,
    now: MonoTime,
) -> Result<StartupRuntime<S>, NativeStartupError> {
    let owner_id = RuntimeOwner {
        store: binding,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    // Store session makes each startup's transport generations disjoint.
    let first = owner_id
        .store
        .session
        .get()
        .checked_mul(10000)
        .ok_or_else(|| error("generation", "session exhausted"))?;
    let last = first
        .checked_add(9999)
        .ok_or_else(|| error("generation", "session exhausted"))?;
    let local = LocalIdentity {
        node: config.node,
        store: owner_id.store,
    };
    let remote = checked(PeerAssignments::from_cores(
        local,
        cores.iter(),
        PeerRosterLimits::default().peers,
    ))?
    .peers()
    .collect::<BTreeMap<_, _>>();
    let stores = authorization.stores(config);
    if remote
        .iter()
        .any(|(node, store)| stores.get(node) != Some(store))
    {
        return Err(error(
            "recovery",
            "provision every retained/rollback peer with its exact store",
        ));
    }
    let group_count = cores.len();
    let shard_limits = startup_shard_limits(log_limits, group_count, config.ready_visit_items)?;
    let owner_limits =
        owner_limits::startup_owner_limits(&cores, shard_limits.max_event_bytes, log_limits)?;
    let mut shard = checked(Shard::new(
        owner_id,
        shard_limits,
        checked(FairScheduler::new(group_count))?,
    ))?;
    // A wider visit quantum is only useful if one proposal and one replication
    // request can carry it; up to 64 this keeps the default core limits.
    let batch_limits = checked(crate::raft::BatchLimits::for_ready_visit(
        config.ready_visit_items,
    ))?;
    for mut core in cores {
        checked(core.configure_batch_limits(batch_limits))?;
        checked(shard.register(core))?;
    }
    let timed = checked(TimedShard::new(
        shard,
        checked(DeadlineQueue::new(owner_id, group_count))?,
        JitterEntropy::new(config.entropy_seed),
        timers,
        now,
    ))?;
    let outbound = checked(NativeOutbound::new(
        OutboundBinding {
            node: config.node,
            store: owner_id.store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        startup_outbound_limits(log_limits)?,
    ))?;
    let roster = checked(PeerRoster::new(
        PeerRosterConfig {
            local,
            outbound: outbound.binding(),
            first_generation: SecureSessionGeneration::new(first)
                .ok_or_else(|| error("generation", "session exhausted"))?,
            last_generation: SecureSessionGeneration::new(last)
                .ok_or_else(|| error("generation", "session exhausted"))?,
            wire_version: config.tls.wire_version(),
            limits: PeerRosterLimits::default(),
            transport_limits: TransportLimits::default(),
        },
        remote.clone(),
        now,
    ))?;
    let ingress = checked(IngressRouter::new(
        IngressBinding {
            owner: owner_id,
            local,
            generation: IngressGeneration::new(1).unwrap(),
        },
        IngressLimits::default(),
    ))?;
    Ok(StartupRuntime {
        owner_id,
        local,
        remote,
        timed,
        owner_limits,
        outbound,
        roster,
        ingress,
    })
}
struct ApplicationRoutes<R, Q, V> {
    results: ApplicationRouter<R>,
    clients: ClientRouter<R>,
    reads: ReadRequests<Q, V>,
}
/// Per-group client capacity for a visit quantum: enough admitted originals to
/// fill several visits while earlier batches are in flight, within the shared
/// router-wide request ceiling. The default 16-item visit keeps the default 64.
fn startup_client_limits(ready_visit_items: usize) -> ClientRouterLimits {
    let mut limits = ClientRouterLimits::default();
    limits.group_requests = ready_visit_items
        .saturating_mul(4)
        .clamp(limits.group_requests, limits.requests);
    limits
}
fn application_routes<R: ApplicationReceipt, Q, V>(
    authorization: RuntimeAuthorization,
    ready_visit_items: usize,
) -> Result<ApplicationRoutes<R, Q, V>, NativeStartupError> {
    let results = checked(ApplicationRouter::new(
        ApplicationRouterBinding::new(authorization, ApplicationRouterGeneration::new(1).unwrap()),
        ApplicationRouterLimits::default(),
    ))?;
    let clients = checked(ClientRouter::new(
        ClientRouterBinding::new(authorization, ClientRouterGeneration::new(1).unwrap()),
        startup_client_limits(ready_visit_items),
    ))?;
    let reads = checked(ReadRequests::new(
        ReadInvocationBinding::new(authorization, ReadInvocationGeneration::new(1).unwrap()),
        ReadInvocationLimits::default(),
        checked(ReadRouter::new(
            ReadRouterBinding::new(authorization, ReadRouterGeneration::new(1).unwrap()),
            ReadRouterLimits::default(),
        ))?,
    ))?;
    Ok(ApplicationRoutes {
        results,
        clients,
        reads,
    })
}
fn transport_factory(
    version: u16,
) -> Result<NativeTransportFactory<NativeWireCodec>, NativeStartupError> {
    let codec = match version {
        1 => NativeWireCodec::new(Default::default()),
        2 => NativeWireCodec::with_membership(Default::default()),
        3 => NativeWireCodec::with_authority(Default::default()),
        4 => NativeWireCodec::with_readiness(Default::default()),
        5 => NativeWireCodec::with_learner_repair(Default::default()),
        6 => NativeWireCodec::with_snapshot_repair(Default::default()),
        7 => NativeWireCodec::with_committed_snapshot_repair(Default::default()),
        8 => NativeWireCodec::with_leadership_transfer(Default::default()),
        _ => return Err(error("wire version", "unsupported native format")),
    };
    let factory = checked(NativeTransportFactory::new(
        checked(codec)?,
        Default::default(),
    ))?;
    Ok(factory)
}
struct PreparedStartup<A> {
    store: Option<NativeLogStore<FileLogIo>>,
    snapshots: BTreeMap<GroupIdentity, NativeSnapshotStore<FileSnapshotIo>>,
    cores: Vec<crate::raft::Raft>,
    applications: BTreeMap<GroupIdentity, A>,
}
fn build<A, C: StartupConnector>(
    config: NativeStartup,
    options: (StartupAuthorization, TimerConfig, StartupStorageOptions),
    application: &mut Option<A>,
    cleanup: &mut Cleanup,
    wake: Arc<dyn WorkerWake>,
    now: MonoTime,
    connector: impl FnOnce(
        &NativeStartup,
        &BTreeMap<NodeId, StoreIdentity>,
        LocalIdentity,
        Arc<dyn WorkerWake>,
        &mut Cleanup,
    ) -> Result<C, NativeStartupError>,
) -> Result<NativeNode<A, C>, NativeStartupError>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let limits = config.limits;
    let group = config.bootstrap.group;
    let parts = build_parts(config, options, application, cleanup, wake, now, connector)?;
    NativeNode::from_parts(parts, limits, now).map_err(|rejected| {
        *application = reject_parts(*rejected.parts, cleanup).remove(&group);
        error("node assembly", rejected.reason)
    })
}
fn build_parts<A, C: StartupConnector>(
    config: NativeStartup,
    options: (StartupAuthorization, TimerConfig, StartupStorageOptions),
    application: &mut Option<A>,
    cleanup: &mut Cleanup,
    wake: Arc<dyn WorkerWake>,
    now: MonoTime,
    connector: impl FnOnce(
        &NativeStartup,
        &BTreeMap<NodeId, StoreIdentity>,
        LocalIdentity,
        Arc<dyn WorkerWake>,
        &mut Cleanup,
    ) -> Result<C, NativeStartupError>,
) -> Result<NativeNodeParts<A, C>, NativeStartupError>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let (authorization, timers, storage) = options;
    let StartupStorage {
        store,
        snapshots,
        core,
    } = recover_storage(
        &config,
        &authorization,
        storage,
        application.as_mut().unwrap(),
    )?;
    let group = config.bootstrap.group;
    let mut prepared = PreparedStartup {
        store: Some(store),
        snapshots: [(group, snapshots)].into(),
        cores: vec![core],
        applications: [(group, application.take().unwrap())].into(),
    };
    let result = assemble_parts(
        config,
        (authorization, timers),
        &mut prepared,
        cleanup,
        wake,
        now,
        connector,
    );
    *application = prepared.applications.remove(&group);
    result
}
fn assemble_parts<A, C: StartupConnector>(
    config: NativeStartup,
    options: (StartupAuthorization, TimerConfig),
    prepared: &mut PreparedStartup<A>,
    cleanup: &mut Cleanup,
    wake: Arc<dyn WorkerWake>,
    now: MonoTime,
    connector: impl FnOnce(
        &NativeStartup,
        &BTreeMap<NodeId, StoreIdentity>,
        LocalIdentity,
        Arc<dyn WorkerWake>,
        &mut Cleanup,
    ) -> Result<C, NativeStartupError>,
) -> Result<NativeNodeParts<A, C>, NativeStartupError>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let (authorization, timers) = options;
    let store = prepared.store.take().unwrap();
    let StartupRuntime {
        owner_id,
        local,
        remote,
        timed,
        owner_limits,
        outbound,
        roster,
        ingress,
    } = prepare_runtime::<C::Session>(
        &config,
        &authorization,
        std::mem::take(&mut prepared.cores),
        store.binding(),
        store.limits(),
        timers,
        now,
    )?;
    let ApplicationRoutes {
        results,
        clients,
        reads,
    } = application_routes(timed.authorization(), config.ready_visit_items)?;
    let factory = transport_factory(config.tls.wire_version())?;
    let worker_limits = startup_worker_limits(store.limits())?;
    cleanup.log = Some(checked(NativeLogWorker::spawn(
        store,
        StorageWorkerGeneration::new(1).unwrap(),
        worker_limits,
        wake.clone(),
    ))?);
    let owner = checked(EffectOwner::new(
        timed,
        cleanup.log.as_ref().unwrap().binding(),
        owner_limits,
    ))?;
    cleanup.snapshots = Some(checked(NativeSnapshotWorker::spawn(
        std::mem::take(&mut prepared.snapshots),
        SnapshotWorkerBinding {
            store: owner_id.store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        },
        SnapshotWorkLimits::default(),
        wake.clone(),
    ))?);
    let router = checked(SnapshotRouter::new(
        owner.identity(),
        cleanup.snapshots.as_ref().unwrap().binding(),
        SnapshotRouterLimits::default(),
    ))?;
    let stores = authorization.stores(&config);
    let connector = connector(&config, stores, local, wake.clone(), cleanup)?;
    Ok(NativeNodeParts {
        local: NativeLocalParts {
            owner,
            persistence: cleanup.log.take().unwrap(),
            applications: std::mem::take(&mut prepared.applications),
            results,
            clients,
            reads,
            outbound,
            snapshots: Some(NodeSnapshots {
                router,
                worker: cleanup.snapshots.take().unwrap(),
            }),
        },
        peers: Some(PeerParts {
            admission_routes: admission_routes(&authorization, &config),
            connector,
            roster,
            factory,
            ingress,
            routes: remote
                .keys()
                .map(|n| (*n, connection_direction(&config, *n)))
                .collect(),
        }),
    })
}

fn reject_parts<A, C: StartupConnector>(
    mut parts: NativeNodeParts<A, C>,
    cleanup: &mut Cleanup,
) -> BTreeMap<GroupIdentity, A>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    cleanup.log = Some(parts.local.persistence);
    cleanup.snapshots = parts.local.snapshots.take().map(|s| s.worker);
    if let Some(peers) = parts.peers.take() {
        peers.connector.reject(cleanup);
    }
    parts.local.applications
}

fn connection_direction(config: &NativeStartup, node: NodeId) -> ConnectDirection<SocketAddr> {
    if config.node < node {
        ConnectDirection::Dial(config.peers[&node].address)
    } else {
        ConnectDirection::Accept
    }
}
fn admission_routes(
    authorization: &StartupAuthorization,
    config: &NativeStartup,
) -> Option<BTreeMap<NodeId, PeerRoute<SocketAddr>>> {
    matches!(authorization, StartupAuthorization::Member(_)).then(|| {
        authorization
            .stores(config)
            .iter()
            .filter(|(node, _)| **node != config.node)
            .map(|(node, store)| {
                (
                    *node,
                    PeerRoute {
                        store: *store,
                        direction: connection_direction(config, *node),
                    },
                )
            })
            .collect()
    })
}
