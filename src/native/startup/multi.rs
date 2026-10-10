// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
mod storage;

/// One bootstrap batch, shared WAL, scheduler, snapshot worker and peer endpoint.
pub const MAX_NATIVE_STARTUP_GROUPS: usize = 256;
/// Explicit native multi-group assembly. The anchor bootstrap in `startup`
/// precedes `additional_groups`; all original group IDs are strictly increasing.
/// This selects member replication, not authority to create or activate owners.
pub struct NativeMultiStartup {
    pub startup: NativeStartup,
    pub additional_groups: Vec<Bootstrap>,
    pub provisioned_stores: BTreeMap<NodeId, StoreIdentity>,
}
/// `application` contains the entire map, possibly partially restored. Reuse the
/// ordinary startup cleanup contract before reopening any returned files.
pub type NativeMultiStartupRejected<A> = NativeStartupRejected<BTreeMap<GroupIdentity, A>>;
impl NativeMultiStartup {
    pub fn validate<A: CheckpointStateMachine>(
        &self,
        applications: &BTreeMap<GroupIdentity, A>,
    ) -> Result<(), NativeStartupError> {
        self.startup.validate_stores(&self.provisioned_stores)?;
        if self.startup.tls.wire_version() < 2
            || self.additional_groups.len() >= MAX_NATIVE_STARTUP_GROUPS
            || self.additional_groups.capacity() > MAX_NATIVE_STARTUP_GROUPS
        {
            return Err(error(
                "configuration",
                "multi-group count or member wire limit",
            ));
        }
        let groups = self.bootstraps();
        if groups
            .windows(2)
            .any(|pair| pair[0].group.id >= pair[1].group.id)
            || !groups
                .iter()
                .map(|g| g.group)
                .eq(applications.keys().copied())
        {
            return Err(error(
                "configuration",
                "exact sorted application/group inventory required",
            ));
        }
        for bootstrap in &groups {
            if bootstrap
                .voter_stores
                .iter()
                .any(|(node, store)| self.provisioned_stores.get(node) != Some(store))
                || self.startup.mode == NativeOpenMode::Create
                    && bootstrap.voter_stores.get(&self.startup.node) != Some(&self.startup.store)
            {
                return Err(error(
                    "configuration",
                    "exact provisioned bootstrap stores required",
                ));
            }
            let app = &applications[&bootstrap.group];
            checked(app.validate_group(bootstrap.group))?;
            if app.applied_index() != 0 {
                return Err(error(
                    "application",
                    "provide fresh applications for verified replay",
                ));
            }
        }
        let mutations = groups
            .into_iter()
            .map(LogMutation::Create)
            .collect::<Vec<_>>();
        let limits = LogLimits::default();
        checked(crate::log::apply_batch(
            &mut BTreeMap::new(),
            &mutations,
            limits,
        ))?;
        checked(NativeLogCodec.encode_batch(1, &mutations, limits))?;
        Ok(())
    }
    fn bootstraps(&self) -> Vec<Bootstrap> {
        std::iter::once(&self.startup.bootstrap)
            .chain(&self.additional_groups)
            .cloned()
            .collect()
    }
    pub fn open<A>(
        self,
        protocol: NativePeerProtocol,
        timers: TimerConfig,
        applications: BTreeMap<GroupIdentity, A>,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeMultiStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_as((protocol, timers, None), applications, wake, now)
    }
    /// Shared multi-group startup with durable peer credential binding.
    pub fn open_with_peer_rotation<A>(
        self,
        rotation: NativePeerRotationStartup,
        timers: TimerConfig,
        applications: BTreeMap<GroupIdentity, A>,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeMultiStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.open_as(
            (rotation.protocol, timers, Some(rotation)),
            applications,
            wake,
            now,
        )
    }
    fn open_as<A>(
        self,
        options: (
            NativePeerProtocol,
            TimerConfig,
            Option<NativePeerRotationStartup>,
        ),
        applications: BTreeMap<GroupIdentity, A>,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNode<A, NativeServiceConnector>, Box<NativeMultiStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let limits = self.startup.limits;
        let parts = self.prepare_as(options, applications, wake, now, false)?;
        NativeNode::from_parts(parts, limits, now).map_err(|rejected| {
            let mut cleanup = Cleanup::default();
            let applications = reject_parts(*rejected.parts, &mut cleanup);
            Box::new(NativeStartupRejected {
                reason: error("node assembly", rejected.reason),
                application: Some(applications),
                cleanup,
            })
        })
    }
    /// Prepare one shared native store and all original groups for host discovery.
    /// Ownership and cleanup match NativeStartup::prepare_for_discovery. QUIC
    /// permits discovered Dial addresses; pins and Accept addresses remain fixed.
    pub fn prepare_for_discovery<A>(
        self,
        protocol: NativePeerProtocol,
        timers: TimerConfig,
        applications: BTreeMap<GroupIdentity, A>,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeMultiStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.prepare_as((protocol, timers, None), applications, wake, now, true)
    }
    /// Prepare shared discovery parts with the same durable peer-rotation binding as open.
    pub fn prepare_discovery_with_peer_rotation<A>(
        self,
        rotation: NativePeerRotationStartup,
        timers: TimerConfig,
        applications: BTreeMap<GroupIdentity, A>,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeMultiStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        self.prepare_as(
            (rotation.protocol, timers, Some(rotation)),
            applications,
            wake,
            now,
            true,
        )
    }
    fn prepare_as<A>(
        self,
        options: (
            NativePeerProtocol,
            TimerConfig,
            Option<NativePeerRotationStartup>,
        ),
        applications: BTreeMap<GroupIdentity, A>,
        wake: Arc<dyn WorkerWake>,
        now: MonoTime,
        discovered_dials: bool,
    ) -> Result<NativeNodeParts<A, NativeServiceConnector>, Box<NativeMultiStartupRejected<A>>>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        let (protocol, timers, rotation) = options;
        let mut cleanup = Cleanup::default();
        let mut prepared = PreparedStartup {
            store: None,
            snapshots: BTreeMap::new(),
            cores: Vec::new(),
            applications,
        };
        let result = (|| {
            self.validate(&prepared.applications)?;
            checked(timers.validate())?;
            if let Some(rotation) = &rotation {
                rotation.validate(&self.startup, &self.provisioned_stores)?;
            }
            if timers
                .election_min_ms
                .checked_add(timers.election_spread_ms - 1)
                .and_then(|wait| now.0.checked_add(wait))
                .is_none()
            {
                return Err(error("timers", "initial election deadline overflow"));
            }
            let socket = Socket::bind(protocol, &self.startup)?;
            let authorization = StartupAuthorization::Member(self.provisioned_stores.clone());
            storage::recover(
                &self.startup,
                &self.bootstraps(),
                &authorization,
                &mut prepared,
            )?;
            assemble_parts(
                self.startup,
                (authorization, timers),
                &mut prepared,
                &mut cleanup,
                wake,
                now,
                |config, stores, local, wake, cleanup| {
                    socket
                        .connect(
                            config,
                            stores,
                            local,
                            wake,
                            cleanup,
                            (now, discovered_dials),
                        )
                        .and_then(|c| peer_rotation::wrap(c, rotation.as_ref(), cleanup))
                },
            )
        })();
        result.map_err(|reason| {
            Box::new(NativeStartupRejected {
                reason,
                application: Some(prepared.applications),
                cleanup,
            })
        })
    }
}
enum Socket {
    Tcp(TcpListener),
    #[cfg(feature = "quic")]
    Quic(std::net::UdpSocket),
}
impl Socket {
    fn bind(
        protocol: NativePeerProtocol,
        config: &NativeStartup,
    ) -> Result<Self, NativeStartupError> {
        match protocol {
            NativePeerProtocol::TcpTls => Ok(Self::Tcp(TcpListener::bind(config.listen)?)),
            #[cfg(feature = "quic")]
            NativePeerProtocol::Quic => {
                let mut addresses = std::collections::BTreeSet::new();
                if config.peers.values().any(|p| {
                    p.address.ip().is_multicast()
                        || p.address == config.listen
                        || p.address.is_ipv4() != config.listen.is_ipv4()
                        || !addresses.insert(p.address)
                }) {
                    return Err(error(
                        "configuration",
                        "distinct compatible QUIC peer addresses required",
                    ));
                }
                Ok(Self::Quic(std::net::UdpSocket::bind(config.listen)?))
            }
        }
    }
    fn connect(
        self,
        config: &NativeStartup,
        stores: &BTreeMap<NodeId, StoreIdentity>,
        local: LocalIdentity,
        wake: Arc<dyn WorkerWake>,
        cleanup: &mut Cleanup,
        timing: (MonoTime, bool),
    ) -> Result<NativeServiceConnector, NativeStartupError> {
        let (now, discovered_dials) = timing;
        #[cfg(not(feature = "quic"))]
        let _ = discovered_dials;
        match self {
            Self::Tcp(listener) => {
                tcp_connector(config, stores, local, listener, wake, cleanup, now)
                    .map(|c| NativeServiceConnector::Tcp(Box::new(c)))
            }
            #[cfg(feature = "quic")]
            Self::Quic(socket) => {
                let peers = pins(config, stores)
                    .into_iter()
                    .map(|(n, p)| (n, (config.peers[&n].address, p)))
                    .collect();
                let make = if discovered_dials {
                    crate::native::quic_connect::NativeQuicConnector::new_with_discovered_dials
                } else {
                    crate::native::quic_connect::NativeQuicConnector::new
                };
                make(
                    NativeConnectConfig {
                        local,
                        limits: ConnectLimits::default(),
                        session: SessionLimits::default(),
                    },
                    config.tls.clone(),
                    peers,
                    socket,
                    now,
                )
                .map(|c| NativeServiceConnector::Quic(Box::new(c)))
                .map_err(|e| error("QUIC connector", e.reason))
            }
        }
    }
}
