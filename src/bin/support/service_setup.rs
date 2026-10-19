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
//! Shared native executable configuration and explicit worker lifecycle.
use std::{collections::BTreeMap, net::SocketAddr, path::Path, sync::Arc};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{connect::*, node::*, startup::*, tls::*},
    quorum::*,
    runtime::*,
};
pub type Failure = Box<dyn std::error::Error>;
pub const MAX_NODE: u64 = 4096;
pub enum PeerInput<'a> {
    Legacy(Option<&'a Path>),
    Deployment(&'a Path),
}
pub fn checked<T, E: std::fmt::Debug>(value: Result<T, E>) -> Result<T, Failure> {
    value.map_err(|e| format!("{e:?}").into())
}
pub fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store_id(id: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id.into()).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
pub(super) fn material(path: &Path, limit: u64) -> Result<Vec<u8>, Failure> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > limit {
        return Err("invalid TLS material size or configuration file size".into());
    }
    Ok(bytes)
}
/// Legacy endpoints cover three bootstrap identities. Explicit deployment
/// declares exact provisioned stores/routes, independently of membership.
pub fn configuration(
    root: &Path,
    id: u64,
    base: u16,
    tls: &Path,
    create: bool,
    input: PeerInput<'_>,
) -> Result<NativeMemberStartup, Failure> {
    let explicit = matches!(input, PeerInput::Deployment(_));
    if !(1..=MAX_NODE).contains(&id)
        || base == 0
        || u64::from(base) + 100 + id > u64::from(u16::MAX)
        || (!explicit && (id > 3 || base > 65432))
    {
        return Err("invalid node or base port".into());
    }
    let mut addresses = (1..=3)
        .map(|n| {
            (
                node(n),
                (
                    (std::net::Ipv4Addr::LOCALHOST, base + n as u16).into(),
                    format!("node{n}.voteboat.test"),
                ),
            )
        })
        .collect::<BTreeMap<NodeId, (SocketAddr, String)>>();
    let mut stores = (1..=3)
        .map(|n| (node(n), store_id(n)))
        .collect::<BTreeMap<_, _>>();
    if let PeerInput::Legacy(Some(path)) = input {
        load_legacy_endpoints(path, &mut addresses)?;
    }
    if let PeerInput::Deployment(path) = input {
        load_deployment(path, id, &mut addresses, &mut stores)?;
    }
    let voters = (1..=3)
        .map(|n| (node(n), store_id(n)))
        .collect::<BTreeMap<_, _>>();
    let mut credentials = checked(NativeTlsConfig::new(TlsCredentials {
        roots: vec![material(&tls.join("ca.der"), 65536)?],
        certificate_chain: vec![material(&tls.join(format!("node{id}.der")), 65536)?],
        private_key: material(&tls.join(format!("node{id}-key.der")), 65536)?,
    }))?;
    if explicit {
        credentials = checked(credentials.with_wire_version(7))?;
    }
    let mut retained_peer_bytes = 0usize;
    let config = NativeStartup {
        directory: root.to_owned(),
        mode: NativeOpenMode::Recover,
        node: node(id),
        store: stores[&node(id)],
        bootstrap: Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: checked(Policy::new(
                Tree::Majority(voters.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            ))?,
            voter_stores: voters,
        },
        listen: addresses[&node(id)].0,
        peers: addresses
            .into_iter()
            .filter(|(n, _)| *n != node(id))
            .map(|(n, (address, server_name))| {
                let certificate = material(&tls.join(format!("node{}.der", n.get())), 65536)?;
                retained_peer_bytes = retained_peer_bytes
                    .checked_add(certificate.capacity())
                    .and_then(|bytes| bytes.checked_add(server_name.capacity()))
                    .ok_or("peer metadata overflow")?;
                if retained_peer_bytes > 1024 * 1024 {
                    return Err("peer metadata limit".into());
                }
                Ok((
                    n,
                    NativeStartupPeer {
                        address,
                        server_name,
                        certificate,
                    },
                ))
            })
            .collect::<Result<_, Failure>>()?,
        tls: credentials,
        entropy_seed: id * 17,
        limits: NodeLimits::default(),
        ready_visit_items: 1024,
    };
    let mut config = NativeMemberStartup {
        startup: config,
        provisioned_stores: stores,
    };
    if explicit {
        config.validate()?;
    } else {
        config.startup.validate()?;
    }
    if create {
        config.startup.mode = NativeOpenMode::Create;
    }
    Ok(config)
}
pub fn open_application<A>(
    config: NativeMemberStartup,
    protocol: NativePeerProtocol,
    timers: TimerConfig,
    member: bool,
    app: A,
) -> Result<NativeNode<A, NativeServiceConnector>, Failure>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    open_application_with_rotation(config, protocol, timers, member, app, None)
}
/// The one readiness-observable wake for this process's single host thread.
/// Workers wake it with a byte on a local socket pair, which the host loop's
/// readiness wait observes alongside its sockets and timers.
pub fn host_wake() -> Arc<voteboat::native::readiness::HostWake> {
    static WAKE: std::sync::OnceLock<Arc<voteboat::native::readiness::HostWake>> =
        std::sync::OnceLock::new();
    WAKE.get_or_init(|| {
        Arc::new(voteboat::native::readiness::HostWake::new().expect("host wake socket pair"))
    })
    .clone()
}
/// Longest a host loop waits with nothing announced; bounds host-side timers
/// such as credential reloads and administrative deadlines.
pub const HOST_MAX_WAIT: std::time::Duration = std::time::Duration::from_millis(10);
/// Gather window before a round that already has queued work.
pub const HOST_GATHER: std::time::Duration = std::time::Duration::from_micros(200);
/// Host-loop readiness state: the wake every worker signals, the reusable
/// descriptor buffer and the wait itself.
pub struct HostWait {
    wake: Arc<voteboat::native::readiness::HostWake>,
    readiness: voteboat::native::readiness::Readiness,
    fds: Vec<voteboat::worker::ReadinessFd>,
    gather: std::time::Duration,
    max_wait: std::time::Duration,
}
impl HostWait {
    pub fn new() -> Self {
        Self::with(HOST_GATHER, HOST_MAX_WAIT)
    }
    /// A host loop with its own gather window (the CPU/latency knob: longer
    /// gathers mean fewer, fuller rounds) and wait cap (the bound on
    /// host-side timers such as credential reloads).
    pub fn with(gather: std::time::Duration, max_wait: std::time::Duration) -> Self {
        Self {
            wake: host_wake(),
            readiness: voteboat::native::readiness::Readiness::default(),
            fds: Vec::new(),
            gather,
            max_wait,
        }
    }
    /// Wait for a socket named by `collect`, a worker wake or `deadline`
    /// instead of polling at a fixed cadence; polling remains the only source
    /// of progress, so a missed or spurious report costs latency only.
    pub fn wait(
        &mut self,
        collect: impl FnOnce(&mut Vec<voteboat::worker::ReadinessFd>),
        deadline: Option<MonoTime>,
        time: MonoTime,
    ) -> std::io::Result<()> {
        self.fds.clear();
        collect(&mut self.fds);
        self.fds.push(self.wake.fd());
        let limit = deadline
            .map(|d| std::time::Duration::from_millis(d.0.saturating_sub(time.0)))
            .map_or(self.max_wait, |d| d.min(self.max_wait));
        if self.fds.iter().any(|f| f.immediate) {
            std::thread::park_timeout(self.gather);
        } else {
            self.readiness.wait(&self.fds, limit)?;
        }
        self.wake.drain();
        Ok(())
    }
}
pub fn open_application_with_rotation<A>(
    mut config: NativeMemberStartup,
    protocol: NativePeerProtocol,
    timers: TimerConfig,
    member: bool,
    app: A,
    rotation: Option<NativePeerRotationStartup>,
) -> Result<NativeNode<A, NativeServiceConnector>, Failure>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let wake: Arc<dyn voteboat::worker::WorkerWake> = host_wake();
    let opened = if member {
        // Explicit recovery only: provisioned routes do not establish assignment.
        // The native member constructor verifies the authoritative WAL/checkpoint.
        if config.startup.tls.wire_version() < 7 {
            config.startup.tls = checked(config.startup.tls.with_wire_version(7))?;
        }
        match rotation {
            Some(rotation) => {
                config.open_with_peer_rotation_and_timers(rotation, timers, app, wake, MonoTime(0))
            }
            None => config.open_with_protocol_and_timers(protocol, timers, app, wake, MonoTime(0)),
        }
    } else {
        match rotation {
            Some(rotation) => config.startup.open_with_peer_rotation_and_timers(
                rotation,
                timers,
                app,
                wake,
                MonoTime(0),
            ),
            None => config.startup.open_with_protocol_and_timers(
                protocol,
                timers,
                app,
                wake,
                MonoTime(0),
            ),
        }
    };
    match opened {
        Ok(node) => Ok(node),
        Err(mut rejected) => {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !rejected.try_cleanup()? {
                if std::time::Instant::now() >= deadline {
                    return Err("startup cleanup timed out; recover before reuse".into());
                }
                std::thread::park_timeout(std::time::Duration::from_millis(1));
            }
            Err(rejected.reason.into())
        }
    }
}
/// Call only after Node has reached Drained. Polling while draining is the host's job.
pub fn join<A>(service: NativeNode<A, NativeServiceConnector>) -> Result<(), Failure>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    use std::time::{Duration, Instant};
    let mut parts = service.into_parts().map_err(|_| "node is not drained")?;
    let mut dialer = parts
        .peers
        .take()
        .ok_or("missing peers")?
        .connector
        .into_dialer()
        .map_err(|_| "connector not drained")?;
    let mut snapshots = parts.local.snapshots.take().ok_or("missing snapshots")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut log_done = false;
    let mut snapshots_done = false;
    loop {
        let dial_done = match &mut dialer {
            Some(d) => checked(d.try_finish())?,
            None => true,
        };
        if !log_done {
            log_done = parts.local.persistence.try_reclaim()?.is_some();
        }
        if !snapshots_done {
            snapshots_done = snapshots.worker.try_reclaim()?.is_some();
        }
        if dial_done && log_done && snapshots_done {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("worker join timed out".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

type PeerAddresses = BTreeMap<NodeId, (SocketAddr, String)>;
fn load_legacy_endpoints(path: &Path, addresses: &mut PeerAddresses) -> Result<(), Failure> {
    addresses.clear();
    let data = material(path, 4096)?;
    for line in std::str::from_utf8(&data)?.lines() {
        let words = line.split_whitespace().collect::<Vec<_>>();
        let [id, address, name] = words.as_slice() else {
            return Err("expected NODE SOCKET_ADDRESS TLS_SERVER_NAME".into());
        };
        let id: u64 = id.parse()?;
        if !(1..=3).contains(&id)
            || addresses
                .insert(node(id), (address.parse()?, name.to_string()))
                .is_some()
        {
            return Err("invalid or duplicate endpoint node".into());
        }
    }
    if addresses.len() != 3 {
        return Err("exactly three peer endpoints required".into());
    }
    Ok(())
}
fn load_deployment(
    path: &Path,
    id: u64,
    addresses: &mut PeerAddresses,
    stores: &mut BTreeMap<NodeId, StoreIdentity>,
) -> Result<(), Failure> {
    addresses.clear();
    stores.clear();
    let data = material(path, 65536)?;
    let mut lines = std::str::from_utf8(&data)?.lines();
    if lines.next() != Some("voteboat-deployment-v1") {
        return Err("expected voteboat-deployment-v1 header".into());
    }
    let mut identities = std::collections::BTreeSet::new();
    let mut sockets = std::collections::BTreeSet::new();
    for line in lines {
        if stores.len() >= 1024 {
            return Err("deployment exceeds 1024 replicas".into());
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        let [number, store, incarnation, address, name] = words.as_slice() else {
            return Err(
                "expected NODE STORE_ID STORE_INCARNATION SOCKET_ADDRESS TLS_SERVER_NAME".into(),
            );
        };
        let number: u64 = number.parse()?;
        let store = StoreIdentity {
            id: StoreId::new(store.parse()?).ok_or("invalid store ID")?,
            incarnation: StoreIncarnation::new(incarnation.parse()?)
                .ok_or("invalid store incarnation")?,
        };
        let address: SocketAddr = address.parse()?;
        if !(1..=MAX_NODE).contains(&number)
            || address.port() == 0
            || address.ip().is_unspecified()
            || name.len() > 253
            || rustls::pki_types::ServerName::try_from(*name).is_err()
            || !identities.insert((store.id, store.incarnation))
            || !sockets.insert(address)
            || stores.insert(node(number), store).is_some()
        {
            return Err("invalid or duplicate deployment identity/endpoint".into());
        }
        addresses.insert(node(number), (address, name.to_string()));
    }
    if !stores.contains_key(&node(id)) {
        return Err("local node missing from deployment".into());
    }
    Ok(())
}
/// The acknowledgement `--no-maintenance` requires; any other value is refused.
pub const NO_MAINTENANCE_ACK: &str = "I-accept-a-bounded-log";
/// Store maintenance choices shared by the three service binaries. Defaults
/// (no flags) keep automatic checkpoints and WAL reclaim on.
#[derive(Clone, Copy, Debug, Default)]
pub struct MaintenanceOptions {
    pub checkpoint_entries: Option<u64>,
    pub wal_reclaim_ms: Option<u64>,
    pub lifetime_bounded: bool,
}
impl MaintenanceOptions {
    pub fn is_flag(flag: &str) -> bool {
        matches!(
            flag,
            "--checkpoint-entries" | "--wal-reclaim-ms" | "--no-maintenance"
        )
    }
    /// Record one flag pair. Each flag may appear once.
    pub fn parse(&mut self, flag: &str, value: &str) -> Result<(), Failure> {
        let positive = |name: &str| -> Result<u64, Failure> {
            match value.parse::<u64>()? {
                0 => Err(format!("{name} must be positive").into()),
                n => Ok(n),
            }
        };
        match flag {
            "--checkpoint-entries" if self.checkpoint_entries.is_none() => {
                self.checkpoint_entries = Some(positive("checkpoint entry threshold")?);
            }
            "--wal-reclaim-ms" if self.wal_reclaim_ms.is_none() => {
                self.wal_reclaim_ms = Some(positive("WAL reclaim interval")?);
            }
            "--no-maintenance" if !self.lifetime_bounded => {
                if value != NO_MAINTENANCE_ACK {
                    return Err(format!(
                        "--no-maintenance disables checkpoints and WAL reclaim; every group then stops accepting writes at its store limits. Pass --no-maintenance {NO_MAINTENANCE_ACK} to accept that"
                    )
                    .into());
                }
                self.lifetime_bounded = true;
            }
            _ => return Err("duplicate or unsupported maintenance option".into()),
        }
        Ok(())
    }
    /// Refuse, before any file is opened, a selection that would fail or be
    /// ignored once serving.
    pub fn validate(&self) -> Result<(), Failure> {
        if self.lifetime_bounded
            && (self.checkpoint_entries.is_some() || self.wal_reclaim_ms.is_some())
        {
            return Err(
                "--no-maintenance cannot be combined with --checkpoint-entries or --wal-reclaim-ms"
                    .into(),
            );
        }
        let max = LogLimits::default().max_entries_per_group / 2;
        if self.checkpoint_entries.is_some_and(|n| n > max as u64) {
            return Err(format!(
                "--checkpoint-entries must be at most {max} (half the retained-entry limit) so a checkpoint can start before the limit"
            )
            .into());
        }
        Ok(())
    }
    /// Select the retention mode before the node is constructed.
    pub fn select(&self, limits: &mut NodeLimits) {
        limits.retention = if self.lifetime_bounded {
            RetentionMode::LifetimeBounded
        } else {
            RetentionMode::Maintained
        };
    }
    /// Replace default policies with explicit overrides after construction.
    pub fn apply<A>(&self, node: &mut NativeNode<A, NativeServiceConnector>) -> Result<(), Failure>
    where
        A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
        A::Receipt: ApplicationReceipt,
    {
        if let Some(min_entries) = self.checkpoint_entries {
            let policy = node
                .checkpoints()
                .policy
                .ok_or("automatic checkpoints unavailable")?;
            checked(node.configure_checkpoints(Some(CheckpointPolicy {
                min_entries,
                ..policy
            })))?;
        }
        if let Some(interval_ms) = self.wal_reclaim_ms {
            let policy = node
                .wal_maintenance()
                .policy
                .ok_or("WAL reclaim unavailable")?;
            checked(node.configure_wal_maintenance(Some(WalMaintenancePolicy {
                interval_ms,
                retry_ms: interval_ms.min(policy.retry_ms),
                ..policy
            })))?;
        }
        Ok(())
    }
}
/// One startup line recording the retention mode and active policies.
pub fn maintenance_startup<A>(node: &NativeNode<A, NativeServiceConnector>) -> String
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let checkpoint = node.checkpoints().policy;
    let wal = node.wal_maintenance().policy;
    format!(
        "maintenance retention={} checkpoint_entries={} checkpoint_scan_ms={} wal_reclaim_ms={} wal_retry_ms={}",
        retention_name(node.retention()),
        checkpoint.map_or("off".into(), |p| p.min_entries.to_string()),
        checkpoint.map_or("off".into(), |p| p.interval_ms.to_string()),
        wal.map_or("off".into(), |p| p.interval_ms.to_string()),
        wal.map_or("off".into(), |p| p.retry_ms.to_string()),
    )
}
fn retention_name(mode: RetentionMode) -> &'static str {
    match mode {
        RetentionMode::Maintained => "maintained",
        RetentionMode::LifetimeBounded => "lifetime-bounded",
    }
}
/// Operator-visible maintenance state for one group: retention mode,
/// checkpoint policy, last result and base, WAL occupancy, last reclaim, last
/// error and consecutive failures. Local observation, not quorum evidence.
pub fn maintenance_status<A>(
    node: &NativeNode<A, NativeServiceConnector>,
    group: GroupIdentity,
) -> String
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let wal = node.wal_maintenance();
    let checkpoints = node.checkpoints();
    let usage = node.wal_usage();
    let reclaimed = wal.last_reclaimed;
    let completion_error = wal
        .last_completion
        .as_ref()
        .and_then(|e| e.result.as_ref().err());
    let checkpoint_result = match &checkpoints.last_result {
        None => "none".to_owned(),
        Some(CheckpointResult::Completed { index, .. }) => format!("completed@{index}"),
        Some(CheckpointResult::Rejected { error, .. }) => format!("rejected:{error:?}"),
    };
    format!(
        "OK retention={} enabled={} pending={} next_ms={:?} last_sequence={} wal_bytes={} wal_capacity={} last_reclaim_ms={:?} before_bytes={} after_bytes={} failures={} skipped={} admission_error={:?} completion_error={:?} checkpoint_enabled={} checkpoint_entries={} checkpoint_pending={} checkpoint_last={} checkpoint_admission_error={:?} leader_noop_pending={} high_water={} high_water_entries={} refused_proposals={} wal_parked={} checkpoint_base={}",
        retention_name(node.retention()),
        wal.policy.is_some(),
        wal.pending.is_some(),
        wal.next_deadline.map(|d| d.0),
        wal.last_completion.as_ref().map_or(0, |e| e.request.sequence),
        usage.map_or(0, |u| u.bytes),
        usage.map_or(0, |u| u.capacity),
        reclaimed.map(|(t, _)| t.0),
        reclaimed.map_or(0, |(_, r)| r.before_bytes),
        reclaimed.map_or(0, |(_, r)| r.after_bytes),
        wal.consecutive_failures,
        wal.skipped,
        wal.last_admission_error,
        completion_error,
        checkpoints.policy.is_some(),
        checkpoints.policy.map_or(0, |p| p.min_entries),
        checkpoints.pending.len(),
        checkpoint_result,
        checkpoints.last_admission_error.as_ref().map(|(_, e)| e),
        node.local()
            .owner
            .core(group)
            .is_some_and(|c| c.leader_noop_pending()),
        wal.high_water,
        wal.high_water_entries,
        wal.refused_proposals,
        usage.map_or(0, |u| u.parked),
        // Kept last: existing operator tooling reads it as the final field.
        node.local()
            .owner
            .core(group)
            .map_or(0, |c| c.state().base_index()),
    )
}
