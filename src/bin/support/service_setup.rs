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
    native::{connect::*, node::*, startup::*, tls::*, worker::*},
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
fn material(path: &Path, limit: u64) -> Result<Vec<u8>, Failure> {
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
    mut config: NativeMemberStartup,
    protocol: NativePeerProtocol,
    member: bool,
    app: A,
) -> Result<NativeNode<A, NativeServiceConnector>, Failure>
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let wake = Arc::new(ThreadWake::current());
    let opened = if member {
        // Explicit recovery only: provisioned routes do not establish assignment.
        // The native member constructor verifies the authoritative WAL/checkpoint.
        if config.startup.tls.wire_version() < 7 {
            config.startup.tls = checked(config.startup.tls.with_wire_version(7))?;
        }
        config.open_with_protocol(protocol, app, wake, MonoTime(0))
    } else {
        config
            .startup
            .open_with_protocol(protocol, app, wake, MonoTime(0))
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
