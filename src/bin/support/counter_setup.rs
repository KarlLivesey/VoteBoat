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
//! Counter-specific CLI configuration; assembly is the public native startup API.
use std::{collections::BTreeMap, net::SocketAddr, path::Path, sync::Arc};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{node::*, startup::*, tls::*, worker::*},
    quorum::*,
    runtime::*,
};
pub type Failure = Box<dyn std::error::Error>;
pub type Service = NativeNode<Counter>;
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
/// Optional endpoint file has exactly three lines: NODE SOCKET_ADDRESS TLS_SERVER_NAME.
pub fn configuration(
    root: &Path,
    id: u64,
    base: u16,
    tls: &Path,
    create: bool,
    endpoints: Option<&Path>,
) -> Result<NativeStartup, Failure> {
    if !(1..=3).contains(&id) || base == 0 || base > 65432 {
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
    if let Some(path) = endpoints {
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
    }
    let voters = (1..=3)
        .map(|n| (node(n), store_id(n)))
        .collect::<BTreeMap<_, _>>();
    let credentials = checked(NativeTlsConfig::new(TlsCredentials {
        roots: vec![material(&tls.join("ca.der"), 65536)?],
        certificate_chain: vec![material(&tls.join(format!("node{id}.der")), 65536)?],
        private_key: material(&tls.join(format!("node{id}-key.der")), 65536)?,
    }))?;
    let config = NativeStartup {
        directory: root.to_owned(),
        mode: if create {
            NativeOpenMode::Create
        } else {
            NativeOpenMode::Recover
        },
        node: node(id),
        store: store_id(id),
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
                Ok((
                    n,
                    NativeStartupPeer {
                        address,
                        server_name,
                        certificate: material(&tls.join(format!("node{}.der", n.get())), 65536)?,
                    },
                ))
            })
            .collect::<Result<_, Failure>>()?,
        tls: credentials,
        entropy_seed: id * 17,
        limits: NodeLimits::default(),
    };
    config.validate()?;
    Ok(config)
}
pub fn open(config: NativeStartup) -> Result<Service, Failure> {
    match config.open(
        checked(Counter::new(10000))?,
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    ) {
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
pub fn join(service: Service) -> Result<(), Failure> {
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
        let dial_done = checked(dialer.try_finish())?;
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
