// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit Raft identities; endpoint hints never provision transport pins.
use super::{command_endpoints, Endpoint, Failure, HintGeneration, PeerIdentity};
use std::collections::{BTreeMap, BTreeSet};
use voteboat::identity::{NodeId, StoreId, StoreIdentity, StoreIncarnation};

pub(super) const HEADER: &str = "voteboat-peer-discovery-v1";
pub(super) struct Peers {
    pub generation: HintGeneration,
    pub endpoints: Vec<Endpoint>,
    pub identities: BTreeMap<u64, PeerIdentity>,
}
pub(super) fn parse(text: &str) -> Result<Peers, Failure> {
    if text.len() > 16 * 1024 {
        return Err("peer discovery exceeds 16 KiB".into());
    }
    let mut lines = text.lines();
    let header = lines
        .next()
        .unwrap_or("")
        .split_whitespace()
        .collect::<Vec<_>>();
    let [HEADER, generation] = header.as_slice() else {
        return Err("expected voteboat-peer-discovery-v1 GENERATION".into());
    };
    let generation = HintGeneration::new(generation.parse()?).ok_or("invalid hint generation")?;
    let mut identities = BTreeMap::new();
    let mut stores = BTreeSet::new();
    let mut endpoints = String::from("voteboat-command-peers-v1\n");
    for line in lines {
        if identities.len() == command_endpoints::MAX_TARGETS {
            return Err("peer discovery exceeds 64 targets".into());
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        let [node, store, incarnation, address, name] = words.as_slice() else {
            return Err(
                "expected NODE STORE_ID STORE_INCARNATION SOCKET_ADDRESS TLS_SERVER_NAME".into(),
            );
        };
        let number = node.parse()?;
        let peer = PeerIdentity {
            node: NodeId::new(number).ok_or("invalid peer node")?,
            store: StoreIdentity {
                id: StoreId::new(store.parse()?).ok_or("invalid peer store")?,
                incarnation: StoreIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid peer store incarnation")?,
            },
        };
        if identities.insert(number, peer).is_some()
            || !stores.insert((peer.store.id, peer.store.incarnation))
        {
            return Err("duplicate peer discovery identity".into());
        }
        endpoints.push_str(&format!("{number} {address} {name}\n"));
    }
    Ok(Peers {
        generation,
        endpoints: command_endpoints::parse(&endpoints)?,
        identities,
    })
}
