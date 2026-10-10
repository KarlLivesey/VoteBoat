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
//! Bounded connection choices, never membership or ownership authority.
use super::{
    command_endpoints::Endpoint,
    setup::{Failure, MAX_NODE},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    net::SocketAddr,
    path::Path,
};
use voteboat::identity::*;
pub struct Authorities {
    pub groups: BTreeMap<GroupIdentity, Vec<Endpoint>>,
    pub pins: Vec<Endpoint>,
}
impl Authorities {
    pub fn load(path: &Path) -> Result<Self, Failure> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(32 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 32 * 1024 {
            return Err("authority map exceeds32KiB".into());
        }
        let mut lines = std::str::from_utf8(&bytes)?.lines();
        if lines.next() != Some("voteboat-authorities-v1") {
            return Err("expected voteboat-authorities-v1".into());
        }
        let mut groups = BTreeMap::<GroupIdentity, Vec<Endpoint>>::new();
        let mut pins = BTreeMap::<u64, Endpoint>::new();
        let mut seen = BTreeSet::new();
        let mut count = 0;
        for line in lines {
            count += 1;
            if count > 64 {
                return Err("authority map exceeds64 endpoints".into());
            }
            let words = line.split_whitespace().collect::<Vec<_>>();
            let [group, incarnation, node, address, name] = words.as_slice() else {
                return Err("expected GROUP INCARNATION NODE ADDRESS TLS_NAME".into());
            };
            let group = GroupIdentity {
                id: GroupId::new(group.parse()?).ok_or("invalid authority group")?,
                incarnation: GroupIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid authority incarnation")?,
            };
            let node: u64 = node.parse()?;
            let address: SocketAddr = address.parse()?;
            if !(1..=MAX_NODE).contains(&node)
                || address.port() == 0
                || address.ip().is_unspecified()
                || address.ip().is_multicast()
                || rustls::pki_types::ServerName::try_from(*name).is_err()
                || !seen.insert((group, node))
            {
                return Err("invalid or duplicate authority endpoint".into());
            }
            if pins.get(&node).is_some_and(|p| p.server_name != *name) {
                return Err("conflicting TLS name for one node identity".into());
            }
            let endpoint = Endpoint {
                node,
                address,
                server_name: (*name).to_owned(),
            };
            pins.entry(node).or_insert_with(|| endpoint.clone());
            let peers = groups.entry(group).or_default();
            if peers.iter().any(|p| p.address == address) {
                return Err("duplicate authority address".into());
            }
            peers.push(endpoint);
            if groups.len() > 32 {
                return Err("authority map exceeds32 authorities".into());
            }
        }
        if groups.is_empty() {
            return Err("empty authority map".into());
        }
        for peers in groups.values_mut() {
            peers.sort_by_key(|p| p.node);
        }
        Ok(Self {
            groups,
            pins: pins.into_values().collect(),
        })
    }
}
