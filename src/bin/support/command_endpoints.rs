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
//! Explicit command routes; endpoint hints never grant service permissions.
use super::setup::{Failure, MAX_NODE};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
};

pub const MAX_TARGETS: usize = 64;
const MAX_BYTES: u64 = 16 * 1024;

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub node: u64,
    pub address: SocketAddr,
    pub server_name: String,
}

pub fn listener(
    selected: Option<SocketAddr>,
    authenticated: bool,
    base: u16,
    node: u64,
) -> Result<SocketAddr, Failure> {
    match selected {
        Some(address) => {
            if !authenticated {
                return Err("--command-listen requires --service-access".into());
            }
            if address.port() == 0 || address.ip().is_multicast() {
                return Err("invalid command listener address".into());
            }
            Ok(address)
        }
        None => Ok((Ipv4Addr::LOCALHOST, base + 100 + node as u16).into()),
    }
}

pub fn targets(
    base: u16,
    node: Option<u64>,
    path: Option<&Path>,
) -> Result<Vec<Endpoint>, Failure> {
    if let Some(path) = path {
        let entries = load(path)?;
        return match node {
            Some(node) => entries
                .into_iter()
                .find(|entry| entry.node == node)
                .map(|entry| vec![entry])
                .ok_or_else(|| "target node missing from command peers".into()),
            None => Ok(entries),
        };
    }
    Ok(node
        .map_or_else(|| vec![1, 2, 3], |n| vec![n])
        .into_iter()
        .map(|node| Endpoint {
            node,
            address: (Ipv4Addr::LOCALHOST, base + 100 + node as u16).into(),
            server_name: format!("node{node}.voteboat.test"),
        })
        .collect())
}

pub fn load(path: &Path) -> Result<Vec<Endpoint>, Failure> {
    parse(&read(path)?)
}

pub fn read(path: &Path) -> Result<String, Failure> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err("command peers exceed 16 KiB".into());
    }
    Ok(String::from_utf8(bytes)?)
}

pub fn parse(text: &str) -> Result<Vec<Endpoint>, Failure> {
    if text.len() as u64 > MAX_BYTES {
        return Err("command peers exceed 16 KiB".into());
    }
    let mut lines = text.lines();
    if lines.next() != Some("voteboat-command-peers-v1") {
        return Err("expected voteboat-command-peers-v1 header".into());
    }
    let mut entries = BTreeMap::new();
    let mut addresses = BTreeSet::new();
    for line in lines {
        if entries.len() == MAX_TARGETS {
            return Err("command peers exceed 64 targets".into());
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        let [node, address, server_name] = words.as_slice() else {
            return Err("expected NODE SOCKET_ADDRESS TLS_SERVER_NAME".into());
        };
        let node = node.parse::<u64>()?;
        let address = address.parse::<SocketAddr>()?;
        if !(1..=MAX_NODE).contains(&node)
            || address.port() == 0
            || address.ip().is_unspecified()
            || address.ip().is_multicast()
            || rustls::pki_types::ServerName::try_from(*server_name).is_err()
            || entries.contains_key(&node)
            || !addresses.insert(address)
        {
            return Err("invalid or duplicate command endpoint".into());
        }
        entries.insert(
            node,
            Endpoint {
                node,
                address,
                server_name: (*server_name).to_owned(),
            },
        );
    }
    if entries.is_empty() {
        return Err("command peers require at least one target".into());
    }
    Ok(entries.into_values().collect())
}
