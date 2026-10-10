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
//! Command endpoints remain hints; the guarded transport owns authentication.
use super::{
    command_endpoints::{self, Endpoint},
    service_access,
    setup::Failure,
};
use std::{
    cell::RefCell,
    net::SocketAddr,
    path::Path,
    rc::Rc,
    time::{Duration, Instant},
};
use voteboat::{discovery::*, native::remote_discovery::*, runtime::MonoTime, secure::*};

pub const ACK: &str = "OK discovery-v1\n";
pub type Server = NativeDiscoveryResponder<Box<dyn SecureSession>, Source>;
#[derive(Clone)]
pub struct Source {
    current: Rc<RefCell<Snapshot>>,
    explicit: bool,
    closed: bool,
}
struct Snapshot {
    endpoints: Vec<Endpoint>,
    generation: HintGeneration,
    last: Option<Update>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Update {
    expected: HintGeneration,
    next: HintGeneration,
    node: u64,
    address: SocketAddr,
}
impl Source {
    pub fn load(path: Option<&Path>, authenticated: bool) -> Result<Option<Self>, Failure> {
        let Some(path) = path else { return Ok(None) };
        if !authenticated {
            return Err("--discovery-peers requires --service-access".into());
        }
        Ok(Some(Self::parse(&command_endpoints::read(path)?)?))
    }
    fn parse(text: &str) -> Result<Self, Failure> {
        let header = text.lines().next().unwrap_or("");
        let (generation, explicit, endpoints) = if header.starts_with("voteboat-discovery-peers-v2")
        {
            let words = header.split_whitespace().collect::<Vec<_>>();
            let ["voteboat-discovery-peers-v2", generation] = words.as_slice() else {
                return Err("expected voteboat-discovery-peers-v2 GENERATION".into());
            };
            let generation =
                HintGeneration::new(generation.parse()?).ok_or("invalid hint generation")?;
            let (_, body) = text.split_once('\n').ok_or("missing discovery endpoints")?;
            let endpoints =
                command_endpoints::parse(&format!("voteboat-command-peers-v1\n{body}"))?;
            (generation, true, endpoints)
        } else {
            (
                HintGeneration::new(1).unwrap(),
                false,
                command_endpoints::parse(text)?,
            )
        };
        Ok(Self {
            current: Rc::new(RefCell::new(Snapshot {
                endpoints,
                generation,
                last: None,
            })),
            explicit,
            closed: false,
        })
    }
    pub fn bind_session(&mut self, session: u64) {
        if !self.explicit {
            self.current.borrow_mut().generation =
                HintGeneration::new(session).expect("nonzero store session");
        }
    }
    pub fn status(&self) -> String {
        let current = self.current.borrow();
        format!(
            "OK generation={} targets={} updates={} durable=false",
            current.generation.get(),
            current.endpoints.len(),
            self.explicit
        )
    }
    pub fn update(&self, words: &[&str]) -> Result<String, String> {
        let [expected, next, node, address] = words else {
            return Err("expected discovery-update EXPECTED NEXT NODE SOCKET_ADDRESS".into());
        };
        let generation = |s: &str| {
            s.parse()
                .ok()
                .and_then(HintGeneration::new)
                .ok_or("invalid hint generation")
        };
        self.replace(Update {
            expected: generation(expected)?,
            next: generation(next)?,
            node: node.parse().map_err(|_| "invalid endpoint node")?,
            address: address.parse().map_err(|_| "invalid endpoint address")?,
        })
    }
    fn replace(&self, update: Update) -> Result<String, String> {
        if self.closed || !self.explicit {
            return Err("endpoint updates require an open v2 discovery source".into());
        }
        let mut current = self.current.borrow_mut();
        if current.last == Some(update) {
            return Ok(format!(
                "OK generation={} duplicate=true durable=false",
                current.generation.get()
            ));
        }
        if update.expected != current.generation || update.next <= update.expected {
            return Err("stale or invalid endpoint generation".into());
        }
        if update.address.port() == 0
            || update.address.ip().is_unspecified()
            || update.address.ip().is_multicast()
            || current
                .endpoints
                .iter()
                .any(|e| e.node != update.node && e.address == update.address)
        {
            return Err("invalid or duplicate endpoint address".into());
        }
        let endpoint = current
            .endpoints
            .iter_mut()
            .find(|e| e.node == update.node)
            .ok_or("endpoint node not provisioned")?;
        endpoint.address = update.address;
        current.generation = update.next;
        current.last = Some(update);
        Ok(format!(
            "OK generation={} duplicate=false durable=false",
            update.next.get()
        ))
    }
    pub fn serve(self, session: Box<dyn SecureSession>, now: MonoTime) -> Result<Server, Failure> {
        let binding = require_authenticated(&*session)
            .map_err(|e| format!("discovery authentication: {e:?}"))?;
        let peer = PeerIdentity {
            node: binding.peer.node,
            store: binding.peer.store.identity,
        };
        NativeDiscoveryResponder::new(session, peer, self, RemoteDiscoveryConfig::default(), now)
            .map_err(|(e, _, _)| format!("discovery setup: {e:?}").into())
    }
}
impl PeerDiscovery for Source {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        if self.closed {
            return Err(DiscoveryError::Closed);
        }
        let current = self.current.borrow();
        let entry = current
            .endpoints
            .iter()
            .find(|e| service_access::transport_identity(e.node, false) == peer)
            .ok_or(DiscoveryError::Missing)?;
        let expires_at = MonoTime(
            now.0
                .checked_add(30_000)
                .ok_or(DiscoveryError::InvalidHint)?,
        );
        Ok(PeerEndpointHint {
            peer,
            generation: current.generation,
            endpoint: entry.address,
            expires_at,
        })
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {
        self.closed = true;
    }
}

/// Resolve before any command is sent; independently provisioned pins/names remain.
pub fn resolve(
    session: Box<dyn SecureSession>,
    source: u64,
    targets: &mut [Endpoint],
    start: Instant,
    deadline: Instant,
) -> Result<(), Failure> {
    let timestamp = || MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let mut discovery = NativeRemotePeerDiscovery::new(
        session,
        service_access::transport_identity(source, false),
        RemoteDiscoveryConfig::default(),
        timestamp(),
    )
    .map_err(|(e, _)| format!("discovery setup: {e:?}"))?;
    for target in targets {
        let peer = service_access::transport_identity(target.node, false);
        loop {
            if Instant::now() >= deadline {
                return Err("discovery deadline expired before command submission".into());
            }
            match discovery.resolve(peer, timestamp()) {
                Ok(hint) => {
                    target.address = hint.endpoint;
                    break;
                }
                Err(DiscoveryError::Unavailable) => (),
                Err(e) => return Err(format!("discovery lookup: {e:?}").into()),
            }
            if let Some(completion) = discovery
                .poll(timestamp(), SessionPollBudget::default())
                .map_err(|e| format!("discovery transport: {e:?}"))?
            {
                completion
                    .result
                    .map_err(|e| format!("discovery reply: {e:?}"))?;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    discovery.close();
    Ok(())
}

#[cfg(test)]
#[path = "command_discovery/tests.rs"]
mod tests;
