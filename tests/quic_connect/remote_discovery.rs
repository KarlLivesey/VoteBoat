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
use super::*;
use voteboat::{discovery::*, native::remote_discovery::*};
#[path = "driven_discovery.rs"]
mod driven;
#[path = "../remote_discovery/renewal.rs"]
mod renewal;
#[path = "../remote_discovery/scenario.rs"]
mod scenario;
fn peer(n: u64) -> PeerIdentity {
    PeerIdentity {
        node: local(n).node,
        store: local(n).store.identity,
    }
}
fn hint(id: u64, generation: u64, expires: u64) -> PeerEndpointHint {
    PeerEndpointHint {
        peer: peer(id),
        generation: HintGeneration::new(generation).unwrap(),
        endpoint: format!("127.0.0.1:{}", 1000 + generation).parse().unwrap(),
        expires_at: MonoTime(expires),
    }
}
#[test]
fn quic_remote_refresh_keeps_unrelated_cached_peer_available() {
    let mut connectors = connectors();
    let (mut sessions, now) = establish(&mut connectors, 0, 1);
    let a = sessions.remove(&(1, 2)).unwrap();
    let b = sessions.remove(&(2, 1)).unwrap();
    scenario::refresh(a, b, MonoTime(now));
}

#[test]
fn quic_authenticated_endpoint_leases_renew_without_changing_generation() {
    let mut connectors = connectors();
    let (mut sessions, now) = establish(&mut connectors, 0, 1);
    let a = sessions.remove(&(1, 2)).unwrap();
    let b = sessions.remove(&(2, 1)).unwrap();
    renewal::exercise(a, b, MonoTime(now));
}

#[path = "../remote_discovery/source_reconnect.rs"]
mod source_reconnect;
#[test]
fn quic_source_reconnect_retains_floors_live_cache_and_drains_pending_lookup() {
    let mut connectors = connectors();
    let (mut sessions, now) = establish(&mut connectors, 0, 1);
    let a = sessions.remove(&(1, 2)).unwrap();
    let b = sessions.remove(&(2, 1)).unwrap();
    drop(sessions);
    let client = connectors.remove(0);
    let server = connectors.remove(0);
    let endpoint = server.local_addr();
    source_reconnect::exercise(a, b, client, server, endpoint, MonoTime(now));
}
