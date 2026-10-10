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
#![cfg(feature = "native")]
#[path = "remote_discovery/host.rs"]
mod host;
#[path = "remote_discovery/renewal.rs"]
mod renewal;
#[path = "remote_discovery/scenario.rs"]
mod scenario;
mod support;
use voteboat::{discovery::*, native::remote_discovery::*, runtime::MonoTime, secure::*};
fn peer(id: u64) -> PeerIdentity {
    PeerIdentity {
        node: support::node(id),
        store: support::identity(id.into()),
    }
}
fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: peer(id).node,
        store: voteboat::identity::StoreBinding {
            identity: peer(id).store,
            session: voteboat::identity::StoreSession::new(1).unwrap(),
        },
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
fn short_io_remote_refresh_keeps_unrelated_cached_peer_available() {
    let (a, b) = host::pair();
    scenario::refresh(a, b, MonoTime(0));
}
#[cfg(feature = "tls")]
#[test]
fn tcp_tls_remote_refresh_keeps_unrelated_cached_peer_available() {
    let (a, b) = support::tls::pair(local(1), local(2), 1);
    scenario::refresh(a, b, MonoTime(0));
}

#[test]
fn short_io_authenticated_endpoint_leases_renew_without_changing_generation() {
    let (a, b) = host::pair();
    renewal::exercise(a, b, MonoTime(0));
}

#[cfg(feature = "tls")]
#[test]
fn tcp_tls_authenticated_endpoint_leases_renew_without_changing_generation() {
    let (a, b) = support::tls::pair(local(1), local(2), 1);
    renewal::exercise(a, b, MonoTime(0));
}
