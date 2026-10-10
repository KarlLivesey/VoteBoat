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
use std::{net::TcpListener, sync::Arc};
use voteboat::{
    connect::*,
    dial::*,
    native::{connect::*, dial::NativeTcpDialer, worker::ThreadWake},
};
fn connector(id: u64, other: u64, listener: Option<TcpListener>) -> NativePeerConnector {
    let dialer = NativeTcpDialer::spawn(
        local(id),
        [(peer(other).node, peer(other).store)]
            .into_iter()
            .collect(),
        DialLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    NativePeerConnector::new(
        NativeConnectConfig {
            local: local(id),
            limits: ConnectLimits::default(),
            session: SessionLimits::default(),
        },
        support::tls::configuration(id),
        [(peer(other).node, support::tls::peer(local(other)))]
            .into_iter()
            .collect(),
        dialer,
        listener,
        MonoTime(0),
    )
    .unwrap_or_else(|e| panic!("connector: {:?}", e.reason))
}
#[test]
fn tcp_source_reconnect_retains_floors_live_cache_and_drains_pending_lookup() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap();
    let client = connector(1, 2, None);
    let server = connector(2, 1, Some(listener));
    let (a, b) = support::tls::pair(local(1), local(2), 1);
    let (client, server) = source_reconnect::exercise(a, b, client, server, endpoint, MonoTime(0));
    for connector in [client, server] {
        let mut dialer = connector.into_dialer().ok().unwrap();
        for _ in 0..1000 {
            if dialer.try_finish().unwrap() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(dialer.try_finish().unwrap());
    }
}
