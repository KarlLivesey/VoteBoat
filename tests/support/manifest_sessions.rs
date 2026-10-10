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
use std::time::{Duration, Instant};
use voteboat::{native::connect::NativePeerProtocol, runtime::MonoTime, secure::*};
pub fn pair(
    protocol: NativePeerProtocol,
    left: LocalIdentity,
    right: LocalIdentity,
    clock: &Instant,
) -> (Box<dyn SecureSession>, Box<dyn SecureSession>) {
    let (mut a, mut b): (Box<dyn SecureSession>, Box<dyn SecureSession>) = match protocol {
        NativePeerProtocol::TcpTls => {
            let (a, b) = super::tls::pair(left, right, 1);
            (Box::new(a), Box::new(b))
        }
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic => {
            use voteboat::native::quic::*;
            let a = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let b = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let aa = a.local_addr().unwrap();
            let ba = b.local_addr().unwrap();
            let options = |local, peer, remote| QuicSessionOptions {
                local,
                peer: super::tls::peer(peer),
                remote,
                generation: voteboat::identity::SecureSessionGeneration::new(1).unwrap(),
                limits: SessionLimits::default(),
            };
            let now = MonoTime(clock.elapsed().as_millis() as u64);
            (
                Box::new(
                    NativeQuicSession::client(
                        a,
                        &super::tls::configuration(left.node.get()),
                        options(left, right, ba),
                        now,
                    )
                    .unwrap(),
                ),
                Box::new(
                    NativeQuicSession::server(
                        b,
                        &super::tls::configuration(right.node.get()),
                        options(right, left, aa),
                        now,
                    )
                    .unwrap(),
                ),
            )
        }
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.state() != SessionState::Ready || b.state() != SessionState::Ready {
        let now = MonoTime(clock.elapsed().as_millis() as u64);
        a.poll(now, SessionPollBudget::default()).unwrap();
        b.poll(now, SessionPollBudget::default()).unwrap();
        assert!(Instant::now() < deadline, "discovery handshake stalled");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    (a, b)
}
