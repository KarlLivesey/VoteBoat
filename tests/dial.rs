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
//! Downstream public contract and native TCP ownership tests.
use std::net::SocketAddr;
use voteboat::{dial::*, identity::*, secure::*, transport::ConnectTicket};
fn ticket(peer: u64, generation: u64) -> ConnectTicket {
    let store = |id| StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    ConnectTicket {
        local: LocalIdentity {
            node: NodeId::new(1).unwrap(),
            store: StoreBinding {
                identity: store(1),
                session: StoreSession::new(1).unwrap(),
            },
        },
        peer: PeerIdentity {
            node: NodeId::new(peer).unwrap(),
            store: store(peer as u128),
        },
        generation: SecureSessionGeneration::new(generation).unwrap(),
    }
}
fn request(connection: ConnectTicket, endpoint: SocketAddr) -> DialRequest<SocketAddr> {
    DialRequest {
        connection,
        endpoint,
        timeout_ms: 100,
    }
}
/// A downstream host's local channel type; no native types required by the seam.
struct HostDialer {
    pending: Option<(DialRequest<SocketAddr>, bool)>,
    generation: u64,
    closed: bool,
}
impl PeerDialer for HostDialer {
    type Endpoint = SocketAddr;
    type Channel = String;
    fn local(&self) -> LocalIdentity {
        ticket(2, 1).local
    }
    fn limits(&self) -> DialLimits {
        DialLimits {
            requests: 1,
            connect_timeout_ms: 100,
        }
    }
    fn outstanding(&self) -> usize {
        usize::from(self.pending.is_some())
    }
    fn submit(&mut self, request: DialRequest<SocketAddr>) -> Result<(), DialRejected<SocketAddr>> {
        let reason = if self.closed {
            Some(DialError::Closed)
        } else if request.connection.local != self.local()
            || request.connection.peer != ticket(2, 1).peer
        {
            Some(DialError::WrongBinding)
        } else if request.connection.generation.get() <= self.generation {
            Some(DialError::StaleConnection)
        } else if request.timeout_ms == 0 || request.timeout_ms > 100 {
            Some(DialError::InvalidRequest)
        } else if self.pending.is_some() {
            Some(DialError::Overloaded)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(DialRejected {
                reason,
                request: Box::new(request),
            });
        }
        self.generation = request.connection.generation.get();
        self.pending = Some((request, false));
        Ok(())
    }
    fn cancel(&mut self, connection: ConnectTicket) -> bool {
        if let Some((r, cancel)) = &mut self.pending {
            if r.connection == connection {
                *cancel = true;
                return true;
            }
        }
        false
    }
    fn poll(&mut self, limit: usize) -> Vec<DialCompletion<String>> {
        if limit == 0 {
            return vec![];
        }
        self.pending
            .take()
            .map(|(r, cancel)| DialCompletion {
                connection: r.connection,
                result: if cancel {
                    Err(DialError::Cancelled)
                } else {
                    Ok(format!("host:{}", r.endpoint))
                },
            })
            .into_iter()
            .collect()
    }
    fn close(&mut self) {
        self.closed = true;
        if let Some((_, cancel)) = &mut self.pending {
            *cancel = true;
        }
    }
}
#[test]
fn host_can_supply_local_channel_type_and_exact_cancelled_receipts() {
    let mut host = HostDialer {
        pending: None,
        generation: 0,
        closed: false,
    };
    let provider: &mut dyn PeerDialer<Endpoint = SocketAddr, Channel = String> = &mut host;
    let address = "127.0.0.1:1234".parse().unwrap();
    provider.submit(request(ticket(2, 1), address)).unwrap();
    assert!(!provider.cancel(ticket(2, 2)));
    assert!(provider.cancel(ticket(2, 1)));
    assert_eq!(provider.outstanding(), 1);
    assert!(provider.poll(0).is_empty());
    let rejection = provider.submit(request(ticket(2, 2), address)).unwrap_err();
    assert_eq!(rejection.reason, DialError::Overloaded);
    assert_eq!(rejection.request.connection, ticket(2, 2));
    let completion = provider.poll(1).pop().unwrap();
    assert_eq!(completion.connection, ticket(2, 1));
    assert_eq!(completion.result, Err(DialError::Cancelled));
    assert!(!provider.cancel(ticket(2, 1)));
    provider.submit(*rejection.request).unwrap();
    assert!(provider
        .poll(1)
        .pop()
        .unwrap()
        .result
        .unwrap()
        .starts_with("host:"));
    assert_eq!(
        provider
            .submit(request(ticket(2, 2), address))
            .unwrap_err()
            .reason,
        DialError::StaleConnection
    );
    provider.submit(request(ticket(2, 3), address)).unwrap();
    provider.close();
    assert!(!provider.is_drained());
    assert_eq!(
        provider.poll(1).pop().unwrap().result,
        Err(DialError::Cancelled)
    );
    assert!(provider.is_drained());
}
#[test]
fn rejects_unbounded_or_zero_limits() {
    for limits in [
        DialLimits {
            requests: 0,
            connect_timeout_ms: 100,
        },
        DialLimits {
            requests: 1025,
            connect_timeout_ms: 100,
        },
        DialLimits {
            requests: 1,
            connect_timeout_ms: 0,
        },
        DialLimits {
            requests: 1,
            connect_timeout_ms: 10001,
        },
    ] {
        assert_eq!(limits.validate(), Err(DialError::InvalidLimits));
    }
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::Arc,
        thread,
        time::{Duration, Instant},
    };
    use voteboat::{
        native::{dial::NativeTcpDialer, worker::ThreadWake},
        worker::WorkerWake,
    };
    fn dialer(requests: usize, wake: Arc<dyn WorkerWake>) -> NativeTcpDialer {
        NativeTcpDialer::spawn(
            ticket(2, 1).local,
            (2..=3)
                .map(|id| (ticket(id, 1).peer.node, ticket(id, 1).peer.store))
                .collect(),
            DialLimits {
                requests,
                connect_timeout_ms: 100,
            },
            wake,
        )
        .unwrap()
    }
    fn terminal(dialer: &mut NativeTcpDialer) -> DialCompletion<TcpStream> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(event) = dialer.poll(1).pop() {
                return event;
            }
            assert!(Instant::now() < deadline, "dial completion missing");
            thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn finish(dialer: &mut NativeTcpDialer) {
        dialer.close();
        assert!(dialer.is_drained());
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dialer.try_finish().unwrap() {
            assert!(Instant::now() < deadline, "worker did not shut down");
            thread::yield_now();
        }
    }
    #[test]
    fn tcp_success_is_nonblocking_and_preserves_exact_scope_and_ownership() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let mut dialer = dialer(1, Arc::new(ThreadWake::current()));
        assert_eq!(dialer.local(), ticket(2, 1).local);
        assert_eq!(dialer.try_finish(), Err(DialError::InvalidRequest));
        dialer.submit(request(ticket(2, 1), endpoint)).unwrap();
        assert_eq!(dialer.outstanding(), 1);
        let rejected = dialer.submit(request(ticket(3, 1), endpoint)).unwrap_err();
        assert_eq!(rejected.reason, DialError::Overloaded);
        assert_eq!(rejected.request.endpoint, endpoint);
        let event = terminal(&mut dialer);
        assert_eq!(event.connection, ticket(2, 1));
        let mut client = event.result.unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        server
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert!(client.nodelay().unwrap());
        assert_eq!(
            client.read(&mut [0]).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        client.write_all(b"dialled").unwrap();
        let mut bytes = [0; 7];
        server.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"dialled");
        assert!(dialer.is_drained());
        assert!(dialer.poll(usize::MAX).is_empty());
        assert_eq!(
            dialer
                .submit(request(ticket(2, 1), endpoint))
                .unwrap_err()
                .reason,
            DialError::StaleConnection
        );
        dialer.submit(*rejected.request).unwrap();
        let _second = terminal(&mut dialer).result.unwrap();
        finish(&mut dialer);
        // Explicit dialer shutdown cannot close a channel already transferred.
        client.write_all(b"alive").unwrap();
        let mut bytes = [0; 5];
        server.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"alive");
    }
    struct Notice(std::sync::mpsc::SyncSender<()>);
    impl WorkerWake for Notice {
        fn wake(&self) {
            let _ = self.0.try_send(());
        }
    }
    #[test]
    fn cancelling_completed_unpolled_success_closes_socket_before_slot_release() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let (wake, woke) = std::sync::mpsc::sync_channel(1);
        let mut dialer = dialer(1, Arc::new(Notice(wake)));
        dialer
            .submit(request(ticket(2, 1), listener.local_addr().unwrap()))
            .unwrap();
        woke.recv_timeout(Duration::from_secs(5)).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        server
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        assert_eq!(dialer.outstanding(), 1, "receipt still owns channel");
        assert!(dialer.cancel(ticket(2, 1)));
        assert_eq!(dialer.outstanding(), 1);
        assert!(matches!(
            terminal(&mut dialer).result,
            Err(DialError::Cancelled)
        ));
        assert_eq!(
            server.read(&mut [0]).unwrap(),
            0,
            "cancelled success must close stream"
        );
        assert!(dialer.is_drained());
        finish(&mut dialer);
    }
    #[test]
    fn refusal_is_terminal_and_close_does_not_stop_another_dialer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let refused = listener.local_addr().unwrap();
        drop(listener);
        let wake = Arc::new(ThreadWake::current());
        let mut first = dialer(1, wake.clone());
        let mut second = dialer(1, wake);
        first.submit(request(ticket(2, 1), refused)).unwrap();
        let event = terminal(&mut first);
        assert_eq!(event.connection, ticket(2, 1));
        assert!(matches!(event.result, Err(DialError::Io(_))));
        finish(&mut first);
        assert_eq!(
            first
                .submit(request(ticket(2, 2), refused))
                .unwrap_err()
                .reason,
            DialError::Closed
        );
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        second
            .submit(request(ticket(2, 2), listener.local_addr().unwrap()))
            .unwrap();
        let _stream = terminal(&mut second).result.unwrap();
        finish(&mut second);
    }
    #[test]
    fn rejects_scope_and_timeout_before_connect_or_generation_consumption() {
        let mut dialer = dialer(1, Arc::new(ThreadWake::current()));
        let endpoint = "127.0.0.1:1234".parse().unwrap();
        let mut wrong = ticket(2, 1);
        wrong.local.store.session = StoreSession::new(2).unwrap();
        assert_eq!(
            dialer.submit(request(wrong, endpoint)).unwrap_err().reason,
            DialError::WrongBinding
        );
        wrong = ticket(2, 1);
        wrong.peer.store.incarnation = StoreIncarnation::new(2).unwrap();
        assert_eq!(
            dialer.submit(request(wrong, endpoint)).unwrap_err().reason,
            DialError::WrongBinding
        );
        assert_eq!(
            dialer
                .submit(request(ticket(4, 1), endpoint))
                .unwrap_err()
                .reason,
            DialError::UnknownPeer
        );
        for timeout_ms in [0, 101] {
            let mut r = request(ticket(2, 1), endpoint);
            r.timeout_ms = timeout_ms;
            assert_eq!(
                dialer.submit(r).unwrap_err().reason,
                DialError::InvalidRequest
            );
        }
        for endpoint in ["127.0.0.1:0", "0.0.0.0:1"] {
            assert_eq!(
                dialer
                    .submit(request(ticket(2, 1), endpoint.parse().unwrap()))
                    .unwrap_err()
                    .reason,
                DialError::InvalidRequest
            );
        }
        assert!(dialer.is_drained());
        assert!(matches!(
            NativeTcpDialer::spawn(
                ticket(2, 1).local,
                BTreeMap::from([(ticket(2, 1).local.node, ticket(2, 1).local.store.identity)]),
                DialLimits::default(),
                Arc::new(ThreadWake::current())
            ),
            Err(DialError::InvalidRequest)
        ));
        // Rejected requests have not consumed the first generation.
        dialer.submit(request(ticket(2, 1), endpoint)).unwrap();
        dialer.close();
        assert!(matches!(
            terminal(&mut dialer).result,
            Err(DialError::Cancelled)
        ));
        finish(&mut dialer);
    }
}
