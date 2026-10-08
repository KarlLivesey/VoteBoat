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
//! One explicitly owned bounded TCP dial thread. No DNS, listener, TLS, hidden
//! runtime or per-group resource is created. Connected streams are nonblocking.
use crate::{
    dial::*, identity::*, secure::LocalIdentity, transport::ConnectTicket, worker::WorkerWake,
};
use std::{
    collections::BTreeMap,
    io,
    net::{SocketAddr, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

struct Job {
    request: DialRequest<SocketAddr>,
    cancelled: Arc<AtomicBool>,
}
struct Pending {
    connection: ConnectTicket,
    cancelled: Arc<AtomicBool>,
}
struct Peer {
    store: StoreIdentity,
    generation: u64,
}
pub struct NativeTcpDialer {
    local: LocalIdentity,
    limits: DialLimits,
    peers: BTreeMap<NodeId, Peer>,
    pending: BTreeMap<NodeId, Pending>,
    sender: Option<SyncSender<Job>>,
    events: Receiver<DialCompletion<TcpStream>>,
    thread: Option<JoinHandle<()>>,
    failed: bool,
}
impl NativeTcpDialer {
    /// The worker only creates sockets; it never modifies a roster, Raft core
    /// or application. A completion wake is a scheduling hint, not readiness
    /// evidence. Failed construction leaves no live thread or socket.
    pub fn spawn(
        local: LocalIdentity,
        peers: BTreeMap<NodeId, StoreIdentity>,
        limits: DialLimits,
        wake: Arc<dyn WorkerWake>,
    ) -> Result<Self, DialError> {
        Self::spawn_with(local, peers, limits, wake, |endpoint, timeout| {
            let stream = TcpStream::connect_timeout(&endpoint, timeout)?;
            stream.set_nonblocking(true)?;
            stream.set_nodelay(true)?;
            Ok(stream)
        })
    }
    fn spawn_with<F>(
        local: LocalIdentity,
        peers: BTreeMap<NodeId, StoreIdentity>,
        limits: DialLimits,
        wake: Arc<dyn WorkerWake>,
        mut connect: F,
    ) -> Result<Self, DialError>
    where
        F: FnMut(SocketAddr, Duration) -> io::Result<TcpStream> + Send + 'static,
    {
        let limits = limits.validate()?;
        if peers.len() > 1024 || peers.contains_key(&local.node) {
            return Err(DialError::InvalidRequest);
        }
        let (sender, requests) = mpsc::sync_channel::<Job>(limits.requests);
        let (out, events) = mpsc::sync_channel(limits.requests);
        let thread = thread::Builder::new()
            .name("voteboat-dial".into())
            .spawn(move || {
                while let Ok(job) = requests.recv() {
                    let mut result = if job.cancelled.load(Ordering::Acquire) {
                        Err(DialError::Cancelled)
                    } else {
                        connect(
                            job.request.endpoint,
                            Duration::from_millis(job.request.timeout_ms),
                        )
                        .map_err(|e| DialError::Io(e.kind()))
                    };
                    if job.cancelled.load(Ordering::Acquire) {
                        // Assigning drops a stream produced by an in-flight connect.
                        result = Err(DialError::Cancelled);
                    }
                    if out
                        .send(DialCompletion {
                            connection: job.request.connection,
                            result,
                        })
                        .is_err()
                    {
                        break;
                    }
                    wake.wake();
                }
            })
            .map_err(|_| DialError::WorkerFailed)?;
        Ok(Self {
            local,
            limits,
            peers: peers
                .into_iter()
                .map(|(node, store)| {
                    (
                        node,
                        Peer {
                            store,
                            generation: 0,
                        },
                    )
                })
                .collect(),
            pending: BTreeMap::new(),
            sender: Some(sender),
            events,
            thread: Some(thread),
            failed: false,
        })
    }
    fn admission(&self, request: &DialRequest<SocketAddr>) -> Result<(), DialError> {
        if self.failed {
            return Err(DialError::WorkerFailed);
        }
        if self.sender.is_none() {
            return Err(DialError::Closed);
        }
        let ticket = request.connection;
        if ticket.local != self.local {
            return Err(DialError::WrongBinding);
        }
        let peer = self
            .peers
            .get(&ticket.peer.node)
            .ok_or(DialError::UnknownPeer)?;
        if ticket.peer.store != peer.store {
            return Err(DialError::WrongBinding);
        }
        if ticket.generation.get() <= peer.generation {
            return Err(DialError::StaleConnection);
        }
        if request.timeout_ms == 0
            || request.timeout_ms > self.limits.connect_timeout_ms
            || request.endpoint.port() == 0
            || request.endpoint.ip().is_unspecified()
        {
            return Err(DialError::InvalidRequest);
        }
        if self.pending.len() >= self.limits.requests
            || self.pending.contains_key(&ticket.peer.node)
        {
            return Err(DialError::Overloaded);
        }
        Ok(())
    }
    /// Nonblocking join after close and terminal delivery. Idempotent after a
    /// successful join. OS cancellation waits for at most the active connect's
    /// timeout (plus OS scheduling); queued cancellations perform no connect.
    pub fn try_finish(&mut self) -> Result<bool, DialError> {
        if self.sender.is_some() {
            return Err(DialError::InvalidRequest);
        }
        if !self.pending.is_empty() {
            return Ok(false);
        }
        let Some(thread) = &self.thread else {
            return Ok(true);
        };
        if !thread.is_finished() {
            return Ok(false);
        }
        self.thread
            .take()
            .unwrap()
            .join()
            .map(|_| true)
            .map_err(|_| DialError::WorkerFailed)
    }
}
impl PeerDialer for NativeTcpDialer {
    type Endpoint = SocketAddr;
    type Channel = TcpStream;
    fn local(&self) -> LocalIdentity {
        self.local
    }
    fn limits(&self) -> DialLimits {
        self.limits
    }
    fn outstanding(&self) -> usize {
        self.pending.len()
    }
    fn submit(&mut self, request: DialRequest<SocketAddr>) -> Result<(), DialRejected<SocketAddr>> {
        if let Err(reason) = self.admission(&request) {
            return Err(DialRejected {
                reason,
                request: Box::new(request),
            });
        }
        let connection = request.connection;
        let cancelled = Arc::new(AtomicBool::new(false));
        let job = Job {
            request,
            cancelled: cancelled.clone(),
        };
        match self.sender.as_ref().unwrap().try_send(job) {
            Ok(()) => {
                self.peers
                    .get_mut(&connection.peer.node)
                    .unwrap()
                    .generation = connection.generation.get();
                self.pending.insert(
                    connection.peer.node,
                    Pending {
                        connection,
                        cancelled,
                    },
                );
                Ok(())
            }
            Err(TrySendError::Full(job)) => Err(DialRejected {
                reason: DialError::Overloaded,
                request: Box::new(job.request),
            }),
            Err(TrySendError::Disconnected(job)) => {
                self.failed = true;
                self.close();
                Err(DialRejected {
                    reason: DialError::WorkerFailed,
                    request: Box::new(job.request),
                })
            }
        }
    }
    fn cancel(&mut self, connection: ConnectTicket) -> bool {
        if let Some(pending) = self.pending.get(&connection.peer.node) {
            if pending.connection == connection {
                pending.cancelled.store(true, Ordering::Release);
                return true;
            }
        }
        false
    }
    fn poll(&mut self, limit: usize) -> Vec<DialCompletion<TcpStream>> {
        let mut results = Vec::new();
        for _ in 0..limit.min(self.limits.requests) {
            let mut event = match self.events.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    if self.pending.is_empty() {
                        break;
                    }
                    self.failed = true;
                    self.close();
                    let Some((_, pending)) = self.pending.first_key_value() else {
                        break;
                    };
                    DialCompletion {
                        connection: pending.connection,
                        result: Err(DialError::WorkerFailed),
                    }
                }
            };
            // Events are constructed solely by our scoped private worker.
            let pending = self.pending.remove(&event.connection.peer.node).unwrap();
            debug_assert_eq!(pending.connection, event.connection);
            if pending.cancelled.load(Ordering::Acquire) && !self.failed {
                // Cancellation can race a success already in the receipt queue.
                event.result = Err(DialError::Cancelled);
            }
            results.push(event);
        }
        results
    }
    fn close(&mut self) {
        for pending in self.pending.values() {
            pending.cancelled.store(true, Ordering::Release);
        }
        self.sender.take();
    }
}
impl Drop for NativeTcpDialer {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{native::worker::ThreadWake, secure::PeerIdentity};
    use std::time::Instant;

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
    fn request(peer: u64) -> DialRequest<SocketAddr> {
        DialRequest {
            connection: ticket(peer, 1),
            endpoint: "127.0.0.1:1".parse().unwrap(),
            timeout_ms: 10,
        }
    }
    fn worker<F>(connect: F) -> NativeTcpDialer
    where
        F: FnMut(SocketAddr, Duration) -> io::Result<TcpStream> + Send + 'static,
    {
        NativeTcpDialer::spawn_with(
            ticket(2, 1).local,
            (2..=3)
                .map(|id| (ticket(id, 1).peer.node, ticket(id, 1).peer.store))
                .collect(),
            DialLimits {
                requests: 2,
                connect_timeout_ms: 10,
            },
            Arc::new(ThreadWake::current()),
            connect,
        )
        .unwrap()
    }
    fn drain(worker: &mut NativeTcpDialer) -> Vec<DialCompletion<TcpStream>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut events = Vec::new();
        while !worker.is_drained() {
            events.extend(worker.poll(1));
            assert!(Instant::now() < deadline);
            thread::park_timeout(Duration::from_millis(1));
        }
        events
    }
    fn finish(worker: &mut NativeTcpDialer) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !worker.try_finish().unwrap() {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
    }
    #[test]
    fn cancellation_retains_active_and_queued_slots_until_actual_worker_receipts() {
        let (started, start) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let mut worker = worker(move |_, timeout| {
            count.fetch_add(1, Ordering::SeqCst);
            assert_eq!(timeout, Duration::from_millis(10));
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
            Err(io::ErrorKind::TimedOut.into())
        });
        worker.submit(request(2)).unwrap();
        start.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.submit(request(3)).unwrap();
        assert!(!worker.cancel(ticket(2, 2)));
        assert!(worker.cancel(ticket(2, 1)));
        assert!(worker.cancel(ticket(3, 1)));
        assert_eq!(worker.outstanding(), 2);
        assert!(worker.poll(0).is_empty());
        assert!(worker.poll(2).is_empty());
        worker.close();
        assert!(!worker.try_finish().unwrap());
        release.send(()).unwrap();
        let events = drain(&mut worker);
        assert_eq!(events.len(), 2);
        assert!(events
            .iter()
            .all(|event| matches!(event.result, Err(DialError::Cancelled))));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "queued cancelled dial must never touch network"
        );
        assert!(!worker.cancel(ticket(2, 1)));
        finish(&mut worker);
        assert!(worker.try_finish().unwrap());
    }
    #[test]
    fn panicked_worker_reports_every_accepted_ticket_once_and_fences_admission() {
        let (started, start) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let mut worker = worker(move |_, _| {
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
            panic!("injected dial failure");
        });
        worker.submit(request(2)).unwrap();
        start.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.submit(request(3)).unwrap();
        release.send(()).unwrap();
        let events = drain(&mut worker);
        assert_eq!(events.len(), 2);
        assert_ne!(events[0].connection, events[1].connection);
        assert!(events
            .iter()
            .all(|event| matches!(event.result, Err(DialError::WorkerFailed))));
        assert!(worker.poll(usize::MAX).is_empty());
        assert_eq!(
            worker.submit(request(2)).unwrap_err().reason,
            DialError::WorkerFailed
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while !worker.thread.as_ref().unwrap().is_finished() {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        assert_eq!(worker.try_finish(), Err(DialError::WorkerFailed));
    }
    #[test]
    fn dropping_owner_cancels_queue_and_stops_worker_after_active_call_returns() {
        let (started, start) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = calls.clone();
        let mut worker = worker(move |_, _| {
            count.fetch_add(1, Ordering::SeqCst);
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(5)).unwrap();
            Err(io::ErrorKind::TimedOut.into())
        });
        worker.submit(request(2)).unwrap();
        start.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.submit(request(3)).unwrap();
        // Retain the join handle only for observing resource lifetime in this test.
        let thread = worker.thread.take().unwrap();
        drop(worker);
        release.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
