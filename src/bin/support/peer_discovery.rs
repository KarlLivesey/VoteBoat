// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! One bounded authenticated discovery-source connection, driven by Node polling.
use super::{
    command_endpoints::{self, Endpoint},
    service_access::{self, ClientAccess},
    setup::{checked, Failure},
};
use std::{path::Path, sync::Arc};
use voteboat::{
    dial::*,
    discovery::*,
    identity::*,
    native::{dial::NativeTcpDialer, remote_discovery::*, worker::ThreadWake},
    runtime::MonoTime,
    secure::*,
    transport::ConnectTicket,
};
#[path = "peer_discovery/assembly.rs"]
mod assembly;
#[path = "peer_discovery/upgrade.rs"]
mod upgrade;
pub(crate) use assembly::open;

pub struct Prepared {
    endpoint: Endpoint,
    access: ClientAccess,
}
impl Prepared {
    pub fn load(path: &Path) -> Result<Self, Failure> {
        let data = super::service_setup::material(path, 4096)?;
        let text = std::str::from_utf8(&data)?;
        let lines = text.lines().collect::<Vec<_>>();
        let ["voteboat-peer-discovery-client-v1", source, principal, tls] = lines.as_slice() else {
            return Err("expected peer discovery header, source, principal and tls lines".into());
        };
        let source = source
            .strip_prefix("source ")
            .ok_or("missing discovery source")?;
        let endpoint =
            command_endpoints::parse(&format!("voteboat-command-peers-v1\n{source}\n"))?.remove(0);
        let principal = principal
            .strip_prefix("principal ")
            .ok_or("missing discovery principal")?
            .parse()?;
        let directory = Path::new(
            tls.strip_prefix("tls ")
                .ok_or("missing discovery TLS directory")?,
        );
        let directory = if directory.is_absolute() {
            directory.to_owned()
        } else {
            path.parent().unwrap_or(Path::new(".")).join(directory)
        };
        let access = ClientAccess::load(&directory, principal, std::slice::from_ref(&endpoint))?;
        Ok(Self { endpoint, access })
    }
    fn start(mut self, session: StoreSession) -> Result<Source, Failure> {
        self.access.local.store.session = session;
        let peer = service_access::transport_identity(self.endpoint.node, false);
        let dialer = checked(NativeTcpDialer::spawn(
            self.access.local,
            [(peer.node, peer.store)].into(),
            DialLimits {
                requests: 1,
                connect_timeout_ms: 1000,
            },
            Arc::new(ThreadWake::current()),
        ))?;
        Ok(Source {
            prepared: self,
            dialer,
            remote: None,
            attempt: None,
            next: SecureSessionGeneration::new(1),
            retry_at: MonoTime(0),
            now: MonoTime(0),
            wanted: false,
            closed: false,
            joined: false,
        })
    }
}
/// End-to-end bound of one source connection attempt.
const ATTEMPT_MS: u64 = 5000;
/// Take the next source connection generation and attempt deadline. A spent
/// generation space, or a deadline past `u64::MAX` ms, takes nothing: the
/// source then refuses lookups with the typed `DiscoveryError::Exhausted`.
fn take_attempt(
    next: &mut Option<SecureSessionGeneration>,
    now: MonoTime,
) -> Option<(SecureSessionGeneration, MonoTime)> {
    let generation = (*next)?;
    let deadline = MonoTime(now.0.checked_add(ATTEMPT_MS)?);
    *next = generation
        .get()
        .checked_add(1)
        .and_then(SecureSessionGeneration::new);
    Some((generation, deadline))
}
struct Attempt {
    ticket: ConnectTicket,
    deadline: MonoTime,
    phase: upgrade::Phase,
}
pub struct Source {
    prepared: Prepared,
    dialer: NativeTcpDialer,
    remote: Option<NativeRemotePeerDiscovery<Box<dyn SecureSession>>>,
    attempt: Option<Attempt>,
    /// Next source connection generation; `None` once `u64::MAX` was used.
    /// After an attach and a failure there is no retry delay, so the floor is
    /// one TCP+TLS handshake per attempt: at 10^4 handshakes/s, 2^64
    /// attempts take about 5.8e7 years.
    next: Option<SecureSessionGeneration>,
    retry_at: MonoTime,
    now: MonoTime,
    wanted: bool,
    closed: bool,
    joined: bool,
}
impl Source {
    fn source_failed(&self) -> bool {
        self.remote.as_ref().is_none_or(|r| r.source_failed())
    }
    /// No further source connection can be started. Never node-fatal:
    /// lookups that need a connection fail with `Exhausted`, per attempt.
    fn exhausted(&self) -> bool {
        self.next.is_none() || self.now.0.checked_add(ATTEMPT_MS).is_none()
    }
    fn time(&mut self, now: MonoTime) -> Result<(), DiscoveryError> {
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack);
        }
        self.now = now;
        Ok(())
    }
    fn begin(&mut self) {
        let Some((generation, deadline)) = take_attempt(&mut self.next, self.now) else {
            self.wanted = false;
            return;
        };
        let ticket = ConnectTicket {
            local: self.prepared.access.local,
            peer: service_access::transport_identity(self.prepared.endpoint.node, false),
            generation,
        };
        match self.dialer.submit(DialRequest {
            connection: ticket,
            endpoint: self.prepared.endpoint.address,
            timeout_ms: 1000,
        }) {
            Ok(()) => {
                self.attempt = Some(Attempt {
                    ticket,
                    deadline,
                    phase: upgrade::Phase::Dialing,
                })
            }
            Err(_) => self.retry_at = MonoTime(self.now.0.saturating_add(100)),
        }
    }
    fn progress(&mut self, budget: SessionPollBudget) -> Result<(), DiscoveryError> {
        let mut attempt = self.attempt.take().expect("active source attempt");
        if self.now >= attempt.deadline || self.closed {
            self.dialer.cancel(attempt.ticket);
        }
        let result = upgrade::poll(
            &mut attempt,
            &mut self.dialer,
            &self.prepared,
            self.now,
            budget,
            self.closed,
        );
        match result {
            Ok(Some(session)) => {
                if let Some(remote) = &mut self.remote {
                    match remote.attach_session(session) {
                        Ok(old) => {
                            if let Some(mut old) = old {
                                old.close();
                            }
                        }
                        Err((_, mut returned)) => {
                            returned.close();
                            self.retry_at = MonoTime(self.now.0.saturating_add(100));
                        }
                    }
                } else {
                    self.remote = Some(
                        NativeRemotePeerDiscovery::new(
                            session,
                            service_access::transport_identity(self.prepared.endpoint.node, false),
                            RemoteDiscoveryConfig {
                                cached_peers: 1024,
                                ..Default::default()
                            },
                            self.now,
                        )
                        .map_err(|(_, mut s)| {
                            s.close();
                            DiscoveryError::WrongBinding
                        })?,
                    );
                }
            }
            Ok(None) => self.attempt = Some(attempt),
            Err(_) => self.retry_at = MonoTime(self.now.0.saturating_add(100)),
        }
        Ok(())
    }
}
impl PeerDiscovery for Source {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        self.time(now)?;
        if self.closed {
            return Err(DiscoveryError::Closed);
        }
        let result = self
            .remote
            .as_mut()
            .map_or(Err(DiscoveryError::Unavailable), |r| r.resolve(peer, now));
        if result.is_err() && self.source_failed() && self.exhausted() {
            // Typed and per lookup: the connector fails this attempt, the
            // roster backs off and counts it; the node stays in service.
            return Err(DiscoveryError::Exhausted);
        }
        if result.is_err() {
            self.wanted = true;
        }
        result
    }
    fn invalidate(&mut self, peer: PeerIdentity, generation: HintGeneration) -> bool {
        self.remote
            .as_mut()
            .is_some_and(|r| r.invalidate(peer, generation))
    }
    fn close(&mut self) {
        self.closed = true;
        self.wanted = false;
        self.dialer.close();
        if let Some(r) = &mut self.remote {
            r.close();
        }
        if let Some(a) = &self.attempt {
            self.dialer.cancel(a.ticket);
        }
    }
}
impl DiscoveryDriver for Source {
    fn poll_discovery(
        &mut self,
        now: MonoTime,
        budget: SessionPollBudget,
    ) -> Result<(), DiscoveryError> {
        budget
            .validate()
            .map_err(|_| DiscoveryError::InvalidLimits)?;
        self.time(now)?;
        if budget.io_calls == 0 {
            return Ok(());
        }
        if self.attempt.is_some() {
            self.progress(budget)?;
        } else if self.closed {
            if let Some(r) = &mut self.remote {
                r.poll_discovery(now, budget)?;
            }
            self.joined = self
                .dialer
                .try_finish()
                .map_err(|_| DiscoveryError::Unavailable)?;
        } else if self.source_failed() {
            if self.wanted && now >= self.retry_at {
                self.begin();
            }
        } else if let Some(r) = &mut self.remote {
            let pending = r.pending().is_some();
            r.poll_discovery(now, budget)?;
            if pending && r.pending().is_none() {
                r.disconnect_idle();
                self.wanted = false;
            }
        }
        Ok(())
    }
    fn discovery_pending(&self) -> bool {
        if self.closed {
            return !self.joined || self.remote.as_ref().is_some_and(|r| r.discovery_pending());
        }
        self.attempt.is_some()
            || (self.wanted && self.source_failed())
            || self.remote.as_ref().is_some_and(|r| r.discovery_pending())
    }
    fn discovery_deadline(&self) -> Option<MonoTime> {
        if self.closed {
            return (!self.joined).then_some(self.now);
        }
        self.attempt.as_ref().map(|a| a.deadline).or_else(|| {
            if self.source_failed() && self.wanted {
                Some(self.retry_at)
            } else {
                self.remote.as_ref().and_then(|r| r.discovery_deadline())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, time::Duration};

    #[test]
    fn source_attempts_end_at_the_generation_space_and_the_clock() {
        let mut next = SecureSessionGeneration::new(u64::MAX - 1);
        let now = MonoTime(7);
        let (first, deadline) = take_attempt(&mut next, now).unwrap();
        assert_eq!((first.get(), deadline), (u64::MAX - 1, MonoTime(5007)));
        assert_eq!(take_attempt(&mut next, now).unwrap().0.get(), u64::MAX);
        assert_eq!(next, None);
        assert_eq!(take_attempt(&mut next, now), None);
        // A deadline past `u64::MAX` takes no generation.
        let mut fresh = SecureSessionGeneration::new(1);
        assert_eq!(take_attempt(&mut fresh, MonoTime(u64::MAX - 4999)), None);
        assert_eq!(fresh.unwrap().get(), 1);
        assert!(!DiscoveryError::Exhausted.retryable());
    }

    fn source(listener: &TcpListener) -> Source {
        let endpoint = Endpoint {
            node: 1,
            address: listener.local_addr().unwrap(),
            server_name: "node1.voteboat.test".into(),
        };
        let access = ClientAccess::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"),
            1,
            std::slice::from_ref(&endpoint),
        )
        .unwrap();
        Prepared { endpoint, access }
            .start(StoreSession::new(1).unwrap())
            .unwrap()
    }

    /// Joins the dial worker. The bound only turns a deadlock into a
    /// failure; it is not a timing assertion.
    fn finish(mut source: Source, now: MonoTime) {
        source.close();
        for _ in 0..600_000 {
            source
                .poll_discovery(now, SessionPollBudget::default())
                .unwrap();
            if !source.discovery_pending() {
                return;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        panic!("hang guard: the source dial worker did not join");
    }

    /// Opus round 1: a spent source generation space returned `Exhausted`
    /// from `poll_discovery`, which the discovery connector, peer driver and
    /// node turned into node recovery. Now only the lookup is refused.
    #[test]
    fn a_spent_source_refuses_lookups_without_failing_the_node() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = service_access::transport_identity(2, false);
        let budget = SessionPollBudget::default();
        for (spent, now) in [(true, 1), (false, u64::MAX - 4999)] {
            let mut s = source(&listener);
            if spent {
                s.next = None;
            }
            let now = MonoTime(now);
            assert_eq!(s.resolve(peer, now), Err(DiscoveryError::Exhausted));
            assert!(!s.discovery_pending(), "no busy retry");
            assert_eq!(s.discovery_deadline(), None);
            s.poll_discovery(now, budget).unwrap();
            assert!(s.attempt.is_none());
            // Every later lookup is refused the same way.
            assert_eq!(s.resolve(peer, now), Err(DiscoveryError::Exhausted));
            finish(s, now);
        }
        // Control: a fresh source wants a connection and starts generation 1.
        let mut s = source(&listener);
        assert_eq!(
            s.resolve(peer, MonoTime(1)),
            Err(DiscoveryError::Unavailable)
        );
        assert!(s.discovery_pending());
        s.poll_discovery(MonoTime(1), budget).unwrap();
        let attempt = s.attempt.as_ref().unwrap();
        assert_eq!(attempt.ticket.generation.get(), 1);
        assert_eq!(attempt.deadline, MonoTime(5001));
        finish(s, MonoTime(1));
    }
}
