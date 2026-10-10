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
            next: 1,
            retry_at: MonoTime(0),
            now: MonoTime(0),
            wanted: false,
            closed: false,
            joined: false,
        })
    }
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
    next: u64,
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
    fn time(&mut self, now: MonoTime) -> Result<(), DiscoveryError> {
        if now < self.now {
            return Err(DiscoveryError::TimeWentBack);
        }
        self.now = now;
        Ok(())
    }
    fn begin(&mut self) {
        let Some(generation) = SecureSessionGeneration::new(self.next) else {
            self.wanted = false;
            return;
        };
        self.next = self.next.checked_add(1).unwrap_or(0);
        let Some(deadline) = self.now.0.checked_add(5000).map(MonoTime) else {
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
        if result.is_err() {
            self.wanted = self.next != 0;
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
