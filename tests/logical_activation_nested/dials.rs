// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Child connectors that report their dial work to the test's clock.
//!
//! A bridge borrows its child nodes exclusively and exposes only their local
//! parts, so a fixture round cannot read a borrowed node's connector. Each
//! child connector is therefore wrapped here: after every call that can
//! change it, it reports its attempts still waiting for the dial worker to
//! the clock's [`DialWork`]. An attempt enters and leaves that stage only
//! inside those calls, so the reported count is exact between calls, and
//! protocol time holds while any child dial anywhere in the test is pending.
use super::harness::DialWork;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use voteboat::{
    connect::{
        ConnectCompletion, ConnectDirection, ConnectError, ConnectLimits, ConnectPollBudget,
        ConnectRejected, ConnectRequest, ConnectUsage, PeerConnector,
    },
    logical_voter::LogicalApplication,
    native::{connect::NativeServiceConnector, dial::NativeTcpDialer, node::NativeNodeParts},
    runtime::{MonoTime, PeerParts},
    secure::{LocalIdentity, PeerIdentity},
    transport::ConnectTicket,
};
type Session = <NativeServiceConnector as PeerConnector>::Session;

/// One cluster's dial outcomes, and a test-only switch that withholds its
/// connectors' polls. Withholding leaves a submitted dial pending with its
/// worker's completion not yet collected, which is how a dial worker the OS
/// has not scheduled yet looks to the node. Off by default.
#[derive(Clone, Default)]
pub struct DialControl(Arc<Control>);
#[derive(Default)]
struct Control {
    withheld: AtomicBool,
    submitted: AtomicUsize,
    connected: AtomicUsize,
    lost: AtomicUsize,
}
impl DialControl {
    pub fn withhold(&self, withheld: bool) {
        self.0.withheld.store(withheld, Ordering::Release);
    }
    /// Dial attempts submitted, completed with a session, and ended without.
    pub fn outcomes(&self) -> (usize, usize, usize) {
        (
            self.0.submitted.load(Ordering::Acquire),
            self.0.connected.load(Ordering::Acquire),
            self.0.lost.load(Ordering::Acquire),
        )
    }
}

/// A child node's own connector, unchanged except that it reports.
pub struct Probed {
    inner: NativeServiceConnector,
    report: Report,
    control: DialControl,
    // Submitted dial attempts whose completion this connector has not returned.
    dials: Vec<ConnectTicket>,
}
/// This connector's contribution to the clock's [`DialWork`], withdrawn when
/// it is dropped.
struct Report {
    work: DialWork,
    dialing: usize,
}
impl Drop for Report {
    fn drop(&mut self) {
        self.work.report(self.dialing, 0);
    }
}

/// Wrap the connector of selected native parts before the node is built.
pub fn probe(
    parts: NativeNodeParts<LogicalApplication, NativeServiceConnector>,
    work: DialWork,
    control: DialControl,
) -> NativeNodeParts<LogicalApplication, Probed> {
    NativeNodeParts {
        local: parts.local,
        peers: parts.peers.map(|peers| PeerParts {
            connector: Probed::new(peers.connector, work, control),
            factory: peers.factory,
            roster: peers.roster,
            ingress: peers.ingress,
            routes: peers.routes,
            admission_routes: peers.admission_routes,
        }),
    }
}

impl Probed {
    fn new(inner: NativeServiceConnector, work: DialWork, control: DialControl) -> Self {
        let mut probed = Self {
            inner,
            report: Report { work, dialing: 0 },
            control,
            dials: Vec::new(),
        };
        probed.refresh();
        probed
    }
    fn refresh(&mut self) {
        let dialing = self.inner.usage().dialing;
        self.report.work.report(self.report.dialing, dialing);
        self.report.dialing = dialing;
    }
    /// The drained connector's dial worker, as
    /// [`NativeServiceConnector::into_dialer`].
    pub fn into_dialer(self) -> Result<Option<NativeTcpDialer>, Box<NativeServiceConnector>> {
        let Self { inner, report, .. } = self;
        let dialer = inner.into_dialer()?;
        drop(report);
        Ok(dialer)
    }
}

impl PeerConnector for Probed {
    type Endpoint = std::net::SocketAddr;
    type Session = Session;
    fn local(&self) -> LocalIdentity {
        self.inner.local()
    }
    fn limits(&self) -> ConnectLimits {
        self.inner.limits()
    }
    fn usage(&self) -> ConnectUsage {
        self.inner.usage()
    }
    #[cfg(unix)]
    fn readiness_fds(&self, out: &mut Vec<voteboat::worker::ReadinessFd>) {
        self.inner.readiness_fds(out);
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.inner.next_deadline()
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        self.inner.supports_peer(peer)
    }
    fn submit(
        &mut self,
        request: ConnectRequest<Self::Endpoint>,
        now: MonoTime,
    ) -> Result<(), ConnectRejected<Self::Endpoint>> {
        let (ticket, dial) = (
            request.ticket,
            matches!(request.direction, ConnectDirection::Dial(_)),
        );
        let result = self.inner.submit(request, now);
        if result.is_ok() && dial {
            self.dials.push(ticket);
            self.control.0.submitted.fetch_add(1, Ordering::AcqRel);
        }
        self.refresh();
        result
    }
    fn cancel(&mut self, ticket: ConnectTicket) -> bool {
        let cancelled = self.inner.cancel(ticket);
        self.refresh();
        cancelled
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        if self.control.0.withheld.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        let result = self.inner.poll(now, budget);
        for done in result.iter().flatten() {
            if let Some(i) = self.dials.iter().position(|t| *t == done.ticket) {
                self.dials.swap_remove(i);
                let outcome = match done.result {
                    Ok(_) => &self.control.0.connected,
                    Err(_) => &self.control.0.lost,
                };
                outcome.fetch_add(1, Ordering::AcqRel);
            }
        }
        self.refresh();
        result
    }
    fn close(&mut self) {
        self.inner.close();
        self.refresh();
    }
    fn is_drained(&self) -> bool {
        self.inner.is_drained()
    }
}
