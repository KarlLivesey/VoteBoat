// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Listener and connection-generation health, appended to the services'
//! `metrics`/`status` replies. Observability only; grants no authority.
use std::fmt::Write;
use voteboat::{
    connect::{AcceptStatus, PeerConnector},
    runtime::PeerDriver,
    transport::PeerTransportFactory,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct AcceptHealth {
    /// `None` without a TCP listener (QUIC) or without a peer driver.
    pub status: Option<AcceptStatus>,
    /// The driver's definition, at its last poll time.
    pub paused: bool,
    pub generations_exhausted: bool,
}
impl AcceptHealth {
    pub fn capture<C: PeerConnector, F: PeerTransportFactory<C::Session>>(
        driver: Option<&PeerDriver<C, F>>,
    ) -> Self {
        driver.map_or_else(Self::default, |d| Self {
            status: d.connector().accept_status(),
            paused: d.accept_paused(),
            generations_exhausted: d.generations_exhausted(),
        })
    }
    pub fn append(&self, reply: &mut String) {
        let a = self.status.unwrap_or_default();
        write!(reply, " peer_accept_listener={} peer_accept_errors={} peer_accept_backoffs={} peer_accept_paused={} peer_accept_last_os_error={} peer_generations_exhausted={}",
            u8::from(self.status.is_some()), a.transient_errors, a.backoffs,
            u8::from(self.paused), a.last_os_error.unwrap_or(0),
            u8::from(self.generations_exhausted)).unwrap();
    }
}
