// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::setup::Service;
use std::fmt::Write;
use voteboat::{outbound::*, runtime::PeerDriverUsage, transport::PeerRosterUsage};

const DETAILS: usize = 8;
struct Peer {
    node: u64,
    bound: bool,
    usage: OutboundUsage,
}
struct Snapshot {
    driver: Option<(PeerDriverUsage, PeerRosterUsage)>,
    authorized: usize,
    bound: usize,
    tracked: usize,
    outbound: OutboundUsage,
    peers: Vec<Peer>,
}
impl Snapshot {
    fn capture(service: &Service) -> Self {
        let mut value = Self {
            driver: None,
            authorized: 0,
            bound: 0,
            tracked: 0,
            outbound: service.local().outbound.usage(),
            peers: Vec::new(),
        };
        if let Some(driver) = service.peers() {
            let roster = driver.roster();
            value.driver = Some((driver.usage(), roster.usage()));
            value.authorized = roster.authorized_peers().count();
            value.bound = roster
                .authorized_peers()
                .filter(|peer| roster.binding(*peer).is_some())
                .count();
            value.tracked = roster.tracked_peers().count();
            value.peers = roster
                .tracked_peers()
                .take(DETAILS)
                .map(|peer| Peer {
                    node: peer.get(),
                    bound: roster.binding(peer).is_some(),
                    usage: service.local().outbound.peer_usage(peer),
                })
                .collect();
        }
        value
    }
    fn append(&self, reply: &mut String) {
        let (d, r) = self.driver.unwrap_or_default();
        write!(reply, " peer_driver={} peer_attempts={} peer_staged_batches={} peer_quarantined_batches={} peer_send_failed={} peer_receive_failed={} peer_connections={} peer_connecting={} peer_reserved_bytes={} peer_authorized={} peer_bound={} peer_tracked={} outbound_batches={} outbound_messages={} outbound_bytes={}",
            u8::from(self.driver.is_some()), d.attempts, d.staged_batches, d.quarantined_batches,
            u8::from(d.failed_send), u8::from(d.failed_receive), r.connections, r.connecting,
            r.reserved_bytes, self.authorized, self.bound, self.tracked,
            self.outbound.batches, self.outbound.messages, self.outbound.bytes).unwrap();
        let count = self.peers.len().min(DETAILS);
        write!(
            reply,
            " peer_details={} peer_omitted={}",
            count,
            self.tracked.saturating_sub(count)
        )
        .unwrap();
        for p in self.peers.iter().take(DETAILS) {
            write!(
                reply,
                " peer_{}_bound={} peer_{}_batches={} peer_{}_messages={} peer_{}_bytes={}",
                p.node,
                u8::from(p.bound),
                p.node,
                p.usage.batches,
                p.node,
                p.usage.messages,
                p.node,
                p.usage.bytes
            )
            .unwrap();
        }
    }
}
pub fn append(service: &Service, reply: &mut String) {
    Snapshot::capture(service).append(reply);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maximum_peer_observations_fit_reply_and_expose_omitted_details() {
        let usage = OutboundUsage {
            batches: usize::MAX,
            messages: usize::MAX,
            bytes: usize::MAX,
        };
        let snapshot = Snapshot {
            driver: Some((
                PeerDriverUsage {
                    attempts: usize::MAX,
                    staged_batches: usize::MAX,
                    quarantined_batches: usize::MAX,
                    failed_send: true,
                    failed_receive: true,
                },
                PeerRosterUsage {
                    connections: usize::MAX,
                    connecting: usize::MAX,
                    reserved_bytes: usize::MAX,
                },
            )),
            authorized: usize::MAX,
            bound: usize::MAX,
            tracked: 16,
            outbound: usage,
            peers: (0..16)
                .map(|i| Peer {
                    node: u64::MAX - i,
                    bound: true,
                    usage,
                })
                .collect(),
        };
        // Reserve a full KiB for the existing counter fields, even at maxima.
        let mut reply = "x".repeat(1024);
        snapshot.append(&mut reply);
        assert!(reply.len() < 4096, "reply length={}", reply.len());
        assert!(reply.contains("peer_details=8 peer_omitted=8"));
        assert!(reply.contains(&format!("peer_{}_bytes={}", u64::MAX - 7, usize::MAX)));
        assert!(!reply.contains(&format!("peer_{}_bound=", u64::MAX - 8)));
        for field in reply.split_whitespace().skip(1) {
            let (_, value) = field.split_once('=').unwrap();
            assert!(value.parse::<u64>().is_ok(), "{field}");
        }
    }
}
