// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real orderly hop retirement then helper restart reconsumes original requests.
use super::{
    channels::*,
    cluster::*,
    history::{finish, write},
    observed_wire::*,
    support::*,
};
use voteboat::{
    identity::StoreSession,
    native::{connect::NativePeerProtocol, relay::*},
    outbound::SendTicket,
    runtime::*,
    secure::*,
};
type FreshChannels = (
    Vec<RelayHop<Box<dyn SecureSession>>>,
    RelayForwarder<Gated, ObservedWire>,
    LocalIdentity,
);
fn channels(c: &Cluster) -> FreshChannels {
    let helper = LocalIdentity {
        store: voteboat::identity::StoreBinding {
            session: StoreSession::new(901).unwrap(),
            ..c.helper_identity.store
        },
        ..c.helper_identity
    };
    let stores = (1..=3).map(|n| (node(n), identity(n as u128))).collect();
    let plan = RelayHelperPlan::new(helper, stores, 8).unwrap();
    let mut voters = vec![];
    let mut hops = vec![];
    for (slot, b) in c.boats.iter().enumerate() {
        let local = LocalIdentity {
            node: node(slot as u64 + 1),
            store: b.local().owner.identity().store,
        };
        let p = NativeRelayPlan::new(
            local,
            PeerIdentity {
                node: node(9),
                store: identity(9),
            },
            (1..=3).map(|n| (node(n), identity(n as u128))).collect(),
            8,
        )
        .unwrap();
        let (a, b, _) = pair(c.protocol, local, helper, 2, c.clock);
        voters.push(
            RelayHop::for_voter(a, p, Default::default())
                .unwrap_or_else(|e| panic!("fresh hop {:?}", e.reason)),
        );
        hops.push(
            RelayHop::for_helper(b, plan.clone(), Default::default())
                .unwrap_or_else(|e| panic!("fresh helper {:?}", e.reason)),
        );
    }
    let f = RelayForwarder::build(RelayForwarderParts {
        plan,
        codec: ObservedWire::new(9, c.observations.clone()),
        limits: Default::default(),
        hops,
    })
    .unwrap_or_else(|e| panic!("fresh helper {:?}", e.reason));
    (voters, f, helper)
}
fn replace(c: &mut Cluster, mut channels: Vec<RelayHop<Box<dyn SecureSession>>>) {
    let wrong = channels.remove(1);
    let binding = wrong.binding();
    let now = c.now();
    let rejected = c.boats[0]
        .replace_peer_channel(wrong, now)
        .expect_err("foreign source refused");
    assert_eq!(rejected.0, NodeError::Peer(PeerDriverError::WrongBinding));
    assert_eq!(rejected.1.authenticated_binding(), Some(binding));
    channels.insert(1, *rejected.1);
    let mut slots = channels.into_iter().map(Some).collect::<Vec<_>>();
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while slots.iter().any(Option::is_some) {
        c.poll();
        let now = c.now();
        for (i, slot) in slots.iter_mut().enumerate() {
            let Some(channel) = slot.take() else {
                continue;
            };
            match c.boats[i].replace_peer_channel(channel, now) {
                Ok(old) => {
                    assert!(old.is_drained());
                    assert_ne!(
                        old.binding().generation,
                        voteboat::identity::SecureSessionGeneration::new(2).unwrap()
                    );
                }
                Err((NodeError::Peer(PeerDriverError::NotQuiescent), original)) => {
                    *slot = Some(*original);
                }
                Err((e, _)) => panic!("replacement refused {:?}", e),
            }
        }
        assert!(
            std::time::Instant::now() < end,
            "actual old IO did not settle"
        );
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
}
fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new(protocol);
    c.campaign();
    write(&mut c, 31, 3);
    c.until(|c| c.value(2) == 3);
    c.held_read_acks.extend([node(2), node(3)]);
    let request = c.boats[0].read(group(352), ()).unwrap();
    c.until(|c| c.held_inputs.len() == 2);
    let context = c
        .observations
        .borrow()
        .iter()
        .find(|r| r.at == 1 && r.kind == Kind::Probe && r.encoded)
        .unwrap()
        .context;
    let before: Vec<SendTicket> = c
        .forwarded_probes
        .iter()
        .filter(|(_, ctx)| *ctx == context)
        .map(|(t, _)| *t)
        .collect();
    assert_eq!(before.len(), 2);
    assert!(c.boats[0].poll_read().is_none());
    c.held_read_acks.clear();
    c.held_inputs.clear();
    c.retiring = true;
    let now = c.now();
    for b in &mut c.boats {
        b.retire_peer_channel(now).unwrap();
        b.retire_peer_channel(now).unwrap();
    }
    let (channels, new_helper, helper_identity) = channels(&c);
    replace(&mut c, channels);
    c.helper.abort();
    let now = c.now();
    c.helper.poll(now, 3, Default::default()).unwrap();
    assert!(c.helper.is_drained());
    let old = std::mem::replace(&mut c.helper, new_helper);
    old.into_parts()
        .unwrap_or_else(|_| panic!("old helper IO not reclaimed"));
    c.helper_identity = helper_identity;
    c.retiring = false;
    let prior = c.forwarded_probes.len();
    let end = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        c.poll();
        if let Some(reply) = c.boats[0].poll_read() {
            assert_eq!(reply.ticket(), request);
            let ReadOutcome::Read { result, .. } = c.boats[0].complete_read(reply).unwrap() else {
                panic!("original read lost");
            };
            assert_eq!(result, Ok(3));
            break;
        }
        assert!(
            std::time::Instant::now() < end,
            "retained original request not reconsumed"
        );
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    c.until(|c| {
        c.forwarded_probes[prior..]
            .iter()
            .any(|(ticket, ctx)| *ctx == context && before.contains(ticket))
    });
    assert!(c.boats.iter().all(|b| b.state() == NodeState::Running));
    std::fs::remove_dir_all(finish(c)).unwrap();
}
#[test]
fn tcp_actual_helper_restart_replays_same_original_and_refuses_foreign_channel_whole() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_actual_helper_restart_replays_same_original_and_refuses_foreign_channel_whole() {
    history(NativePeerProtocol::Quic);
}
