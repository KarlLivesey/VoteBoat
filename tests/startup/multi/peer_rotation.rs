// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::peer_rotation::{generation, material, record};
use voteboat::{credential_reload::*, native::credential_journal::*};

fn config_at(h: &History, n: usize, changed: bool) -> NativeMultiStartup {
    let mut result = selected(&h.root, &h.addresses, n, NativeOpenMode::Recover);
    if changed {
        let key = if n == 1 { 1 } else { 5 - n };
        result.startup.tls = selected(&h.root, &h.addresses, key, NativeOpenMode::Recover)
            .startup
            .tls;
        for (node, peer) in &mut result.startup.peers {
            let key = if node.get() == 1 {
                1
            } else {
                5 - node.get() as usize
            };
            let source = selected(&h.root, &h.addresses, key, NativeOpenMode::Recover);
            // The selected key's own certificate appears in any other peer map.
            let other = selected(
                &h.root,
                &h.addresses,
                if key == 1 { 2 } else { 1 },
                NativeOpenMode::Recover,
            );
            peer.certificate = other.startup.peers[&source.startup.node]
                .certificate
                .clone();
            peer.server_name = format!("node{key}.voteboat.test");
        }
    }
    result
}
fn journal(h: &History, n: usize) -> NativeCredentialJournal {
    let config = config_at(h, n, false);
    NativeCredentialJournal::new(
        FileCredentialRecord::new(h.root.join(n.to_string()).join("PEER-CREDENTIAL-RELOAD")),
        voteboat::secure::PeerIdentity {
            node: config.startup.node,
            store: config.startup.store,
        },
    )
    .unwrap_or_else(|(e, _)| panic!("journal: {e:?}"))
}
fn reopen(h: &mut History, next: u64, changed: bool) {
    h.clock = Instant::now();
    h.nodes = (1..=3)
        .map(|n| {
            let config = config_at(h, n, changed);
            let rotation = NativePeerRotationStartup::from_journal(
                h.protocol,
                generation(next),
                &journal(h, n),
            )
            .unwrap();
            config
                .open_with_peer_rotation(
                    rotation,
                    timers(),
                    apps(&groups()),
                    Arc::new(ThreadWake::current()),
                    MonoTime(0),
                )
                .unwrap()
        })
        .collect();
    assert!(h
        .nodes
        .iter()
        .all(|n| n.peers().unwrap().credential_generation() == Some(generation(next))));
}
fn prepare(h: &History, next: u64, changed: bool) {
    for n in 1..=3 {
        let config = config_at(h, n, changed);
        let record = record(&config.startup, &config.provisioned_stores, next);
        let mut journal = journal(h, n);
        journal.publish(record).unwrap();
        journal.publish(record).unwrap();
        assert_eq!(journal.latest().unwrap(), Some(record));
    }
}
fn publish(h: &mut History, next: u64, changed: bool, count: usize) {
    for n in 1..=count {
        let config = config_at(h, n, changed);
        let expected = material(&config.startup, &config.provisioned_stores);
        let previous = h.nodes[n - 1]
            .replace_peer_credentials(generation(next - 1), generation(next), expected)
            .ok()
            .unwrap();
        assert_ne!(
            previous.digest(),
            journal(h, n).latest().unwrap().unwrap().digest
        );
    }
}
fn reject_stale(h: &History) {
    let config = config_at(h, 1, false);
    let record = journal(h, 1).latest().unwrap().unwrap();
    let mut error = config
        .open_with_peer_rotation(
            NativePeerRotationStartup {
                protocol: h.protocol,
                generation: generation(2),
                latest: Some(record),
            },
            timers(),
            apps(&groups()),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .err()
        .unwrap();
    assert_eq!(error.reason.stage, "peer credentials");
    assert!(error.try_cleanup().unwrap());
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut h = History::new(protocol);
    h.close();
    reopen(&mut h, 1, false);
    for b in groups() {
        h.write(b.group, 11, 7, 7, false);
    }
    prepare(&h, 2, true);
    publish(&mut h, 2, true, 3);
    for b in groups() {
        h.write(b.group, 11, 7, 7, true);
        h.write(b.group, 12, 3, 10, false);
    }
    if checkpoint {
        h.checkpoint();
    }
    h.close();
    reject_stale(&h);
    reopen(&mut h, 2, true);
    for b in groups() {
        h.write(b.group, 12, 3, 10, true);
    }
    // The last node never publishes in memory: its durable original request
    // must still recover, exactly like a lost preparation reply/process exit.
    prepare(&h, 3, false);
    publish(&mut h, 3, false, 2);
    h.close();
    reopen(&mut h, 3, false);
    for b in groups() {
        h.write(b.group, 11, 7, 7, true);
        h.write(b.group, 13, 5, 15, false);
    }
    h.close();
    std::fs::remove_dir_all(&h.root).unwrap();
}
#[test]
fn tcp_peer_rotation_and_unobserved_durable_rollout_recover_all_groups() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_peer_rotation_checkpoint_and_unobserved_rollout_recover_all_groups() {
    history(NativePeerProtocol::Quic, true);
}
