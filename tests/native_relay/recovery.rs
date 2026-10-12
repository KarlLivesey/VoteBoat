// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual native checkpoint/WAL reopen preserves application IDs and fresh scopes.
use super::{
    cluster::*,
    history::{finish, read, write},
    support::*,
};
use voteboat::{
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    runtime::*,
};
fn history(protocol: NativePeerProtocol) {
    let mut c = Cluster::new(protocol);
    c.campaign();
    write(&mut c, 35221, 3);
    write(&mut c, 35222, 4);
    c.until(|c| (0..3).all(|i| c.value(i) == 7));
    for b in &mut c.boats {
        b.control(group(352), NodeControl::Checkpoint).unwrap();
    }
    c.until(|c| {
        c.boats.iter().all(|b| {
            b.local()
                .owner
                .core(group(352))
                .unwrap()
                .state()
                .snapshot
                .is_some()
        })
    });
    let old = c
        .boats
        .iter()
        .map(|b| b.local().owner.identity().store)
        .collect::<Vec<_>>();
    let old_helper = c.helper_identity;
    let selected = c.protocol;
    assert!(
        !c.root.join("9").exists(),
        "helper owns no WAL/application directory"
    );
    let root = finish(c);
    let mut c = Cluster::open(root, selected, NativeOpenMode::Recover);
    assert_ne!(
        old_helper.store, c.helper_identity.store,
        "fresh helper transport incarnation"
    );
    for (i, b) in c.boats.iter().enumerate() {
        assert_eq!(old[i].identity, b.local().owner.identity().store.identity);
        assert_ne!(old[i].session, b.local().owner.identity().store.session);
        assert_eq!(c.value(i), 7);
    }
    c.campaign();
    let retry = write(&mut c, 35222, 4);
    assert!(retry.duplicate);
    assert_eq!(read(&mut c), 7);
    write(&mut c, 35223, 5);
    assert_eq!(read(&mut c), 12);
    c.until(|c| (0..3).all(|i| c.value(i) == 12));
    std::fs::remove_dir_all(finish(c)).unwrap();
}
#[test]
fn tcp_relay_native_checkpoint_and_wal_reopen_preserve_dedup_and_incarnations() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_relay_native_checkpoint_and_wal_reopen_preserve_dedup_and_incarnations() {
    history(NativePeerProtocol::Quic);
}
