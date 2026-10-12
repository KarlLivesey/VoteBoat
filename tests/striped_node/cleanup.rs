// SPDX-License-Identifier: RPL-1.5
use super::construction::Boat;
use std::time::{Duration, Instant};
use voteboat::{identity::GroupIdentity, log::LogStore};
pub fn join(node: Boat, group: GroupIdentity) {
    let mut parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("original striped Node not drained"));
    let mut dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("original peer connector not drained"));
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    let (mut wal, mut snapshot) = (false, false);
    loop {
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !wal {
            if let Some(store) = parts.local.persistence.try_reclaim().unwrap() {
                let state = store.state(group).unwrap();
                let core = parts.local.owner.core(group).unwrap().state();
                assert_eq!(
                    state, *core,
                    "worker reclaimed exact original authoritative prefix"
                );
                assert!(state.commit_index >= state.base_index());
                // Type is StripedLogStore<FileStripedIo>; dropping this exact store releases all lane locks.
                drop(store);
                wal = true;
            }
        }
        if !snapshot {
            snapshot = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if dial && wal && snapshot {
            return;
        }
        assert!(
            Instant::now() < end,
            "striped worker/snapshot/dial joins bounded"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
