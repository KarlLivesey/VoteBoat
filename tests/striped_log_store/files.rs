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
use super::*;
use std::{fs, path::PathBuf};
struct Directories {
    root: PathBuf,
    lanes: Vec<PathBuf>,
}
impl Directories {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-stripe-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&root).unwrap();
        let lanes = (0..3).map(|i| root.join(format!("lane{i}"))).collect();
        Self { root, lanes }
    }
}
impl Drop for Directories {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}
#[test]
fn native_file_stripes_reclaim_recover_and_preserve_exclusive_fixed_layout() {
    let dirs = Directories::new("reopen");
    let io = FileStripedIo::create(dirs.lanes.clone()).unwrap();
    let mut store =
        StripedLogStore::create_striped(io, identity(1), LogLimits::default(), stripes()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let t = add(&mut store, 29, true);
    store.barrier(&t).unwrap();
    for _ in 0..24 {
        let s = store.state(group(1)).unwrap();
        append(
            &mut store,
            vec![update(&s, s.hard_state.term + 1, s.commit_index, None)],
        );
    }
    let expected = store.state(group(1)).unwrap();
    assert!(
        FileStripedIo::open(dirs.lanes.clone()).is_err(),
        "original selected store retains locks"
    );
    assert!(voteboat::native::log_store::FileLogIo::create(&dirs.lanes[0]).is_err());
    let reclaimed = store.reclaim(store.limits().max_wal_bytes).unwrap();
    assert!(reclaimed.after_bytes < reclaimed.before_bytes);
    let binding = store.binding();
    drop(store);
    let reopened = StripedLogStore::recover_striped(
        FileStripedIo::open(dirs.lanes.clone()).unwrap(),
        identity(1),
        LogLimits::default(),
        stripes(),
    )
    .unwrap();
    assert_eq!(reopened.state(group(1)).unwrap(), expected);
    assert!(reopened.binding().session > binding.session);
    drop(reopened);
    assert!(
        voteboat::native::log_store::FileLogIo::create(&dirs.lanes[0]).is_err(),
        "backend changes require explicit migration even without a live lock"
    );
    for lane in &dirs.lanes {
        assert!(!lane.join("log.wal").exists());
        assert!(!lane.join("MANIFEST").exists());
    }
    let mut swapped = dirs.lanes.clone();
    swapped.swap(0, 1);
    assert!(
        FileStripedIo::open(swapped).is_err(),
        "fixed lane IDs cannot be reordered"
    );
}
#[test]
fn native_lost_device_keeps_diagnostic_control_and_refuses_payload_recovery() {
    let dirs = Directories::new("lost");
    let mut store = StripedLogStore::create_striped(
        FileStripedIo::create(dirs.lanes.clone()).unwrap(),
        identity(1),
        LogLimits::default(),
        stripes(),
    )
    .unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let t = add(&mut store, 4, true);
    store.barrier(&t).unwrap();
    let expected = store.state(group(1)).unwrap();
    drop(store);
    fs::remove_dir_all(&dirs.lanes[1]).unwrap();
    assert!(FileStripedIo::open(dirs.lanes.clone()).is_err());
    let mut surviving = FileStripedIo::open_surviving(dirs.lanes.clone()).unwrap();
    let controls = StripedJournal::inspect_control(
        &mut surviving,
        identity(1),
        LogLimits::default(),
        stripes(),
    )
    .unwrap();
    assert_eq!(controls.groups[&group(1)].hard_state, expected.hard_state);
    assert_eq!(
        controls.groups[&group(1)].commit_index,
        expected.commit_index
    );
    assert!(StripedLogStore::recover_striped(
        surviving,
        identity(1),
        LogLimits::default(),
        stripes()
    )
    .is_err());
}
