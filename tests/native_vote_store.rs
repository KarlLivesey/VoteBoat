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
#![cfg(feature = "native")]
use std::{
    cell::RefCell,
    fs, io,
    path::PathBuf,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
use voteboat::{contracts::*, identity::*, native::vote_store::*, quorum::*, vote::*};
// Process creation can briefly inherit another thread's locked descriptor
// before exec closes it. Keep these file/reopen fixtures out of that window;
// the production nonblocking exclusive-lock behavior must remain unchanged.
static FILE_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn n(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn identity() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn record(g: u128, term: u64, candidate: u64) -> VoteRecord {
    VoteRecord {
        group: group(g),
        configuration: ConfigurationId::new(1).unwrap(),
        hard_state: HardState {
            term,
            voted_for: Some(n(candidate)),
        },
    }
}
fn request(candidate: u64, term: u64) -> VoteRequest {
    VoteRequest {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        candidate: n(candidate),
        term,
        last_log_term: 0,
        last_log_index: 0,
    }
}
fn voter<S: VoteStore>(store: &S) -> Voter {
    let policy = Policy::new(
        Tree::Majority((1..=3).map(|id| Tree::Voter(n(id))).collect()),
        Limits::default(),
    )
    .unwrap();
    let recovered = store
        .recovered()
        .get(&group(1))
        .copied()
        .unwrap_or(VoteRecord {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            hard_state: HardState::default(),
        });
    Voter::recover(n(1), recovered, policy, store.binding(), (0, 0)).unwrap()
}

#[derive(Clone, Copy, Debug, Default)]
enum Fault {
    #[default]
    None,
    Append(usize),
    Sync,
    PublishBefore,
    PublishAfter,
}
#[derive(Default)]
struct Device {
    log: Vec<u8>,
    durable_log: Vec<u8>,
    manifest: Option<Vec<u8>>,
    fault: Fault,
}
impl Device {
    fn power_loss(&mut self) {
        self.log = self.durable_log.clone();
        self.fault = Fault::None;
    }
}
#[derive(Clone, Default)]
struct ModelIo(Rc<RefCell<Device>>);
impl VoteIo for ModelIo {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        self.0
            .borrow()
            .manifest
            .clone()
            .ok_or(io::ErrorKind::NotFound.into())
    }
    fn read_log(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let state = self.0.borrow();
        if state.log.len() > limit {
            return Err(io::Error::other("capacity"));
        }
        Ok(state.log.clone())
    }
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut state = self.0.borrow_mut();
        if let Fault::Append(cut) = state.fault {
            state.log.extend(&bytes[..cut.min(bytes.len())]);
            return Err(io::Error::other("injected short write"));
        }
        state.log.extend(bytes);
        Ok(())
    }
    fn sync_log(&mut self) -> io::Result<()> {
        let mut state = self.0.borrow_mut();
        if matches!(state.fault, Fault::Sync) {
            return Err(io::Error::other("injected sync failure"));
        }
        state.durable_log = state.log.clone();
        Ok(())
    }
    fn truncate_log(&mut self, len: u64) -> io::Result<()> {
        self.0.borrow_mut().log.truncate(len as usize);
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut state = self.0.borrow_mut();
        if matches!(state.fault, Fault::PublishBefore) {
            return Err(io::Error::other("publication before rename failed"));
        }
        state.manifest = Some(bytes.to_vec());
        if matches!(state.fault, Fault::PublishAfter) {
            return Err(io::Error::other("publication after rename uncertain"));
        }
        Ok(())
    }
}

#[test]
fn failure_at_every_append_byte_never_releases_a_ballot() {
    // One record frame is 96 bytes: 32 header + 48 record + 16 trailer.
    for cut in 0..=96 {
        let io = ModelIo::default();
        let mut store =
            NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()).unwrap();
        let mut v = voter(&store);
        io.0.borrow_mut().fault = Fault::Append(cut);
        assert!(drive_vote(&mut v, &mut store, request(2, 1)).is_err());
        assert_eq!(v.request(request(3, 1)), Err(VoteError::Fenced));
        assert_eq!(
            store.append_votes(vec![record(1, 1, 3)]),
            Err(StorageError::Fenced)
        );
        drop(store);
        io.0.borrow_mut().power_loss();
        let mut recovered =
            NativeVoteStore::recover(io, identity(), VoteStoreLimits::default()).unwrap();
        let mut v = voter(&recovered);
        // The first ballot never escaped and all volatile writes were lost.
        assert!(
            drive_vote(&mut v, &mut recovered, request(3, 1))
                .unwrap()
                .granted
        );
    }
}
#[test]
fn failed_sync_or_manifest_publication_fences_then_recovers_safely() {
    for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
        let io = ModelIo::default();
        let mut store =
            NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()).unwrap();
        let mut v = voter(&store);
        assert!(
            drive_vote(&mut v, &mut store, request(2, 1))
                .unwrap()
                .granted
        );
        io.0.borrow_mut().fault = fault;
        assert!(drive_vote(&mut v, &mut store, request(3, 2)).is_err());
        assert_eq!(v.request(request(2, 2)), Err(VoteError::Fenced));
        drop(store);
        io.0.borrow_mut().power_loss();
        let mut store =
            NativeVoteStore::recover(io, identity(), VoteStoreLimits::default()).unwrap();
        let recovered = store.recovered()[&group(1)].hard_state;
        assert!(recovered == record(1, 1, 2).hard_state || recovered == record(1, 2, 3).hard_state);
        let mut v = voter(&store);
        // A second candidate in the recovered term can never receive a ballot.
        let other = if recovered.voted_for == Some(n(2)) {
            3
        } else {
            2
        };
        assert!(
            !drive_vote(&mut v, &mut store, request(other, recovered.term))
                .unwrap()
                .granted
        );
    }
}
#[test]
fn interrupted_tail_is_discarded_but_acknowledged_prefix_cannot_be_truncated() {
    let io = ModelIo::default();
    let mut store =
        NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()).unwrap();
    let first = store.append_votes(vec![record(1, 1, 2)]).unwrap();
    store.barrier(&[first]).unwrap();
    let prefix = io.0.borrow().log.clone();
    store.append_votes(vec![record(1, 2, 3)]).unwrap();
    let full = io.0.borrow().log.clone();
    drop(store);
    for cut in 0..96 {
        let image = ModelIo::default();
        image.0.borrow_mut().manifest = io.0.borrow().manifest.clone();
        image.0.borrow_mut().log = full[..prefix.len() + cut].to_vec();
        let store = NativeVoteStore::recover(image.clone(), identity(), VoteStoreLimits::default())
            .unwrap();
        assert_eq!(
            store.recovered()[&group(1)].hard_state,
            record(1, 1, 2).hard_state
        );
        assert_eq!(image.0.borrow().log.len(), prefix.len());
    }
    for cut in 0..prefix.len() {
        let image = ModelIo::default();
        image.0.borrow_mut().manifest = io.0.borrow().manifest.clone();
        image.0.borrow_mut().log = prefix[..cut].to_vec();
        assert!(matches!(
            NativeVoteStore::recover(image, identity(), VoteStoreLimits::default()),
            Err(StorageError::Corrupt(_))
        ));
    }
}
#[test]
fn every_bit_of_a_complete_frame_and_manifest_is_checked() {
    let io = ModelIo::default();
    let mut store =
        NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()).unwrap();
    let ticket = store.append_votes(vec![record(1, 1, 2)]).unwrap();
    store.barrier(&[ticket]).unwrap();
    drop(store);
    let log = io.0.borrow().log.clone();
    let manifest = io.0.borrow().manifest.clone().unwrap();
    for bit in 0..(log.len() + manifest.len()) * 8 {
        let image = ModelIo::default();
        let mut l = log.clone();
        let mut m = manifest.clone();
        if bit / 8 < l.len() {
            l[bit / 8] ^= 1 << (bit % 8);
        } else {
            m[bit / 8 - l.len()] ^= 1 << (bit % 8);
        }
        image.0.borrow_mut().log = l;
        image.0.borrow_mut().manifest = Some(m);
        assert!(
            NativeVoteStore::recover(image, identity(), VoteStoreLimits::default()).is_err(),
            "bit={bit}"
        );
    }
}
#[test]
fn scoped_barriers_reject_future_foreign_old_session_and_duplicate_tickets() {
    let io = ModelIo::default();
    let mut store =
        NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()).unwrap();
    let first = store.append_votes(vec![record(1, 1, 2)]).unwrap();
    let second = store.append_votes(vec![record(2, 1, 3)]).unwrap();
    let wrong_store = StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(2).unwrap(),
            ..identity()
        },
        ..first.binding
    };
    for ticket in [
        WriteTicket {
            binding: wrong_store,
            ..first
        },
        WriteTicket {
            sequence: second.sequence + 1,
            ..first
        },
    ] {
        assert_eq!(store.barrier(&[ticket]), Err(StorageError::StaleTicket));
    }
    assert_eq!(store.barrier(&[second]).unwrap().tickets, vec![second]);
    assert_eq!(store.barrier(&[second]), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&[first]).unwrap().tickets, vec![first]);
    drop(store);
    let mut store = NativeVoteStore::recover(io, identity(), VoteStoreLimits::default()).unwrap();
    assert!(store.binding().session > first.binding.session);
    assert_eq!(store.barrier(&[first]), Err(StorageError::StaleTicket));
}
#[test]
fn limits_and_atomic_ballot_validation_are_enforced_before_submission() {
    let io = ModelIo::default();
    let limits = VoteStoreLimits {
        max_batch_records: 2,
        max_pending_batches: 1,
        max_groups: 2,
        max_wal_bytes: 512,
    };
    let mut store = NativeVoteStore::create(io.clone(), identity(), limits).unwrap();
    let ticket = store
        .append_votes(vec![record(1, 1, 2), record(2, 1, 3)])
        .unwrap();
    assert!(matches!(
        store.append_votes(vec![record(1, 2, 3)]),
        Err(StorageError::Rejected(_))
    ));
    store.barrier(&[ticket]).unwrap();
    let len = io.0.borrow().log.len();
    // The first record is valid but the second revotes within the same term:
    // rejection must not partly mutate the admitted state or physical log.
    assert!(matches!(
        store.append_votes(vec![record(1, 2, 3), record(2, 1, 2)]),
        Err(StorageError::Rejected(_))
    ));
    assert_eq!(io.0.borrow().log.len(), len);
    assert!(matches!(
        store.append_votes(vec![record(1, 1, 3)]),
        Err(StorageError::Rejected(_))
    ));
    assert!(matches!(
        store.append_votes(vec![record(3, 1, 2)]),
        Err(StorageError::Rejected(_))
    ));
    let ticket = store.append_votes(vec![record(1, 2, 3)]).unwrap();
    store.barrier(&[ticket]).unwrap();
    let ticket = store.append_votes(vec![record(1, 3, 3)]).unwrap();
    store.barrier(&[ticket]).unwrap();
    let ticket = store.append_votes(vec![record(1, 4, 3)]).unwrap();
    store.barrier(&[ticket]).unwrap();
    assert!(matches!(
        store.append_votes(vec![record(1, 5, 3)]),
        Err(StorageError::Rejected(_))
    ));
}

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "voteboat-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct HostCodec {
    version: u32,
    calls: Rc<RefCell<usize>>,
}
impl VoteLogCodec for HostCodec {
    fn format_version(&self) -> u32 {
        self.version
    }
    fn encode(&self, sequence: u64, records: &[VoteRecord]) -> Vec<u8> {
        *self.calls.borrow_mut() += 1;
        NativeVoteCodec.encode(sequence, records)
    }
    fn frame_length(&self, bytes: &[u8], limits: VoteStoreLimits) -> Result<usize, StorageError> {
        *self.calls.borrow_mut() += 1;
        NativeVoteCodec.frame_length(bytes, limits)
    }
    fn decode(
        &self,
        bytes: &[u8],
        limits: VoteStoreLimits,
    ) -> Result<(u64, Vec<VoteRecord>), StorageError> {
        *self.calls.borrow_mut() += 1;
        NativeVoteCodec.decode(bytes, limits)
    }
}

#[test]
fn downstream_codec_is_used_and_incompatible_formats_rejected_before_writes() {
    let io = ModelIo::default();
    let calls = Rc::new(RefCell::new(0));
    assert!(matches!(
        NativeVoteStore::create_with_codec(
            io.clone(),
            identity(),
            VoteStoreLimits::default(),
            HostCodec {
                version: 99,
                calls: calls.clone()
            }
        ),
        Err(StorageError::Rejected(_))
    ));
    assert!(io.0.borrow().manifest.is_none());
    let mut store = NativeVoteStore::create_with_codec(
        io.clone(),
        identity(),
        VoteStoreLimits::default(),
        HostCodec {
            version: 1,
            calls: calls.clone(),
        },
    )
    .unwrap();
    let ticket = store.append_votes(vec![record(1, 1, 2)]).unwrap();
    store.barrier(&[ticket]).unwrap();
    assert_eq!(*calls.borrow(), 1);
    drop(store);
    let store = NativeVoteStore::recover_with_codec(
        io,
        identity(),
        VoteStoreLimits::default(),
        HostCodec {
            version: 1,
            calls: calls.clone(),
        },
    )
    .unwrap();
    assert_eq!(store.recovered()[&group(1)], record(1, 1, 2));
    assert_eq!(*calls.borrow(), 3);
}

#[test]
fn complete_unacknowledged_writes_can_survive_and_creation_cannot_reset_a_store() {
    let io = ModelIo::default();
    let mut store =
        NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()).unwrap();
    store.append_votes(vec![record(1, 1, 2)]).unwrap();
    drop(store);
    // Process crash, no power loss: accepted bytes may still be in OS cache.
    assert!(matches!(
        NativeVoteStore::create(io.clone(), identity(), VoteStoreLimits::default()),
        Err(StorageError::Rejected(_))
    ));
    let mut store = NativeVoteStore::recover(io, identity(), VoteStoreLimits::default()).unwrap();
    assert_eq!(store.recovered()[&group(1)], record(1, 1, 2));
    let mut v = voter(&store);
    assert!(
        !drive_vote(&mut v, &mut store, request(3, 1))
            .unwrap()
            .granted
    );
}

#[test]
fn child_process_votes() {
    let Ok(path) = std::env::var("VOTEBOAT_CRASH_TEST_PATH") else {
        return;
    };
    let mut store = NativeVoteStore::create(
        FileVoteIo::create(path).unwrap(),
        identity(),
        VoteStoreLimits::default(),
    )
    .unwrap();
    let mut v = voter(&store);
    assert!(
        drive_vote(&mut v, &mut store, request(2, 1))
            .unwrap()
            .granted
    );
    // Abrupt process exit after success, without Rust destructors. This tests
    // process death, not physical device-cache loss.
    std::process::exit(0);
}

#[test]
fn acknowledged_vote_survives_abrupt_child_process_exit() {
    let _files = FILE_TESTS.lock().unwrap();
    let directory = Temp::new();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_process_votes", "--nocapture"])
        .env("VOTEBOAT_CRASH_TEST_PATH", &directory.0)
        .status()
        .unwrap();
    assert!(status.success());
    let mut store = NativeVoteStore::recover(
        FileVoteIo::open(&directory.0).unwrap(),
        identity(),
        VoteStoreLimits::default(),
    )
    .unwrap();
    let mut v = voter(&store);
    assert!(
        !drive_vote(&mut v, &mut store, request(3, 1))
            .unwrap()
            .granted
    );
}

#[test]
fn native_files_recover_ballot_and_exclude_concurrent_writers() {
    let _files = FILE_TESTS.lock().unwrap();
    let directory = Temp::new();
    let mut store = NativeVoteStore::create(
        FileVoteIo::create(&directory.0).unwrap(),
        identity(),
        VoteStoreLimits::default(),
    )
    .unwrap();
    let mut v = voter(&store);
    assert!(
        drive_vote(&mut v, &mut store, request(2, 1))
            .unwrap()
            .granted
    );
    assert!(FileVoteIo::open(&directory.0).is_err());
    drop(store);
    assert!(FileVoteIo::create(&directory.0).is_err());
    let mut store = NativeVoteStore::recover(
        FileVoteIo::open(&directory.0).unwrap(),
        identity(),
        VoteStoreLimits::default(),
    )
    .unwrap();
    let mut v = voter(&store);
    assert!(
        !drive_vote(&mut v, &mut store, request(3, 1))
            .unwrap()
            .granted
    );
    assert!(
        drive_vote(&mut v, &mut store, request(3, 2))
            .unwrap()
            .granted
    );
}
#[test]
fn missing_wal_and_wrong_identity_are_not_empty_success() {
    let _files = FILE_TESTS.lock().unwrap();
    let directory = Temp::new();
    let store = NativeVoteStore::create(
        FileVoteIo::create(&directory.0).unwrap(),
        identity(),
        VoteStoreLimits::default(),
    )
    .unwrap();
    drop(store);
    let wrong = StoreIdentity {
        incarnation: StoreIncarnation::new(2).unwrap(),
        ..identity()
    };
    assert!(matches!(
        NativeVoteStore::recover(
            FileVoteIo::open(&directory.0).unwrap(),
            wrong,
            VoteStoreLimits::default()
        ),
        Err(StorageError::WrongIdentity)
    ));
    fs::remove_file(directory.0.join("votes.wal")).unwrap();
    assert!(FileVoteIo::open(&directory.0).is_err());
    assert!(FileVoteIo::create(&directory.0).is_err());
}
