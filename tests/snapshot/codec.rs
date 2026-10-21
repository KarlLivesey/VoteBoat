// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::cell::Cell;

#[derive(Clone, Copy)]
enum Mode {
    Exact,
    Altered,
    Truncated,
}
struct HostCodec(Rc<Cell<Mode>>);
impl SnapshotCodec for HostCodec {
    fn format_version(&self) -> u32 {
        2
    }
    fn prefix(
        &self,
        m: &SnapshotMetadata,
        n: usize,
        l: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError> {
        NativeSnapshotCodec.prefix(m, n, l)
    }
    fn finish(&self, b: &[u8], l: SnapshotLimits) -> Result<[u8; 4], StorageError> {
        NativeSnapshotCodec.finish(b, l)
    }
    fn decode(&self, b: &[u8], l: SnapshotLimits) -> Result<Snapshot, StorageError> {
        let mut snapshot = NativeSnapshotCodec.decode(b, l)?;
        match self.0.get() {
            Mode::Exact => (),
            Mode::Altered => snapshot.application[0] ^= 1,
            Mode::Truncated => {
                snapshot.application.pop();
            }
        }
        Ok(snapshot)
    }
}
fn codec(mode: Mode) -> HostCodec {
    HostCodec(Rc::new(Cell::new(mode)))
}
fn create_with<I: SnapshotIo>(io: I, codec: HostCodec) -> NativeSnapshotStore<I, HostCodec> {
    NativeSnapshotStore::create_with_codec(
        io,
        snapshot_identity(),
        SnapshotLimits::default(),
        codec,
    )
    .unwrap()
}
fn reopen_with<I: SnapshotIo>(io: I) -> NativeSnapshotStore<I, HostCodec> {
    NativeSnapshotStore::recover_with_codec(
        io,
        snapshot_identity(),
        SnapshotLimits::default(),
        codec(Mode::Exact),
    )
    .unwrap()
}
fn application() -> Counter {
    let mut application = Counter::new(20).unwrap();
    application
        .apply_batch(&[command(1, 1, 7), command(2, 2, 3), command(3, 1, 7)])
        .unwrap();
    application
}
fn checkpoint(application: &Counter) -> Vec<u8> {
    application.checkpoint(4096).unwrap()
}
fn assert_retry(snapshot: &Snapshot) {
    assert_eq!(snapshot.metadata, metadata(3));
    assert_eq!(snapshot.application, checkpoint(&application()));
    let mut restored = Counter::new(20).unwrap();
    restored
        .restore_checkpoint(restored.schema_version(), 3, &snapshot.application)
        .unwrap();
    restored.apply_batch(&[command(4, 1, 7)]).unwrap();
    assert_eq!(
        restored.read_applied(4),
        Ok(10),
        "original ID must remain deduplicated"
    );
    assert_eq!(restored.retained_receipts(), 2);
}
#[test]
fn changed_or_truncated_decode_fences_before_sync_and_preserves_pinned_retry_checkpoint() {
    for (mode, power_loss) in [Mode::Altered, Mode::Truncated]
        .into_iter()
        .flat_map(|mode| [false, true].map(|power_loss| (mode, power_loss)))
    {
        let io = MemoryIo::default();
        let provider = codec(Mode::Exact);
        let behavior = provider.0.clone();
        let mut store = create_with(io.clone(), provider);
        let original = publish(&mut store, metadata(3), &checkpoint(&application()));
        store.pin_for_log(original.reference()).unwrap();
        let binding = store.binding();
        let before = io.0.borrow().clone();
        io.0.borrow_mut().trace.clear();
        behavior.set(mode);
        let ticket = store.begin(metadata(4), 3).unwrap();
        store.write_chunk(ticket, 0, &[7, 8, 9]).unwrap();
        assert_eq!(
            store.seal(ticket),
            Err(StorageError::Corrupt(
                "snapshot codec changed metadata/application"
            ))
        );
        assert_eq!(store.load(), Err(StorageError::Fenced));
        assert_eq!(store.abort(ticket), Err(StorageError::Fenced));
        assert_eq!(io.0.borrow().manifest, before.manifest);
        assert_eq!(io.0.borrow().synced, before.synced);
        assert_eq!(io.0.borrow().trace, ["begin", "append"]);
        drop(store);
        if power_loss {
            io.0.borrow_mut().power_loss();
        }
        let mut recovered = reopen_with(io);
        assert_eq!(recovered.binding().identity, binding.identity);
        assert!(recovered.binding().session > binding.session);
        assert_retry(&recovered.load().unwrap().unwrap());
        assert_retry(&recovered.load_pinned(original.reference()).unwrap());
        assert_eq!(
            recovered.publish(original.sealed),
            Err(StorageError::StaleTicket)
        );
        assert_eq!(
            recovered.write_chunk(ticket, 0, &[1]),
            Err(StorageError::StaleTicket)
        );
        let next = publish(&mut recovered, metadata(4), &[1, 2, 3]);
        assert!(next.sealed.ticket.generation > original.sealed.ticket.generation);
        assert_retry(&recovered.load_pinned(original.reference()).unwrap());
    }
}
#[test]
fn transparent_host_codec_reopens_real_checkpoint_and_preserves_retry_and_pin() {
    let path = std::env::temp_dir().join(format!("voteboat-snapshot-codec-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let mut store = create_with(FileSnapshotIo::create(&path).unwrap(), codec(Mode::Exact));
    let original = publish(&mut store, metadata(3), &checkpoint(&application()));
    store.pin_for_log(original.reference()).unwrap();
    let binding = store.binding();
    drop(store);
    let mut recovered = reopen_with(FileSnapshotIo::open(&path).unwrap());
    assert!(recovered.binding().session > binding.session);
    assert_retry(&recovered.load().unwrap().unwrap());
    assert_retry(&recovered.load_pinned(original.reference()).unwrap());
    assert_eq!(
        recovered.publish(original.sealed),
        Err(StorageError::StaleTicket)
    );
    let next = publish(&mut recovered, metadata(4), &[4, 5, 6]);
    recovered.pin_for_log(next.reference()).unwrap();
    assert_retry(&recovered.load_pinned(original.reference()).unwrap());
    drop(recovered);
    let mut recovered = reopen_with(FileSnapshotIo::open(&path).unwrap());
    assert_eq!(recovered.load().unwrap().unwrap().application, [4, 5, 6]);
    assert_retry(&recovered.load_pinned(original.reference()).unwrap());
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn substituted_file_decode_preserves_previous_root_after_fenced_close_and_reopen() {
    let path = std::env::temp_dir().join(format!(
        "voteboat-snapshot-codec-refusal-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&path).unwrap();
    let provider = codec(Mode::Exact);
    let behavior = provider.0.clone();
    let mut store = create_with(FileSnapshotIo::create(&path).unwrap(), provider);
    let original = publish(&mut store, metadata(3), &checkpoint(&application()));
    store.pin_for_log(original.reference()).unwrap();
    let manifest = std::fs::read(path.join("MANIFEST")).unwrap();
    behavior.set(Mode::Altered);
    let ticket = store.begin(metadata(4), 3).unwrap();
    store.write_chunk(ticket, 0, &[7, 8, 9]).unwrap();
    assert_eq!(
        store.seal(ticket),
        Err(StorageError::Corrupt(
            "snapshot codec changed metadata/application"
        ))
    );
    drop(store);
    assert_eq!(std::fs::read(path.join("MANIFEST")).unwrap(), manifest);
    let mut recovered = reopen_with(FileSnapshotIo::open(&path).unwrap());
    assert_retry(&recovered.load().unwrap().unwrap());
    assert_retry(&recovered.load_pinned(original.reference()).unwrap());
    assert_eq!(
        recovered.write_chunk(ticket, 0, &[1]),
        Err(StorageError::StaleTicket)
    );
    publish(&mut recovered, metadata(4), &[3, 2, 1]);
    assert_retry(&recovered.load_pinned(original.reference()).unwrap());
    drop(recovered);
    std::fs::remove_dir_all(path).unwrap();
}
