// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::native::log_store::NativeLogStore;

type SelectedLog = ObservedLog<NativeLogStore<publication::BenchJournal>>;
fn setup(selected: Publication, label: &str) -> (std::path::PathBuf, SelectedLog, Trace) {
    let root = std::env::temp_dir().join(format!(
        "voteboat-publication-observer-{label}-{selected:?}-{}",
        std::process::id()
    ));
    let (journal, trace) = selected.open(&root, NativeOpenMode::Create, 1).unwrap();
    let inner = NativeLogStore::create(journal, store(1), LogLimits::default()).unwrap();
    let observed = ObservedLog {
        inner,
        trace: trace.clone(),
    };
    (root, observed, trace)
}
fn bootstrap() -> Bootstrap {
    Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node(1)), Limits::default()).unwrap(),
        voter_stores: [(node(1), store(1))].into(),
    }
}
#[test]
fn selected_native_diagnostics_retain_exact_append_bytes_and_logical_errors() {
    for selected in [Publication::Atomic, Publication::Witness] {
        let (root, mut log, trace) = setup(selected, "logical");
        let path = root.join("log.wal");
        let before = std::fs::metadata(&path).unwrap().len();
        let tickets = log
            .append_batch(vec![LogMutation::Create(bootstrap())])
            .unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len() - before;
        assert!(bytes > 0);
        assert_eq!(trace.snapshot().encoded_bytes, bytes);
        log.barrier(&tickets).unwrap();
        assert_eq!(log.barrier(&tickets), Err(StorageError::StaleTicket));
        assert_eq!(
            log.barrier(&[]),
            Err(StorageError::Rejected("barrier dependency budget"))
        );
        let native = trace.1.as_ref().unwrap().snapshot();
        assert_eq!(
            native.append.errors + native.log_sync.errors + native.manifest.errors,
            0
        );
        assert_eq!(
            trace.snapshot().errors,
            2,
            "logical errors survive native primitive snapshots"
        );
        assert_eq!(
            trace.snapshot().encoded_bytes,
            bytes,
            "snapshot and barrier do not double-count append bytes"
        );
        drop(log);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn native_and_upper_failure_observations_are_summed_without_claiming_unique_errors() {
    let (root, mut log, trace) = setup(Publication::Atomic, "primitive");
    let tickets = log
        .append_batch(vec![LogMutation::Create(bootstrap())])
        .unwrap();
    std::fs::create_dir(root.join("MANIFEST.tmp")).unwrap();
    assert!(log.barrier(&tickets).is_err());
    let native = trace.1.as_ref().unwrap().snapshot();
    assert_eq!(native.manifest.errors, 1);
    assert_eq!(native.append.errors + native.log_sync.errors, 0);
    assert_eq!(
        trace.snapshot().errors,
        2,
        "one primitive failure plus its upper-layer observation"
    );
    assert_eq!(
        trace.snapshot().errors,
        2,
        "reading a snapshot never accumulates observations again"
    );
    drop(log);
    std::fs::remove_dir_all(root).unwrap();
}
