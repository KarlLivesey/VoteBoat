// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    snapshot_worker_cases as cases,
    support::{self, *},
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    native::{snapshot_store::*, snapshot_worker::NativeSnapshotWorker, worker::ThreadWake},
    snapshot::*,
    snapshot_worker::*,
};

fn limits() -> SnapshotLimits {
    SnapshotLimits {
        max_application_bytes: 32,
        max_metadata_bytes: 256,
        max_chunk_bytes: 4,
    }
}

fn spawn<S: SnapshotRetention + Send + 'static>(
    stores: BTreeMap<voteboat::identity::GroupIdentity, S>,
    generation: u64,
) -> NativeSnapshotWorker<S> {
    let mut binding = cases::binding();
    binding.generation = voteboat::identity::SnapshotWorkerGeneration::new(generation).unwrap();
    NativeSnapshotWorker::spawn(
        stores,
        binding,
        SnapshotWorkLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap()
}

fn reclaim<S: SnapshotRetention + Send + 'static>(
    worker: &mut NativeSnapshotWorker<S>,
) -> BTreeMap<voteboat::identity::GroupIdentity, S> {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(stores) = worker.try_reclaim().unwrap() {
            return stores;
        }
        assert!(Instant::now() < until, "snapshot worker join deadline");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

#[test]
fn shared_worker_close_and_queries_use_host_snapshot_contracts() {
    let stores = (1..=2)
        .map(|id| {
            let mut store = support::snapshot::HostSnapshots::new();
            store.identity = SnapshotIdentity {
                store: identity(2),
                group: group(id),
            };
            store.binding.identity = identity(2);
            store.limits = limits();
            (group(id), store)
        })
        .collect();
    let mut worker = spawn(stores, 1);
    assert!(worker.try_reclaim().is_err());
    let pins = cases::close_preserves_accepted_work(&mut worker);
    let stores = reclaim(&mut worker);
    assert!(worker.try_reclaim().is_err());
    let mut worker = spawn(stores, 2);
    cases::recovered_queries(&mut worker, pins);
    assert_eq!(reclaim(&mut worker).len(), 2);
}

#[test]
fn shared_worker_close_and_queries_survive_native_file_reopen() {
    let root = std::env::temp_dir().join(format!(
        "voteboat-worker-conformance-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let identity_for = |id| SnapshotIdentity {
        store: identity(2),
        group: group(id),
    };
    let stores = (1..=2)
        .map(|id| {
            (
                group(id),
                NativeSnapshotStore::create(
                    FileSnapshotIo::create(root.join(id.to_string())).unwrap(),
                    identity_for(id),
                    limits(),
                )
                .unwrap(),
            )
        })
        .collect();
    let mut worker = spawn(stores, 1);
    let pins = cases::close_preserves_accepted_work(&mut worker);
    drop(reclaim(&mut worker));
    drop(worker);
    let stores = (1..=2)
        .map(|id| {
            (
                group(id),
                NativeSnapshotStore::recover(
                    FileSnapshotIo::open(root.join(id.to_string())).unwrap(),
                    identity_for(id),
                    limits(),
                )
                .unwrap(),
            )
        })
        .collect();
    let mut worker = spawn(stores, 2);
    cases::recovered_queries(&mut worker, pins);
    drop(reclaim(&mut worker));
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}
