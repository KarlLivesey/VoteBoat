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
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::native::{
    snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
    snapshot_worker::NativeSnapshotWorker,
    worker::ThreadWake,
};
#[test]
fn original_native_worker_load_keeps_the_live_wal_session_and_exact_anchor() {
    let root = std::env::temp_dir().join(format!("voteboat-export331-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let (mut log, mut core, app) = committed(1);
    let original_session = log.binding();
    let mut snapshots = NativeSnapshotStore::create(
        FileSnapshotIo::create(root.join("snap")).unwrap(),
        SnapshotIdentity {
            store: identity(1),
            group: group(1),
        },
        SnapshotLimits {
            max_application_bytes: ALLOWANCE,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 7,
        },
    )
    .unwrap();
    let reference = checkpoint_application(&core, &app, &mut snapshots)
        .unwrap()
        .reference();
    compact_replica(&mut core, &mut log, &mut snapshots, &app, reference).unwrap();
    let mut owner = providers::owner(&log, core);
    let binding = SnapshotWorkerBinding {
        store: original_session,
        generation: SnapshotWorkerGeneration::new(1).unwrap(),
    };
    let mut worker = NativeSnapshotWorker::spawn(
        [(group(1), snapshots)].into(),
        binding,
        SnapshotWorkLimits::default(),
        Arc::new(ThreadWake::current()),
    )
    .unwrap();
    let mut router = SnapshotRouter::new(
        owner.identity(),
        binding,
        SnapshotRouterLimits {
            requests: 2,
            max_image_bytes: 2 * ALLOWANCE,
        },
    )
    .unwrap();
    let ticket = router
        .export_checkpoint(&mut owner, &mut worker, &app, group(1))
        .unwrap();
    assert_eq!(ticket.reference, reference);
    let held = router.usage();
    let mut app = app;
    let deadline = Instant::now() + Duration::from_secs(5);
    let output = await_original_load(&mut router, &mut owner, &mut worker, &mut app, deadline);
    assert!(worker.is_drained());
    assert_eq!(router.usage(), held);
    assert!(!router.is_drained());
    let SnapshotExportOutcome::Loaded(image) = router.complete_export(output).unwrap() else {
        panic!("native image");
    };
    assert_eq!(image.application, app.checkpoint(ALLOWANCE).unwrap());
    assert!(reference.matches(&image));
    assert_eq!(worker.binding().store, original_session);
    assert_eq!(
        owner.core(group(1)).unwrap().state().snapshot,
        Some(reference)
    );
    assert_eq!(log.binding(), original_session);
    assert!(router.is_drained());
    worker.close();
    let mut stores = loop {
        if let Some(stores) = worker.try_reclaim().unwrap() {
            break stores;
        }
        assert!(
            Instant::now() < deadline,
            "original native worker did not join"
        );
        std::thread::yield_now();
    };
    assert_eq!(
        stores
            .get_mut(&group(1))
            .unwrap()
            .load_pinned(reference)
            .unwrap(),
        image
    );
    drop(stores);
    drop(worker);
    std::fs::remove_dir_all(root).unwrap();
}

fn await_original_load<W: SnapshotWorker>(
    router: &mut SnapshotRouter,
    owner: &mut Owner,
    worker: &mut W,
    app: &mut Counter,
    deadline: Instant,
) -> SnapshotExportCompletion {
    loop {
        for event in worker.poll(1) {
            router.deliver(owner, app, event, MonoTime(0)).unwrap();
        }
        if let Some(output) = router.poll_export() {
            return output;
        }
        assert!(
            Instant::now() < deadline,
            "original native load did not complete"
        );
        std::thread::yield_now();
    }
}
