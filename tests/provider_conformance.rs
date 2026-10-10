// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#[path = "provider_conformance/admission.rs"]
mod admission_cases;
#[path = "provider_conformance/log_store.rs"]
mod cases;
#[path = "provider_conformance/credential.rs"]
mod credential_cases;
#[cfg(feature = "native")]
#[path = "provider_conformance/credential_native.rs"]
mod credential_native;
#[path = "provider_conformance/snapshot.rs"]
mod snapshot_cases;
#[cfg(feature = "native")]
#[path = "provider_conformance/snapshot_native.rs"]
mod snapshot_native;
#[cfg(feature = "native")]
#[path = "provider_conformance/snapshot_worker.rs"]
mod snapshot_worker_cases;
#[cfg(feature = "native")]
#[path = "provider_conformance/snapshot_worker_native.rs"]
mod snapshot_worker_native;
mod support;
use support::*;
use voteboat::log::LogStore;

#[test]
fn host_snapshots_preserve_stages_exact_references_and_two_log_anchors() {
    let mut store = support::snapshot::HostSnapshots::new();
    snapshot_cases::stage_identity_and_limits(&mut store);
    snapshot_cases::abort_preserves_root(&mut store);
    let pins = snapshot_cases::two_pins(&mut store);
    snapshot_cases::reconcile(&mut store, pins);
}

#[test]
fn host_snapshot_bindings_cannot_publish_or_pin_each_others_images() {
    let mut first = support::snapshot::HostSnapshots::new();
    let mut second = support::snapshot::HostSnapshots::new();
    second.identity.store = identity(2);
    second.binding.identity = identity(2);
    snapshot_cases::cross_store(&mut first, &mut second);
    drop(first);
    snapshot_cases::assert_image(&mut second, 1);
}

#[test]
fn host_log_store_satisfies_scoped_ticket_and_range_obligations_without_native() {
    let mut first = HostLogStore::new(1);
    let mut second = HostLogStore::new(2);
    cases::cross_store(&mut first, &mut second);
    cases::exercise(&mut first, 38);
    drop(first);
    assert_eq!(second.state(group(50)).unwrap().bootstrap, bootstrap(50, 3));
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::{log::LogLimits, native::log_store::*};

    #[test]
    fn model_log_store_satisfies_same_obligations_and_recovers_original_history() {
        let io = ModelIo::default();
        let mut first =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        let mut second =
            NativeLogStore::create(ModelIo::default(), identity(2), LogLimits::default()).unwrap();
        cases::cross_store(&mut first, &mut second);
        cases::exercise(&mut first, 38);
        let old = first.binding();
        let states = [1, 2, 50].map(|id| first.state(group(id)).unwrap());
        drop(first);
        io.0.borrow_mut().power_loss();
        let mut reopened = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        cases::reopened(&mut reopened, old, &states);
        assert!(second.state(group(50)).is_ok());
    }

    #[test]
    fn file_log_store_satisfies_same_obligations_and_rejects_prior_session_tickets() {
        let root =
            std::env::temp_dir().join(format!("voteboat-conformance-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut first = NativeLogStore::create(
            FileLogIo::create(root.join("first")).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let mut second = NativeLogStore::create(
            FileLogIo::create(root.join("second")).unwrap(),
            identity(2),
            LogLimits::default(),
        )
        .unwrap();
        cases::cross_store(&mut first, &mut second);
        cases::exercise(&mut first, 38);
        let old = first.binding();
        let states = [1, 2, 50].map(|id| first.state(group(id)).unwrap());
        drop(first);
        let mut reopened = NativeLogStore::recover(
            FileLogIo::open(root.join("first")).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        cases::reopened(&mut reopened, old, &states);
        assert!(second.state(group(50)).is_ok());
        drop(reopened);
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }
}
