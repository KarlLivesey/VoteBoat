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
use super::source_fixture::*;
use voteboat::{
    application::*, bucket_counter::*, identity::*, routed::*, routing::*, transfer_source::*,
};
type App = RoutedApplication<BucketCounter<Policy>, Policy>;
fn selected(limit: usize) -> App {
    routed()
        .with_scoped_fencing(limit)
        .unwrap_or_else(|r| panic!("{:?}", r.0))
}
fn ready(limit: usize) -> App {
    let mut a = selected(limit);
    a.apply_batch(&[entry(1, 100, a.bootstrap_command(100000).unwrap())])
        .unwrap();
    a
}
fn freeze(a: u16, b: u16) -> Vec<u8> {
    encode_scope_fence(grant().input().epoch, range(a, b))
}
fn read(a: &App, key: u8) -> RoutedRead<i64> {
    a.read_at(
        a.applied_index(),
        RoutedQuery {
            hint: hint(key),
            key: vec![key],
            query: vec![key],
        },
    )
    .unwrap()
}
#[test]
fn retained_data_continues_while_transferred_scope_stays_fenced_after_replay() {
    let mut a = ready(2);
    let log = vec![
        entry(2, 1, data(1, 7)),
        entry(3, 2, data(200, 11)),
        entry(4, 200, freeze(0, 128)),
        entry(5, 3, data(1, 20)),
        entry(6, 4, data(200, 2)),
        entry(7, 2, data(200, 11)),
        entry(8, 200, freeze(0, 128)),
    ];
    let bound = a.receipt_bytes_bound(&log).unwrap();
    let receipts = a.apply_batch(&log).unwrap();
    assert!(bound >= receipts.len() * std::mem::size_of_val(&receipts[0]));
    let fence = a.scoped_fences()[0];
    assert_eq!(fence.fence.index, 4);
    assert_eq!(receipts[2].outcome, RoutedOutcome::ScopeFenced(fence));
    assert_eq!(
        receipts[3].outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    );
    assert_eq!(receipts[6].outcome, RoutedOutcome::ScopeFenced(fence));
    assert!(matches!(
        receipts[5].outcome,
        RoutedOutcome::Applied(BucketReceipt {
            duplicate: true,
            ..
        })
    ));
    assert_eq!(read(&a, 1), RoutedRead::Rejected(RoutingError::Fenced));
    assert_eq!(read(&a, 200), RoutedRead::Served(13));
    assert_eq!(a.application().value(&[1]), Ok(7));
    assert_eq!(a.application().outbox().count(), 3);
    assert_eq!(a.fence(), None);
    let image = a.checkpoint(100000).unwrap();
    let mut restored = selected(2);
    restored.restore_checkpoint(2, 8, &image).unwrap();
    assert_eq!(restored.checkpoint(100000).unwrap(), image);
    let mut replay = ready(2);
    replay.apply_batch(&log).unwrap();
    assert_eq!(replay.checkpoint(100000).unwrap(), image);
    restored.apply_batch(&[entry(9, 5, data(200, 3))]).unwrap();
    assert_eq!(read(&restored, 200), RoutedRead::Served(16));
    assert_eq!(
        read(&restored, 1),
        RoutedRead::Rejected(RoutingError::Fenced)
    );
    // Full-source retirement remains irreversible and dominates every remaining range.
    restored
        .apply_batch(&[entry(10, 300, encode_fence(grant().input().epoch))])
        .unwrap();
    assert_eq!(
        read(&restored, 200),
        RoutedRead::Rejected(RoutingError::Fenced)
    );
    let whole = restored.checkpoint(100000).unwrap();
    let mut recovered = selected(2);
    recovered.restore_checkpoint(2, 10, &whole).unwrap();
    assert_eq!(recovered.checkpoint(100000).unwrap(), whole);
}
#[test]
fn scoped_pending_order_control_capacity_and_conflicts_are_atomic() {
    let mut a = ready(1);
    let f = freeze(0, 128);
    let moved = data(1, 7);
    let kept = data(200, 11);
    assert!(a
        .validate_proposal(op(1), &moved, [(op(200), f.as_slice())].into_iter())
        .is_err());
    assert!(a
        .validate_proposal(op(2), &kept, [(op(200), f.as_slice())].into_iter())
        .is_ok());
    assert!(a
        .validate_proposal(op(200), &f, [(op(200), kept.as_slice())].into_iter())
        .is_err());
    assert!(a
        .validate_proposal(op(200), &kept, [(op(200), f.as_slice())].into_iter())
        .is_err());
    assert!(a
        .validate_proposal(
            op(201),
            &freeze(128, 256),
            [(op(200), f.as_slice())].into_iter()
        )
        .is_err());
    assert!(a
        .validate_proposal(op(200), &f, [(op(200), f.as_slice())].into_iter())
        .is_ok());
    a.apply_batch(&[entry(2, 200, f.clone())]).unwrap();
    let original = a.checkpoint(100000).unwrap();
    assert!(a
        .apply_batch(&[entry(3, 2, kept), entry(4, 201, freeze(128, 256))])
        .is_err());
    assert_eq!(a.checkpoint(100000).unwrap(), original);
    for (id, bytes, outcome) in [
        (200, freeze(128, 256), RoutedOutcome::OperationConflict),
        (
            201,
            freeze(64, 128),
            RoutedOutcome::Rejected(RoutingError::Fenced),
        ),
        (
            202,
            encode_scope_fence(OwnershipEpoch::new(2).unwrap(), range(128, 256)),
            RoutedOutcome::Rejected(RoutingError::EpochMismatch),
        ),
    ] {
        let index = a.applied_index() + 1;
        assert_eq!(
            a.apply_batch(&[entry(index, id, bytes)]).unwrap()[0].outcome,
            outcome
        );
    }
    assert_eq!(a.scoped_fences().len(), 1);
    // Ordinary dedup capacity cannot consume the separately bound fence slot.
    let mut all = ready(1);
    for i in 0..32 {
        all.apply_batch(&[entry(i + 2, i as u128 + 1, data(200, 1))])
            .unwrap();
    }
    all.apply_batch(&[entry(34, 200, freeze(0, 128))]).unwrap();
    assert_eq!(all.scoped_fences().len(), 1);
}
#[test]
fn profile_binding_and_malformed_checkpoints_cannot_erase_scoped_fences() {
    assert!(routed().with_scoped_fencing(0).is_err());
    assert!(routed().with_scoped_fencing(257).is_err());
    assert!(ready(1).with_scoped_fencing(2).is_err());
    assert!(selected(1).with_scoped_fencing(2).is_err());
    assert!(TransferSource::new(selected(1), 65536).is_err());
    let mut legacy = routed();
    legacy
        .apply_batch(&[entry(1, 100, legacy.bootstrap_command(100000).unwrap())])
        .unwrap();
    assert!(legacy
        .apply_batch(&[entry(2, 200, freeze(0, 128))])
        .is_err());
    let mut a = ready(2);
    a.apply_batch(&[entry(2, 200, freeze(0, 128)), entry(3, 2, data(200, 11))])
        .unwrap();
    let image = a.checkpoint(100000).unwrap();
    assert!(legacy.restore_checkpoint(2, 3, &image).is_err());
    assert!(selected(1).restore_checkpoint(2, 3, &image).is_err());
    for length in 0..image.len() {
        let original = a.checkpoint(100000).unwrap();
        assert!(a.restore_checkpoint(2, 3, &image[..length]).is_err());
        assert_eq!(a.checkpoint(100000).unwrap(), original);
    }
    // Rewrite scoped index/epoch/range/operation into contradictory state.
    let bind_len = u32::from_le_bytes(image[16..20].try_into().unwrap()) as usize;
    let record = 20 + bind_len + 1 + 24 + 1 + 2;
    for (offset, replacement) in [
        (record, 100u128.to_le_bytes().to_vec()),
        (record + 16, 1u64.to_le_bytes().to_vec()),
        (record + 24, 2u64.to_le_bytes().to_vec()),
        (record + 32, [0, 0, 0, 0].to_vec()),
    ] {
        let mut bad = image.clone();
        bad[offset..offset + replacement.len()].copy_from_slice(&replacement);
        assert!(a.restore_checkpoint(2, 3, &bad).is_err());
        assert_eq!(a.checkpoint(100000).unwrap(), image);
    }
}

#[cfg(feature = "native")]
use super::support;
#[cfg(feature = "native")]
#[test]
fn native_scoped_fence_frame_cuts_recover_old_or_exact_fence_with_retained_service() {
    use support::{Fault, ModelIo};
    use voteboat::{log::*, native::log_store::*};
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut log,
            vec![LogMutation::Create(support::bootstrap(20, 3))],
        );
        let a = selected(2);
        let prefix = vec![
            entry(1, 100, a.bootstrap_command(100000).unwrap()),
            entry(2, 1, data(1, 7)),
            entry(3, 2, data(200, 11)),
        ];
        let state = log.state(group(20)).unwrap();
        support::append(
            &mut log,
            vec![support::update(
                &state,
                1,
                3,
                Some(Suffix {
                    from: 1,
                    entries: prefix,
                }),
            )],
        );
        (io, log)
    };
    let (_, log) = seed();
    let state = log.state(group(20)).unwrap();
    let mutation = support::update(
        &state,
        1,
        4,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 200, freeze(0, 128))],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), limits)
        .unwrap();
    let mut old = false;
    let mut complete = false;
    for fault in (0..=frame.len()).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let (io, mut log) = seed();
        io.0.borrow_mut().fault = fault;
        if let Ok(tickets) = log.append_batch(vec![mutation.clone()]) {
            assert!(log.barrier(&tickets).is_err());
        }
        drop(log);
        io.0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(io, support::identity(1), limits).unwrap();
        let state = log.state(group(20)).unwrap();
        let mut a = selected(2);
        a.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        if state.commit_index == 3 {
            old = true;
            assert!(a.scoped_fences().is_empty());
            assert_eq!(read(&a, 1), RoutedRead::Served(7));
        } else {
            complete = true;
            assert_eq!(state.commit_index, 4);
            assert_eq!(a.scoped_fences()[0].fence.index, 4);
            assert_eq!(read(&a, 1), RoutedRead::Rejected(RoutingError::Fenced));
        }
        a.apply_batch(&[entry(state.commit_index + 1, 3, data(200, 2))])
            .unwrap();
        assert_eq!(read(&a, 200), RoutedRead::Served(13));
        assert_eq!(a.application().value(&[1]), Ok(7));
    }
    assert!(old && complete);
}

#[test]
fn scoped_fence_requires_one_exact_local_owned_range_and_valid_data_context() {
    let mut m = grant().into_input();
    m.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(20)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(21)),
        },
    ]);
    let mut a = RoutedApplication::new(
        group(20),
        ResponsibilityManifest::new(m).unwrap(),
        BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap(),
        Policy,
        routed().limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error))
    .with_scoped_fencing(2)
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    a.apply_batch(&[entry(1, 100, a.bootstrap_command(100000).unwrap())])
        .unwrap();
    assert_eq!(
        a.apply_batch(&[entry(2, 200, freeze(64, 192))]).unwrap()[0].outcome,
        RoutedOutcome::Rejected(RoutingError::WrongOwner)
    );
    assert_eq!(
        a.apply_batch(&[entry(3, 201, freeze(128, 256))]).unwrap()[0].outcome,
        RoutedOutcome::Rejected(RoutingError::WrongOwner)
    );
    let mut h = hint(1);
    h.bucket = 200;
    let spoof = encode_routed(h, &[1], &encode_add(&[1], 7, b"", 1024).unwrap(), 4096).unwrap();
    assert!(a
        .validate_proposal(op(1), &spoof, std::iter::empty())
        .is_err());
    assert!(a.scoped_fences().is_empty());
    a.apply_batch(&[entry(4, 202, freeze(64, 128))]).unwrap();
    assert_eq!(a.scoped_fences()[0].scope, range(64, 128));
}
