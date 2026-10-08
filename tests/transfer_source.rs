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
use voteboat::{
    application::*, bucket_counter::*, routed::*, routing::*, scope::*, transfer::*,
    transfer_source::*,
};
#[path = "transfer_source/fixtures.rs"]
mod fixtures;
use fixtures::*;

#[test]
fn same_batch_freeze_retains_exact_boundary_while_outer_prefix_advances() {
    let mut s = fresh();
    let entries = [
        entry(1, 100, s.bootstrap_command(100000).unwrap()),
        entry(2, 1, data(1, 7)),
        entry(3, 2, data(200, 11)),
        entry(4, 200, freeze()),
        noop(5),
        entry(6, 3, data(1, 20)),
        entry(7, 200, freeze()),
    ];
    let bound = s.receipt_bytes_bound(&entries).unwrap();
    let receipts = s.apply_batch(&entries).unwrap();
    assert_eq!(receipts.len(), 6);
    assert!(bound >= receipts.capacity() * std::mem::size_of_val(&receipts[0]));
    let fence = s.fence().unwrap();
    assert_eq!(fence.index, 4);
    assert_eq!(fence.operation, op(200));
    assert_eq!(receipts[3].outcome, RoutedOutcome::Fenced(fence));
    assert_eq!(
        receipts[4].outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    );
    assert_eq!(receipts[5].outcome, RoutedOutcome::Fenced(fence));
    assert_eq!(s.applied_index(), 7);
    assert_eq!(s.routed().applied_index(), 4);
    let left = s.export_target(group(21), 65536).unwrap();
    let right = s.export_target(group(22), 65536).unwrap();
    assert_eq!(left.source_applied(), 4);
    assert_eq!(right.source_applied(), 4);
    let mut a = BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap();
    a.import_scopes(std::slice::from_ref(&left), 1).unwrap();
    assert_eq!(a.value(&[1]), Ok(7));
    let retry = encode_add(&[1], 7, b"effect", 1024).unwrap();
    let receipt = a.apply_batch(&[entry(2, 1, retry)]).unwrap().remove(0);
    assert!(receipt.duplicate);
    assert_eq!(a.outbox().count(), 1);
    s.apply_batch(&[noop(8), entry(9, 200, freeze())]).unwrap();
    assert_eq!(s.export_target(group(21), 65536).unwrap(), left);
    assert_eq!(
        s.read_at(
            9,
            SourceQuery::Data(RoutedQuery {
                hint: hint(1),
                key: vec![1],
                query: vec![1]
            })
        )
        .unwrap(),
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert!(s
        .read_at(
            10,
            SourceQuery::Data(RoutedQuery {
                hint: hint(1),
                key: vec![1],
                query: vec![1]
            })
        )
        .is_err());
}
#[test]
fn checkpoint_and_wal_replay_preserve_fence_intent_and_original_export() {
    let mut s = ready();
    let log = [
        entry(2, 1, data(1, 7)),
        entry(3, 200, freeze()),
        noop(4),
        entry(5, 200, freeze()),
    ];
    s.apply_batch(&log).unwrap();
    let image = s.export_target(group(21), 65536).unwrap();
    let bytes = s.checkpoint(100000).unwrap();
    assert!(s.checkpoint(bytes.len() - 1).is_err());
    let mut restored = fresh();
    restored.restore_checkpoint(1, 5, &bytes).unwrap();
    assert_eq!(restored.checkpoint(100000).unwrap(), bytes);
    restored
        .apply_batch(&[noop(6), entry(7, 200, freeze())])
        .unwrap();
    assert_eq!(restored.export_target(group(21), 65536).unwrap(), image);
    let mut replay = ready();
    replay.apply_batch(&log).unwrap();
    assert_eq!(replay.checkpoint(100000).unwrap(), bytes);
    assert_eq!(replay.frozen_intent(), Some(&intent()));
}
#[test]
fn rejects_key_mismatch_unbound_fence_and_partial_batch_without_mutation() {
    let mut s = ready();
    let original = s.checkpoint(100000).unwrap();
    let payload = encode_add(&[200], 3, b"", 1024).unwrap();
    let wrong = encode_routed(hint(1), &[1], &payload, 4096).unwrap();
    assert!(s
        .validate_proposal(op(2), &wrong, std::iter::empty())
        .is_err());
    assert!(s
        .apply_batch(&[entry(2, 1, data(1, 7)), entry(3, 2, wrong)])
        .is_err());
    assert_eq!(s.checkpoint(100000).unwrap(), original);
    assert!(s
        .apply_batch(&[entry(2, 200, encode_fence(grant().input().epoch))])
        .is_err());
    assert!(s
        .apply_batch(&[entry(2, 100, s.routed().bootstrap_command(100000).unwrap())])
        .is_err());
    assert!(s.export_target(group(21), 65536).is_err());
}
#[test]
fn export_budget_binding_refuses_before_fence_and_pending_data_is_checked() {
    let mut tiny = TransferSource::new(routed(), 1).unwrap_or_else(|r| panic!("{:?}", r.0));
    tiny.apply_batch(&[entry(1, 100, tiny.bootstrap_command(100000).unwrap())])
        .unwrap();
    let original = tiny.checkpoint(100000).unwrap();
    assert!(tiny
        .validate_proposal(op(200), &freeze(), std::iter::empty())
        .is_err());
    assert!(tiny.apply_batch(&[entry(2, 200, freeze())]).is_err());
    assert_eq!(tiny.checkpoint(100000).unwrap(), original);
    let mut normal = fresh();
    assert!(normal
        .apply_batch(&[entry(1, 100, tiny.bootstrap_command(100000).unwrap())])
        .is_err());
    let s = ready();
    let pending = data(1, 7);
    assert!(s
        .validate_proposal(
            op(200),
            &freeze(),
            [(op(1), pending.as_slice())].into_iter()
        )
        .is_ok());
    assert!(s
        .validate_proposal(
            op(3),
            &data(1, 1),
            [(op(200), freeze().as_slice())].into_iter()
        )
        .is_err());
    assert!(s
        .validate_proposal(
            op(200),
            &freeze(),
            std::iter::repeat_n((op(1), pending.as_slice()), MAX_ROUTED_PENDING + 1)
        )
        .is_err());
}
#[test]
fn all_truncations_changed_intent_and_checkpoint_bindings_refuse_atomically() {
    let mut s = ready();
    let original = s.checkpoint(100000).unwrap();
    let command = freeze();
    for end in 0..command.len() {
        assert!(s
            .apply_batch(&[entry(2, 200, command[..end].to_vec())])
            .is_err());
        assert_eq!(s.checkpoint(100000).unwrap(), original);
    }
    s.apply_batch(&[entry(2, 200, command)]).unwrap();
    let bytes = s.checkpoint(100000).unwrap();
    for end in 0..bytes.len() {
        let mut r = fresh();
        let old = r.checkpoint(100000).unwrap();
        assert!(r.restore_checkpoint(1, 2, &bytes[..end]).is_err());
        assert_eq!(r.checkpoint(100000).unwrap(), old);
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(fresh().restore_checkpoint(1, 2, &trailing).is_err());
    assert!(fresh().restore_checkpoint(2, 2, &bytes).is_err());
    assert!(fresh().restore_checkpoint(1, 3, &bytes).is_err());
    let mut changed = intent().after().clone().into_input();
    changed.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(group(23)),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(24)),
        },
    ]);
    let changed =
        TransferIntent::new(grant(), ResponsibilityManifest::new(changed).unwrap()).unwrap();
    let command = Source::freeze_command(&changed, 32776).unwrap();
    assert_eq!(
        s.apply_batch(&[entry(3, 200, command)]).unwrap()[0].outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    );
    assert_eq!(s.frozen_intent(), Some(&intent()));
}
#[test]
fn constructor_returns_original_application_and_provider_bound_covers_full_lifetime() {
    let app = routed();
    let binding = app.bootstrap_command(100000).unwrap();
    let (_, returned) = match TransferSource::new(app, 0) {
        Ok(_) => panic!("bad capacity"),
        Err(r) => r,
    };
    assert_eq!(returned.bootstrap_command(100000).unwrap(), binding);
    let mut s = ready();
    let bound = s
        .routed()
        .application()
        .export_scope_bound(range(0, 128))
        .unwrap();
    s.apply_batch(&[entry(2, 1, data(1, 7)), entry(3, 200, freeze())])
        .unwrap();
    assert_eq!(
        s.routed()
            .application()
            .export_scope_bound(range(0, 128))
            .unwrap(),
        bound
    );
    assert!(
        s.export_target(group(21), bound)
            .unwrap()
            .payload_capacity()
            <= bound
    );
    assert!(s.export_target(group(21), 1).is_err());
    assert!(s.export_target(group(99), 65536).is_err());
}

#[test]
fn status_query_is_prefix_checked_and_charges_fixed_and_nested_capacity() {
    let mut s = ready();
    assert_eq!(
        s.read_at(1, SourceQuery::Freeze).unwrap(),
        SourceRead::Freeze(None)
    );
    assert!(s.read_at(2, SourceQuery::Freeze).is_err());
    s.apply_batch(&[entry(2, 200, freeze())]).unwrap();
    let result = s.read_at(2, SourceQuery::Freeze).unwrap();
    let bound = s.read_result_bound(&SourceQuery::Freeze).unwrap();
    let nested = s.read_result_bytes(&result, 65536).unwrap();
    assert_eq!(bound, std::mem::size_of_val(&result) + nested);
    assert!(s.read_result_bytes(&result, nested - 1).is_err());
    assert_eq!(s.query_bytes(&SourceQuery::Freeze, 0), Ok(0));
    let SourceRead::Freeze(Some(status)) = result else {
        panic!("missing freeze status")
    };
    assert_eq!(status.fence, s.fence().unwrap());
    assert_eq!(status.intent, intent());
}
