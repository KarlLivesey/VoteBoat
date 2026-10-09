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
    application::*, bucket_counter::*, identity::*, log::*, routed::*, routing::*, scope::*,
    scoped_source::*, transfer::ContentDigest,
};
#[path = "transfer_source/fixtures.rs"]
mod fixtures;
use fixtures::*;
type Scoped = ScopedTransferSource<BucketCounter<Policy>, Policy>;
fn selected(budget: usize) -> Scoped {
    ScopedTransferSource::new(
        routed()
            .with_scoped_fencing(2)
            .unwrap_or_else(|e| panic!("{:?}", e.0)),
        budget,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0))
}
fn ready_scoped(budget: usize) -> Scoped {
    let mut s = selected(budget);
    s.apply_batch(&[entry(1, 100, s.bootstrap_command(100000).unwrap())])
        .unwrap();
    s
}
fn fence(a: u16, b: u16) -> Vec<u8> {
    encode_scope_fence(grant().input().epoch, range(a, b))
}
fn read(s: &Scoped, key: u8) -> ScopedSourceRead<i64> {
    s.read_at(
        s.applied_index(),
        ScopedSourceQuery::Data(RoutedQuery {
            hint: hint(key),
            key: vec![key],
            query: vec![key],
        }),
    )
    .unwrap()
}
#[test]
fn immutable_export_stays_at_original_f_while_retained_data_and_retries_advance() {
    let mut s = ready_scoped(65536);
    s.apply_batch(&[
        entry(2, 1, data(1, 7)),
        entry(3, 2, data(200, 11)),
        entry(4, 200, fence(0, 128)),
    ])
    .unwrap();
    let image = s.export(op(200), 65536).unwrap();
    let digest = ContentDigest::scope_image(&image);
    assert_eq!(image.source_applied(), 4);
    let remaining = s.remaining_export_bytes();
    s.apply_batch(&[
        entry(5, 3, data(200, 2)),
        entry(6, 2, data(200, 11)),
        noop(7),
        entry(8, 200, fence(0, 128)),
        entry(9, 4, data(1, 30)),
    ])
    .unwrap();
    assert_eq!(s.export(op(200), 65536).unwrap(), image);
    assert_eq!(s.remaining_export_bytes(), remaining);
    assert_eq!(
        read(&s, 200),
        ScopedSourceRead::Data(RoutedRead::Served(13))
    );
    assert_eq!(
        read(&s, 1),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    let ScopedSourceRead::Frozen(Some(status)) =
        s.read_at(9, ScopedSourceQuery::Frozen(op(200))).unwrap()
    else {
        panic!("freeze")
    };
    assert_eq!(status.fence.fence.index, 4);
    assert_eq!(status.digest, digest);
    assert_eq!(s.query_bytes(&ScopedSourceQuery::Frozen(op(200)), 0), Ok(0));
    let result = ScopedSourceRead::Frozen(Some(status));
    assert_eq!(s.read_result_bytes(&result, 0), Ok(0));
    assert_eq!(
        s.read_result_bound(&ScopedSourceQuery::Frozen(op(200)))
            .unwrap(),
        std::mem::size_of_val(&result)
    );
    assert!(s.read_at(10, ScopedSourceQuery::Frozen(op(200))).is_err());
    assert!(s.export(op(200), 1).is_err());
    assert!(s.export(op(201), 65536).is_err());
    // Provider import is staging data only, and grants no ownership/activation.
    let mut imported = BucketCounter::new(range(0, 128), Policy, bucket_limits()).unwrap();
    imported
        .import_scopes(std::slice::from_ref(&image), 1)
        .unwrap();
    let retry = imported
        .apply_batch(&[entry(2, 1, encode_add(&[1], 7, b"effect", 1024).unwrap())])
        .unwrap()
        .remove(0);
    assert!(retry.duplicate);
    assert_eq!(imported.value(&[1]), Ok(7));
    assert_eq!(imported.outbox().count(), 1);
    let bytes = s.checkpoint(100000).unwrap();
    let mut recovered = selected(65536);
    recovered.restore_checkpoint(1, 9, &bytes).unwrap();
    assert_eq!(recovered.checkpoint(100000).unwrap(), bytes);
    assert_eq!(recovered.remaining_export_bytes(), remaining);
    assert_eq!(recovered.export(op(200), 65536).unwrap(), image);
    recovered
        .apply_batch(&[entry(10, 5, data(200, 3))])
        .unwrap();
    assert_eq!(
        read(&recovered, 200),
        ScopedSourceRead::Data(RoutedRead::Served(16))
    );
    assert_eq!(recovered.export(op(200), 65536).unwrap(), image);
    recovered
        .apply_batch(&[entry(11, 300, encode_fence(grant().input().epoch))])
        .unwrap();
    assert_eq!(
        read(&recovered, 200),
        ScopedSourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(recovered.export(op(200), 65536).unwrap(), image);
    let whole = recovered.checkpoint(100000).unwrap();
    let mut retired = selected(65536);
    retired.restore_checkpoint(1, 11, &whole).unwrap();
    assert_eq!(retired.checkpoint(100000).unwrap(), whole);
    assert_eq!(retired.export(op(200), 65536).unwrap(), image);
}
#[test]
fn bounded_export_reservations_and_pending_conflicts_reject_without_fencing() {
    let bound = routed()
        .application()
        .export_scope_bound(range(0, 128))
        .unwrap();
    let mut s = ready_scoped(bound);
    let f = fence(0, 128);
    let next = fence(128, 256);
    assert!(s
        .validate_proposal(op(201), &next, [(op(200), f.as_slice())].into_iter())
        .is_err());
    let original = s.checkpoint(100000).unwrap();
    assert!(s
        .apply_batch(&[entry(2, 200, f.clone()), entry(3, 201, next.clone())])
        .is_err());
    assert_eq!(s.checkpoint(100000).unwrap(), original);
    assert!(s.routed().scoped_fences().is_empty());
    s.apply_batch(&[entry(2, 200, f.clone())]).unwrap();
    assert_eq!(s.remaining_export_bytes(), 0);
    let image = s.checkpoint(100000).unwrap();
    let mut reopened = selected(bound);
    reopened.restore_checkpoint(1, 2, &image).unwrap();
    assert_eq!(reopened.remaining_export_bytes(), 0);
    assert!(reopened
        .validate_proposal(op(201), &next, std::iter::empty())
        .is_err());
    assert!(reopened
        .validate_proposal(op(200), &f, std::iter::empty())
        .is_ok());
    let kept = data(200, 2);
    assert!(reopened
        .validate_proposal(op(3), &kept, std::iter::empty())
        .is_ok());
    let payload = encode_add(&[200], 7, b"", 1024).unwrap();
    let wrong = encode_routed(hint(1), &[1], &payload, 4096).unwrap();
    assert!(s
        .validate_proposal(op(4), &wrong, std::iter::empty())
        .is_err());
    assert!(s.apply_batch(&[entry(3, 4, wrong)]).is_err());
    assert_eq!(s.checkpoint(100000).unwrap(), image);
    // Lifecycle identities must not hide retained provider data operations.
    let mut existing = ready_scoped(65536);
    existing.apply_batch(&[entry(2, 1, data(200, 7))]).unwrap();
    assert!(existing
        .validate_proposal(op(1), &f, std::iter::empty())
        .is_err());
    assert!(existing.apply_batch(&[entry(3, 1, f)]).is_err());
}
#[test]
fn checkpoint_images_profiles_and_contradictory_reservations_refuse_atomically() {
    assert!(ScopedTransferSource::new(routed(), 65536).is_err());
    assert!(ScopedTransferSource::new(
        routed()
            .with_scoped_fencing(1)
            .unwrap_or_else(|_| panic!("profile")),
        0
    )
    .is_err());
    let mut s = ready_scoped(65536);
    s.apply_batch(&[
        entry(2, 1, data(1, 7)),
        entry(3, 200, fence(0, 128)),
        entry(4, 2, data(200, 11)),
    ])
    .unwrap();
    let image = s.checkpoint(100000).unwrap();
    assert!(s.checkpoint(image.len() - 1).is_err());
    assert!(selected(65535).restore_checkpoint(1, 4, &image).is_err());
    for len in 0..image.len() {
        assert!(s.restore_checkpoint(1, 4, &image[..len]).is_err());
        assert_eq!(s.checkpoint(100000).unwrap(), image);
    }
    let inner_len = u32::from_le_bytes(image[24..28].try_into().unwrap()) as usize;
    let record = 28 + inner_len + 2;
    let bound = routed()
        .application()
        .export_scope_bound(range(0, 128))
        .unwrap();
    let mut inflated = image.clone();
    inflated[record..record + 8].copy_from_slice(&((bound + 1) as u64).to_le_bytes());
    assert!(s.restore_checkpoint(1, 4, &inflated).is_err());
    for (offset, bytes) in [
        (record, 1u64.to_le_bytes().to_vec()),
        (record + 8, 201u128.to_le_bytes().to_vec()),
        (record + 24, vec![0; 32]),
        (record + 88, 2u64.to_le_bytes().to_vec()),
    ] {
        let mut bad = image.clone();
        bad[offset..offset + bytes.len()].copy_from_slice(&bytes);
        assert!(s.restore_checkpoint(1, 4, &bad).is_err());
        assert_eq!(s.checkpoint(100000).unwrap(), image);
    }
    let mut bad = image.clone();
    *bad.last_mut().unwrap() ^= 1;
    assert!(s.restore_checkpoint(1, 4, &bad).is_err());
    assert_eq!(s.checkpoint(100000).unwrap(), image);
}
#[cfg(feature = "native")]
mod support;
#[cfg(feature = "native")]
#[test]
fn native_frame_cuts_keep_fence_and_original_image_together_with_retained_service() {
    use support::{Fault, ModelIo};
    use voteboat::native::log_store::*;
    let limits = LogLimits::default();
    let seed = || {
        let io = ModelIo::default();
        let mut log = NativeLogStore::create(io.clone(), support::identity(1), limits).unwrap();
        support::append(
            &mut log,
            vec![LogMutation::Create(support::bootstrap(20, 3))],
        );
        let app = selected(65536);
        let state = log.state(group(20)).unwrap();
        support::append(
            &mut log,
            vec![support::update(
                &state,
                1,
                3,
                Some(Suffix {
                    from: 1,
                    entries: vec![
                        entry(1, 100, app.bootstrap_command(100000).unwrap()),
                        entry(2, 1, data(1, 7)),
                        entry(3, 2, data(200, 11)),
                    ],
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
            entries: vec![entry(4, 200, fence(0, 128))],
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
        let mut app = selected(65536);
        app.apply_batch(
            &state
                .entries
                .into_iter()
                .filter(|e| e.index <= state.commit_index)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        if state.commit_index == 3 {
            old = true;
            assert!(app.export(op(200), 65536).is_err());
            assert!(app.routed().scoped_fences().is_empty());
        } else {
            complete = true;
            assert_eq!(state.commit_index, 4);
            assert_eq!(app.export(op(200), 65536).unwrap().source_applied(), 4);
        }
        let before = app.export(op(200), 65536).ok();
        app.apply_batch(&[entry(state.commit_index + 1, 3, data(200, 2))])
            .unwrap();
        assert_eq!(
            read(&app, 200),
            ScopedSourceRead::Data(RoutedRead::Served(13))
        );
        assert_eq!(app.export(op(200), 65536).ok(), before);
    }
    assert!(old && complete);
}

#[derive(Clone)]
struct HostProvider {
    inner: BucketCounter<Policy>,
    wrong_boundary: bool,
}
impl StateMachine for HostProvider {
    type Receipt = BucketReceipt;
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(&mut self, e: &[LogEntry]) -> Result<Vec<Self::Receipt>, ApplicationError> {
        self.inner.apply_batch(e)
    }
}
impl BoundedStateMachine for HostProvider {
    fn receipt_bytes_bound(&self, e: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.inner.receipt_bytes_bound(e)
    }
}
impl ProposalAdmission for HostProvider {
    fn validate_proposal<'a>(
        &self,
        op: OperationId,
        b: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.inner.validate_proposal(op, b, pending)
    }
}
impl CheckpointStateMachine for HostProvider {
    fn schema_version(&self) -> u64 {
        self.inner.schema_version()
    }
    fn checkpoint(&self, n: usize) -> Result<Vec<u8>, ApplicationError> {
        self.inner.checkpoint(n)
    }
    fn restore_checkpoint(&mut self, s: u64, a: u64, b: &[u8]) -> Result<(), ApplicationError> {
        self.inner.restore_checkpoint(s, a, b)
    }
}
impl ScopeStateMachine for HostProvider {
    fn scope(&self) -> BucketRange {
        self.inner.scope()
    }
    fn scheme(&self) -> PartitionScheme {
        self.inner.scheme()
    }
    fn command_key<'a>(&self, b: &'a [u8]) -> Result<&'a [u8], ApplicationError> {
        self.inner.command_key(b)
    }
    fn contains_operation(&self, id: OperationId) -> bool {
        self.inner.contains_operation(id)
    }
    fn export_scope_bound(&self, r: BucketRange) -> Result<usize, ApplicationError> {
        self.inner.export_scope_bound(r)
    }
    fn export_scope(&self, r: BucketRange, max: usize) -> Result<ScopeImage, ApplicationError> {
        let image = self.inner.export_scope(r, max)?;
        let mut bytes = Vec::with_capacity(image.bytes().len() + 128);
        bytes.extend(image.bytes());
        ScopeImage::new(
            image.schema(),
            image.scheme(),
            image.scope(),
            image.source_applied() + u64::from(self.wrong_boundary),
            bytes,
        )
        .map_err(|e| e.0)
    }
    fn import_scopes(&mut self, images: &[ScopeImage], i: u64) -> Result<(), ApplicationError> {
        self.inner.import_scopes(images, i)
    }
}
#[test]
fn host_provider_padded_images_have_stable_reservations_and_bad_boundary_never_fences() {
    let make = |wrong_boundary| {
        let base = routed();
        let host = HostProvider {
            inner: BucketCounter::new(range(0, 256), Policy, bucket_limits()).unwrap(),
            wrong_boundary,
        };
        let routed = RoutedApplication::new(group(20), grant(), host, Policy, base.limits())
            .unwrap_or_else(|e| panic!("{:?}", e.error))
            .with_scoped_fencing(2)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
        ScopedTransferSource::new(routed, 65536).unwrap_or_else(|e| panic!("{:?}", e.0))
    };
    let mut good = make(false);
    good.apply_batch(&[
        entry(1, 100, good.bootstrap_command(100000).unwrap()),
        entry(2, 1, data(1, 7)),
        entry(3, 200, fence(0, 128)),
    ])
    .unwrap();
    let remaining = good.remaining_export_bytes();
    let image = good.export(op(200), 65536).unwrap();
    let checkpoint = good.checkpoint(100000).unwrap();
    let mut restored = make(false);
    restored.restore_checkpoint(1, 3, &checkpoint).unwrap();
    assert_eq!(restored.remaining_export_bytes(), remaining);
    assert_eq!(restored.export(op(200), 65536).unwrap(), image);
    good.apply_batch(&[entry(4, 2, data(200, 11))]).unwrap();
    restored.apply_batch(&[entry(4, 2, data(200, 11))]).unwrap();
    assert_eq!(good.remaining_export_bytes(), remaining);
    assert_eq!(
        good.checkpoint(100000).unwrap(),
        restored.checkpoint(100000).unwrap()
    );
    let mut bad = make(true);
    bad.apply_batch(&[
        entry(1, 100, bad.bootstrap_command(100000).unwrap()),
        entry(2, 1, data(1, 7)),
    ])
    .unwrap();
    let original = bad.checkpoint(100000).unwrap();
    assert!(bad
        .validate_proposal(op(200), &fence(0, 128), std::iter::empty())
        .is_err());
    assert!(bad.apply_batch(&[entry(3, 200, fence(0, 128))]).is_err());
    assert_eq!(bad.checkpoint(100000).unwrap(), original);
    assert!(bad.routed().scoped_fences().is_empty());
    assert!(bad.export(op(200), 65536).is_err());
}
