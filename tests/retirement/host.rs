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
use super::{fixture, source_fixture as base};
use std::sync::Arc;
use voteboat::{
    application::*, bucket_counter::*, identity::*, log::*, retirement::*, routed::*, routing::*,
    scope::*, transfer_source::*,
};

// The host's provider has an owned live-state resource, allocated only after
// data appears. The guard's immutable initial template therefore cannot retain
// it. Clones share this diagnostic lifetime token, never mutable data state.
#[derive(Clone)]
struct Host {
    inner: BucketCounter<base::Policy>,
    live: Option<Arc<()>>,
}
impl Host {
    fn note_live(&mut self) {
        if self.inner.applied_index() >= 2 && self.live.is_none() {
            self.live = Some(Arc::new(()));
        }
    }
}
impl StateMachine for Host {
    type Receipt = BucketReceipt;
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<BucketReceipt>, ApplicationError> {
        let result = self.inner.apply_batch(entries)?;
        self.note_live();
        Ok(result)
    }
}
impl BoundedStateMachine for Host {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.inner.receipt_bytes_bound(entries)
    }
}
impl ProposalAdmission for Host {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.inner.validate_proposal(operation, bytes, pending)
    }
}
impl CheckpointStateMachine for Host {
    fn schema_version(&self) -> u64 {
        self.inner.schema_version()
    }
    fn checkpoint(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.inner.checkpoint(limit)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.inner.restore_checkpoint(schema, index, bytes)?;
        self.note_live();
        Ok(())
    }
}
impl ReadableStateMachine for Host {
    type Query = Vec<u8>;
    type ReadResult = i64;
    fn read_at(&self, index: u64, query: Vec<u8>) -> Result<i64, ApplicationError> {
        self.inner.read_at(index, query)
    }
}
impl BoundedReadableStateMachine for Host {
    fn query_bytes(&self, q: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        self.inner.query_bytes(q, limit)
    }
    fn read_result_bound(&self, q: &Vec<u8>) -> Result<usize, ApplicationError> {
        self.inner.read_result_bound(q)
    }
    fn read_result_bytes(&self, r: &i64, limit: usize) -> Result<usize, ApplicationError> {
        self.inner.read_result_bytes(r, limit)
    }
}
impl ScopeStateMachine for Host {
    fn scope(&self) -> BucketRange {
        self.inner.scope()
    }
    fn scheme(&self) -> PartitionScheme {
        self.inner.scheme()
    }
    fn command_key<'a>(&self, command: &'a [u8]) -> Result<&'a [u8], ApplicationError> {
        self.inner.command_key(command)
    }
    fn contains_operation(&self, operation: OperationId) -> bool {
        self.inner.contains_operation(operation)
    }
    fn export_scope_bound(&self, scope: BucketRange) -> Result<usize, ApplicationError> {
        self.inner.export_scope_bound(scope)
    }
    fn export_scope(
        &self,
        scope: BucketRange,
        limit: usize,
    ) -> Result<ScopeImage, ApplicationError> {
        self.inner.export_scope(scope, limit)
    }
    fn import_scopes(&mut self, images: &[ScopeImage], index: u64) -> Result<(), ApplicationError> {
        self.inner.import_scopes(images, index)?;
        self.note_live();
        Ok(())
    }
}
fn fresh() -> RetirementGuard<TransferSource<Host, base::Policy>> {
    let routed = RoutedApplication::new(
        base::group(20),
        base::grant(),
        Host {
            inner: BucketCounter::new(base::range(0, 256), base::Policy, base::bucket_limits())
                .unwrap(),
            live: None,
        },
        base::Policy,
        RoutedLimits {
            operations: 32,
            semantic_bytes: 8192,
            payload_bytes: 1024,
            inner_checkpoint_bytes: base::bucket_limits().checkpoint_bound().unwrap(),
        },
    )
    .unwrap_or_else(|e| panic!("{:?}", e.error));
    RetirementGuard::new(TransferSource::new(routed, 65536).unwrap_or_else(|e| panic!("{:?}", e.0)))
        .unwrap_or_else(|e| panic!("{:?}", e.0))
}
#[test]
fn host_provider_lifetime_ends_only_on_successful_retirement_and_stays_absent_on_restore() {
    let (reference, targets, decision) = fixture::active_split();
    let proof = fixture::split_proof(&reference, &targets, &decision);
    let mut source = fresh();
    source.apply_batch(&fixture::source_log()).unwrap();
    assert_eq!(
        source.freeze_status().unwrap(),
        reference.freeze_status().unwrap()
    );
    for g in [21, 22] {
        assert_eq!(
            source.export_target(base::group(g), 65536).unwrap(),
            reference.export_target(base::group(g), 65536).unwrap()
        );
    }
    let witness = Arc::downgrade(
        source
            .owner()
            .unwrap()
            .routed()
            .application()
            .live
            .as_ref()
            .unwrap(),
    );
    let bytes = source
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    source
        .validate_proposal(base::op(200), &bytes, std::iter::empty())
        .unwrap();
    assert!(witness.upgrade().is_some());
    assert!(source
        .apply_batch(&[base::entry(5, 200, bytes.clone()), base::noop(7)])
        .is_err());
    assert!(witness.upgrade().is_some());
    source.apply_batch(&[base::entry(5, 200, bytes)]).unwrap();
    assert!(witness.upgrade().is_none());
    let cp = source.checkpoint(500000).unwrap();
    let mut recovered = fresh();
    recovered
        .restore_checkpoint(RETIREMENT_GUARD_SCHEMA, 5, &cp)
        .unwrap();
    assert!(recovered.owner().is_none());
    assert_eq!(recovered.status(), source.status());
    assert_eq!(
        recovered.freeze_status().unwrap(),
        source.freeze_status().unwrap()
    );
}
