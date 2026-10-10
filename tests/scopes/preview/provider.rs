// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[derive(Clone)]
struct View {
    inner: BucketCounter<HostPolicy>,
    schema: u64,
    bound: Option<usize>,
}
impl StateMachine for View {
    type Receipt = BucketReceipt;
    fn applied_index(&self) -> u64 {
        self.inner.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Self::Receipt>, ApplicationError> {
        self.inner.apply_batch(entries)
    }
}
impl CheckpointStateMachine for View {
    fn schema_version(&self) -> u64 {
        self.schema
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.inner.checkpoint(max)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.inner.restore_checkpoint(schema, index, bytes)
    }
}
impl ScopeStateMachine for View {
    fn scope(&self) -> BucketRange {
        self.inner.scope()
    }
    fn scheme(&self) -> PartitionScheme {
        self.inner.scheme()
    }
    fn command_key<'a>(&self, c: &'a [u8]) -> Result<&'a [u8], ApplicationError> {
        self.inner.command_key(c)
    }
    fn contains_operation(&self, o: OperationId) -> bool {
        self.inner.contains_operation(o)
    }
    fn export_scope_bound(&self, s: BucketRange) -> Result<usize, ApplicationError> {
        self.bound
            .map_or_else(|| self.inner.export_scope_bound(s), Ok)
    }
    fn export_scope(&self, _: BucketRange, _: usize) -> Result<ScopeImage, ApplicationError> {
        panic!("preview must not export data")
    }
    fn import_scopes(&mut self, _: &[ScopeImage], _: u64) -> Result<(), ApplicationError> {
        panic!("preview must not import data")
    }
}
#[test]
fn rejects_incompatible_schemas_and_impossible_provider_bounds_without_copying_data() {
    let intent = intent();
    let (config, replicas) = configuration();
    let mut source_app = View {
        inner: fresh(range(0, 256)),
        schema: 17,
        bound: None,
    };
    let mut apps = [
        View {
            inner: fresh(range(0, 128)),
            schema: 18,
            bound: None,
        },
        View {
            inner: fresh(range(128, 256)),
            schema: 17,
            bound: None,
        },
    ];
    assert_eq!(
        preview_transfer(
            &intent,
            &[source(&intent, &source_app)],
            &targets(&intent, &apps, &config, &replicas),
            100000
        ),
        Err(PreviewError::IncompatibleApplication(group(21)))
    );
    apps[0].schema = 17;
    assert!(preview_transfer(
        &intent,
        &[source(&intent, &source_app)],
        &targets(&intent, &apps, &config, &replicas),
        100000
    )
    .is_ok());
    for invalid in [0, usize::MAX] {
        source_app.bound = Some(invalid);
        assert_eq!(
            preview_transfer(
                &intent,
                &[source(&intent, &source_app)],
                &targets(&intent, &apps, &config, &replicas),
                100000
            ),
            Err(PreviewError::Budget)
        );
    }
}
