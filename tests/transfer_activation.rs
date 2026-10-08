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
    application::*, bucket_counter::*, identity::*, routed::*, routing::*, transfer_publication::*,
    transfer_target::*,
};
#[path = "transfer_target/fixtures.rs"]
mod fixture;
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
use source_fixture::{entry, noop, op};
fn observation() -> TargetActivation {
    TargetActivation {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: TransferPublicationStatus {
            publication_operation: op(201),
            index: 4,
            publication: fixture::publication(),
        },
    }
}
fn imported() -> fixture::Target {
    let mut t = fixture::ready();
    t.apply_batch(&[entry(2, 200, fixture::load())]).unwrap();
    t
}
fn active() -> fixture::Target {
    let mut t = imported();
    let command = t.activation_command(&observation(), 65536).unwrap();
    t.apply_batch(&[entry(3, 200, command)]).unwrap();
    t
}
fn hint(key: u8) -> RouteHint {
    let mut h = source_fixture::hint(key);
    h.group = source_fixture::group(if key < 128 { 21 } else { 22 });
    h.scope = if key < 128 {
        source_fixture::range(0, 128)
    } else {
        source_fixture::range(128, 256)
    };
    h.epoch = OwnershipEpoch::new(2).unwrap();
    h.generation = RouteGeneration::new(2).unwrap();
    h
}
fn data(key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn query(key: u8) -> TargetQuery<Vec<u8>> {
    TargetQuery::Data(RoutedQuery {
        hint: hint(key),
        key: vec![key],
        query: vec![key],
    })
}
#[test]
fn activation_requires_exact_import_and_cannot_be_inferred_from_new_epoch() {
    let mut t = fixture::fresh();
    let command = imported()
        .activation_command(&observation(), 65536)
        .unwrap();
    for stage in 0..2 {
        let before = t.checkpoint(200000).unwrap();
        assert!(t.activation_command(&observation(), 65536).is_err());
        assert!(t
            .validate_proposal(op(200), &command, std::iter::empty())
            .is_err());
        assert!(t
            .apply_batch(&[entry(t.applied_index() + 1, 200, command.clone())])
            .is_err());
        assert_eq!(t.checkpoint(200000).unwrap(), before);
        assert_eq!(
            t.read_at(t.applied_index(), query(1)).unwrap(),
            TargetRead::NotActive
        );
        if stage == 0 {
            t.apply_batch(&[entry(1, 200, t.bootstrap_command(65536).unwrap())])
                .unwrap();
        }
    }
    let mut t = imported();
    assert_eq!(t.read_at(2, query(1)).unwrap(), TargetRead::NotActive);
    assert!(t
        .validate_proposal(op(3), &data(1, 1), std::iter::empty())
        .is_err());
    let mut changed = observation();
    changed.decision.index = 0;
    assert!(t.activation_command(&changed, 65536).is_err());
    changed = observation();
    changed.decision.publication_operation = op(200);
    assert!(t.activation_command(&changed, 65536).is_err());
    // A structurally valid different import observation must not authorize us.
    let original = observation();
    let p = &original.decision.publication;
    let mut targets = p.targets().to_vec();
    targets[0].imported.index += 1;
    changed = original.clone();
    changed.decision.publication = TransferPublication::new(
        p.operation(),
        p.intent().clone(),
        p.sources().to_vec(),
        targets,
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    assert!(t.activation_command(&changed, 65536).is_err());
    let receipt = t.apply_batch(&[entry(3, 999, command)]).unwrap().remove(0);
    assert_eq!(receipt.outcome, TargetOutcome::OperationConflict);
    assert!(t.status().activated.is_none());
}
#[test]
fn original_activation_retry_and_imported_data_results_survive_writes() {
    let mut t = imported();
    let command = t.activation_command(&observation(), 65536).unwrap();
    let entries = [
        entry(3, 200, command.clone()),
        entry(4, 1, data(1, 7)),
        entry(5, 3, data(1, 2)),
        entry(6, 200, command.clone()),
        noop(7),
    ];
    let bound = t.receipt_bytes_bound(&entries).unwrap();
    let receipts = t.apply_batch(&entries).unwrap();
    assert_eq!(
        bound,
        receipts.len() * std::mem::size_of::<TargetReceipt<BucketReceipt>>()
    );
    assert!(receipts.iter().all(|r| r.nested_bytes(0) == Ok(0)));
    let status = t.status().activated.unwrap();
    assert_eq!(status.index, 3);
    assert_eq!(status.publication_index, 4);
    assert_eq!(receipts[0].outcome, TargetOutcome::Activated(status));
    assert_eq!(receipts[3].outcome, TargetOutcome::Activated(status));
    let TargetOutcome::Applied(retry) = &receipts[1].outcome else {
        panic!("retry")
    };
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, BucketOutcome::Value(7));
    assert_eq!(t.read_at(7, query(1)).unwrap(), TargetRead::Data(9));
    assert_eq!(t.application().outbox().count(), 2);
    let mut changed = observation();
    changed.metadata_configuration = ConfigurationId::new(2).unwrap();
    let different = t.activation_command(&changed, 65536).unwrap();
    assert!(t
        .validate_proposal(op(200), &different, std::iter::empty())
        .is_err());
    assert_eq!(
        t.apply_batch(&[entry(8, 200, different)]).unwrap()[0].outcome,
        TargetOutcome::OperationConflict
    );
    assert_eq!(t.status().activated, Some(status));
}
#[test]
fn active_routes_payload_keys_and_imported_operation_ids_cannot_cross_ownership() {
    let mut t = active();
    for (i, bytes) in [source_fixture::data(1, 1), data(200, 1)]
        .into_iter()
        .enumerate()
    {
        assert!(t
            .validate_proposal(op(3), &bytes, std::iter::empty())
            .is_err());
        assert!(matches!(
            t.apply_batch(&[entry(4 + i as u64, 3, bytes)]).unwrap()[0].outcome,
            TargetOutcome::Rejected(_)
        ));
    }
    assert_eq!(t.application().value(&[1]), Ok(7));
    let before = t.checkpoint(200000).unwrap();
    let mismatch = encode_routed(
        hint(1),
        &[1],
        &encode_add(&[2], 3, b"", 1024).unwrap(),
        4096,
    )
    .unwrap();
    assert!(t
        .validate_proposal(op(3), &mismatch, std::iter::empty())
        .is_err());
    assert!(t.apply_batch(&[entry(6, 3, mismatch)]).is_err());
    assert_eq!(t.checkpoint(200000).unwrap(), before);
    let receipt = t
        .apply_batch(&[entry(6, 1, data(1, 99))])
        .unwrap()
        .remove(0);
    let TargetOutcome::Applied(receipt) = receipt.outcome else {
        panic!("provider conflict")
    };
    assert_eq!(receipt.outcome, BucketOutcome::OperationConflict);
    assert_eq!(t.application().value(&[1]), Ok(7));
    let mut q = hint(1);
    q.epoch = OwnershipEpoch::new(1).unwrap();
    assert_eq!(
        t.read_at(
            6,
            TargetQuery::Data(RoutedQuery {
                hint: q,
                key: vec![1],
                query: vec![1]
            })
        )
        .unwrap(),
        TargetRead::Rejected(RoutingError::EpochMismatch)
    );
    assert!(t.read_at(7, query(1)).is_err());
    assert_eq!(
        t.read_result_bound(&query(1)).unwrap(),
        std::mem::size_of::<TargetRead<i64>>()
    );
}
#[test]
fn pending_phase_projection_admits_activation_then_data_without_mutation() {
    let t = fixture::fresh();
    let bootstrap = t.bootstrap_command(65536).unwrap();
    let load = fixture::load();
    let activate = imported()
        .activation_command(&observation(), 65536)
        .unwrap();
    let pending = vec![
        (op(200), bootstrap.as_slice()),
        (op(200), load.as_slice()),
        (op(200), activate.as_slice()),
    ];
    assert_eq!(
        t.validate_proposal(op(3), &data(1, 2), pending.into_iter())
            .unwrap(),
        std::mem::size_of::<TargetReceipt<BucketReceipt>>()
    );
    assert_eq!(t.applied_index(), 0);
    assert!(t.status().activated.is_none());
    assert!(t
        .validate_proposal(
            op(3),
            &data(1, 2),
            [(op(200), bootstrap.as_slice()), (op(200), load.as_slice())].into_iter()
        )
        .is_err());
    let log = [
        entry(1, 200, bootstrap),
        entry(2, 200, load),
        entry(3, 200, activate),
        entry(4, 3, data(1, 2)),
    ];
    assert_eq!(
        t.receipt_bytes_bound(&log).unwrap(),
        4 * std::mem::size_of::<TargetReceipt<BucketReceipt>>()
    );
}
#[test]
fn activation_checkpoint_truncations_and_wal_replay_preserve_original_authority() {
    let mut t = active();
    let activation = t.activation_command(&observation(), 65536).unwrap();
    t.apply_batch(&[entry(4, 3, data(1, 2)), noop(5)]).unwrap();
    let checkpoint = t.checkpoint(200000).unwrap();
    assert_eq!(&checkpoint[..8], b"VBTRGT03");
    let mut recovered = fixture::fresh();
    let pristine = recovered.checkpoint(200000).unwrap();
    for end in 0..checkpoint.len() {
        assert!(recovered
            .restore_checkpoint(TRANSFER_TARGET_SCHEMA, 5, &checkpoint[..end])
            .is_err());
        assert_eq!(recovered.checkpoint(200000).unwrap(), pristine);
    }
    recovered
        .restore_checkpoint(TRANSFER_TARGET_SCHEMA, 5, &checkpoint)
        .unwrap();
    assert_eq!(recovered.status(), t.status());
    assert_eq!(recovered.read_at(5, query(1)).unwrap(), TargetRead::Data(9));
    assert_eq!(
        recovered
            .apply_batch(&[entry(6, 200, activation.clone())])
            .unwrap()[0]
            .outcome,
        TargetOutcome::Activated(t.status().activated.unwrap())
    );
    let mut replay = fixture::fresh();
    replay
        .apply_batch(&[
            entry(1, 200, replay.bootstrap_command(65536).unwrap()),
            entry(2, 200, fixture::load()),
            entry(3, 200, activation.clone()),
            entry(4, 3, data(1, 2)),
            noop(5),
        ])
        .unwrap();
    assert_eq!(replay.checkpoint(200000).unwrap(), checkpoint);
    let mut inactive = imported();
    let before = inactive.checkpoint(200000).unwrap();
    for end in 0..activation.len() {
        assert!(inactive
            .apply_batch(&[entry(3, 200, activation[..end].to_vec())])
            .is_err());
        assert_eq!(inactive.checkpoint(200000).unwrap(), before);
    }
    assert!(t.checkpoint(checkpoint.len() - 1).is_err());
    assert!(t
        .activation_command(&observation(), activation.len() - 1)
        .is_err());
}
#[test]
fn old_inactive_checkpoint_format_restores_without_activation() {
    let t = imported();
    let mut bytes = t.checkpoint(200000).unwrap();
    let binding = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let load_offset = 20 + binding + 16;
    let load_len =
        u32::from_le_bytes(bytes[load_offset..load_offset + 4].try_into().unwrap()) as usize;
    let activation_offset = load_offset + 4 + load_len;
    bytes.drain(activation_offset + 12..activation_offset + 12 + 36);
    bytes.drain(activation_offset..activation_offset + 12);
    bytes[..8].copy_from_slice(b"VBTRGT01");
    let mut restored = fixture::fresh();
    restored.restore_checkpoint(1, 2, &bytes).unwrap();
    assert_eq!(restored.status(), t.status());
    assert_eq!(
        restored.read_at(2, query(1)).unwrap(),
        TargetRead::NotActive
    );
    restored
        .apply_batch(&[entry(
            3,
            200,
            restored.activation_command(&observation(), 65536).unwrap(),
        )])
        .unwrap();
    assert_eq!(restored.read_at(3, query(1)).unwrap(), TargetRead::Data(7));
}

#[test]
fn old_active_checkpoint_format_retains_activation_and_can_continue_serving() {
    let t = active();
    let mut bytes = t.checkpoint(200000).unwrap();
    let binding = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    let load_offset = 20 + binding + 16;
    let load_len =
        u32::from_le_bytes(bytes[load_offset..load_offset + 4].try_into().unwrap()) as usize;
    let activation_offset = load_offset + 4 + load_len;
    let activation_len = u32::from_le_bytes(
        bytes[activation_offset + 8..activation_offset + 12]
            .try_into()
            .unwrap(),
    ) as usize;
    let boundary_offset = activation_offset + 12 + activation_len;
    bytes.drain(boundary_offset..boundary_offset + 36);
    bytes[..8].copy_from_slice(b"VBTRGT02");
    let mut restored = fixture::fresh();
    assert_eq!(
        restored.restore_checkpoint(TRANSFER_TARGET_SCHEMA, 3, &bytes),
        Err(ApplicationError::UnsupportedSchema)
    );
    restored.restore_checkpoint(1, 3, &bytes).unwrap();
    assert_eq!(restored.status(), t.status());
    assert_eq!(restored.fence(), None);
    assert_eq!(restored.read_at(3, query(1)).unwrap(), TargetRead::Data(7));
    restored.apply_batch(&[entry(4, 3, data(1, 2))]).unwrap();
    assert_eq!(restored.read_at(4, query(1)).unwrap(), TargetRead::Data(9));
}

// Downstream provider exercises non-Copy nested receipts and read results whose
// bounds change after import; no private API or replacement persistence owner.
#[derive(Clone)]
struct Host(voteboat::bucket_counter::BucketCounter<source_fixture::Policy>);
#[derive(Clone, Debug, Eq, PartialEq)]
struct HostReceipt {
    inner: BucketReceipt,
    output: Vec<u8>,
}
impl ApplicationReceipt for HostReceipt {
    fn index(&self) -> u64 {
        self.inner.index
    }
    fn operation(&self) -> OperationId {
        self.inner.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        if self.output.capacity() > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(self.output.capacity())
        }
    }
}
impl Host {
    fn capacity(&self) -> usize {
        if self.0.value(&[1]).unwrap() != 0 {
            32
        } else {
            8
        }
    }
}
impl StateMachine for Host {
    type Receipt = HostReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(
        &mut self,
        entries: &[voteboat::log::LogEntry],
    ) -> Result<Vec<HostReceipt>, ApplicationError> {
        let capacity = self.capacity();
        Ok(self
            .0
            .apply_batch(entries)?
            .into_iter()
            .map(|inner| HostReceipt {
                inner,
                output: Vec::with_capacity(capacity),
            })
            .collect())
    }
}
impl BoundedStateMachine for Host {
    fn receipt_bytes_bound(
        &self,
        entries: &[voteboat::log::LogEntry],
    ) -> Result<usize, ApplicationError> {
        Ok(entries
            .iter()
            .filter(|e| matches!(e.payload, voteboat::log::EntryPayload::Command { .. }))
            .count()
            * (std::mem::size_of::<HostReceipt>() + self.capacity()))
    }
}
impl ProposalAdmission for Host {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)?;
        Ok(std::mem::size_of::<HostReceipt>() + self.capacity())
    }
}
impl CheckpointStateMachine for Host {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(max)
    }
    fn restore_checkpoint(&mut self, s: u64, a: u64, b: &[u8]) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(s, a, b)
    }
}
impl voteboat::scope::ScopeStateMachine for Host {
    fn contains_operation(&self, operation: OperationId) -> bool {
        voteboat::scope::ScopeStateMachine::contains_operation(&self.0, operation)
    }
    fn scope(&self) -> BucketRange {
        voteboat::scope::ScopeStateMachine::scope(&self.0)
    }
    fn scheme(&self) -> PartitionScheme {
        voteboat::scope::ScopeStateMachine::scheme(&self.0)
    }
    fn command_key<'a>(&self, b: &'a [u8]) -> Result<&'a [u8], ApplicationError> {
        voteboat::scope::ScopeStateMachine::command_key(&self.0, b)
    }
    fn export_scope_bound(&self, s: BucketRange) -> Result<usize, ApplicationError> {
        voteboat::scope::ScopeStateMachine::export_scope_bound(&self.0, s)
    }
    fn export_scope(
        &self,
        s: BucketRange,
        max: usize,
    ) -> Result<voteboat::scope::ScopeImage, ApplicationError> {
        voteboat::scope::ScopeStateMachine::export_scope(&self.0, s, max)
    }
    fn import_scopes(
        &mut self,
        i: &[voteboat::scope::ScopeImage],
        at: u64,
    ) -> Result<(), ApplicationError> {
        voteboat::scope::ScopeStateMachine::import_scopes(&mut self.0, i, at)
    }
}
impl ReadableStateMachine for Host {
    type Query = Vec<u8>;
    type ReadResult = Vec<u8>;
    fn read_at(&self, a: u64, q: Vec<u8>) -> Result<Vec<u8>, ApplicationError> {
        let mut result = Vec::with_capacity(16);
        result.extend(self.0.read_at(a, q)?.to_le_bytes());
        Ok(result)
    }
}
impl BoundedReadableStateMachine for Host {
    fn query_bytes(&self, q: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        self.0.query_bytes(q, limit)
    }
    fn read_result_bound(&self, q: &Vec<u8>) -> Result<usize, ApplicationError> {
        self.0.read_result_bound(q)?;
        Ok(std::mem::size_of::<Vec<u8>>() + 16)
    }
    fn read_result_bytes(&self, r: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        if r.capacity() > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(r.capacity())
        }
    }
}
#[test]
fn downstream_provider_accounts_nested_receipts_after_import_and_active_reads() {
    let mut t = TransferTarget::new(
        source_fixture::group(21),
        op(200),
        source_fixture::intent(),
        Host(
            BucketCounter::new(
                source_fixture::range(0, 128),
                source_fixture::Policy,
                source_fixture::bucket_limits(),
            )
            .unwrap(),
        ),
        source_fixture::Policy,
        fixture::limits(),
    )
    .unwrap_or_else(|e| panic!("{:?}", e.0));
    // Same bootstrap bytes because provider persistent schema/image are identical.
    let activation = imported()
        .activation_command(&observation(), 65536)
        .unwrap();
    let entries = [
        entry(1, 200, t.bootstrap_command(65536).unwrap()),
        entry(2, 200, fixture::load()),
        entry(3, 200, activation),
        entry(4, 3, data(1, 2)),
    ];
    assert_eq!(
        t.receipt_bytes_bound(&entries).unwrap(),
        4 * std::mem::size_of::<TargetReceipt<HostReceipt>>() + 32
    );
    let receipts = t.apply_batch(&entries).unwrap();
    assert_eq!(receipts[3].nested_bytes(32), Ok(32));
    assert!(receipts[3].nested_bytes(31).is_err());
    assert_eq!(
        t.validate_proposal(op(4), &data(1, 2), std::iter::empty())
            .unwrap(),
        std::mem::size_of::<TargetReceipt<HostReceipt>>() + 32
    );
    let result = t.read_at(4, query(1)).unwrap();
    assert_eq!(
        t.read_result_bound(&query(1)).unwrap(),
        std::mem::size_of_val(&result) + 16
    );
    assert_eq!(t.read_result_bytes(&result, 16), Ok(16));
    assert!(t.read_result_bytes(&result, 15).is_err());
    let TargetRead::Data(bytes) = result else {
        panic!("host result")
    };
    assert_eq!(i64::from_le_bytes(bytes.try_into().unwrap()), 9);
}

#[test]
fn full_imported_retry_history_keeps_reserved_activation_available() {
    use voteboat::{transfer_publication::*, transfer_source::*};
    let mut source = source_fixture::ready();
    for id in 1..=32 {
        source
            .apply_batch(&[entry(id + 1, id.into(), source_fixture::data(1, 1))])
            .unwrap();
    }
    source
        .apply_batch(&[entry(34, 200, source_fixture::freeze())])
        .unwrap();
    let SourceRead::Freeze(Some(status)) = source.read_at(34, SourceQuery::Freeze).unwrap() else {
        panic!("fence")
    };
    let evidence = SourceFenceEvidence::from_status(ConfigurationId::new(1).unwrap(), status)
        .unwrap_or_else(|e| panic!("{:?}", e.0));
    let mut targets = Vec::new();
    let mut left = None;
    for group in [21, 22] {
        let mut target = fixture::fresh_for(group);
        let import = fixture::from_source(&source, group, ConfigurationId::new(1).unwrap());
        target
            .apply_batch(&[
                entry(1, 200, target.bootstrap_command(65536).unwrap()),
                entry(2, 200, target.import_command(&import, 65536).unwrap()),
            ])
            .unwrap();
        targets.push(
            TargetReadyEvidence::from_status(ConfigurationId::new(1).unwrap(), target.status())
                .unwrap_or_else(|e| panic!("{:?}", e.0)),
        );
        if group == 21 {
            left = Some(target);
        }
    }
    let publication =
        TransferPublication::new(op(200), source_fixture::intent(), vec![evidence], targets)
            .unwrap_or_else(|e| panic!("{:?}", e.0));
    let observation = TargetActivation {
        metadata_configuration: ConfigurationId::new(1).unwrap(),
        decision: TransferPublicationStatus {
            publication_operation: op(201),
            index: 4,
            publication,
        },
    };
    let mut target = left.unwrap();
    let activation = target.activation_command(&observation, 65536).unwrap();
    assert!(target
        .validate_proposal(op(200), &activation, std::iter::empty())
        .is_ok());
    target.apply_batch(&[entry(3, 200, activation)]).unwrap();
    assert!(target
        .validate_proposal(op(33), &data(1, 1), std::iter::empty())
        .is_err());
    let TargetOutcome::Applied(retry) = target
        .apply_batch(&[entry(4, 1, data(1, 1))])
        .unwrap()
        .remove(0)
        .outcome
    else {
        panic!("retry")
    };
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, BucketOutcome::Value(1));
    assert_eq!(target.read_at(4, query(1)).unwrap(), TargetRead::Data(32));
}
