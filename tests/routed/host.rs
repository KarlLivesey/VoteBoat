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
use std::mem::size_of;
#[derive(Clone, Debug, Eq, PartialEq)]
struct HeapReceipt {
    counter: CounterReceipt,
    annotation: Vec<u8>,
}
impl ApplicationReceipt for HeapReceipt {
    fn index(&self) -> u64 {
        self.counter.index
    }
    fn operation(&self) -> OperationId {
        self.counter.operation
    }
    fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
        if self.annotation.capacity() > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(self.annotation.capacity())
        }
    }
}
#[derive(Clone)]
struct HostApplication(Counter);
impl StateMachine for HostApplication {
    type Receipt = HeapReceipt;
    fn validate_group(&self, g: GroupIdentity) -> Result<(), ApplicationError> {
        if g == group(20) {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<HeapReceipt>, ApplicationError> {
        self.0.apply_batch(entries).map(|rs| {
            rs.into_iter()
                .map(|counter| HeapReceipt {
                    counter,
                    annotation: vec![42; 8],
                })
                .collect()
        })
    }
}
impl BoundedStateMachine for HostApplication {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        Ok(entries
            .iter()
            .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
            .count()
            * (size_of::<HeapReceipt>() + 8))
    }
}
impl ProposalAdmission for HostApplication {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        pending: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(operation, bytes, pending)?;
        Ok(size_of::<HeapReceipt>() + 8)
    }
}
impl ReadableStateMachine for HostApplication {
    type Query = Vec<u8>;
    type ReadResult = Vec<u8>;
    fn read_at(&self, index: u64, _: Vec<u8>) -> Result<Vec<u8>, ApplicationError> {
        self.0.read_at(index, ()).map(|v| v.to_le_bytes().to_vec())
    }
}
impl BoundedReadableStateMachine for HostApplication {
    fn query_bytes(&self, q: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        if q.capacity() > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(q.capacity())
        }
    }
    fn read_result_bound(&self, _: &Vec<u8>) -> Result<usize, ApplicationError> {
        Ok(size_of::<Vec<u8>>() + 8)
    }
    fn read_result_bytes(&self, r: &Vec<u8>, limit: usize) -> Result<usize, ApplicationError> {
        if r.capacity() > limit {
            Err(ApplicationError::ReceiptBudget)
        } else {
            Ok(r.capacity())
        }
    }
}
impl CheckpointStateMachine for HostApplication {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, limit: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(limit)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        index: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(schema, index, bytes)
    }
}
#[test]
fn host_application_nested_capacity_bounds_and_group_constraints_are_preserved() {
    let make = || {
        RoutedApplication::new(
            group(20),
            grant(),
            HostApplication(Counter::new(64).unwrap()),
            HostPolicy,
            limits(),
        )
        .unwrap_or_else(|r| panic!("{:?}", r.error))
    };
    let mut app = make();
    let boot = entry(
        1,
        10000,
        app.bootstrap_command(MAX_ROUTED_COMMAND_BYTES).unwrap(),
    );
    assert_eq!(
        app.receipt_bytes_bound(std::slice::from_ref(&boot))
            .unwrap(),
        size_of::<RoutedReceipt<HeapReceipt>>()
    );
    assert_eq!(app.apply_batch(&[boot]).unwrap()[0].nested_bytes(0), Ok(0));
    let bytes = data(10, 7);
    let expected = size_of::<RoutedReceipt<HeapReceipt>>() + 8;
    assert_eq!(
        app.validate_proposal(OperationId::new(1).unwrap(), &bytes, std::iter::empty()),
        Ok(expected)
    );
    let write = entry(2, 1, bytes.clone());
    assert_eq!(
        app.receipt_bytes_bound(std::slice::from_ref(&write)),
        Ok(expected)
    );
    let receipt = app.apply_batch(&[write]).unwrap().remove(0);
    assert_eq!(receipt.nested_bytes(8), Ok(8));
    assert_eq!(
        receipt.nested_bytes(7),
        Err(ApplicationError::ReceiptBudget)
    );
    let query = || RoutedQuery {
        hint: hint(10),
        key: vec![10],
        query: vec![9; 16],
    };
    assert_eq!(app.query_bytes(&query(), 17), Ok(17));
    assert_eq!(
        app.query_bytes(&query(), 16),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(
        app.read_result_bound(&query()),
        Ok(size_of::<RoutedRead<Vec<u8>>>() + 8)
    );
    let result = app.read_at(2, query()).unwrap();
    assert_eq!(result, RoutedRead::Served(7i64.to_le_bytes().to_vec()));
    assert_eq!(app.read_result_bytes(&result, 8), Ok(8));
    assert_eq!(
        app.read_result_bytes(&result, 7),
        Err(ApplicationError::ReceiptBudget)
    );
    let snapshot = app.checkpoint(64 * 1024).unwrap();
    let mut reopened = make();
    reopened
        .restore_checkpoint(ROUTED_APPLICATION_SCHEMA, 2, &snapshot)
        .unwrap();
    assert!(matches!(
        reopened.apply_batch(&[entry(3, 1, bytes)]).unwrap()[0].outcome,
        RoutedOutcome::Applied(HeapReceipt {
            counter: CounterReceipt {
                duplicate: true,
                ..
            },
            ..
        })
    ));
    let mut wrong = grant().into_input();
    wrong.execution = ExecutionMode::Single(group(21));
    let returned = RoutedApplication::new(
        group(21),
        ResponsibilityManifest::new(wrong.clone()).unwrap(),
        HostApplication(Counter::new(64).unwrap()),
        HostPolicy,
        limits(),
    )
    .err()
    .unwrap();
    assert_eq!(returned.error, ApplicationError::InvalidCommand);
    assert_eq!(returned.grant.input(), &wrong);
    assert_eq!(returned.application.applied_index(), 0);
    assert_eq!(returned.policy.scheme(), HostPolicy.scheme());
}

#[test]
fn scoped_profile_preserves_host_receipt_and_read_capacity_contracts() {
    let make = || {
        RoutedApplication::new(
            group(20),
            grant(),
            HostApplication(Counter::new(64).unwrap()),
            HostPolicy,
            limits(),
        )
        .unwrap_or_else(|r| panic!("{:?}", r.error))
        .with_scoped_fencing(2)
        .unwrap_or_else(|r| panic!("{:?}", r.0))
    };
    let mut a = make();
    a.apply_batch(&[entry(
        1,
        10000,
        a.bootstrap_command(MAX_ROUTED_COMMAND_BYTES).unwrap(),
    )])
    .unwrap();
    a.apply_batch(&[entry(
        2,
        200,
        encode_scope_fence(grant().input().epoch, BucketRange::new(0, 64).unwrap()),
    )])
    .unwrap();
    let bytes = data(100, 7);
    let expected = size_of::<RoutedReceipt<HeapReceipt>>() + 8;
    assert_eq!(
        a.validate_proposal(OperationId::new(1).unwrap(), &bytes, std::iter::empty()),
        Ok(expected)
    );
    assert_eq!(
        a.receipt_bytes_bound(&[entry(3, 1, bytes.clone())]),
        Ok(expected)
    );
    let r = a.apply_batch(&[entry(3, 1, bytes)]).unwrap().remove(0);
    assert_eq!(r.nested_bytes(8), Ok(8));
    assert!(r.nested_bytes(7).is_err());
    let query = |key| RoutedQuery {
        hint: hint(key),
        key: vec![key],
        query: vec![9; 16],
    };
    assert_eq!(a.query_bytes(&query(100), 17), Ok(17));
    assert_eq!(
        a.read_at(3, query(100)).unwrap(),
        RoutedRead::Served(7i64.to_le_bytes().to_vec())
    );
    assert_eq!(
        a.read_at(3, query(1)).unwrap(),
        RoutedRead::Rejected(RoutingError::Fenced)
    );
    let image = a.checkpoint(100000).unwrap();
    let mut recovered = make();
    recovered.restore_checkpoint(2, 3, &image).unwrap();
    assert_eq!(
        recovered.read_at(3, query(1)).unwrap(),
        RoutedRead::Rejected(RoutingError::Fenced)
    );
    assert_eq!(
        recovered.read_at(3, query(100)).unwrap(),
        RoutedRead::Served(7i64.to_le_bytes().to_vec())
    );
}
