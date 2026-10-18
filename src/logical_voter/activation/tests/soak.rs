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
//! Deterministic business-write soak driven by log index only: no clock, no
//! threads, no ports. The inner application has an explicit bounded
//! session/sequence contract, so the wrapper's own retention is what is tested.
use super::*;
use std::mem::size_of;

const CLIENT: u128 = 7;
const WRITES: u64 = 200_000;
const BATCH: u64 = 64;
const SESSION_SCHEMA: u64 = 0x5345_5353_0001;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionOutcome {
    Value(i64),
    /// An older sequence than the retained one; never executed.
    Expired,
    /// The retained sequence with different content; never executed.
    Conflict,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SessionReceipt {
    index: u64,
    operation: OperationId,
    outcome: SessionOutcome,
    duplicate: bool,
}
impl ApplicationReceipt for SessionReceipt {
    fn index(&self) -> u64 {
        self.index
    }
    fn operation(&self) -> OperationId {
        self.operation
    }
    fn nested_bytes(&self, _: usize) -> Result<usize, ApplicationError> {
        Ok(0)
    }
}
/// Retains only the last sequence of one client session and its outcome.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SessionCounter {
    applied: u64,
    value: i64,
    last: Option<(u64, [u8; 8], i64)>,
}
fn session_op(seq: u64) -> OperationId {
    OperationId::new((CLIENT << 64) | u128::from(seq)).unwrap()
}
fn sequence(operation: OperationId) -> Result<u64, ApplicationError> {
    let id = operation.get();
    if id >> 64 != CLIENT || id as u64 == 0 {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(id as u64)
}
impl StateMachine for SessionCounter {
    type Receipt = SessionReceipt;
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn apply_batch(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<SessionReceipt>, ApplicationError> {
        let mut next = self.clone();
        let mut out = Vec::new();
        for entry in entries {
            if next.applied.checked_add(1) != Some(entry.index) {
                return Err(ApplicationError::IndexGap);
            }
            if let EntryPayload::Command { operation, bytes } = &entry.payload {
                let seq = sequence(*operation)?;
                let delta: [u8; 8] = bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| ApplicationError::InvalidCommand)?;
                let (outcome, duplicate) = match next.last {
                    Some((last, original, value)) if seq == last => (
                        if original == delta {
                            SessionOutcome::Value(value)
                        } else {
                            SessionOutcome::Conflict
                        },
                        true,
                    ),
                    Some((last, ..)) if seq < last => (SessionOutcome::Expired, true),
                    _ => {
                        next.value = next
                            .value
                            .checked_add(i64::from_le_bytes(delta))
                            .ok_or(ApplicationError::InvalidCommand)?;
                        next.last = Some((seq, delta, next.value));
                        (SessionOutcome::Value(next.value), false)
                    }
                };
                out.push(SessionReceipt {
                    index: entry.index,
                    operation: *operation,
                    outcome,
                    duplicate,
                });
            }
            next.applied = entry.index;
        }
        *self = next;
        Ok(out)
    }
}
impl BoundedStateMachine for SessionCounter {
    fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
        Ok(entries.len() * size_of::<SessionReceipt>())
    }
}
impl ProposalAdmission for SessionCounter {
    fn validate_proposal<'a>(
        &self,
        operation: OperationId,
        bytes: &[u8],
        _: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        sequence(operation)?;
        if bytes.len() != 8 {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(size_of::<SessionReceipt>())
    }
}
impl CheckpointStateMachine for SessionCounter {
    fn schema_version(&self) -> u64 {
        SESSION_SCHEMA
    }
    fn checkpoint(&self, max: usize) -> Result<Vec<u8>, ApplicationError> {
        let mut b = Vec::with_capacity(41);
        b.extend(self.applied.to_le_bytes());
        b.extend(self.value.to_le_bytes());
        let (seq, delta, value) = self.last.unwrap_or_default();
        b.push(u8::from(self.last.is_some()));
        b.extend(seq.to_le_bytes());
        b.extend(delta);
        b.extend(value.to_le_bytes());
        if b.len() > max {
            return Err(ApplicationError::ReceiptBudget);
        }
        Ok(b)
    }
    fn restore_checkpoint(
        &mut self,
        schema: u64,
        applied: u64,
        bytes: &[u8],
    ) -> Result<(), ApplicationError> {
        if schema != SESSION_SCHEMA {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if bytes.len() != 41 {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let u = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let next = Self {
            applied: u(0),
            value: u(8) as i64,
            last: (bytes[16] == 1)
                .then(|| (u(17), bytes[25..33].try_into().unwrap(), u(33) as i64)),
        };
        if next.applied != applied || bytes[16] > 1 || next.checkpoint(41)? != bytes {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        *self = next;
        Ok(())
    }
}

type Wrapper = LogicalActivationApplication<SessionCounter>;
fn wrapper() -> Wrapper {
    LogicalActivationApplication::new(
        group(1),
        vec![mapping()],
        LogicalActivationLimits::default(),
        SessionCounter::default(),
    )
    .unwrap()
}
/// Applies one batch to the primary and, while it exists, to the restored copy.
struct Soak {
    a: Wrapper,
    shadow: Option<(Wrapper, u64)>,
    wrapper_bytes: BTreeMap<(usize, Option<OperationId>), usize>,
}
impl Soak {
    fn admit(&self, entries: &[(OperationId, Vec<u8>)], what: &str) {
        for (i, (id, bytes)) in entries.iter().enumerate() {
            let pending = entries[..i].iter().map(|(p, b)| (*p, b.as_slice()));
            let admitted = self.a.validate_proposal(*id, bytes, pending);
            assert!(admitted.is_ok(), "{what} {id:?} refused {admitted:?}");
        }
    }
    fn apply(
        &mut self,
        entries: Vec<(OperationId, Vec<u8>)>,
    ) -> Vec<ActivationReceipt<SessionReceipt>> {
        let start = self.a.applied_index() + 1;
        let entries: Vec<_> = entries
            .into_iter()
            .enumerate()
            .map(|(i, (id, bytes))| entry(start + i as u64, id, bytes))
            .collect();
        let receipts = self.a.apply_batch(&entries).unwrap();
        if let Some((shadow, left)) = &mut self.shadow {
            assert_eq!(shadow.apply_batch(&entries).unwrap(), receipts);
            *left = left.saturating_sub(entries.len() as u64);
            if *left == 0 {
                assert_eq!(shadow.checkpoint(usize::MAX), self.a.checkpoint(usize::MAX));
                self.shadow = None;
            }
        }
        receipts
    }
    fn control(&mut self, operation: OperationId, c: ControlCommand) -> ChildActivationReceipt {
        let bytes = codec::command(&c, CONTROL_BYTES).unwrap();
        self.admit(&[(operation, bytes.clone())], "control");
        let ActivationReceipt::Control(r) = self.apply(vec![(operation, bytes)]).remove(0) else {
            panic!("control receipt")
        };
        r
    }
    /// The wrapper's own share of the checkpoint depends only on control state.
    fn check_wrapper_bytes(&mut self) {
        let total = self.a.checkpoint(usize::MAX).unwrap().len();
        let own = total - self.a.inner().checkpoint(usize::MAX).unwrap().len();
        let key = (self.a.control.records.len(), self.a.active());
        let expected = *self.wrapper_bytes.entry(key).or_insert(own);
        assert_eq!(own, expected, "wrapper checkpoint share grew at {key:?}");
    }
    fn activation(&mut self, k: u64) {
        let i = intent(2 * k);
        let competing = intent(2 * k + 1);
        let begun = self.control(
            i.operations.begin,
            ControlCommand::Begin(Box::new(i.clone())),
        );
        assert_eq!(begun.outcome, ChildActivationOutcome::Begun);
        let busy = self.control(
            competing.operations.begin,
            ControlCommand::Begin(Box::new(competing.clone())),
        );
        assert_eq!(
            busy.outcome,
            ChildActivationOutcome::Busy(i.operations.begin)
        );
        let outcome = self.control(i.operations.outcome, final_command(&i));
        assert_eq!(outcome.outcome, ChildActivationOutcome::Completed);
        let release = ControlCommand::Release {
            begin: i.operations.begin,
            outcome: outcome.first,
        };
        let released = self.control(i.operations.release, release);
        assert_eq!(released.outcome, ChildActivationOutcome::Released);
        assert_eq!(self.a.active(), None);
    }
}
fn delta(seq: u64) -> [u8; 8] {
    // Varying content keeps every retry's content check meaningful.
    (1 + (seq % 3) as i64).to_le_bytes()
}
#[test]
fn two_hundred_thousand_distinct_business_writes_keep_bounded_wrapper_state() {
    let mut s = Soak {
        a: wrapper(),
        shadow: None,
        wrapper_bytes: BTreeMap::new(),
    };
    let mut expected = 0i64;
    let mut seq = 0u64;
    s.check_wrapper_bytes();
    while seq < WRITES {
        let writes: Vec<_> = (seq + 1..=seq + BATCH)
            .map(|q| (session_op(q), s.a.business(&delta(q)).unwrap()))
            .collect();
        s.admit(&writes, "business write");
        let receipts = s.apply(writes);
        for (r, q) in receipts.iter().zip(seq + 1..) {
            expected += i64::from_le_bytes(delta(q));
            assert!(matches!(
                r,
                ActivationReceipt::Business(SessionReceipt { outcome: SessionOutcome::Value(v), duplicate: false, operation, .. })
                    if *v == expected && *operation == session_op(q)
            ));
        }
        seq += BATCH;
        // A lost receipt for the last write, and a late retry of the batch's first.
        let retries = vec![
            (session_op(seq), s.a.business(&delta(seq)).unwrap()),
            (
                session_op(seq - BATCH + 1),
                s.a.business(&delta(seq - BATCH + 1)).unwrap(),
            ),
        ];
        s.admit(&retries, "retry");
        let receipts = s.apply(retries);
        assert!(matches!(
            receipts[0],
            ActivationReceipt::Business(SessionReceipt { outcome: SessionOutcome::Value(v), duplicate: true, .. }) if v == expected
        ));
        assert!(matches!(
            receipts[1],
            ActivationReceipt::Business(SessionReceipt {
                outcome: SessionOutcome::Expired,
                ..
            })
        ));
        assert_eq!(s.a.inner().value, expected);
        if seq % 50_000 < BATCH {
            s.activation(seq / 50_000);
        }
        s.check_wrapper_bytes();
        if seq % 25_000 < BATCH {
            let checkpoint = s.a.checkpoint(usize::MAX).unwrap();
            let mut restored = wrapper();
            restored
                .restore_checkpoint(LOGICAL_ACTIVATION_SCHEMA, s.a.applied_index(), &checkpoint)
                .unwrap();
            assert_eq!(restored.checkpoint(usize::MAX).unwrap(), checkpoint);
            s.shadow = Some((restored, 1_000));
        }
    }
    assert_eq!(s.a.control.records.len(), 8);
    assert_eq!(s.wrapper_bytes.len(), 5);
}
