// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "../../support/history.rs"]
mod checker;
use checker::{Action, Call, CheckError, Reply};

type Scope = (ResponsibilityIdentity, u16);
#[derive(Clone, Debug)]
struct ObservedCall {
    hint: RouteHint,
    call: Call,
}
#[derive(Clone, Debug, Default)]
pub(super) struct Trace {
    event: u64,
    calls: Vec<ObservedCall>,
}
pub(super) struct Invocation {
    hint: RouteHint,
    action: Action,
    event: u64,
}
impl Trace {
    pub(super) fn write(&mut self, hint: RouteHint, operation: u128, delta: i64) -> Invocation {
        self.begin(hint, Action::Write { operation, delta })
    }
    pub(super) fn read(&mut self, hint: RouteHint) -> Invocation {
        self.begin(hint, Action::Read)
    }
    fn begin(&mut self, hint: RouteHint, action: Action) -> Invocation {
        assert!(hint.scope.contains(hint.bucket));
        self.event += 1;
        Invocation {
            hint,
            action,
            event: self.event,
        }
    }
    pub(super) fn value(&mut self, invocation: Invocation, value: i64) {
        self.finish(invocation, Reply::Value(value));
    }
    pub(super) fn receipt(
        &mut self,
        invocation: Invocation,
        receipt: voteboat::bucket_counter::BucketReceipt,
        observed: bool,
    ) {
        let Action::Write { operation, .. } = invocation.action else {
            panic!("write receipt")
        };
        assert_eq!(receipt.operation.get(), operation);
        let reply = if !observed {
            Reply::Unknown
        } else {
            match receipt.outcome {
                BucketOutcome::Value(value) => Reply::Value(value),
                BucketOutcome::Overflow => Reply::Overflow,
                BucketOutcome::OperationConflict => Reply::Conflict,
            }
        };
        self.finish(invocation, reply);
    }
    fn finish(&mut self, invocation: Invocation, reply: Reply) {
        self.event += 1;
        self.calls.push(ObservedCall {
            hint: invocation.hint,
            call: Call {
                invoked: invocation.event,
                completed: self.event,
                action: invocation.action,
                reply,
            },
        });
        assert!(self.calls.len() <= 32);
    }
    fn check(&self) -> Result<BTreeMap<Scope, Vec<usize>>, CheckError> {
        let mut scopes: BTreeMap<Scope, Vec<Call>> = BTreeMap::new();
        for entry in &self.calls {
            scopes
                .entry((entry.hint.responsibility, entry.hint.bucket))
                .or_default()
                .push(entry.call.clone());
        }
        scopes
            .into_iter()
            .map(|(scope, calls)| {
                checker::check(&calls, checker::Limits::default()).map(|order| (scope, order))
            })
            .collect()
    }
    pub(super) fn verify(&self) {
        let witnesses = self
            .check()
            .unwrap_or_else(|error| panic!("{error:?}: {self:#?}"));
        assert_eq!(witnesses.len(), 2);
        for bucket in [1, 200] {
            let calls = self
                .calls
                .iter()
                .filter(|entry| entry.hint.bucket == bucket)
                .collect::<Vec<_>>();
            let owners = calls
                .iter()
                .map(|entry| (entry.hint.group, entry.hint.epoch))
                .collect::<BTreeSet<_>>();
            assert_eq!(owners.len(), 2);
            assert!(calls.iter().any(|entry| entry.call.reply == Reply::Unknown));
            assert!(calls.iter().all(|entry| {
                entry.hint.application == calls[0].hint.application
                    && entry.hint.scheme == calls[0].hint.scheme
            }));
        }
        let mut bad_duplicate = self.clone();
        let duplicate = bad_duplicate
            .calls
            .iter_mut()
            .find(|entry| {
                entry.hint.epoch.get() == 2
                    && matches!(entry.call.action, Action::Write { operation: 1, .. })
            })
            .unwrap();
        duplicate.call.reply = Reply::Value(8);
        assert_eq!(bad_duplicate.check(), Err(CheckError::NotLinearizable));
        let folded = self
            .calls
            .iter()
            .map(|entry| entry.call.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            checker::check(&folded, checker::Limits::default()),
            Err(CheckError::NotLinearizable)
        );
        println!("split scoped history: {} calls, {} bucket witnesses, changed duplicate/folded scope refused",
            self.calls.len(), witnesses.len());
    }
}

pub(super) fn seed(rig: &mut Split) {
    for (key, operation, delta) in [(1, 1, 7), (200, 2, 11)] {
        let invocation = rig
            .history
            .write(source_fixture::hint(key), operation, delta);
        let receipt = propose_recovering(
            &mut rig.source,
            &rig.clock,
            20,
            operation,
            source_fixture::data(key, delta),
        );
        let RoutedOutcome::Applied(receipt) = receipt.outcome else {
            panic!("source write")
        };
        rig.history.receipt(invocation, receipt, true);
    }
}
