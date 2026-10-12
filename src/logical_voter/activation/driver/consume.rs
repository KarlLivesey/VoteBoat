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
impl<P: LogicalActivationParent> ChildActivationDriver<'_, P> {
    pub(super) fn consume(&mut self) -> Result<bool, ChildActivationDriverError> {
        match self.pending {
            Some(Attempt::Client(ticket)) => {
                let Some(output) = self.parent.poll_client() else {
                    return Ok(false);
                };
                if output.ticket() != ticket
                    || !self.client_scope(ticket)
                    || !valid_client(output.outcome(), ticket.operation, self.intent.operations)
                {
                    self.fault = Some(Fault::Client(Box::new(output)));
                    return Err(self.fail(ChildActivationDriverError::ProviderViolation));
                }
                let outcome = match self.parent.complete_client(output) {
                    Ok(outcome) => outcome,
                    Err(rejected) => {
                        self.fault = Some(Fault::Client(rejected.completion));
                        return Err(self.fail(ChildActivationDriverError::Client(rejected.reason)));
                    }
                };
                self.pending = None;
                if !self.stopped {
                    self.client_outcome(outcome);
                }
                Ok(true)
            }
            Some(Attempt::Read(ticket)) => {
                let Some(output) = self.parent.poll_read() else {
                    return Ok(false);
                };
                if output.ticket() != ticket
                    || ticket.binding != self.bindings.2
                    || ticket.group != self.intent.mapping.parent
                    || !valid_read(output.outcome(), ticket)
                {
                    self.fault = Some(Fault::Read(Box::new(output)));
                    return Err(self.fail(ChildActivationDriverError::ProviderViolation));
                }
                let outcome = match self.parent.complete_read(output) {
                    Ok(outcome) => outcome,
                    Err(rejected) => {
                        self.fault = Some(Fault::Read(rejected.completion));
                        return Err(self.fail(ChildActivationDriverError::Read(rejected.reason)));
                    }
                };
                self.pending = None;
                if !self.stopped {
                    self.read_outcome(ticket, outcome)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn client_scope(&self, ticket: ClientTicket) -> bool {
        ticket.binding == self.bindings.1
            && ticket.group == self.intent.mapping.parent
            && (ticket.operation == self.intent.operations.begin
                || self
                    .terminal
                    .as_ref()
                    .is_some_and(|(operation, _)| *operation == ticket.operation))
    }
    fn client_outcome(&mut self, outcome: ClientOutcome<ActivationReceipt<P::Receipt>>) {
        self.phase = ChildActivationDriverPhase::Observe;
        if let ClientOutcome::Applied {
            receipt: ActivationReceipt::Control(r),
            ..
        } = outcome
        {
            if r.operation == self.intent.operations.begin {
                self.first = Some(r.first);
            }
            self.phase = match r.outcome {
                ChildActivationOutcome::Busy(_) => ChildActivationDriverPhase::Busy,
                ChildActivationOutcome::OperationConflict => ChildActivationDriverPhase::Conflict,
                _ => ChildActivationDriverPhase::Observe,
            };
        }
    }
    fn read_outcome(
        &mut self,
        ticket: ReadInvocationTicket,
        outcome: ReadOutcome<ActivationRead<P::Read>>,
    ) -> Result<(), ChildActivationDriverError> {
        self.phase = ChildActivationDriverPhase::Observe;
        let ReadOutcome::Read {
            barrier,
            result: Ok(ActivationRead::Status(status)),
        } = outcome
        else {
            return Ok(());
        };
        let Some(status) = status else {
            self.phase = ChildActivationDriverPhase::Propose;
            return Ok(());
        };
        if status.intent != self.intent {
            self.phase = ChildActivationDriverPhase::Conflict;
            return Ok(());
        }
        if !status_valid(&status) || self.first.is_some_and(|p| p != status.begin.first) {
            return Err(self.fail(ChildActivationDriverError::ProviderViolation));
        }
        if [Some(status.begin), status.outcome, status.release]
            .into_iter()
            .flatten()
            .any(|r| r.first.index > barrier.index() || r.first.term > barrier.term())
        {
            return Ok(());
        }
        self.first = Some(status.begin.first);
        self.phase = if status.release.is_some() {
            ChildActivationDriverPhase::Released
        } else if status.outcome.is_some() {
            ChildActivationDriverPhase::Completed
        } else {
            match status.begin.outcome {
                ChildActivationOutcome::Begun => ChildActivationDriverPhase::Begun,
                ChildActivationOutcome::Busy(_) => ChildActivationDriverPhase::Busy,
                _ => ChildActivationDriverPhase::Conflict,
            }
        };
        self.status = Some(status.clone());
        if status.begin.outcome == ChildActivationOutcome::Begun {
            let observed = ObservedActivationIntent {
                intent: status.intent,
                ticket,
                barrier,
                first: status.begin.first,
            };
            if !observed.matches(self.bindings, &self.intent) {
                return Err(self.fail(ChildActivationDriverError::ProviderViolation));
            }
            self.observed = Some(observed);
        }
        Ok(())
    }
}
fn valid_client<R>(
    outcome: &ClientOutcome<ActivationReceipt<R>>,
    operation: OperationId,
    ids: ChildActivationOperations,
) -> bool {
    match outcome {
        ClientOutcome::Applied {
            position,
            receipt: ActivationReceipt::Control(r),
        } => {
            r.operation == operation
                && r.index == position.index
                && position.index > 0
                && position.term > 0
                && r.first.index > 0
                && r.first.term > 0
                && r.first.index <= position.index
                && r.first.term <= position.term
                && (r.outcome == ChildActivationOutcome::OperationConflict
                    || operation == ids.begin
                        && matches!(
                            r.outcome,
                            ChildActivationOutcome::Begun | ChildActivationOutcome::Busy(_)
                        )
                    || operation == ids.outcome && r.outcome == ChildActivationOutcome::Completed
                    || operation == ids.release && r.outcome == ChildActivationOutcome::Released)
        }
        ClientOutcome::Applied { .. } => false,
        _ => true,
    }
}
fn valid_read<R>(outcome: &ReadOutcome<ActivationRead<R>>, ticket: ReadInvocationTicket) -> bool {
    match outcome {
        ReadOutcome::Read { barrier, result } => {
            barrier.group() == ticket.group
                && barrier.request() == ticket.request
                && !matches!(
                    result,
                    Ok(ActivationRead::Business(_) | ActivationRead::Active(_))
                )
        }
        _ => true,
    }
}
fn status_valid(s: &ChildActivationStatus) -> bool {
    s.begin.operation == s.intent.operations.begin
        && (s.outcome.is_some() == s.child_final.is_some())
        && s.child_final.is_none_or(|p| p.index > 0 && p.term > 0)
        && s.begin.first.index > 0
        && s.begin.first.term > 0
        && s.begin.index >= s.begin.first.index
        && s.outcome.is_none_or(|r| {
            r.operation == s.intent.operations.outcome
                && r.outcome == ChildActivationOutcome::Completed
                && r.first.index > s.begin.first.index
                && r.first.term >= s.begin.first.term
                && r.index >= r.first.index
        })
        && s.release.is_none_or(|r| {
            s.outcome.is_some_and(|o| {
                r.operation == s.intent.operations.release
                    && r.outcome == ChildActivationOutcome::Released
                    && r.first.index > o.first.index
                    && r.first.term >= o.first.term
                    && r.index >= r.first.index
            })
        })
}
