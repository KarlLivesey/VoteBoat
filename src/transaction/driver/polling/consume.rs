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
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > TransferDriver<'_, S, T, E, W, O, H, C, F>
where
    C::Endpoint: Clone,
{
    pub(super) fn consume_one(&mut self) -> bool {
        let count = self.nodes.len() * 2;
        for _ in 0..count {
            let slot = self.output_cursor;
            self.output_cursor = (slot + 1) % count;
            let node = slot / 2;
            if slot.is_multiple_of(2) && !self.fault_client(node) {
                if let Some(output) = self.nodes[node].poll_client() {
                    self.consume_client(node, output);
                    return true;
                }
            }
            if !slot.is_multiple_of(2) && !self.fault_read(node) {
                if let Some(output) = self.nodes[node].poll_read() {
                    self.consume_read(node, output);
                    return true;
                }
            }
        }
        false
    }
    fn consume_client(&mut self, node: usize, output: ClientCompletion<TransferReceipt>) {
        let ticket = output.ticket();
        let sequence = self.work.iter().find_map(|(sequence, w)| match &w.attempt {
            Some(Attempt::Client { ticket: t, .. }) if *t == ticket => Some(*sequence),
            _ => None,
        });
        let outcome = match self.nodes[node].complete_client(output) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.retained.push(RetainedOutput::Client {
                    node,
                    output: error.completion,
                });
                self.fail(DriverError::Client {
                    node,
                    reason: Box::new(error.reason),
                });
                return;
            }
        };
        let Some(sequence) = sequence else {
            self.fail(DriverError::ProviderViolation);
            return;
        };
        let mut work = self.work.remove(&sequence).expect("correlated work");
        let attempt = work.attempt.take().expect("correlated client");
        let result = if self.stopped {
            Ok(())
        } else {
            client_result(&mut work, node, ticket, attempt, outcome)
        };
        self.restore_work(sequence, work);
        if let Err(error) = result {
            self.fail(error);
        }
    }
    fn consume_read(&mut self, node: usize, output: ReadCompletion<TransferRead>) {
        let ticket = output.ticket();
        let sequence = self.work.iter().find_map(|(sequence, w)| match &w.attempt {
            Some(Attempt::Read { ticket: t, .. }) if *t == ticket => Some(*sequence),
            _ => None,
        });
        let outcome = match self.nodes[node].complete_read(output) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.retained.push(RetainedOutput::Read {
                    node,
                    output: error.completion,
                });
                self.fail(DriverError::Read {
                    node,
                    reason: Box::new(error.reason),
                });
                return;
            }
        };
        let Some(sequence) = sequence else {
            self.fail(DriverError::ProviderViolation);
            return;
        };
        let mut work = self.work.remove(&sequence).expect("correlated work");
        let attempt = work.attempt.take().expect("correlated read");
        let result = if self.stopped {
            Ok(())
        } else {
            self.read_result(&mut work, node, ticket, attempt, outcome)
        };
        self.restore_work(sequence, work);
        if let Err(error) = result {
            self.fail(error);
        }
    }
    fn read_result(
        &self,
        work: &mut Work,
        node: usize,
        ticket: ReadInvocationTicket,
        attempt: Attempt,
        outcome: ReadOutcome<TransferRead>,
    ) -> Result<(), DriverError> {
        let Attempt::Read {
            node: expected,
            ticket: original,
            query,
        } = attempt
        else {
            return Err(DriverError::ProviderViolation);
        };
        if node != expected
            || ticket != original
            || ticket.binding != self.nodes[node].local().reads.binding()
            || ticket.group != self.profile.coordinator()
            || query != TransferQuery::Status(work.ticket.transaction)
        {
            return Err(DriverError::ProviderViolation);
        }
        match outcome {
            ReadOutcome::Read { barrier, result } => {
                if barrier.group() != ticket.group || barrier.request() != ticket.request {
                    return Err(DriverError::ProviderViolation);
                }
                let TransferRead::Status(status) = result.map_err(DriverError::Application)? else {
                    return Err(DriverError::Inconsistent);
                };
                if let Some(status) = &status {
                    self.check_spec(status.spec)?;
                }
                work.status(status)
            }
            ReadOutcome::Unavailable(_) => {
                work.retry_status();
                Ok(())
            }
            ReadOutcome::NotRead(reason) if super::intake::retry_raft(&reason) => {
                work.retry_status();
                Ok(())
            }
            ReadOutcome::NotRead(reason) => Err(DriverError::Read {
                node,
                reason: Box::new(ReadInvocationError::Consensus(reason)),
            }),
        }
    }
}
fn client_result(
    work: &mut Work,
    node: usize,
    ticket: ClientTicket,
    attempt: Attempt,
    outcome: ClientOutcome<TransferReceipt>,
) -> Result<(), DriverError> {
    let Attempt::Client {
        node: expected,
        ticket: original,
        command,
        bytes,
    } = attempt
    else {
        return Err(DriverError::ProviderViolation);
    };
    let command = *command;
    if expected != node
        || original != ticket
        || command
            .operation(ticket.group)
            .map_err(|_| DriverError::ProviderViolation)?
            != ticket.operation
        || codec::decode(&bytes).map_err(|_| DriverError::ProviderViolation)? != command
        || work.command()? != (ticket.group, command)
    {
        return Err(DriverError::ProviderViolation);
    }
    match outcome {
        ClientOutcome::Applied { position, receipt } => {
            if position.index == 0 || position.term == 0 || position.index != receipt.index {
                return Err(DriverError::ProviderViolation);
            }
            work.receipt(ticket.group, command, receipt)
        }
        ClientOutcome::Unknown(_) => {
            work.retry_status();
            Ok(())
        }
        ClientOutcome::NotProposed(reason) if super::intake::retry_raft(&reason) => {
            work.retry_status();
            Ok(())
        }
        ClientOutcome::NotProposed(reason) => Err(DriverError::Client {
            node,
            reason: Box::new(ClientError::Consensus(reason)),
        }),
    }
}
