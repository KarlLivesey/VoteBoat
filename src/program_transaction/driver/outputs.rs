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
    > ProgramDriver<'_, S, T, E, W, O, H, C, F>
where
    C::Endpoint: Clone,
{
    pub fn poll_completion(&mut self) -> Option<ProgramDriverCompletion> {
        let sequence = self.ready.pop_front()?;
        let work = self.work.get_mut(&sequence)?;
        work.exported = true;
        Some(ProgramDriverCompletion {
            ticket: work.ticket,
            outcome: work.outcome?,
        })
    }
    pub fn complete(
        &mut self,
        completion: ProgramDriverCompletion,
    ) -> Result<ProgramDriverOutcome, ProgramDriverCompletionRejected> {
        let valid = completion.ticket.binding == self.binding
            && self.work.get(&completion.ticket.sequence).is_some_and(|w| {
                w.ticket == completion.ticket && w.exported && w.outcome == Some(completion.outcome)
            });
        if !valid {
            return Err(ProgramDriverCompletionRejected {
                reason: ProgramDriverError::WrongTicket,
                completion: Box::new(completion),
            });
        }
        self.work.remove(&completion.ticket.sequence);
        Ok(completion.outcome)
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn failure(&self) -> Option<&ProgramDriverError> {
        self.failure.as_ref()
    }
    /// Both driver observation credits and actual Node client/read reservations
    /// must be returned. This does not shut down host-owned Nodes or providers.
    pub fn is_drained(&self) -> bool {
        self.work.is_empty()
            && self.ready.is_empty()
            && self.retained.is_empty()
            && self
                .nodes
                .iter()
                .all(|n| n.local().clients.is_drained() && n.local().reads.is_drained())
    }
    pub fn usage(&self) -> ProgramDriverUsage {
        ProgramDriverUsage {
            requests: self.work.len(),
            pending: self.work.values().filter(|w| w.outcome.is_none()).count(),
            outputs: self.ready.len(),
            exported: self.work.values().filter(|w| w.exported).count(),
            command_bytes: self
                .work
                .values()
                .map(|w| match &w.attempt {
                    Some(Attempt::Client { bytes, .. }) => bytes.capacity(),
                    _ => 0,
                })
                .sum(),
            nested_bytes: self.work.values().map(Work::nested_bytes).sum(),
            retained_faults: self.retained.len(),
            retained_output_bytes: self
                .retained
                .iter()
                .map(RetainedOutput::retained_bytes)
                .sum(),
        }
    }
    pub fn progress(
        &self,
        ticket: ProgramDriverTicket,
    ) -> Result<ProgramDriverTransactionProgress, ProgramDriverError> {
        let work = self
            .work
            .get(&ticket.sequence)
            .filter(|w| w.ticket == ticket)
            .ok_or(ProgramDriverError::WrongTicket)?;
        let (node, operation) = match &work.attempt {
            Some(Attempt::Client { node, ticket, .. }) => (Some(*node), Some(ticket.operation)),
            Some(Attempt::Read { node, .. }) => (Some(*node), None),
            None => (None, None),
        };
        Ok(ProgramDriverTransactionProgress {
            stage: work.stage,
            outstanding: work.attempt.is_some(),
            node,
            operation,
        })
    }
    /// Stop observing without compensation or unlocking. Keep advancing and
    /// consuming outputs until is_drained; cancelling an exported-ready Node
    /// output can return StaleTicket and the original output is still consumed.
    pub fn stop_waiting(&mut self) -> Result<(), ProgramDriverError> {
        self.closed = true;
        self.stopped = true;
        let mut error = None;
        for work in self.work.values_mut() {
            let result = match &work.attempt {
                Some(Attempt::Client { node, ticket, .. }) => self.nodes[*node]
                    .cancel_client(*ticket)
                    .map_err(|reason| ProgramDriverError::Client {
                        node: *node,
                        reason: Box::new(reason),
                    }),
                Some(Attempt::Read { node, ticket, .. }) => self.nodes[*node]
                    .cancel_read(*ticket)
                    .map_err(|reason| ProgramDriverError::Read {
                        node: *node,
                        reason: Box::new(reason),
                    }),
                None => Ok(()),
            };
            if let Err(reason) = result {
                if !stale_cancel(&reason) && error.is_none() {
                    error = Some(reason);
                }
            }
        }
        if let Some(error) = error {
            self.failure.get_or_insert(error.clone());
            Err(error)
        } else {
            self.publish_stopped();
            Ok(())
        }
    }
    pub(super) fn fail(&mut self, error: ProgramDriverError) {
        let first = self.failure.is_none();
        self.failure.get_or_insert(error);
        if first {
            let _ = self.stop_waiting();
        }
    }
    pub(super) fn publish_stopped(&mut self) {
        for (sequence, work) in &mut self.work {
            if self.stopped && work.outcome.is_none() && work.attempt.is_none() {
                work.finish(ProgramDriverOutcome::Unknown {
                    transaction: work.ticket.transaction,
                    reason: if self.failure.is_some() {
                        ProgramDriverUnknown::DriverFailed
                    } else {
                        ProgramDriverUnknown::Stopped
                    },
                });
                self.ready.push_back(*sequence);
            }
        }
    }
    pub(super) fn restore_work(&mut self, sequence: u64, work: Work) {
        if work.outcome.is_some() && !work.exported && !self.ready.contains(&sequence) {
            self.ready.push_back(sequence);
        }
        self.work.insert(sequence, work);
    }
    pub(super) fn fault_client(&self, node: usize) -> bool {
        self.retained
            .iter()
            .any(|o| matches!(o,RetainedOutput::Client{node:n,..} if *n==node))
    }
    pub(super) fn fault_read(&self, node: usize) -> bool {
        self.retained
            .iter()
            .any(|o| matches!(o,RetainedOutput::Read{node:n,..} if *n==node))
    }
    // A retained contract-error output still owns the exact Node reservation.
    // Retry consumption for cleanup only; no phase authority comes from it.
    pub(super) fn cleanup_retained(&mut self, limit: usize) -> usize {
        let mut old = std::mem::take(&mut self.retained);
        self.retained = old.split_off(limit.min(old.len()));
        let count = old.len();
        for output in old {
            match output {
                RetainedOutput::Client { node, output } => {
                    if let Err(e) = self.nodes[node].complete_client(*output) {
                        self.retained.push(RetainedOutput::Client {
                            node,
                            output: e.completion,
                        });
                    }
                }
                RetainedOutput::Read { node, output } => {
                    if let Err(e) = self.nodes[node].complete_read(*output) {
                        self.retained.push(RetainedOutput::Read {
                            node,
                            output: e.completion,
                        });
                    }
                }
            }
        }
        count
    }
    pub(super) fn reconcile_stopped(&mut self) {
        if !self.stopped {
            return;
        }
        for work in self.work.values_mut() {
            let drained = match &work.attempt {
                Some(Attempt::Client { node, .. }) => {
                    self.nodes[*node].local().clients.is_drained()
                }
                Some(Attempt::Read { node, .. }) => self.nodes[*node].local().reads.is_drained(),
                None => false,
            };
            if drained {
                work.attempt = None;
            }
        }
        self.publish_stopped();
    }
}
fn stale_cancel(error: &ProgramDriverError) -> bool {
    matches!(error,ProgramDriverError::Client{reason,..} if **reason==ClientError::StaleTicket)
        || matches!(error,ProgramDriverError::Read{reason,..} if **reason==ReadInvocationError::StaleTicket)
}
