// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original write clocks survive later requests; inspection grants no authority.
use super::{
    host::Host,
    requests, setup,
    watchdog::{ClientAttempt, Watchdog},
};
use std::time::{Duration, Instant};
use voteboat::{application::*, identity::OperationId, log::EntryPayload, runtime::*};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteResult {
    pub ticket: ClientTicket,
    pub position: ProposalPosition,
    pub receipt: CounterReceipt,
}
pub struct WriteProgress {
    watch: Watchdog,
    attempts: Vec<ClientAttempt>,
    operation: OperationId,
    bytes: [u8; 16],
    result: Option<WriteResult>,
    convergence_start: Option<Instant>,
    converged: bool,
}
impl WriteProgress {
    pub fn new(operation: OperationId, bytes: [u8; 16]) -> Self {
        Self {
            watch: Watchdog::new(),
            attempts: Vec::with_capacity(requests::ATTEMPTS),
            operation,
            bytes,
            result: None,
            convergence_start: None,
            converged: false,
        }
    }
    pub fn admit(&mut self, attempt: ClientAttempt) {
        assert!(self.attempts.len() < requests::ATTEMPTS);
        assert_eq!(attempt.ticket.operation, self.operation);
        self.attempts.push(attempt);
    }
    pub fn consumed(&mut self, now: MonoTime, result: WriteResult) {
        let current = self.attempts.last_mut().unwrap();
        assert_eq!(current.ticket, result.ticket);
        assert_eq!(current.position, Some(result.position));
        assert_eq!(result.ticket.operation, self.operation);
        assert_eq!(result.receipt.operation, self.operation);
        assert_eq!(result.receipt.index, result.position.index);
        assert_eq!(result.receipt.outcome, CounterOutcome::Value(5));
        assert!(self.watch.within_absolute_bound());
        current.observing = false;
        assert!(self.result.is_none());
        assert!(self.convergence_start.is_none());
        self.watch.applied(now, self.attempts.len() - 1);
        self.convergence_start = Some(Instant::now());
        self.result = Some(result);
    }
    pub fn attempt(&self) -> &ClientAttempt {
        self.attempts.last().unwrap()
    }
    pub fn stop_observing(&mut self) {
        self.attempts.last_mut().unwrap().observing = false;
    }
    pub fn observe(&mut self, host: &Host<'_, '_>) {
        if self.converged {
            return;
        }
        for (ordinal, attempt) in self.attempts.iter_mut().enumerate() {
            self.watch.client(host, attempt, &self.bytes, ordinal);
        }
        self.converged = self.within_phase_bound()
            && self
                .result
                .as_ref()
                .is_some_and(|result| self.exact_all(host, result));
        if self.converged {
            eprintln!(
                "nested original write converged now={:?} result={:?}",
                host.lower.now(),
                self.result
            );
        }
    }
    fn exact_all(&self, host: &Host<'_, '_>, result: &WriteResult) -> bool {
        let attempt = self.attempts.last().unwrap();
        host.nodes.iter().enumerate().all(|(slot, node)| {
            let local = node.local();
            assert_eq!(local.owner.identity(), attempt.owners[slot]);
            let core = local.owner.core(result.ticket.group).unwrap();
            assert_eq!(core.membership().id(), attempt.configurations[slot]);
            let exact = core.state().entry_at(result.position.index).is_some_and(|entry| {
                entry.term == result.position.term && matches!(&entry.payload,
                EntryPayload::Command { operation, bytes } if *operation == self.operation && bytes.as_slice() == self.bytes.as_slice())
            });
            exact && core.state().commit_index >= result.position.index
                && local.applications[&result.ticket.group].applied_index() >= result.position.index
        })
    }
    pub fn valid(&self) -> bool {
        self.converged || self.within_phase_bound()
    }
    fn within_phase_bound(&self) -> bool {
        self.convergence_start.map_or_else(
            || self.watch.within_absolute_bound(),
            |start| start.elapsed() < Duration::from_secs(90),
        )
    }
    pub fn done(&self) -> bool {
        self.converged
    }
    pub fn result(&self) -> &WriteResult {
        self.result.as_ref().unwrap()
    }
    pub fn diagnostic(&self) -> String {
        format!(
            "operation={:?},bytes={:?},attempts={:?},result={:?},convergence_start={:?},converged={},watch={:?}",
            self.operation, self.bytes, self.attempts, self.result, self.convergence_start,
            self.converged, self.watch
        )
    }
    pub fn assert_final(&self, host: &Host<'_, '_>) {
        let result = self.result();
        assert!(self.converged && self.exact_all(host, result));
        assert_eq!(result.receipt.index, result.position.index);
        assert_eq!(result.receipt.operation, self.operation);
        for node in &host.nodes {
            assert_eq!(
                node.local().applications[&setup::top().group]
                    .read_at(result.position.index, ())
                    .unwrap(),
                5
            );
        }
    }
}
pub fn finish(host: &mut Host<'_, '_>) {
    host.phase = "upper_convergence";
    assert_eq!(host.writes.len(), 2);
    assert!(host.writes[1].result().receipt.duplicate);
    while host.writes.iter().any(|write| !write.done()) {
        host.poll();
    }
    for write in &host.writes {
        write.assert_final(host);
    }
}
