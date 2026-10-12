// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Immutable snapshots of original retained read invocation ownership.
use super::*;
/// Diagnostic state, not a quorum barrier, output or release capability.
/// A later snapshot may miss transitions wholly completed between samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadInvocationInspection {
    ticket: ReadInvocationTicket,
    admission: AdmissionTicket,
    cancellation: Option<AdmissionTicket>,
    stepped: bool,
    terminal: bool,
    has_output: bool,
    exported: bool,
}
impl ReadInvocationInspection {
    pub fn ticket(self) -> ReadInvocationTicket {
        self.ticket
    }
    pub fn admission(self) -> AdmissionTicket {
        self.admission
    }
    pub fn cancellation(self) -> Option<AdmissionTicket> {
        self.cancellation
    }
    pub fn stepped(self) -> bool {
        self.stepped
    }
    pub fn terminal(self) -> bool {
        self.terminal
    }
    pub fn has_output(self) -> bool {
        self.has_output
    }
    pub fn exported(self) -> bool {
        self.exported
    }
}
impl<Q, R> ReadRequests<Q, R> {
    /// Inspect only this router's exact retained original ticket. Closing or
    /// failure does not conceal retained work; reclaimed/foreign tickets refuse.
    pub fn inspect(
        &self,
        ticket: ReadInvocationTicket,
    ) -> Result<ReadInvocationInspection, ReadInvocationError> {
        let pending = self
            .pending
            .get(&ticket.sequence)
            .filter(|p| ticket.binding == self.binding && p.ticket == ticket)
            .ok_or(ReadInvocationError::StaleTicket)?;
        Ok(ReadInvocationInspection {
            ticket: pending.ticket,
            admission: pending.admission,
            cancellation: pending.cancellation,
            stepped: pending.stepped,
            terminal: pending.terminal,
            has_output: pending.has_output,
            exported: pending.exported,
        })
    }
}
