// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::setup::{checked, Failure};
use std::time::{Duration, Instant};

/// One bounded local attempt per original intent and leader term. Restart may
/// retry durable Pending work; deadlines never erase a replicated intent.
pub struct Driver {
    pub(super) group: GroupIdentity,
    seen: Option<(OperationId, u64, u64)>,
    deadline: Option<Instant>,
    pending: Option<ClientTicket>,
    suspended: Option<OperationId>,
}
impl Driver {
    pub fn new(group: GroupIdentity) -> Self {
        Self {
            group,
            seen: None,
            deadline: None,
            pending: None,
            suspended: None,
        }
    }
    pub(crate) fn hold(&mut self, operation: OperationId) {
        self.suspended = Some(operation);
    }
    pub(crate) fn release(&mut self, operation: OperationId) {
        if self.suspended == Some(operation) {
            self.suspended = None;
            self.seen = None;
            self.deadline = None;
        }
    }
    pub(crate) fn retry(&mut self, record: LeadershipRecord) {
        self.resume(record);
    }
    pub(super) fn resume(&mut self, record: LeadershipRecord) {
        if record.phase == LeadershipPhase::Pending && self.pending.is_none() {
            self.seen = None;
            self.deadline = None;
            if self.suspended == Some(record.intent.request.operation) {
                self.suspended = None;
            }
        }
    }
    pub(super) fn suspend(
        &mut self,
        service: &mut Service,
        record: LeadershipRecord,
    ) -> Result<(), String> {
        self.suspended = Some(record.intent.request.operation);
        self.cancel_local(service, Some(record.intent.request.operation))?;
        if let Some(ticket) = self.pending.take() {
            service
                .cancel_client(ticket)
                .map_err(|e| format!("{e:?}"))?;
        }
        Ok(())
    }
    fn cancel_local(
        &mut self,
        service: &mut Service,
        operation: Option<OperationId>,
    ) -> Result<(), String> {
        if let Some(transfer) = service
            .local()
            .owner
            .core(self.group)
            .and_then(|c| c.leadership_transfer())
            .filter(|t| operation.is_none_or(|op| t.request.operation == op))
        {
            service
                .control(
                    self.group,
                    NodeControl::CancelLeadershipTransfer {
                        context: transfer.context,
                    },
                )
                .map_err(|e| format!("{e:?}"))?;
        }
        self.deadline = None;
        Ok(())
    }
    pub fn complete(&mut self, ticket: ClientTicket, outcome: &ClientOutcome<Receipt>) {
        if self.pending == Some(ticket) {
            self.pending = None;
            self.deadline = None;
            if !matches!(outcome, ClientOutcome::Applied { .. }) {
                eprintln!("leadership completion unresolved: {outcome:?}; inspect status or resume original operation");
            }
        }
    }
    pub fn tick(&mut self, service: &mut Service, quit: bool) -> Result<(), Failure> {
        if quit {
            self.cancel_local(service, None)?;
            if let Some(ticket) = self.pending.take() {
                checked(service.cancel_client(ticket))?;
            }
            return Ok(());
        }
        if self.deadline.is_some_and(|end| Instant::now() >= end) {
            self.cancel_local(service, self.seen.map(|s| s.0))?;
            if let Some(ticket) = self.pending.take() {
                checked(service.cancel_client(ticket))?;
            }
            eprintln!("leadership local attempt expired; durable intent remains pending");
            return Ok(());
        }
        let Ok(app) = application_for(service, self.group) else {
            return Ok(());
        };
        let Some(record) = app.pending() else {
            self.deadline = None;
            return Ok(());
        };
        let op = record.intent.request.operation;
        if self.suspended == Some(op) || self.pending.is_some() {
            return Ok(());
        }
        let Ok(core) = core(service, self.group) else {
            return Ok(());
        };
        let identity = (op, record.index, core.state().hard_state.term);
        if self.seen == Some(identity) {
            return Ok(());
        }
        let action = app.leadership_action(core).map_err(|e| format!("{e:?}"))?;
        match action {
            LeadershipAction::Wait => (),
            LeadershipAction::Transfer(request) => {
                // Mark even a rejection: an unchanged failure is not a busy loop.
                self.seen = Some(identity);
                self.deadline = Some(Instant::now() + Duration::from_secs(5));
                if let Err(e) =
                    service.control(self.group, NodeControl::TransferLeadership(request))
                {
                    self.deadline = None;
                    eprintln!("leadership admission refused: {e:?}; inspect status or resume");
                }
            }
            LeadershipAction::Complete(command) => {
                self.seen = Some(identity);
                match propose_for(service, self.group, command) {
                    Ok(ticket) => {
                        self.pending = Some(ticket);
                        self.deadline = Some(Instant::now() + Duration::from_secs(5));
                    }
                    Err(e) => {
                        eprintln!("leadership completion refused: {e}; inspect status or resume")
                    }
                }
            }
        }
        Ok(())
    }
}
