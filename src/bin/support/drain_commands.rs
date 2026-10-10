// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Durable local maintenance and an explicit membership-plan workflow.
use super::{
    counter_application::Receipt,
    leadership_commands,
    setup::{checked, group, Failure, Service},
    Connection, Pending, Phase,
};
use std::{path::Path, thread::JoinHandle, time::Instant};
use voteboat::{
    drain::*, identity::*, maintenance::*, native::drain_journal::*, raft::*, runtime::*,
    secure::PeerIdentity,
};
#[path = "drain_commands/commands.rs"]
mod commands;
#[path = "drain_commands/plan.rs"]
mod plan;
#[path = "drain_commands/validation.rs"]
mod validation;
pub use plan::load as load_plan;
use validation::{handoff, retained_configuration};

struct Attempt {
    record: DrainRecord,
    intent: LeadershipIntent,
    ticket: Option<ClientTicket>,
    deadline: Instant,
}
type Publication = JoinHandle<(NativeDrainJournal, Result<(), DrainJournalError>)>;
pub struct Driver {
    plan: Option<MembershipDrainPlan>,
    journal: Option<NativeDrainJournal>,
    latest: Option<DrainRecord>,
    attempt: Option<Attempt>,
    worker: Option<Publication>,
    reply: Option<(u64, OperationId, String)>,
}
pub fn is_command(word: Option<&str>) -> bool {
    matches!(
        word,
        Some("drain-node" | "drain-status" | "resume-drain" | "cancel-drain" | "drain-stop")
    )
}
impl Driver {
    pub fn finish(&mut self) -> Result<(), Failure> {
        if let Some(worker) = self.worker.take() {
            let (journal, result) = worker.join().map_err(|_| "drain worker panicked")?;
            self.journal = Some(journal);
            checked(result)?;
        }
        Ok(())
    }
    pub fn busy(&self) -> bool {
        self.attempt.is_some() || self.worker.is_some()
    }
    pub fn open(
        service: &mut Service,
        root: &Path,
        create: bool,
        plan: Option<MembershipDrainPlan>,
    ) -> Result<Self, Failure> {
        let core = service.local().owner.core(group()).ok_or("missing group")?;
        let owner = PeerIdentity {
            node: core.local_node(),
            store: core.storage_binding().identity,
        };
        let io = FileDrainRecord::new(root.join("drain.record"));
        let journal = if create {
            NativeDrainJournal::initialize(io, owner)
        } else {
            NativeDrainJournal::recover(io, owner)
        }
        .map_err(|(e, _)| format!("drain journal: {e:?}"))?;
        let latest = checked(journal.latest())?;
        plan::restore(plan.as_ref(), latest.as_ref(), owner)?;
        checked(service.restore_drain(&journal))?;
        Ok(Self {
            plan,
            journal: Some(journal),
            latest,
            attempt: None,
            worker: None,
            reply: None,
        })
    }
    fn publish(&mut self, record: DrainRecord) -> Result<(), String> {
        if self.worker.is_some() {
            return Err("drain publication busy".into());
        }
        let mut journal = self.journal.take().ok_or("drain journal unavailable")?;
        self.worker = Some(
            std::thread::Builder::new()
                .name("voteboat-drain".into())
                .spawn(move || {
                    let result = journal.publish(record);
                    (journal, result)
                })
                .map_err(|e| format!("drain worker: {e}"))?,
        );
        Ok(())
    }
    pub fn complete(&mut self, ticket: ClientTicket, outcome: &ClientOutcome<Receipt>) {
        let Some(attempt) = self.attempt.as_mut().filter(|a| a.ticket == Some(ticket)) else {
            return;
        };
        attempt.ticket = None;
        if !matches!(outcome, ClientOutcome::Applied {
            receipt: Receipt::Administration { outcome: MaintenanceOutcome::Recorded(r), .. }, ..
        } if r.intent == attempt.intent)
        {
            self.reply = Some((
                attempt.record.sequence,
                attempt.record.request.operation,
                format!("UNKNOWN drain handoff={outcome:?}; retry original drain identity"),
            ));
        }
    }
    pub fn tick(
        &mut self,
        service: &mut Service,
        leadership: &mut leadership_commands::Driver,
        connection: &mut Option<Connection>,
    ) -> Result<(), Failure> {
        if self.journal.is_none() && self.worker.is_none() {
            return Err("drain journal lost after publication failure; recover required".into());
        }
        if self.worker.as_ref().is_some_and(|w| w.is_finished()) {
            let (journal, result) = self
                .worker
                .take()
                .unwrap()
                .join()
                .map_err(|_| "drain worker panicked")?;
            checked(result)?;
            checked(service.restore_drain(&journal))?;
            self.latest = checked(journal.latest())?;
            self.journal = Some(journal);
            let record = self.latest.as_ref().ok_or("missing published drain")?;
            leadership.release(record.request.operation);
            self.reply = Some((
                record.sequence,
                record.request.operation,
                self.status(service),
            ));
            self.attempt = None;
        }
        self.advance(service, leadership)?;
        if let Some((sequence, operation, text)) = self.reply.take() {
            if let Some(c) = connection.as_mut().filter(|c| {
                matches!(c.phase,
                Phase::Pending(Pending::Drain(s, op)) if s == sequence && op == operation)
            }) {
                c.reply(text);
            }
        }
        Ok(())
    }
    fn advance(
        &mut self,
        service: &mut Service,
        leadership: &mut leadership_commands::Driver,
    ) -> Result<(), Failure> {
        let Some(attempt) = &self.attempt else {
            return Ok(());
        };
        if self.worker.is_some() {
            return Ok(());
        }
        let op = attempt.record.request.operation;
        if self.reply.is_some() || Instant::now() >= attempt.deadline {
            leadership.release(op);
            if let Some(ticket) = attempt.ticket {
                checked(service.cancel_client(ticket))?;
            }
            if self.reply.is_none() {
                self.reply = Some((
                    attempt.record.sequence,
                    op,
                    "UNKNOWN drain handoff deadline; retry original drain identity".into(),
                ));
            }
            self.attempt = None;
            return Ok(());
        }
        // The application view advances only after durable committed application.
        if attempt.ticket.is_none()
            && handoff(service, op).is_some_and(|r| r.intent == attempt.intent)
        {
            self.publish(attempt.record.clone())?;
        }
        Ok(())
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        // Keep the directory lock until an outstanding publication ends.
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
