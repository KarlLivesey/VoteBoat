// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    setup::{checked, Failure, Service},
    Connection, Pending, Phase,
};
use std::{path::Path, thread::JoinHandle};
use voteboat::{drain::*, native::drain_journal::*, secure::PeerIdentity};
#[path = "group_drain/commands.rs"]
mod commands;

type Publication = JoinHandle<(NativeDrainJournal, Result<(), DrainJournalError>)>;
pub struct Driver {
    plan: Option<MembershipDrainPlan>,
    journal: Option<NativeDrainJournal>,
    latest: Option<DrainRecord>,
    worker: Option<Publication>,
}
impl Driver {
    pub fn open(
        service: &mut Service,
        root: &Path,
        create: bool,
        plan: Option<MembershipDrainPlan>,
    ) -> Result<Self, Failure> {
        let group = service
            .local()
            .owner
            .groups()
            .next()
            .ok_or("missing drain group")?;
        let core = service
            .local()
            .owner
            .core(group)
            .ok_or("missing drain core")?;
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
        .map_err(|(error, _)| format!("drain journal: {error:?}"))?;
        let latest = checked(journal.latest())?;
        if let Some(plan) = &plan {
            let mut expected = checked(plan.record(latest.as_ref().map_or(1, |r| r.sequence)))?;
            if expected.owner != owner {
                return Err("group drain belongs to another owner".into());
            }
            if let Some(record) = &latest {
                expected.phase = record.phase;
                if &expected != record {
                    return Err("group drain differs from original durable plan".into());
                }
            }
        } else if latest.is_some() {
            return Err("group drain journal requires the original group drain plan".into());
        }
        checked(service.restore_drain(&journal))?;
        Ok(Self {
            plan,
            journal: Some(journal),
            latest,
            worker: None,
        })
    }
    pub fn busy(&self) -> bool {
        self.worker.is_some()
    }
    pub fn finish(&mut self) -> Result<(), Failure> {
        if let Some(worker) = self.worker.take() {
            let (journal, result) = worker.join().map_err(|_| "group drain worker panicked")?;
            self.journal = Some(journal);
            checked(result)?;
        }
        Ok(())
    }
    fn publish(&mut self, record: DrainRecord) -> Result<(), String> {
        if self.busy() {
            return Err("group drain publication busy".into());
        }
        let mut journal = self
            .journal
            .take()
            .ok_or("group drain journal unavailable")?;
        self.worker = Some(
            std::thread::Builder::new()
                .name("voteboat-group-drain".into())
                .spawn(move || {
                    let result = journal.publish(record);
                    (journal, result)
                })
                .map_err(|e| format!("group drain worker: {e}"))?,
        );
        Ok(())
    }
    pub fn tick(
        &mut self,
        service: &mut Service,
        connection: &mut Option<Connection>,
    ) -> Result<(), Failure> {
        if self.journal.is_none() && self.worker.is_none() {
            return Err("group drain publication lost; recover required".into());
        }
        if self.worker.as_ref().is_some_and(|w| w.is_finished()) {
            self.finish()?;
            let journal = self.journal.as_ref().ok_or("missing group drain journal")?;
            checked(service.restore_drain(journal))?;
            self.latest = checked(journal.latest())?;
            let record = self
                .latest
                .as_ref()
                .ok_or("missing published group drain")?;
            if let Some(c) = connection.as_mut().filter(|c| matches!(c.phase, Phase::Pending(Pending::Drain(seq, op)) if seq == record.sequence && op == record.request.operation)) {
                c.reply(self.status(service));
            }
        }
        Ok(())
    }
    fn ready(&self, service: &Service) -> Result<bool, String> {
        if self.busy()
            || self
                .latest
                .as_ref()
                .is_none_or(|r| r.phase != DrainPhase::Active)
        {
            return Ok(false);
        }
        service
            .membership_drain_ready(
                self.plan.as_ref().ok_or("missing group drain plan")?,
                self.journal.as_ref().ok_or("missing group drain journal")?,
            )
            .map_err(|e| format!("{e:?}"))
    }
    fn status(&self, service: &Service) -> String {
        let Some(record) = &self.latest else {
            return format!(
                "OK phase=Absent multi=true publication_pending={}",
                self.busy()
            );
        };
        let digest = record
            .plan
            .map(|d| {
                d.as_bytes()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            })
            .unwrap_or_default();
        format!("OK sequence={} operation={} phase={:?} multi=true membership_change=true groups={} plan_digest={} ready={} publication_pending={} evidence=local_durable retained_replica=true cancellation_scope=local_gate", record.sequence, record.request.operation.get(), record.phase, record.request.groups.len(), digest, self.ready(service).unwrap_or(false), self.busy())
    }
}
impl Drop for Driver {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
