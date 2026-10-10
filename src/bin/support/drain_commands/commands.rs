// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::time::Duration;
fn identity(words: &[&str]) -> Result<(u64, OperationId), String> {
    if words.len() < 3 {
        return Err("expected drain command SEQUENCE OP".into());
    }
    let sequence = words[1]
        .parse::<u64>()
        .ok()
        .filter(|s| *s != 0)
        .ok_or("invalid drain sequence")?;
    let operation = words[2]
        .parse()
        .ok()
        .and_then(OperationId::new)
        .ok_or("invalid operation")?;
    Ok((sequence, operation))
}
fn output(text: String) -> Phase {
    Phase::Output {
        bytes: format!("{text}\n").into_bytes(),
        sent: 0,
    }
}
impl Driver {
    fn start(
        &mut self,
        service: &mut Service,
        leadership: &mut leadership_commands::Driver,
        words: &[&str],
    ) -> Result<Phase, String> {
        let (sequence, operation) = identity(words)?;
        let intent = validation::intent(service, words)?;
        let record = DrainRecord {
            owner: intent.source,
            sequence,
            request: LocalDrainRequest {
                operation,
                groups: vec![DrainGroup {
                    group: group(),
                    configuration: intent.request.configuration,
                }],
            },
            phase: DrainPhase::Active,
            plan: None,
        };
        if let Some(latest) = &self.latest {
            if latest.sequence == sequence && latest.request.operation == operation {
                if latest.request != record.request
                    || handoff(service, operation).is_none_or(|r| r.intent != intent)
                {
                    return Err("conflicting original drain intent".into());
                }
                return Ok(output(self.status(service)));
            }
            if !record.follows(latest) {
                return Err("conflicting or stale drain identity".into());
            }
        }
        if self.attempt.is_some() || self.worker.is_some() || service.local_drain_status().is_some()
        {
            return Err("drain busy; inspect or cancel original identity".into());
        }
        if handoff(service, operation).is_some_and(|r| r.intent != intent) {
            return Err("conflicting handoff identity".into());
        }
        let ticket = leadership_commands::propose(service, LeadershipCommand::Begin(intent))?;
        leadership.hold(operation);
        self.attempt = Some(Attempt {
            record,
            intent,
            ticket: Some(ticket),
            deadline: Instant::now() + Duration::from_secs(10),
        });
        Ok(Phase::Pending(Pending::Drain(sequence, operation)))
    }
    pub fn command(
        &mut self,
        service: &mut Service,
        leadership: &mut leadership_commands::Driver,
        words: &[&str],
        quit: &mut bool,
    ) -> Result<Phase, String> {
        if words.first() == Some(&"drain-node") {
            return self.start(service, leadership, words);
        }
        let (sequence, operation) = identity(words)?;
        if words.len() != 3 {
            return Err("expected drain command SEQUENCE OP".into());
        }
        let record = self
            .latest
            .as_ref()
            .filter(|r| r.sequence == sequence && r.request.operation == operation)
            .ok_or(
                "no matching durable local drain; retry original drain-node if outcome was unknown",
            )?
            .clone();
        match words[0] {
            "drain-status" => Ok(output(self.status(service))),
            "resume-drain" => {
                if record.phase != DrainPhase::Active {
                    return Err("drain is cancelled".into());
                }
                retained_configuration(service, validation::configuration(&record)?)?;
                let handoff =
                    handoff(service, operation).ok_or("awaiting original replicated handoff")?;
                leadership.retry(handoff);
                Ok(output(self.status(service)))
            }
            "cancel-drain" => {
                if self.worker.is_some() || self.attempt.is_some() {
                    return Err("drain publication busy".into());
                }
                if record.phase == DrainPhase::Cancelled {
                    return Ok(output(self.status(service)));
                }
                let mut cancelled = record;
                cancelled.phase = DrainPhase::Cancelled;
                self.publish(cancelled)?;
                Ok(Phase::Pending(Pending::Drain(sequence, operation)))
            }
            "drain-stop" => {
                self.ready(service)?;
                *quit = true;
                Ok(output(format!(
                    "OK sequence={sequence} operation={} stopping=true retained_replica=true",
                    operation.get()
                )))
            }
            _ => Err("invalid drain command".into()),
        }
    }
    fn ready(&self, service: &Service) -> Result<(), String> {
        let record = self.latest.as_ref().ok_or("no durable drain")?;
        if record.phase != DrainPhase::Active || self.worker.is_some() || self.attempt.is_some() {
            return Err("drain is not durably active".into());
        }
        let configuration = validation::configuration(record)?;
        retained_configuration(service, configuration)?;
        let handoff = handoff(service, record.request.operation)
            .ok_or("original handoff not locally applied")?;
        if handoff.intent.source != record.owner
            || handoff.intent.request.configuration != configuration
            || !matches!(handoff.phase, LeadershipPhase::Completed { .. })
        {
            return Err("original handoff is not completed".into());
        }
        if !service
            .local_drain_status()
            .is_some_and(|s| s.locally_quiescent)
        {
            return Err("drain not locally quiescent".into());
        }
        Ok(())
    }
    pub(super) fn status(&self, service: &Service) -> String {
        let Some(record) = &self.latest else {
            return "OK phase=Absent evidence=local_durable".into();
        };
        let local = service.local_drain_status();
        format!("OK sequence={} operation={} phase={:?} ready={} resuming={} handoff={:?} publication_pending={} evidence=local_durable retained_replica=true cancellation_scope=local_gate",
            record.sequence, record.request.operation.get(), record.phase, self.ready(service).is_ok(),
            local.as_ref().is_some_and(|s| s.resuming), handoff(service, record.request.operation).map(|r| r.phase), self.worker.is_some())
    }
}
