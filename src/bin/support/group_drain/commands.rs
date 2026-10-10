// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::identity::OperationId;

fn output(text: String) -> Phase {
    Phase::Output {
        bytes: format!("{text}\n").into_bytes(),
        sent: 0,
    }
}
impl Driver {
    fn start(
        &mut self,
        service: &Service,
        sequence: u64,
        operation: OperationId,
    ) -> Result<Phase, String> {
        let plan = self
            .plan
            .as_ref()
            .ok_or("group drain requires --group-drain-plan")?;
        let record = plan.record(sequence).map_err(|e| format!("{e:?}"))?;
        if record.request.operation != operation {
            return Err("operation differs from original group drain".into());
        }
        if self.busy() {
            return Err("group drain publication busy".into());
        }
        if let Some(old) = &self.latest {
            if old.sequence == sequence && old.request == record.request {
                return Ok(output(self.status(service)));
            }
            if !record.follows(old) {
                return Err("conflicting group drain identity".into());
            }
        }
        for entry in plan.assignments() {
            let core = service
                .local()
                .owner
                .core(entry.group)
                .ok_or("missing group drain assignment")?;
            let original = plan
                .groups()
                .iter()
                .find(|g| g.group == entry.group)
                .map(|g| &g.original)
                .or_else(|| {
                    plan.retained_learners()
                        .iter()
                        .find(|g| g.group == entry.group)
                        .map(|g| &g.original)
                })
                .ok_or("missing original configuration")?;
            if core.is_fenced()
                || core.membership().joint().is_some()
                || core.membership().stable() != original
                || core.membership().last_configuration_index() > core.state().commit_index
            {
                return Err("group drain requires original committed configurations".into());
            }
        }
        self.publish(record)?;
        Ok(Phase::Pending(Pending::Drain(sequence, operation)))
    }
    pub fn command(
        &mut self,
        service: &Service,
        words: &[&str],
        quit: &mut bool,
    ) -> Result<Phase, String> {
        let [verb, sequence, operation, extra @ ..] = words else {
            return Err("expected drain command SEQUENCE OP".into());
        };
        let sequence = sequence
            .parse::<u64>()
            .ok()
            .filter(|n| *n != 0)
            .ok_or("invalid drain sequence")?;
        let operation = OperationId::new(operation.parse().map_err(|_| "invalid drain operation")?)
            .ok_or("invalid drain operation")?;
        if *verb == "drain-node" && extra.is_empty() {
            return self.start(service, sequence, operation);
        }
        let record = self
            .latest
            .as_ref()
            .filter(|r| r.sequence == sequence && r.request.operation == operation)
            .ok_or("no matching durable group drain")?;
        if *verb == "drain-group" {
            let [offset] = extra else {
                return Err("expected drain-group SEQUENCE OP OFFSET".into());
            };
            return self
                .row(service, offset.parse().map_err(|_| "invalid group offset")?)
                .map(output);
        }
        if !extra.is_empty() {
            return Err("unexpected group drain arguments".into());
        }
        match *verb {
            "drain-status" => Ok(output(self.status(service))),
            "resume-drain" if record.phase == DrainPhase::Active => {
                Ok(output(self.status(service)))
            }
            "cancel-drain" => {
                if record.phase == DrainPhase::Cancelled {
                    return Ok(output(self.status(service)));
                }
                let mut record = record.clone();
                record.phase = DrainPhase::Cancelled;
                self.publish(record)?;
                Ok(Phase::Pending(Pending::Drain(sequence, operation)))
            }
            "drain-stop" => {
                if !self.ready(service)? {
                    return Err("group drain is not locally ready".into());
                }
                *quit = true;
                Ok(output(format!("OK sequence={sequence} operation={} stopping=true multi=true retained_replica=true", operation.get())))
            }
            _ => Err("invalid or inactive group drain command".into()),
        }
    }
    fn row(&self, service: &Service, offset: usize) -> Result<String, String> {
        let plan = self.plan.as_ref().ok_or("missing group drain plan")?;
        let journal = self
            .journal
            .as_ref()
            .ok_or("group drain publication pending")?;
        let record = plan.verify(journal).map_err(|e| format!("{e:?}"))?;
        let entry = plan
            .assignments()
            .get(offset)
            .ok_or("group offset outside plan")?;
        let core = service
            .local()
            .owner
            .core(entry.group)
            .ok_or("missing group")?;
        let done = matches!(
            plan.next(journal, core).map_err(|e| format!("{e:?}"))?,
            MembershipDrainAction::Completed
        );
        let mut text = format!("OK sequence={} operation={} offset={offset} groups={} group={} incarnation={} configuration={} done={done} source={} source_store={} source_incarnation={}", record.sequence, record.request.operation.get(), plan.assignments().len(), entry.group.id.get(), entry.group.incarnation.get(), entry.configuration.get(), record.owner.node.get(), record.owner.store.id.get(), record.owner.store.incarnation.get());
        if let Some(voter) = plan.groups().iter().find(|g| g.group == entry.group) {
            text.push_str(&format!(
                " kind=voter target={} store={} store_incarnation={} configuration_operation={}",
                voter.handoff.node.get(),
                voter.handoff.store.id.get(),
                voter.handoff.store.incarnation.get(),
                voter.change.joint.operation.get()
            ));
        } else {
            text.push_str(" kind=retained");
        }
        Ok(text)
    }
}
