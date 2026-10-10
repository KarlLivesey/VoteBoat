// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded operator ownership over the durable maintenance application.
use super::{
    counter_application::{ReadResult, Receipt},
    setup::{group, Service},
    Connection, Pending, Phase,
};
use voteboat::{identity::*, maintenance::*, raft::*, runtime::*, secure::PeerIdentity};
#[path = "leadership_commands/driver.rs"]
mod driver;
pub use driver::Driver;

pub fn is_command(word: Option<&str>) -> bool {
    matches!(
        word,
        Some("move-leader" | "leadership-status" | "resume-leadership" | "cancel-leadership")
    )
}
fn operation(text: &str) -> Result<OperationId, String> {
    text.parse()
        .ok()
        .and_then(OperationId::new)
        .ok_or_else(|| "invalid operation ID".into())
}
pub(super) fn application(
    service: &Service,
) -> Result<&Maintenance<voteboat::application::Counter>, String> {
    application_for(service, group())
}
fn application_for(
    service: &Service,
    group: GroupIdentity,
) -> Result<&Maintenance<voteboat::application::Counter>, String> {
    service
        .local()
        .applications
        .get(&group)
        .ok_or("missing group")?
        .maintenance()
}
fn core(service: &Service, group: GroupIdentity) -> Result<&Raft, String> {
    let core = service.local().owner.core(group).ok_or("missing group")?;
    if core.role() != Role::Leader {
        return Err("NOT_LEADER".into());
    }
    Ok(core)
}
fn lookup(
    service: &Service,
    group: GroupIdentity,
    operation: OperationId,
) -> Result<LeadershipRecord, String> {
    core(service, group)?;
    application_for(service, group)?
        .record(operation)
        .ok_or_else(|| "inconclusive local absence; query leadership-status".into())
}
fn submit(
    service: &mut Service,
    group: GroupIdentity,
    command: LeadershipCommand,
) -> Result<Phase, String> {
    let ticket = propose_for(service, group, command)?;
    Ok(Phase::Pending(Pending::Write(ticket)))
}
pub(super) fn propose(
    service: &mut Service,
    command: LeadershipCommand,
) -> Result<ClientTicket, String> {
    propose_for(service, group(), command)
}
fn propose_for(
    service: &mut Service,
    group: GroupIdentity,
    command: LeadershipCommand,
) -> Result<ClientTicket, String> {
    service
        .propose_maintenance(ClientRequest {
            group,
            operation: command.intent().request.operation,
            bytes: command.encode().map_err(|e| format!("{e:?}"))?,
        })
        .map_err(|r| match r.reason {
            ClientError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
            e => format!("{e:?}"),
        })
}
fn begin(service: &mut Service, group: GroupIdentity, words: &[&str]) -> Result<Phase, String> {
    let ["move-leader", op, config, target, store, incarnation] = words else {
        return Err("expected move-leader OP CONFIG TARGET STORE INC".into());
    };
    let op = operation(op)?;
    let core = core(service, group)?;
    let source = application_for(service, group)?.record(op).map_or(
        PeerIdentity {
            node: core.local_node(),
            store: core.storage_binding().identity,
        },
        |r| r.intent.source,
    );
    let intent = LeadershipIntent {
        source,
        request: LeadershipTransferRequest {
            operation: op,
            configuration: config
                .parse()
                .ok()
                .and_then(ConfigurationId::new)
                .ok_or("invalid configuration")?,
            target: PeerIdentity {
                node: target
                    .parse()
                    .ok()
                    .and_then(NodeId::new)
                    .ok_or("invalid target")?,
                store: StoreIdentity {
                    id: store
                        .parse()
                        .ok()
                        .and_then(StoreId::new)
                        .ok_or("invalid store")?,
                    incarnation: incarnation
                        .parse()
                        .ok()
                        .and_then(StoreIncarnation::new)
                        .ok_or("invalid incarnation")?,
                },
            },
        },
    };
    submit(service, group, LeadershipCommand::Begin(intent))
}
impl Driver {
    pub fn command(&mut self, service: &mut Service, words: &[&str]) -> Result<Phase, String> {
        application_for(service, self.group)?;
        match words {
            ["move-leader", ..] => begin(service, self.group, words),
            ["leadership-status", op] | ["resume-leadership", op] => {
                let op = operation(op)?;
                if words[0] == "resume-leadership" {
                    let record = lookup(service, self.group, op)?;
                    self.resume(record);
                }
                let ticket = service
                    .read_maintenance(self.group, MaintenanceQuery::Leadership(op))
                    .map_err(|r| match r.reason {
                        ReadInvocationError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
                        e => format!("{e:?}"),
                    })?;
                eprintln!("leadership status admitted operation={}", op.get());
                Ok(Phase::Pending(Pending::Read(ticket)))
            }
            ["cancel-leadership", op] => {
                let record = lookup(service, self.group, operation(op)?)?;
                if record.phase != LeadershipPhase::Pending {
                    let ticket = service
                        .read_maintenance(
                            self.group,
                            MaintenanceQuery::Leadership(record.intent.request.operation),
                        )
                        .map_err(|r| format!("{:?}", r.reason))?;
                    return Ok(Phase::Pending(Pending::Read(ticket)));
                }
                self.suspend(service, record)?;
                Ok(Phase::Pending(Pending::CancelLeadership(
                    self.group, record,
                )))
            }
            _ => Err("invalid leadership command".into()),
        }
    }
    pub fn advance_cancel(&mut self, service: &mut Service, connection: &mut Option<Connection>) {
        let Some(c) = connection else {
            return;
        };
        let Phase::Pending(Pending::CancelLeadership(group, record)) = c.phase else {
            return;
        };
        if group != self.group {
            return;
        }
        if service
            .local()
            .owner
            .core(self.group)
            .is_some_and(|c| c.leadership_transfer().is_some())
        {
            return;
        }
        match submit(
            service,
            self.group,
            LeadershipCommand::Cancel {
                intent: record.intent,
                index: record.index,
            },
        ) {
            Ok(phase) => c.phase = phase,
            Err(e) => c.reply(format!("ERR {e}")),
        }
    }
}
pub fn record(record: LeadershipRecord) -> String {
    let i = record.intent;
    format!("OK operation={} configuration={} source={} source_store={} source_incarnation={} target={} target_store={} target_incarnation={} intent_index={} intent_term={} phase={:?}",
        i.request.operation.get(), i.request.configuration.get(), i.source.node.get(), i.source.store.id.get(), i.source.store.incarnation.get(),
        i.request.target.node.get(), i.request.target.store.id.get(), i.request.target.store.incarnation.get(), record.index, record.term, record.phase)
}
pub fn receipt(receipt: &Receipt) -> String {
    match receipt {
        Receipt::Data(r) => format!("OK outcome={:?} duplicate={}", r.outcome, r.duplicate),
        Receipt::Administration {
            outcome: MaintenanceOutcome::Recorded(r),
            ..
        } => record(*r),
        Receipt::Administration { outcome, .. } => format!("ERR maintenance={outcome:?}"),
    }
}
pub fn read(value: &ReadResult) -> String {
    match value {
        ReadResult::Data(value) => format!("OK value={value}"),
        ReadResult::Leadership(Some(r)) => {
            format!("{} evidence=quorum_read historical=true", record(*r))
        }
        ReadResult::Leadership(None) => "OK phase=Absent evidence=quorum_read".into(),
    }
}
