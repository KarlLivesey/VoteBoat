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
//! Bounded configuration intent observation over the selected effect owner.
use super::*;
use crate::{log::EntryPayload, membership::ConfigurationRecord};

#[derive(Clone, Copy, Debug)]
pub struct ConfigurationRequestLimits {
    pub requests: usize,
    pub bytes: usize,
}
impl Default for ConfigurationRequestLimits {
    fn default() -> Self {
        Self {
            requests: 1024,
            bytes: 16 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConfigurationTicket {
    admission: AdmissionTicket,
    operation: OperationId,
}
impl ConfigurationTicket {
    pub fn admission(self) -> AdmissionTicket {
        self.admission
    }
    pub fn operation(self) -> OperationId {
        self.operation
    }
}
#[derive(Debug)]
pub struct ConfigurationRequest {
    pub group: GroupIdentity,
    pub proposal: ConfigurationProposal,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationRequestError {
    InvalidLimits,
    WrongOwner,
    Closed,
    Overloaded,
    StaleTicket,
    ProviderViolation,
    Runtime(RuntimeError),
}
#[derive(Debug)]
pub struct ConfigurationRejected {
    pub reason: ConfigurationRequestError,
    pub request: Box<ConfigurationRequest>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationUnknown {
    CancelledWait,
    LeadershipChanged,
    HistoryUnavailable,
    OwnerFailed,
    Shutdown,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigurationOutcome {
    /// Exact configuration record and position are in the durable committed
    /// prefix. This is not finalization of a later joint/final operation.
    Committed(ProposalPosition),
    NotProposed(RaftError),
    /// Observation ended; accepted work may still commit. Never rollback.
    Unknown(ConfigurationUnknown),
}
#[derive(Debug)]
pub struct ConfigurationCompletion {
    pub ticket: ConfigurationTicket,
    pub outcome: ConfigurationOutcome,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigurationResumption {
    Submitted(ConfigurationTicket),
    Completed,
    WaitForCommit,
    NotFoundLocally,
}
struct Pending {
    ticket: ConfigurationTicket,
    record: ConfigurationRecord,
    position: Option<ProposalPosition>,
    outcome: Option<ConfigurationOutcome>,
    bytes: usize,
}
/// One observer per effect-owner lifetime. Admission sequence belongs to the
/// owner, so replacing an empty observer cannot alias an old ticket. Requests
/// and retained outcomes share the same count/byte budgets, one per group.
/// Poll transfers a fixed-size result and releases observer credits; the host
/// must budget exported results. Queued/persisted work remains owned by the
/// effect owner even when a wait is cancelled or this observer is dropped.
pub struct ConfigurationRequests {
    owner: RuntimeOwner,
    limits: ConfigurationRequestLimits,
    pending: BTreeMap<u64, Pending>,
    bytes: usize,
    closed: bool,
}
impl ConfigurationRequests {
    pub fn new(
        owner: RuntimeOwner,
        limits: ConfigurationRequestLimits,
    ) -> Result<Self, ConfigurationRequestError> {
        if limits.requests == 0
            || limits.requests > 65536
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
        {
            return Err(ConfigurationRequestError::InvalidLimits);
        }
        Ok(Self {
            owner,
            limits,
            pending: BTreeMap::new(),
            bytes: 0,
            closed: false,
        })
    }
    pub fn submit<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &mut EffectOwner<S, T, E>,
        request: ConfigurationRequest,
    ) -> Result<ConfigurationTicket, ConfigurationRejected> {
        let checked = (|| {
            if owner.identity() != self.owner {
                return Err(ConfigurationRequestError::WrongOwner);
            }
            if self.closed {
                return Err(ConfigurationRequestError::Closed);
            }
            if self.pending.len() == self.limits.requests
                || self
                    .pending
                    .values()
                    .any(|p| p.ticket.admission.group == request.group)
            {
                return Err(ConfigurationRequestError::Overloaded);
            }
            let bytes = request
                .proposal
                .record
                .retained_bytes()
                .checked_add(size_of::<Pending>())
                .ok_or(ConfigurationRequestError::Overloaded)?;
            if bytes > self.limits.bytes.saturating_sub(self.bytes) {
                return Err(ConfigurationRequestError::Overloaded);
            }
            owner
                .core(request.group)
                .ok_or(ConfigurationRequestError::Runtime(
                    RuntimeError::UnknownGroup,
                ))?;
            Ok(bytes)
        })();
        let bytes = match checked {
            Ok(v) => v,
            Err(reason) => {
                return Err(ConfigurationRejected {
                    reason,
                    request: Box::new(request),
                })
            }
        };
        let ConfigurationRequest { group, proposal } = request;
        let record = proposal.record.clone();
        let admission = match owner.admit_tracked(group, Event::Configure(Box::new(proposal))) {
            Ok(t) => t,
            Err(rejected) => {
                let Event::Configure(proposal) = *rejected.event else {
                    unreachable!()
                };
                return Err(ConfigurationRejected {
                    reason: ConfigurationRequestError::Runtime(rejected.reason),
                    request: Box::new(ConfigurationRequest {
                        group,
                        proposal: *proposal,
                    }),
                });
            }
        };
        let ticket = ConfigurationTicket {
            admission,
            operation: record.operation,
        };
        self.pending.insert(
            admission.sequence,
            Pending {
                ticket,
                record,
                position: None,
                outcome: None,
                bytes,
            },
        );
        self.bytes += bytes;
        Ok(ticket)
    }
    /// Call after every advancement before further administration/compaction.
    /// Steps and durable core state both come from the same selected owner.
    pub fn observe<S: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &mut self,
        owner: &EffectOwner<S, T, E>,
        steps: &[OwnerStep],
    ) -> Result<(), ConfigurationRequestError> {
        if owner.identity() != self.owner {
            return Err(ConfigurationRequestError::WrongOwner);
        }
        for step in steps {
            let Some(ticket) = step.admission else {
                continue;
            };
            let Some(p) = self.pending.get_mut(&ticket.sequence) else {
                continue;
            };
            if ticket != p.ticket.admission
                || step.operation != Some(p.record.operation)
                || step.visit.group != ticket.group
                || step.visit.owner != ticket.owner
            {
                return Err(ConfigurationRequestError::ProviderViolation);
            }
            if p.outcome.is_some() {
                continue;
            }
            if let Some(error) = &step.error {
                p.outcome = Some(ConfigurationOutcome::NotProposed(error.clone()));
            } else if let Some(position) = step.proposed {
                p.position = Some(position);
            } else {
                return Err(ConfigurationRequestError::ProviderViolation);
            }
        }
        for p in self.pending.values_mut().filter(|p| p.outcome.is_none()) {
            let Some(core) = owner.core(p.ticket.admission.group) else {
                p.outcome = Some(ConfigurationOutcome::Unknown(
                    ConfigurationUnknown::OwnerFailed,
                ));
                continue;
            };
            if let Some(position) = p.position {
                let log = core.state();
                let entry = log.entry_at(position.index);
                let exact = entry.is_some_and(|e| e.term == position.term && matches!(&e.payload, EntryPayload::Configuration(record) if **record == p.record));
                if exact && log.commit_index >= position.index {
                    p.outcome = Some(ConfigurationOutcome::Committed(position));
                    continue;
                }
                if log.base_index() >= position.index {
                    p.outcome = Some(ConfigurationOutcome::Unknown(
                        ConfigurationUnknown::HistoryUnavailable,
                    ));
                    continue;
                }
                // Before persistence completion the entry is not yet in state.
                if !core.has_pending_dependency() && !exact {
                    p.outcome = Some(ConfigurationOutcome::Unknown(
                        ConfigurationUnknown::LeadershipChanged,
                    ));
                    continue;
                }
            }
            if p.position.is_some_and(|position| {
                core.role() != Role::Leader || core.state().hard_state.term != position.term
            }) || owner.is_failed()
            {
                p.outcome = Some(ConfigurationOutcome::Unknown(
                    ConfigurationUnknown::LeadershipChanged,
                ));
            }
        }
        Ok(())
    }
    pub fn cancel_wait(
        &mut self,
        ticket: ConfigurationTicket,
    ) -> Result<(), ConfigurationRequestError> {
        let p = self
            .pending
            .get_mut(&ticket.admission.sequence)
            .filter(|p| p.ticket == ticket)
            .ok_or(ConfigurationRequestError::StaleTicket)?;
        if p.outcome.is_none() {
            p.outcome = Some(ConfigurationOutcome::Unknown(
                ConfigurationUnknown::CancelledWait,
            ));
        }
        Ok(())
    }
    pub fn poll(&mut self) -> Option<ConfigurationCompletion> {
        let sequence = self
            .pending
            .iter()
            .find_map(|(&n, p)| p.outcome.as_ref().map(|_| n))?;
        let p = self.pending.remove(&sequence).unwrap();
        self.bytes -= p.bytes;
        Some(ConfigurationCompletion {
            ticket: p.ticket,
            outcome: p.outcome.unwrap(),
        })
    }
    pub fn close(&mut self, reason: ConfigurationUnknown) {
        self.closed = true;
        for p in self.pending.values_mut().filter(|p| p.outcome.is_none()) {
            p.outcome = Some(ConfigurationOutcome::Unknown(reason));
        }
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty()
    }
}
