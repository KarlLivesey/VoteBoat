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
impl LogicalPersistencePort {
    pub(in crate::logical_voter) fn flush(&mut self) {
        while let Some(event) = self.held.pop_front() {
            match self.events.try_send(event) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => {
                    self.held.push_front(event);
                    break;
                }
                Err(TrySendError::Disconnected(event)) => {
                    self.held.push_front(event);
                    self.fenced = true;
                    break;
                }
            }
        }
    }
    pub(in crate::logical_voter) fn receive(&mut self) -> Result<(), StorageError> {
        if self.pending.is_some() || !self.held.is_empty() {
            return Ok(());
        }
        let batch = match self.requests.try_recv() {
            Ok(batch) => batch,
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return Ok(()),
        };
        let unit = &batch.units[0];
        let mut state = BTreeMap::from([(self.mapping.parent, self.parent.clone())]);
        match apply_batch(
            &mut state,
            &[LogMutation::Update(unit.update.clone())],
            self.limits,
        ) {
            Ok(()) => {
                self.pending = Some(PendingPersist {
                    batch,
                    next: state
                        .remove(&self.mapping.parent)
                        .expect("validated original group"),
                    operation: None,
                })
            }
            Err(error) => {
                self.fenced = true;
                self.held.push_back(WorkerEvent::Failed {
                    request: batch.ticket,
                    visits: batch.units.iter().map(|u| u.visit).collect(),
                    error: error.clone(),
                });
                return Err(error);
            }
        }
        Ok(())
    }
    pub(in crate::logical_voter) fn finish(
        &mut self,
        observation: &CommittedLogicalAction,
    ) -> Result<(), StorageError> {
        let pending = self.pending.as_ref().ok_or(StorageError::StaleTicket)?;
        let unit = &pending.batch.units[0];
        let Some((origin, update, generation)) = observation.persist() else {
            return Err(StorageError::StaleTicket);
        };
        if origin.request != pending.batch.ticket
            || origin.visit != unit.visit
            || update != &unit.update
            || generation != self.parent.generation
            || Some(observation.receipt().operation) != pending.operation
            || observation.mapping() != self.mapping
            || observation.representative() != self.representative
        {
            return Err(StorageError::StaleTicket);
        }
        let receipt = observation.receipt();
        if receipt.outcome != LogicalOutcome::Mutated {
            self.fenced = true;
            self.held.push_back(WorkerEvent::Failed {
                request: pending.batch.ticket,
                visits: pending.batch.units.iter().map(|u| u.visit).collect(),
                error: StorageError::Rejected("committed logical mutation refusal"),
            });
            self.pending = None;
            return Ok(());
        }
        if receipt.parent_revision != Some(pending.next.revision)
            || receipt.parent_generation != Some(pending.next.generation)
        {
            return Err(StorageError::Corrupt(
                "logical mutation receipt image mismatch",
            ));
        }
        let ticket = LogTicket {
            binding: self.binding.store,
            batch: pending.batch.ticket.sequence,
            group: self.mapping.parent,
            revision: pending.next.revision,
            generation: pending.next.generation,
            last_index: pending.next.last_index(),
            term: pending.next.hard_state.term,
        };
        self.held.push_back(WorkerEvent::Written {
            request: pending.batch.ticket,
            admissions: vec![(unit.visit, ticket)],
        });
        self.held.push_back(WorkerEvent::Durable {
            request: pending.batch.ticket,
            visits: vec![unit.visit],
            completion: DurableLog {
                tickets: vec![ticket],
            },
        });
        self.parent = pending.next.clone();
        self.pending = None;
        Ok(())
    }
}
