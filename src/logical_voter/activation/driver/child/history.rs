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
//! Original fresh retained history, not a caller-supplied position assertion.
use super::*;
use crate::logical_voter::bridge::activation::ObservedLogicalConfiguration;
impl<P: LogicalActivationParent> ChildActivationDriver<'_, P> {
    pub(super) fn child_history(
        &mut self,
        observed: ObservedLogicalConfiguration,
    ) -> Result<usize, ChildActivationDriverError> {
        let auth = self
            .authorization
            .as_ref()
            .ok_or(ChildActivationDriverError::WrongBinding)?;
        let h = &observed.history;
        let phase = self.child.phase;
        let operation = auth.record(if phase == 0 { 0 } else { 1 }).operation;
        if h.child != self.intent.mapping.child
            || h.operation != operation
            || observed.ticket.group != h.child
            || observed.ticket.binding.owner != observed.owner
            || observed.ticket.request != observed.barrier.request()
            || observed.barrier.group() != h.child
            || h.entries.len() > if phase == 0 { 1 } else { 2 }
        {
            return Err(self.fail(ChildActivationDriverError::ProviderViolation));
        }
        for (i, e) in h.entries.iter().enumerate() {
            let expected: &ConfigurationRecord = auth.record(if phase == 0 { 0 } else { i + 1 });
            if e.payload != EntryPayload::Configuration(Box::new(expected.clone()))
                || e.index == 0
                || e.term == 0
                || e.index > h.child_applied
                || e.index > observed.barrier.index()
                || e.term > observed.barrier.term()
                || i > 0 && (e.index <= h.entries[i - 1].index || e.term < h.entries[i - 1].term)
            {
                return Err(self.fail(ChildActivationDriverError::ProviderViolation));
            }
        }
        let entries = h.entries.len();
        if phase == 0 && h.entries.len() == 1 {
            let retained = std::mem::size_of::<ObservedLogicalConfiguration>()
                .saturating_add(
                    h.entries
                        .capacity()
                        .saturating_mul(std::mem::size_of::<LogEntry>()),
                )
                .saturating_add(
                    h.entries
                        .iter()
                        .map(LogEntry::retained_payload_bytes)
                        .fold(0usize, usize::saturating_add),
                );
            if retained > self.child.learners_reservation {
                return Err(self.fail(ChildActivationDriverError::Overloaded));
            }
            self.child.learners = Some(observed);
            self.child.phase = 1;
        } else if phase != 0 && h.entries.len() == 2 {
            let e = &h.entries[1];
            let position = ActivationPosition {
                index: e.index,
                term: e.term,
            };
            if self.child.final_position.is_some_and(|p| p != position)
                || self
                    .status
                    .as_ref()
                    .and_then(|s| s.child_final)
                    .is_some_and(|p| p != position)
            {
                return Err(self.fail(ChildActivationDriverError::ProviderViolation));
            }
            self.child.final_position = Some(position);
        }
        Ok(entries)
    }
}
