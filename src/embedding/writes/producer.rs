// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

impl<R> WriteSubmission<R> {
    pub fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
    pub fn binding(&self) -> WriteBinding {
        self.key.shared.binding()
    }
    /// Host must compare this binding to its actual core before extraction.
    /// Copying into the existing owned core input preserves the immutable witness.
    pub fn take_next(&mut self) -> Result<Option<ExtractedOriginal<R>>, WriteError> {
        let mut state = self.key.shared.lock()?;
        self.key.check(&state)?;
        if state.closed.is_some() || self.key.shared.registry.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        if self.next == self.len {
            return Ok(None);
        }
        let cell = self.key.original(&mut state, self.next)?;
        if cell.producer != Producer::Unextracted {
            return Err(WriteError::AlreadyTerminal);
        }
        let operation = cell.operation.ok_or(WriteError::ProviderViolation)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(cell.bytes.len())
            .map_err(|_| WriteError::Allocation)?;
        if bytes.capacity() > self.key.shared.limits.command_bytes {
            return Err(WriteError::ProviderViolation);
        }
        bytes.extend_from_slice(&cell.bytes);
        cell.producer = Producer::Extracted;
        state.owners += 1;
        let reply = OriginalReply {
            key: Lease::new(self.key.copy()),
            ordinal: self.next,
        };
        self.next += 1;
        Ok(Some((operation, bytes, reply)))
    }
}
impl<R> Drop for WriteSubmission<R> {
    fn drop(&mut self) {
        let shared = self.key.shared.clone();
        let wake = {
            let (mut state, poisoned) = match shared.state.lock() {
                Ok(state) => (state, false),
                Err(p) => (p.into_inner(), true),
            };
            let mut wake = if poisoned {
                state.fail(WriteUnknown::OwnerFailed)
            } else {
                None
            };
            if self.key.check(&state).is_ok() {
                for ordinal in self.next..self.len {
                    let cell = &mut state.cells[ordinal];
                    if cell.producer == Producer::Unextracted {
                        cell.producer = Producer::Terminal;
                        if cell.observation == Observation::Open && cell.output.is_none() {
                            cell.output = Some(OriginalOutcome::NotSubmitted);
                        }
                    }
                }
                if wake.is_none()
                    && state
                        .waiter
                        .as_ref()
                        .is_some_and(|(i, _)| *i >= self.next && *i < self.len)
                {
                    wake = state.waiter.take().map(|(_, w)| w);
                }
            }
            self.key.retire_locked(&mut state, &shared);
            if poisoned {
                shared.state.clear_poison();
            }
            wake
        };
        drop(shared);
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}
impl<R> OriginalReply<R> {
    /// Bind once, using the exact entry installed/proposed by THIS actual host.
    /// This expectation grants routing identity only, never commit authority.
    pub fn bind_proposal(
        &mut self,
        domain: &WriteDomain<R>,
        entry: &LogEntry,
    ) -> Result<(), WriteError> {
        self.domain(domain)?;
        let mut state = self.key.shared.lock()?;
        let cell = self.key.original(&mut state, self.ordinal)?;
        if cell.producer != Producer::Extracted {
            return Err(WriteError::AlreadyTerminal);
        }
        if entry.index == 0 || entry.term == 0 || !cell.matches(entry) {
            return Err(WriteError::ProviderViolation);
        }
        cell.producer = Producer::Bound(ProposalPosition {
            index: entry.index,
            term: entry.term,
        });
        Ok(())
    }
    fn domain(&self, domain: &WriteDomain<R>) -> Result<(), WriteError> {
        if !Arc::ptr_eq(&self.key.shared.registry, &domain.registry)
            || self.key.shared.binding() != domain.binding()
        {
            return Err(WriteError::WrongBinding);
        }
        if domain.registry.closed.load(Ordering::Acquire) {
            return Err(WriteError::Closed);
        }
        Ok(())
    }
    /// Host contract: the WHOLE actual committed interval has applied and all
    /// receipt cardinality/identity/nested bounds passed before this call.
    /// Failure returns the exact R and consumes the producer. Stop/fence at that
    /// ordinal; neither successful prefixes nor application work can roll back.
    pub fn publish_applied(
        self,
        domain: &WriteDomain<R>,
        entry: &LogEntry,
        receipt: R,
        nested_receipt_bytes: usize,
    ) -> Result<(), DeliveryRejected<R>> {
        let checked = self.check_publish(domain, entry, nested_receipt_bytes);
        let position = match checked {
            Ok(position) => position,
            Err(reason) => return Err(DeliveryRejected { reason, receipt }),
        };
        // resolve validates again under the same state mutex as cancellation.
        // R is returned on every failure; it never passes through a discarded send.
        self.deliver(position, receipt)
    }
    fn check_publish(
        &self,
        domain: &WriteDomain<R>,
        entry: &LogEntry,
        nested: usize,
    ) -> Result<ProposalPosition, WriteError> {
        self.domain(domain)?;
        if size_of::<R>()
            .checked_add(nested)
            .is_none_or(|n| n > self.key.shared.limits.receipt_bytes)
        {
            return Err(WriteError::ProviderViolation);
        }
        let mut state = self.key.shared.lock()?;
        let cell = self.key.original(&mut state, self.ordinal)?;
        let Producer::Bound(position) = cell.producer else {
            return Err(WriteError::ProviderViolation);
        };
        if position.index != entry.index || position.term != entry.term || !cell.matches(entry) {
            return Err(WriteError::ProviderViolation);
        }
        Ok(position)
    }
    fn deliver(
        mut self,
        position: ProposalPosition,
        receipt: R,
    ) -> Result<(), DeliveryRejected<R>> {
        let mut receipt = Some(receipt);
        let shared = self.key.shared.clone();
        let result = (|| {
            let mut state = shared.lock()?;
            let cell = self.key.original(&mut state, self.ordinal)?;
            if cell.producer != Producer::Bound(position) {
                return Err(WriteError::AlreadyTerminal);
            }
            if cell.observation != Observation::Open {
                return Err(WriteError::ConsumerClosed);
            }
            if cell.output.is_some() {
                return Err(WriteError::ProviderViolation);
            }
            cell.producer = Producer::Terminal;
            cell.output = Some(OriginalOutcome::Applied {
                position,
                receipt: receipt.take().unwrap(),
            });
            self.key.retire_locked(&mut state, &shared);
            Ok(
                if state
                    .waiter
                    .as_ref()
                    .is_some_and(|(i, _)| *i == self.ordinal)
                {
                    state.waiter.take().map(|(_, w)| w)
                } else {
                    None
                },
            )
        })();
        // Success has already destroyed its affine key. On failure, Drop
        // consumes the failed producer before any further receiver work.
        drop(self);
        drop(shared);
        match result {
            Ok(wake) => {
                if let Some(wake) = wake {
                    wake.wake();
                }
                Ok(())
            }
            Err(reason) => Err(DeliveryRejected {
                reason,
                receipt: receipt.unwrap(),
            }),
        }
    }
    /// Only before entering a call which could have proposed this original.
    pub fn not_submitted(mut self) -> Result<(), WriteError> {
        {
            let mut state = self.key.shared.lock()?;
            if self.key.original(&mut state, self.ordinal)?.producer != Producer::Extracted {
                return Err(WriteError::ProviderViolation);
            }
        }
        self.key
            .resolve(self.ordinal, OriginalOutcome::NotSubmitted)
    }
    pub fn unknown(mut self, reason: WriteUnknown) -> Result<(), WriteError> {
        self.key
            .resolve(self.ordinal, OriginalOutcome::Unknown(reason))
    }
}
impl<R> Drop for OriginalReply<R> {
    fn drop(&mut self) {
        if self.key.is_live() {
            let _ = self.key.resolve(
                self.ordinal,
                OriginalOutcome::Unknown(WriteUnknown::OwnerFailed),
            );
        }
    }
}
