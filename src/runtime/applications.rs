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
//! Bounded application outcomes; persistence and core commitment remain separate.
use super::*;
use crate::application::*;
use crate::log::EntryPayload;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationRouterBinding {
    pub owner: RuntimeOwner,
    /// Fresh host-reserved generation within this runtime lifetime.
    pub generation: ApplicationRouterGeneration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationResultTicket {
    pub binding: ApplicationRouterBinding,
    pub sequence: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct ApplicationRouterLimits {
    pub batches: usize,
    pub receipts: usize,
    pub bytes: usize,
    /// Reserved for committed batches without commands (e.g. election no-ops).
    pub control_batches: usize,
    pub control_bytes: usize,
    pub batch_receipts: usize,
    pub batch_bytes: usize,
}
impl Default for ApplicationRouterLimits {
    fn default() -> Self {
        Self {
            batches: 64,
            receipts: 65536,
            bytes: 16 * 1024 * 1024,
            control_batches: 4,
            control_bytes: 64 * 1024,
            batch_receipts: 16384,
            batch_bytes: 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ApplicationResultUsage {
    pub batches: usize,
    pub receipts: usize,
    pub bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationRouteError {
    InvalidLimits,
    WrongBinding,
    WrongEffect,
    Closed,
    Fenced,
    Overloaded,
    ResultTooLarge,
    Exhausted,
    StaleResult,
    ProviderViolation,
    Application(ApplicationError),
    Owner(EffectOwnerError),
}
#[derive(Debug)]
pub struct ApplicationRouteRejected {
    pub reason: ApplicationRouteError,
    pub lease: Box<EffectLease>,
}
/// Opaque owned output. Polling transfers observation, not credits. Keep it
/// intact until complete returns its receipts to separately budgeted host storage.
#[derive(Debug)]
pub struct ApplicationResults<R> {
    ticket: ApplicationResultTicket,
    effect: EffectTicket,
    through: u64,
    receipts: Vec<R>,
    positions: Box<[ProposalPosition]>,
}
impl<R> ApplicationResults<R> {
    pub fn ticket(&self) -> ApplicationResultTicket {
        self.ticket
    }
    pub fn effect(&self) -> EffectTicket {
        self.effect
    }
    pub fn through(&self) -> u64 {
        self.through
    }
    pub fn receipts(&self) -> &[R] {
        &self.receipts
    }
    /// Original verified committed position for each receipt, in the same order.
    /// Retained after logical log compaction; only this router constructs it.
    pub fn positions(&self) -> &[ProposalPosition] {
        &self.positions
    }
}
#[derive(Debug)]
pub struct ApplicationResultRejected<R> {
    pub reason: ApplicationRouteError,
    pub results: Box<ApplicationResults<R>>,
}
struct Held<R> {
    ticket: ApplicationResultTicket,
    output: Option<ApplicationResults<R>>,
    charged: ApplicationResultUsage,
    control: bool,
}
/// Serial application over exact Committed leases. Output capacity is reserved
/// before invoking the host application. Receipts escape only after valid ordered
/// apply and effect-owner release. Written storage or proposal admission cannot
/// enter this path. No I/O, executor, application store or core is constructed.
/// Host application errors/contract violations fence the owner for recovery.
pub struct ApplicationRouter<R> {
    binding: ApplicationRouterBinding,
    limits: ApplicationRouterLimits,
    held: BTreeMap<u64, Held<R>>,
    ready: VecDeque<u64>,
    usage: ApplicationResultUsage,
    bulk: ApplicationResultUsage,
    sequence: u64,
    closed: bool,
    failed: bool,
}
impl<R: ApplicationReceipt> ApplicationRouter<R> {
    pub fn new(
        binding: ApplicationRouterBinding,
        limits: ApplicationRouterLimits,
    ) -> Result<Self, ApplicationRouteError> {
        if limits.batches < 2
            || limits.batches > 4096
            || limits.receipts == 0
            || limits.receipts > 1_048_576
            || limits.bytes == 0
            || limits.bytes > 1024 * 1024 * 1024
            || limits.control_batches == 0
            || limits.control_batches >= limits.batches
            || limits.control_bytes < size_of::<Held<R>>()
            || limits.control_bytes >= limits.bytes
            || limits.batch_receipts == 0
            || limits.batch_receipts > limits.receipts
            || limits.batch_receipts > 65536
            || limits.batch_bytes == 0
            || limits
                .batch_bytes
                .checked_add(
                    limits
                        .batch_receipts
                        .saturating_mul(size_of::<ProposalPosition>()),
                )
                .and_then(|n| n.checked_add(size_of::<Held<R>>()))
                .is_none_or(|n| n > limits.bytes - limits.control_bytes)
        {
            return Err(ApplicationRouteError::InvalidLimits);
        }
        Ok(Self {
            binding,
            limits,
            held: BTreeMap::new(),
            ready: VecDeque::with_capacity(limits.batches),
            usage: ApplicationResultUsage::default(),
            bulk: ApplicationResultUsage::default(),
            sequence: 0,
            closed: false,
            failed: false,
        })
    }
    pub fn binding(&self) -> ApplicationRouterBinding {
        self.binding
    }
    pub fn limits(&self) -> ApplicationRouterLimits {
        self.limits
    }
    pub fn usage(&self) -> ApplicationResultUsage {
        self.usage
    }
    pub fn is_fenced(&self) -> bool {
        self.failed
    }
    pub fn close(&mut self) {
        self.closed = true;
    }
    pub fn is_drained(&self) -> bool {
        self.held.is_empty()
    }
    pub fn submit<
        A: BoundedStateMachine<Receipt = R>,
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        lease: EffectLease,
        application: &mut A,
        now: MonoTime,
    ) -> Result<ApplicationResultTicket, ApplicationRouteRejected> {
        let preflight = (|| {
            if self.failed {
                return Err(ApplicationRouteError::Fenced);
            }
            if self.closed {
                return Err(ApplicationRouteError::Closed);
            }
            if owner.identity() != self.binding.owner {
                return Err(ApplicationRouteError::WrongBinding);
            }
            owner
                .validate_lease(&lease)
                .map_err(ApplicationRouteError::Owner)?;
            let Effect::Committed(entries) = &lease.effect else {
                return Err(ApplicationRouteError::WrongEffect);
            };
            let Some(first) = entries.first() else {
                return Err(ApplicationRouteError::WrongEffect);
            };
            let through = entries.last().unwrap().index;
            let state = owner.core(lease.ticket.visit.group).unwrap().state();
            application
                .validate_group(state.bootstrap.group)
                .map_err(ApplicationRouteError::Application)?;
            if through > state.commit_index
                || entries.iter().any(|e| state.entry_at(e.index) != Some(e))
            {
                return Err(ApplicationRouteError::WrongEffect);
            }
            if application.applied_index().checked_add(1) != Some(first.index)
                || entries
                    .windows(2)
                    .any(|e| e[0].index.checked_add(1) != Some(e[1].index))
            {
                return Err(ApplicationRouteError::Application(
                    ApplicationError::IndexGap,
                ));
            }
            let count = entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count();
            if count > self.limits.batch_receipts {
                return Err(ApplicationRouteError::ResultTooLarge);
            }
            let bound = application
                .receipt_bytes_bound(entries)
                .map_err(ApplicationRouteError::Application)?;
            if bound > self.limits.batch_bytes
                || count.checked_mul(size_of::<R>()).is_none_or(|n| n > bound)
            {
                return Err(ApplicationRouteError::ResultTooLarge);
            }
            let bytes = bound
                .checked_add(
                    count
                        .checked_mul(size_of::<ProposalPosition>())
                        .ok_or(ApplicationRouteError::ResultTooLarge)?,
                )
                .and_then(|n| n.checked_add(size_of::<Held<R>>()))
                .ok_or(ApplicationRouteError::ResultTooLarge)?;
            let control = count == 0;
            let l = self.limits;
            if self.usage.batches >= l.batches
                || count > l.receipts - self.usage.receipts
                || bytes > l.bytes - self.usage.bytes
                || (!control
                    && (self.bulk.batches >= l.batches - l.control_batches
                        || bytes > (l.bytes - l.control_bytes).saturating_sub(self.bulk.bytes)))
            {
                return Err(ApplicationRouteError::Overloaded);
            }
            let sequence = self
                .sequence
                .checked_add(1)
                .ok_or(ApplicationRouteError::Exhausted)?;
            Ok((count, bound, bytes, control, sequence, through))
        })();
        let (count, bound, bytes, control, sequence, through) = match preflight {
            Ok(v) => v,
            Err(reason) => {
                return Err(ApplicationRouteRejected {
                    reason,
                    lease: Box::new(lease),
                })
            }
        };
        // Synchronous serialized call: no other intake can spend this reservation.
        // Allocation by an untrusted/broken in-process provider cannot be sandboxed.
        let Effect::Committed(entries) = &lease.effect else {
            unreachable!()
        };
        let applied = application.apply_batch(entries);
        let receipts = match applied {
            Ok(r) => r,
            Err(error) => {
                self.failed = true;
                let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
                return Err(ApplicationRouteRejected {
                    reason: ApplicationRouteError::Application(error),
                    lease: Box::new(lease),
                });
            }
        };
        let valid = (|| {
            if application.applied_index() != through || receipts.len() != count {
                return None;
            }
            let mut retained = receipts.capacity().checked_mul(size_of::<R>())?;
            if retained > bound {
                return None;
            }
            let mut expected = entries.iter().filter_map(|e| match e.payload {
                EntryPayload::Command { operation, .. } => Some((e.index, operation)),
                _ => None,
            });
            for receipt in &receipts {
                if expected.next() != Some((receipt.index(), receipt.operation())) {
                    return None;
                }
                retained = retained.checked_add(receipt.nested_bytes(bound - retained).ok()?)?;
                if retained > bound {
                    return None;
                }
            }
            Some(())
        })()
        .is_some();
        if !valid {
            self.failed = true;
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            // Invalid receipts are never exposed as client results. The app may
            // already have advanced; recovery, not blind retry, resolves this.
            return Err(ApplicationRouteRejected {
                reason: ApplicationRouteError::ProviderViolation,
                lease: Box::new(lease),
            });
        }
        let mut positions = Vec::with_capacity(count);
        positions.extend(
            entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .map(|e| ProposalPosition {
                    index: e.index,
                    term: e.term,
                }),
        );
        let positions = positions.into_boxed_slice();
        let effect = lease.ticket;
        if let Err(rejected) = owner.release(lease, through, now) {
            self.failed = true;
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            return Err(ApplicationRouteRejected {
                reason: ApplicationRouteError::Owner(rejected.reason),
                lease: rejected.lease,
            });
        }
        self.sequence = sequence;
        let ticket = ApplicationResultTicket {
            binding: self.binding,
            sequence,
        };
        let charged = ApplicationResultUsage {
            batches: 1,
            receipts: count,
            bytes,
        };
        self.usage.batches += 1;
        self.usage.receipts += count;
        self.usage.bytes += bytes;
        if !control {
            self.bulk.batches += 1;
            self.bulk.receipts += count;
            self.bulk.bytes += bytes;
        }
        self.held.insert(
            sequence,
            Held {
                ticket,
                output: Some(ApplicationResults {
                    ticket,
                    effect,
                    through,
                    receipts,
                    positions,
                }),
                charged,
                control,
            },
        );
        self.ready.push_back(sequence);
        Ok(ticket)
    }
    pub fn poll(&mut self) -> Option<ApplicationResults<R>> {
        let sequence = self.ready.pop_front()?;
        self.held.get_mut(&sequence)?.output.take()
    }
    /// Consumes exactly one original envelope. Its receipts may then be moved
    /// into host-owned buffers under a separate budget; complete proves no new
    /// durability or client delivery beyond the already ordered application.
    pub fn complete(
        &mut self,
        output: ApplicationResults<R>,
    ) -> Result<Vec<R>, ApplicationResultRejected<R>> {
        let valid = output.ticket.binding == self.binding
            && self
                .held
                .get(&output.ticket.sequence)
                .is_some_and(|h| h.ticket == output.ticket && h.output.is_none());
        if !valid {
            return Err(ApplicationResultRejected {
                reason: ApplicationRouteError::StaleResult,
                results: Box::new(output),
            });
        }
        let held = self.held.remove(&output.ticket.sequence).unwrap();
        self.usage.batches -= held.charged.batches;
        self.usage.receipts -= held.charged.receipts;
        self.usage.bytes -= held.charged.bytes;
        if !held.control {
            self.bulk.batches -= held.charged.batches;
            self.bulk.receipts -= held.charged.receipts;
            self.bulk.bytes -= held.charged.bytes;
        }
        Ok(output.receipts)
    }
}
