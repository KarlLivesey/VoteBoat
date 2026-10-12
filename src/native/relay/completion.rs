// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Local queue credit release is separate from original remote request lifetime.
use super::*;
use crate::raft::Message;

pub struct RelayRequestScopes {
    owner: RuntimeOwner,
    ticket: SendTicket,
    scopes: Vec<OriginalScope>,
}
impl RelayRequestScopes {
    pub(in crate::native::relay) fn request_count(&self) -> usize {
        self.scopes
            .iter()
            .filter(|s| matches!(s, OriginalScope::Request(_)))
            .count()
    }
    pub(in crate::native::relay) fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.scopes.capacity() * std::mem::size_of::<OriginalScope>()
            + self.request_count() * std::mem::size_of::<RequestWindow>()
    }
    pub fn ticket(&self) -> SendTicket {
        self.ticket
    }
    pub fn current_requests<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
    ) -> usize {
        if owner.identity() != self.owner {
            return 0;
        }
        self.scopes
            .iter()
            .filter(|scope| match scope {
                OriginalScope::Request(w) => {
                    owner.core(w.group()).is_some_and(|core| w.current(core))
                }
                _ => false,
            })
            .count()
    }
    /// Correlation only, not quorum or durability evidence. The real receiving
    /// core independently checks the original response and its own durable state.
    pub fn matches_reply<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        original: SendTicket,
        source: AuthenticatedSourceView,
        reply: &Message,
    ) -> bool {
        owner.identity() == self.owner
            && original == self.ticket
            && source.receiving_access().local.store == self.owner.store
            && source.receiving_access().local.node == self.ticket.binding.node
            && owner.core(reply.group).is_some_and(|core| {
                self.scopes.iter().any(|scope| {
                    matches!(scope,
                    OriginalScope::Request(w) if w.accepts_reply(core, source.source(), reply))
                })
            })
    }
}
pub struct RelayLocalSend {
    pub local: LocalSendCompletion,
    /// Kept even on local failure: remote delivery may already have occurred.
    pub requests: RelayRequestScopes,
}
pub struct RelayCompletionRejected {
    pub reason: RelayOutputError,
    pub output: Option<Box<NativeRelayBatch>>,
    /// A violating provider may consume the batch and return an unrelated
    /// completion. Retain scopes; do not fabricate the lost original allocation.
    pub requests: Option<RelayRequestScopes>,
}
impl NativeRelayBatch {
    pub(in crate::native::relay) fn same_requests(&self, requests: &RelayRequestScopes) -> bool {
        self.owner == requests.owner
            && self.ticket().peer == requests.ticket.peer
            && self.scopes == requests.scopes
            && self
                .scopes
                .iter()
                .all(|s| matches!(s, OriginalScope::Request(_)))
    }
    pub(in crate::native::relay) fn matches_pending_reply<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &self,
        owner: &EffectOwner<Q, T, E>,
        original: SendTicket,
        source: AuthenticatedSourceView,
        reply: &Message,
    ) -> bool {
        self.owner == owner.identity() && original == self.ticket()
            && source.receiving_access().local.store == self.owner.store
            && source.receiving_access().local.node == self.ticket().binding.node
            && owner.core(reply.group).is_some_and(|core| self.scopes.iter().any(|s| matches!(s, OriginalScope::Request(w) if w.accepts_reply(core, source.source(), reply))))
    }
    /// Call only after the hop has released its actual encoded/frame ownership.
    /// This does not acknowledge a physical voter or retire its protocol scope.
    pub fn complete<O: OutboundQueue>(
        self,
        outbound: &mut O,
        result: LocalSendResult,
    ) -> Result<RelayLocalSend, Box<RelayCompletionRejected>> {
        if outbound.binding() != self.batch.ticket.binding {
            return Err(Box::new(RelayCompletionRejected {
                reason: RelayOutputError::WrongBinding,
                output: Some(Box::new(self)),
                requests: None,
            }));
        }
        let Self {
            owner,
            batch,
            scopes,
            limits,
        } = self;
        let ticket = batch.ticket;
        match outbound.complete(batch, result) {
            Ok(local) if local.ticket == ticket && local.result == result => Ok(RelayLocalSend {
                local,
                requests: RelayRequestScopes {
                    owner,
                    ticket,
                    scopes,
                },
            }),
            Ok(_) => Err(Box::new(RelayCompletionRejected {
                reason: RelayOutputError::ProviderViolation,
                output: None,
                requests: Some(RelayRequestScopes {
                    owner,
                    ticket,
                    scopes,
                }),
            })),
            Err(rejected) => Err(Box::new(RelayCompletionRejected {
                reason: RelayOutputError::Outbound(rejected.reason),
                output: Some(Box::new(Self {
                    owner,
                    batch: *rejected.batch,
                    scopes,
                    limits,
                })),
                requests: None,
            })),
        }
    }
}
