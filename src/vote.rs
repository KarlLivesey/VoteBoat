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
//! Deterministic RequestVote gate. Not yet a complete Raft state machine.
use crate::{contracts::*, identity::*, quorum::Policy};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VoteRequest {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    /// Must be authenticated by the host before delivery.
    pub candidate: NodeId,
    pub term: u64,
    pub last_log_term: u64,
    pub last_log_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VoteResponse {
    pub group: GroupIdentity,
    pub configuration: ConfigurationId,
    pub voter: NodeId,
    pub candidate: NodeId,
    pub term: u64,
    pub granted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VoteEffect {
    Persist(VoteRecord),
    Reply(VoteResponse),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoteError {
    Busy,
    WrongContext,
    InvalidRecovery,
    InvalidRequest,
    WrongCompletion,
    Fenced,
}

#[derive(Debug)]
struct Pending {
    record: VoteRecord,
    response: VoteResponse,
    ticket: Option<WriteTicket>,
}

#[derive(Debug)]
pub struct Voter {
    node: NodeId,
    group: GroupIdentity,
    configuration: ConfigurationId,
    policy: Policy,
    binding: StoreBinding,
    durable: HardState,
    last_log: (u64, u64),
    pending: Option<Pending>,
    last_ticket: u64,
    fenced: bool,
}

impl Voter {
    /// Recovery and initial membership are explicit host-authorized inputs.
    /// `last_log` is (term,index) from the same verified store history.
    pub fn recover(
        node: NodeId,
        record: VoteRecord,
        policy: Policy,
        binding: StoreBinding,
        last_log: (u64, u64),
    ) -> Result<Self, VoteError> {
        if !policy.voters().contains(&node)
            || record
                .hard_state
                .voted_for
                .is_some_and(|v| !policy.voters().contains(&v))
            || !record.hard_state.follows(HardState::default())
            || last_log.0 > record.hard_state.term
            || ((last_log.0 == 0) != (last_log.1 == 0))
        {
            return Err(VoteError::InvalidRecovery);
        }
        Ok(Self {
            node,
            group: record.group,
            configuration: record.configuration,
            policy,
            binding,
            durable: record.hard_state,
            last_log,
            pending: None,
            last_ticket: 0,
            fenced: false,
        })
    }

    pub fn hard_state(&self) -> HardState {
        self.durable
    }

    pub fn request(&mut self, request: VoteRequest) -> Result<VoteEffect, VoteError> {
        if self.fenced {
            return Err(VoteError::Fenced);
        }
        if self.pending.is_some() {
            return Err(VoteError::Busy);
        }
        if request.group != self.group
            || request.configuration != self.configuration
            || !self.policy.voters().contains(&request.candidate)
        {
            return Err(VoteError::WrongContext);
        }
        if request.term == 0
            || request.last_log_term > request.term
            || ((request.last_log_term == 0) != (request.last_log_index == 0))
        {
            return Err(VoteError::InvalidRequest);
        }
        let mut next = self.durable;
        if request.term > next.term {
            next = HardState {
                term: request.term,
                voted_for: None,
            };
        }
        let granted = request.term == next.term
            && (next.voted_for.is_none() || next.voted_for == Some(request.candidate))
            && (request.last_log_term, request.last_log_index) >= self.last_log;
        if granted {
            next.voted_for = Some(request.candidate);
        }
        let response = VoteResponse {
            group: self.group,
            configuration: self.configuration,
            voter: self.node,
            candidate: request.candidate,
            term: next.term,
            granted,
        };
        if next == self.durable {
            return Ok(VoteEffect::Reply(response));
        }
        let record = VoteRecord {
            group: self.group,
            configuration: self.configuration,
            hard_state: next,
        };
        self.pending = Some(Pending {
            record,
            response,
            ticket: None,
        });
        Ok(VoteEffect::Persist(record))
    }

    /// Called exactly once after this effect's append admission. Caller binds
    /// the returned ticket to that append, never to unrelated provider work.
    pub fn admitted(&mut self, ticket: WriteTicket) -> Result<(), VoteError> {
        if self.fenced {
            return Err(VoteError::Fenced);
        }
        let pending = self.pending.as_mut().ok_or(VoteError::WrongCompletion)?;
        if ticket.binding != self.binding
            || ticket.sequence <= self.last_ticket
            || pending.ticket.is_some()
        {
            return Err(VoteError::WrongCompletion);
        }
        pending.ticket = Some(ticket);
        self.last_ticket = ticket.sequence;
        Ok(())
    }

    /// Only the exact admitted dependency releases a ballot/term response.
    pub fn durable(&mut self, completion: &DurableVotes) -> Result<VoteResponse, VoteError> {
        if self.fenced {
            return Err(VoteError::Fenced);
        }
        let pending = self.pending.as_ref().ok_or(VoteError::WrongCompletion)?;
        let ticket = pending.ticket.ok_or(VoteError::WrongCompletion)?;
        if !completion.tickets.contains(&ticket) {
            return Err(VoteError::WrongCompletion);
        }
        let pending = self.pending.take().expect("pending checked above");
        self.durable = pending.record.hard_state;
        Ok(pending.response)
    }

    /// Rejected or uncertain storage stops this voter until explicit recovery.
    pub fn storage_failed(&mut self) {
        self.fenced = true;
        self.pending = None;
    }
}

/// Convenience driver over the PUBLIC seam. No provider internals or hidden I/O.
pub fn drive_vote<S: VoteStore>(
    voter: &mut Voter,
    store: &mut S,
    request: VoteRequest,
) -> Result<VoteResponse, DriveError> {
    if store.binding() != voter.binding {
        return Err(DriveError::Core(VoteError::WrongContext));
    }
    match voter.request(request).map_err(DriveError::Core)? {
        VoteEffect::Reply(response) => Ok(response),
        VoteEffect::Persist(record) => {
            let result = (|| {
                let ticket = store
                    .append_votes(vec![record])
                    .map_err(DriveError::Storage)?;
                voter.admitted(ticket).map_err(DriveError::Core)?;
                let durable = store.barrier(&[ticket]).map_err(DriveError::Storage)?;
                voter.durable(&durable).map_err(DriveError::Core)
            })();
            if result.is_err() {
                voter.storage_failed();
            }
            result
        }
    }
}

#[derive(Debug)]
pub enum DriveError {
    Core(VoteError),
    Storage(StorageError),
}
