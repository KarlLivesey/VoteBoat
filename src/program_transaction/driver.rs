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
//! Explicit bounded host driver. Only consumed original Node outputs supply
//! protocol facts. Drop does no I/O, cancellation, compensation or unlocking.
use super::*;
use crate::{
    application::{BoundedReadableStateMachine, ProposalAdmission},
    connect::PeerConnector,
    identity::*,
    outbound::OutboundQueue,
    raft::{RaftError, Role},
    runtime::*,
    snapshot_worker::SnapshotWorker,
    transport::PeerTransportFactory,
    worker::PersistenceWorker,
};
use std::{
    collections::{BTreeMap, VecDeque},
    mem::size_of,
};
mod types;
pub use types::*;
mod construction;
mod outputs;
mod polling;
mod protocol;
#[cfg(test)]
mod tests;

type ProgramNode<S, T, E, W, O, H, C, F> = Node<S, T, E, ProgramApplication, W, O, H, C, F>;
type ProgramCohort<S, T, E, W, O, H, C, F> = [ProgramNode<S, T, E, W, O, H, C, F>];
/// Exclusive borrowed consumer of the selected Nodes' client/read streams.
/// Business slots remain owned through exported completion consumption.
/// A provider contract error retains the original rejected opaque output and
/// disables phase production; advance continues bounded cleanup attempts.
pub struct ProgramDriver<
    'a,
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    C: PeerConnector,
    F: PeerTransportFactory<C::Session>,
> {
    nodes: &'a mut ProgramCohort<S, T, E, W, O, H, C, F>,
    profile: ProgramProfile,
    limits: ProgramDriverLimits,
    binding: ProgramDriverBinding,
    keys: BTreeMap<GroupIdentity, Vec<KeyId>>,
    work: BTreeMap<u64, Work>,
    ready: VecDeque<u64>,
    sequence: u64,
    node_cursor: usize,
    output_cursor: usize,
    work_cursor: u64,
    now: Option<MonoTime>,
    closed: bool,
    stopped: bool,
    failure: Option<ProgramDriverError>,
    retained: Vec<RetainedOutput>,
}
#[derive(Debug)]
enum Attempt {
    Client {
        node: usize,
        ticket: ClientTicket,
        command: Box<Command>,
        bytes: Vec<u8>,
    },
    Read {
        node: usize,
        ticket: ReadInvocationTicket,
        query: ProgramQuery,
    },
}
struct Work {
    ticket: ProgramDriverTicket,
    program: Option<Program>,
    stage: ProgramDriverStage,
    decision: Option<Decision>,
    facts: Vec<PhaseFact>,
    decision_fact: Option<PhaseFact>,
    all_fact: Option<PhaseFact>,
    refusal: Option<(usize, PrepareRefusal)>,
    attempt: Option<Attempt>,
    route: usize,
    outcome: Option<ProgramDriverOutcome>,
    exported: bool,
}
impl Work {
    fn nested_bytes(&self) -> usize {
        self.program.as_ref().map_or(0, Program::nested_bytes)
            + self.facts.capacity() * size_of::<PhaseFact>()
            + match &self.attempt {
                Some(Attempt::Client { command, .. }) => {
                    size_of::<Command>()
                        + command.program.nested_bytes()
                        + std::mem::size_of_val(&*command.facts)
                }
                _ => 0,
            }
    }
    fn new(
        ticket: ProgramDriverTicket,
        program: Option<Program>,
        stage: ProgramDriverStage,
    ) -> Self {
        Self {
            ticket,
            program,
            stage,
            decision: None,
            facts: Vec::new(),
            decision_fact: None,
            all_fact: None,
            refusal: None,
            attempt: None,
            route: 0,
            outcome: None,
            exported: false,
        }
    }
}
enum RetainedOutput {
    Client {
        node: usize,
        output: Box<ClientCompletion<ProgramReceipt>>,
    },
    Read {
        node: usize,
        output: Box<ReadCompletion<ProgramRead>>,
    },
}

impl RetainedOutput {
    fn retained_bytes(&self) -> usize {
        match self {
            Self::Client { .. } => size_of::<ClientCompletion<ProgramReceipt>>(),
            Self::Read { output, .. } => {
                size_of::<ReadCompletion<ProgramRead>>()
                    + match output.outcome() {
                        ReadOutcome::Read {
                            result: Ok(ProgramRead::Status(Some(status))),
                            ..
                        } => size_of::<ProgramStatus>() + status.nested_bytes(),
                        ReadOutcome::Read {
                            result: Ok(ProgramRead::Keys(keys)),
                            ..
                        } => std::mem::size_of_val(&**keys),
                        _ => 0,
                    }
            }
        }
    }
}
