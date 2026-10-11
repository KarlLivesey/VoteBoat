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
impl<
        'a,
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > TransferDriver<'a, S, T, E, W, O, H, C, F>
where
    C::Endpoint: Clone,
{
    /// Metadata is checked before borrowing accepted work. Select 1..64 Nodes
    /// with any hosted subset of the three profile groups; every group must be
    /// represented. No client/read work may belong to another consumer.
    pub fn new(
        profile: TransferProfile,
        generation: TransferDriverGeneration,
        limits: TransferDriverLimits,
        nodes: &'a mut TransferCohort<S, T, E, W, O, H, C, F>,
    ) -> Result<Self, DriverError> {
        let accounts = validate(profile, limits, nodes)?;
        let owner = nodes[0].local().clients.binding().owner;
        Ok(Self {
            nodes,
            profile,
            limits,
            binding: DriverBinding { owner, generation },
            accounts,
            work: BTreeMap::new(),
            ready: VecDeque::with_capacity(limits.active()),
            sequence: 0,
            node_cursor: 0,
            output_cursor: 0,
            work_cursor: 0,
            now: None,
            closed: false,
            stopped: false,
            failure: None,
            retained: Vec::new(),
        })
    }
    fn intake(&self, id: TransactionId) -> Result<DriverTicket, DriverError> {
        if self.closed {
            return Err(DriverError::Closed);
        }
        if self.work.len() == self.limits.active() {
            return Err(DriverError::Overloaded);
        }
        if self.work.values().any(|w| w.ticket.transaction == id) {
            return Err(DriverError::InFlight);
        }
        Ok(DriverTicket {
            binding: self.binding,
            sequence: self.sequence.checked_add(1).ok_or(DriverError::Exhausted)?,
            transaction: id,
        })
    }
    pub(super) fn check_spec(&self, spec: TransferSpec) -> Result<(), DriverError> {
        if spec.profile() != self.profile {
            return Err(DriverError::InvalidAssembly);
        }
        for account in [spec.debit(), spec.credit()] {
            let slot = self
                .profile
                .participant(account.group())
                .ok_or(DriverError::InvalidAssembly)?;
            if !self.accounts[slot].contains(&account.account()) {
                return Err(DriverError::Application(ApplicationError::InvalidCommand));
            }
        }
        Ok(())
    }
    pub fn submit(&mut self, spec: TransferSpec) -> Result<DriverTicket, DriverSubmitRejected> {
        self.submit_inner(spec)
            .map_err(|reason| DriverSubmitRejected {
                reason,
                spec: Box::new(spec),
            })
    }
    fn submit_inner(&mut self, spec: TransferSpec) -> Result<DriverTicket, DriverError> {
        let ticket = self.intake(spec.id())?;
        self.check_spec(spec)?;
        let command = Command::start(spec);
        let bytes = codec::encode(command).map_err(DriverError::Application)?;
        let pending: Vec<_> = self
            .work
            .values()
            .filter_map(|w| w.spec)
            .map(|s| {
                (
                    s.phases().coordinator()[0],
                    codec::encode(Command::start(s)).expect("checked Start"),
                )
            })
            .collect();
        let node = self
            .route(self.profile.coordinator(), 0)
            .ok_or(DriverError::InvalidAssembly)?;
        let app = &self.nodes[node].local().applications[&self.profile.coordinator()];
        app.validate_proposal(
            spec.phases().coordinator()[0],
            &bytes,
            pending.iter().map(|(op, b)| (*op, b.as_slice())),
        )
        .map_err(DriverError::Application)?;
        let actual = self.nodes[node]
            .propose(ClientRequest {
                group: self.profile.coordinator(),
                operation: spec.phases().coordinator()[0],
                bytes: bytes.clone(),
            })
            .map_err(|e| DriverError::Client {
                node,
                reason: Box::new(e.reason),
            })?;
        let mut work = Work::new(ticket, Some(spec), DriverStage::Start);
        work.attempt = Some(Attempt::Client {
            node,
            ticket: actual,
            command: Box::new(command),
            bytes,
        });
        self.sequence = ticket.sequence;
        self.work.insert(ticket.sequence, work);
        Ok(ticket)
    }
    pub fn resume(
        &mut self,
        transaction: TransactionId,
    ) -> Result<DriverTicket, DriverResumeRejected> {
        self.resume_inner(transaction)
            .map_err(|reason| DriverResumeRejected {
                reason,
                transaction,
            })
    }
    fn resume_inner(&mut self, id: TransactionId) -> Result<DriverTicket, DriverError> {
        let ticket = self.intake(id)?;
        let node = self
            .route(self.profile.coordinator(), 0)
            .ok_or(DriverError::InvalidAssembly)?;
        let query = TransferQuery::Status(id);
        let actual = self.nodes[node]
            .read(self.profile.coordinator(), query)
            .map_err(|e| DriverError::Read {
                node,
                reason: Box::new(e.reason),
            })?;
        let mut work = Work::new(ticket, None, DriverStage::CoordinatorStatus);
        work.attempt = Some(Attempt::Read {
            node,
            ticket: actual,
            query,
        });
        self.sequence = ticket.sequence;
        self.work.insert(ticket.sequence, work);
        Ok(ticket)
    }
    pub(super) fn route(&self, group: GroupIdentity, offset: usize) -> Option<usize> {
        let available: Vec<_> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| {
                n.state() == NodeState::Running && n.local().applications.contains_key(&group)
            })
            .map(|(i, _)| i)
            .collect();
        available
            .iter()
            .copied()
            .find(|i| {
                self.nodes[*i]
                    .local()
                    .owner
                    .core(group)
                    .is_some_and(|c| c.role() == Role::Leader)
            })
            .or_else(|| available.get(offset % available.len().max(1)).copied())
    }
}
fn validate<
    S: ReadyScheduler,
    T: TimerService,
    E: ElectionEntropy,
    W: PersistenceWorker,
    O: OutboundQueue,
    H: SnapshotWorker,
    C: PeerConnector,
    F: PeerTransportFactory<C::Session>,
>(
    profile: TransferProfile,
    limits: TransferDriverLimits,
    nodes: &TransferCohort<S, T, E, W, O, H, C, F>,
) -> Result<[Vec<AccountId>; 2], DriverError>
where
    C::Endpoint: Clone,
{
    if nodes.is_empty() || nodes.len() > 64 {
        return Err(DriverError::InvalidAssembly);
    }
    let mut copies: [Option<&TransferApplication>; 3] = [None; 3];
    let groups = [
        profile.coordinator(),
        profile.participants()[0],
        profile.participants()[1],
    ];
    for (i, node) in nodes.iter().enumerate() {
        if node.state() != NodeState::Running
            || !node.local().clients.is_drained()
            || !node.local().reads.is_drained()
        {
            return Err(DriverError::InvalidAssembly);
        }
        if nodes[..i].iter().any(|n| {
            n.local().clients.binding() == node.local().clients.binding()
                || n.local().reads.binding() == node.local().reads.binding()
        }) {
            return Err(DriverError::InvalidAssembly);
        }
        let client = node.local().clients.limits();
        let read = node.local().reads.limits();
        if client.command_bytes < TRANSFER_COMMAND_BYTES
            || client.receipt_bytes < size_of::<TransferReceipt>()
            || read.result_bytes < size_of::<TransferRead>() + size_of::<TransferStatus>()
        {
            return Err(DriverError::InvalidAssembly);
        }
        for (slot, group) in groups.iter().enumerate() {
            if let Some(app) = node.local().applications.get(group) {
                let role = if slot == 0 {
                    TransferRole::Coordinator
                } else {
                    TransferRole::Participant(slot - 1)
                };
                if app.group() != *group || app.profile() != profile || app.role() != role {
                    return Err(DriverError::InvalidAssembly);
                }
                if let Some(old) = copies[slot] {
                    if old.limits() != app.limits()
                        || old
                            .accounts
                            .iter()
                            .map(|(id, a)| (*id, a.opening))
                            .ne(app.accounts.iter().map(|(id, a)| (*id, a.opening)))
                    {
                        return Err(DriverError::InvalidAssembly);
                    }
                } else {
                    copies[slot] = Some(app);
                }
            }
        }
    }
    let [Some(c), Some(p0), Some(p1)] = copies else {
        return Err(DriverError::InvalidAssembly);
    };
    if limits.active() > c.limits().transactions()
        || p0.limits().transactions() < c.limits().transactions()
        || p1.limits().transactions() < c.limits().transactions()
    {
        return Err(DriverError::InvalidAssembly);
    }
    Ok([p0.account_ids().collect(), p1.account_ids().collect()])
}
