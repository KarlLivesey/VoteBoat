// SPDX-License-Identifier: RPL-1.5
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
    > ProgramDriver<'a, S, T, E, W, O, H, C, F>
where
    C::Endpoint: Clone,
{
    /// Exclusive original client/read consumer; fixed opening catalogs are
    /// checked independently from each replica's current applied state.
    pub fn new(
        profile: ProgramProfile,
        generation: ProgramDriverGeneration,
        limits: ProgramDriverLimits,
        nodes: &'a mut ProgramCohort<S, T, E, W, O, H, C, F>,
    ) -> Result<Self, ProgramDriverError> {
        let keys = validate(&profile, limits, nodes)?;
        let owner = nodes[0].local().clients.binding().owner;
        Ok(Self {
            nodes,
            profile,
            limits,
            binding: ProgramDriverBinding { owner, generation },
            keys,
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
    fn intake(&self, id: TransactionId) -> Result<ProgramDriverTicket, ProgramDriverError> {
        if self.closed {
            return Err(ProgramDriverError::Closed);
        }
        if self.work.len() == self.limits.active() {
            return Err(ProgramDriverError::Overloaded);
        }
        if self.work.values().any(|w| w.ticket.transaction == id) {
            return Err(ProgramDriverError::InFlight);
        }
        Ok(ProgramDriverTicket {
            binding: self.binding,
            sequence: self
                .sequence
                .checked_add(1)
                .ok_or(ProgramDriverError::Exhausted)?,
            transaction: id,
        })
    }
    pub(super) fn check_program(&self, program: &Program) -> Result<(), ProgramDriverError> {
        if program.profile() != &self.profile {
            return Err(ProgramDriverError::InvalidAssembly);
        }
        for participant in program.participants() {
            let keys = self
                .keys
                .get(&participant.group())
                .ok_or(ProgramDriverError::InvalidAssembly)?;
            if participant.keys().iter().any(|k| !keys.contains(&k.key)) {
                return Err(ProgramDriverError::Application(
                    ApplicationError::InvalidCommand,
                ));
            }
        }
        Ok(())
    }
    pub fn submit(
        &mut self,
        program: Program,
    ) -> Result<ProgramDriverTicket, ProgramDriverSubmitRejected> {
        self.submit_inner(&program)
            .map_err(|reason| ProgramDriverSubmitRejected {
                reason,
                program: Box::new(program),
            })
    }
    fn submit_inner(
        &mut self,
        program: &Program,
    ) -> Result<ProgramDriverTicket, ProgramDriverError> {
        let ticket = self.intake(program.id())?;
        self.check_program(program)?;
        let command = Command::start(program.clone());
        let bytes = codec::encode(&command).map_err(ProgramDriverError::Application)?;
        let operation = command
            .operation(program.coordinator())
            .map_err(ProgramDriverError::Application)?;
        let pending: Vec<_> = self
            .work
            .values()
            .filter_map(|w| w.program.as_ref())
            .filter(|p| p.coordinator() == program.coordinator())
            .map(|p| {
                (
                    p.operation(0).expect("checked operation"),
                    codec::encode(&Command::start(p.clone())).expect("checked Start"),
                )
            })
            .collect();
        let node = self
            .route(program.coordinator(), 0)
            .ok_or(ProgramDriverError::InvalidAssembly)?;
        let app = &self.nodes[node].local().applications[&program.coordinator()];
        app.validate_proposal(
            operation,
            &bytes,
            pending.iter().map(|(op, b)| (*op, b.as_slice())),
        )
        .map_err(ProgramDriverError::Application)?;
        let actual = self.nodes[node]
            .propose(ClientRequest {
                group: program.coordinator(),
                operation,
                bytes: bytes.clone(),
            })
            .map_err(|e| ProgramDriverError::Client {
                node,
                reason: Box::new(e.reason),
            })?;
        let mut work = Work::new(ticket, Some(program.clone()), ProgramDriverStage::Start);
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
    ) -> Result<ProgramDriverTicket, ProgramDriverResumeRejected> {
        self.resume_inner(transaction)
            .map_err(|reason| ProgramDriverResumeRejected {
                reason,
                transaction,
            })
    }
    fn resume_inner(
        &mut self,
        id: TransactionId,
    ) -> Result<ProgramDriverTicket, ProgramDriverError> {
        let ticket = self.intake(id)?;
        let group = self.profile.coordinator(id);
        let node = self
            .route(group, 0)
            .ok_or(ProgramDriverError::InvalidAssembly)?;
        let query = ProgramQuery::Status(id);
        let actual =
            self.nodes[node]
                .read(group, query.clone())
                .map_err(|e| ProgramDriverError::Read {
                    node,
                    reason: Box::new(e.reason),
                })?;
        let mut work = Work::new(ticket, None, ProgramDriverStage::CoordinatorStatus);
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
    profile: &ProgramProfile,
    limits: ProgramDriverLimits,
    nodes: &ProgramCohort<S, T, E, W, O, H, C, F>,
) -> Result<BTreeMap<GroupIdentity, Vec<KeyId>>, ProgramDriverError>
where
    C::Endpoint: Clone,
{
    if nodes.is_empty() || nodes.len() > 64 {
        return Err(ProgramDriverError::InvalidAssembly);
    }
    let mut copies: BTreeMap<GroupIdentity, &ProgramApplication> = BTreeMap::new();
    for (i, node) in nodes.iter().enumerate() {
        if node.state() != NodeState::Running
            || !node.local().clients.is_drained()
            || !node.local().reads.is_drained()
            || nodes[..i].iter().any(|n| {
                n.local().clients.binding() == node.local().clients.binding()
                    || n.local().reads.binding() == node.local().reads.binding()
            })
        {
            return Err(ProgramDriverError::InvalidAssembly);
        }
        for group in profile.coordinators().iter().chain(profile.participants()) {
            if let Some(app) = node.local().applications.get(group) {
                validate_copy(*group, profile, app, copies.get(group).copied())?;
                let result = app
                    .read_result_bound(&ProgramQuery::Status(
                        TransactionId::new(1).expect("nonzero"),
                    ))
                    .map_err(ProgramDriverError::Application)?;
                if node.local().clients.limits().command_bytes < PROGRAM_COMMAND_BYTES
                    || node.local().clients.limits().receipt_bytes < size_of::<ProgramReceipt>()
                    || node.local().reads.limits().result_bytes < result
                {
                    return Err(ProgramDriverError::InvalidAssembly);
                }
                copies.entry(*group).or_insert(app);
            }
        }
    }
    if copies.len() != profile.coordinators().len() + profile.participants().len() {
        return Err(ProgramDriverError::InvalidAssembly);
    }
    let quota: usize = profile
        .coordinators()
        .iter()
        .map(|g| copies[g].transaction_capacity())
        .sum();
    if limits.active() > quota
        || profile
            .participants()
            .iter()
            .any(|g| copies[g].transaction_capacity() < quota)
    {
        return Err(ProgramDriverError::InvalidAssembly);
    }
    Ok(profile
        .participants()
        .iter()
        .map(|g| (*g, copies[g].key_ids().collect()))
        .collect())
}
fn validate_copy(
    group: GroupIdentity,
    profile: &ProgramProfile,
    app: &ProgramApplication,
    old: Option<&ProgramApplication>,
) -> Result<(), ProgramDriverError> {
    let role = if profile.coordinators().contains(&group) {
        ProgramRole::Coordinator
    } else {
        ProgramRole::Participant
    };
    if app.group() != group
        || app.profile() != profile
        || app.role() != role
        || old.is_some_and(|old| {
            old.limits() != app.limits()
                || old
                    .keys
                    .iter()
                    .map(|(id, k)| (*id, k.opening))
                    .ne(app.keys.iter().map(|(id, k)| (*id, k.opening)))
        })
    {
        return Err(ProgramDriverError::InvalidAssembly);
    }
    Ok(())
}
