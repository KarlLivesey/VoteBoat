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
//! Trusted intent scope and bounded owner-driven automatic or remote administration.
use super::setup::{application, checked, group, Failure, Service, MAX_NODE};
use std::{
    collections::BTreeMap,
    io::Read,
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    membership::*,
    native::{administration::*, placement::*},
    placement::*,
    quorum::*,
    raft::*,
    runtime::*,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Automatic,
    Provisioned,
    Targets,
}
struct DynamicPolicy {
    placement: NativePlacementAuthorizer,
    requirements: ReadinessRequirements,
}
pub enum RecordSubmission {
    Pending(OperationId),
    Reply(String),
}
pub struct Administration {
    plan: Option<NativeAdministrationPlan>,
    dynamic: Option<DynamicPolicy>,
    target: Option<ConfigurationRecord>,
    pending: Option<ConfigurationTicket>,
    proofs: BTreeMap<NodeId, PromotionReadiness>,
    waiting: Option<(NodeId, Instant)>,
    next: Instant,
    stopped: bool,
    remote: bool,
    requested: Option<OperationId>,
    reply: Option<String>,
}
pub(super) fn node(text: &str) -> Result<NodeId, Failure> {
    let number: u64 = text.parse()?;
    if !(1..=MAX_NODE).contains(&number) {
        return Err("invalid administration node".into());
    }
    Ok(NodeId::new(number).unwrap())
}
pub(super) fn cid(text: &str) -> Result<ConfigurationId, Failure> {
    ConfigurationId::new(text.parse()?).ok_or_else(|| "invalid configuration ID".into())
}
pub(super) fn tree<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
    depth: usize,
    remaining: &mut usize,
) -> Result<Tree, Failure> {
    super::policy_input::tree(tokens, depth, remaining, MAX_NODE)
}

fn parse_record<'a>(
    kind: &str,
    mut tokens: impl Iterator<Item = &'a str>,
    replicas: &BTreeMap<NodeId, ReplicaPlacement>,
) -> Result<ConfigurationRecord, Failure> {
    let operation = OperationId::new(tokens.next().ok_or("missing operation")?.parse()?)
        .ok_or("invalid operation")?;
    let expected = cid(tokens.next().ok_or("missing expected configuration")?)?;
    let id = cid(tokens.next().ok_or("missing configuration ID")?)?;
    let change = match kind {
        "final" => ConfigurationChange::Final { id },
        "learners" | "joint" => {
            let target = if kind == "joint" {
                cid(tokens.next().ok_or("missing final configuration")?)?
            } else {
                id
            };
            let learners = tokens.next().ok_or("missing learner list")?;
            let mut assignments = BTreeMap::new();
            if learners != "-" {
                for item in learners.split(',') {
                    let id = node(item)?;
                    let store = replicas
                        .get(&id)
                        .ok_or("learner missing replica declaration")?
                        .store;
                    if assignments.insert(id, store).is_some() {
                        return Err("duplicate learner".into());
                    }
                }
            }
            let policy = checked(Policy::new(
                tree(&mut tokens, 0, &mut 16384)?,
                Limits::default(),
            ))?;
            let voters = policy
                .voters()
                .iter()
                .map(|id| {
                    replicas
                        .get(id)
                        .map(|p| (*id, p.store))
                        .ok_or("voter missing replica declaration")
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            let next = checked(Configuration::new(target, policy, voters, assignments))?;
            if kind == "joint" {
                ConfigurationChange::Joint { id, next }
            } else {
                ConfigurationChange::Learners(next)
            }
        }
        _ => return Err("expected replica, learners, joint or final".into()),
    };
    if tokens.next().is_some() {
        return Err("trailing administration fields".into());
    }

    Ok(ConfigurationRecord {
        operation,
        expected,
        change,
    })
}

fn replica<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
    stores: &BTreeMap<NodeId, StoreIdentity>,
) -> Result<(NodeId, ReplicaPlacement), Failure> {
    let id = node(tokens.next().ok_or("missing replica node")?)?;
    let domain = FailureDomainId::new(tokens.next().ok_or("missing failure domain")?.parse()?)
        .ok_or("invalid failure domain")?;
    let store = *stores
        .get(&id)
        .ok_or("administration replica missing from deployment")?;
    if let Some(exact) = tokens.next() {
        let expected = StoreIdentity {
            id: StoreId::new(exact.parse()?).ok_or("invalid exact store")?,
            incarnation: StoreIncarnation::new(
                tokens.next().ok_or("missing store incarnation")?.parse()?,
            )
            .ok_or("invalid store incarnation")?,
        };
        if expected != store {
            return Err("administration store differs from exact planned store".into());
        }
    }
    if tokens.next().is_some() {
        return Err("trailing replica fields".into());
    }
    Ok((id, ReplicaPlacement { store, domain }))
}

impl Administration {
    pub fn load(
        path: &Path,
        stores: &BTreeMap<NodeId, StoreIdentity>,
        mode: Mode,
    ) -> Result<Self, Failure> {
        let remote = mode != Mode::Automatic;
        let mut data = Vec::new();
        std::fs::File::open(path)?
            .take(65537)
            .read_to_end(&mut data)?;
        if data.len() > 65536 {
            return Err("administration file exceeds 64 KiB".into());
        }
        let mut lines = std::str::from_utf8(&data)?.lines();
        if lines.next() != Some("voteboat-counter-admin-v1") {
            return Err("expected voteboat-counter-admin-v1 header".into());
        }
        let words = lines
            .next()
            .ok_or("missing placement requirements")?
            .split_whitespace()
            .collect::<Vec<_>>();
        let ["placement", minimum, survive] = words.as_slice() else {
            return Err("expected placement MINIMUM_DOMAINS true|false".into());
        };
        let requirements = PlacementRequirements {
            minimum_voting_domains: minimum.parse()?,
            survive_any_single_domain_loss: survive.parse()?,
        };
        let mut replicas = BTreeMap::new();
        let mut records = Vec::new();
        for line in lines {
            let mut tokens = line.split_whitespace();
            let kind = tokens.next().ok_or("blank administration line")?;
            if kind == "replica" {
                if !records.is_empty() {
                    return Err("replica declarations must precede intents".into());
                }
                let (id, placement) = replica(&mut tokens, stores)?;
                if replicas.insert(id, placement).is_some() {
                    return Err("duplicate replica".into());
                }
                continue;
            }
            if mode == Mode::Targets {
                return Err("remote policy must not contain operation intents".into());
            }
            if records.len() >= MAX_ADMINISTRATION_INTENTS {
                return Err("too many administration intents".into());
            }
            records.push(parse_record(kind, tokens, &replicas)?);
        }
        // Vec growth can otherwise exceed the backend's retained-capacity ceiling.
        records.shrink_to_fit();
        let placement = NativePlacementAuthorizer::new(group(), replicas, requirements)
            .map_err(|(e, _)| format!("{e:?}"))?;
        let requirements = application()?.readiness_requirements();
        let (plan, dynamic) = if mode == Mode::Targets {
            (
                None,
                Some(DynamicPolicy {
                    placement,
                    requirements,
                }),
            )
        } else {
            (
                Some(
                    NativeAdministrationPlan::new(group(), placement, requirements, records)
                        .map_err(|e| format!("{:?}", e.reason))?,
                ),
                None,
            )
        };
        Ok(Self {
            plan,
            dynamic,
            target: None,
            pending: None,
            proofs: BTreeMap::new(),
            waiting: None,
            next: Instant::now(),
            stopped: remote,
            remote,
            requested: None,
            reply: None,
        })
    }
    fn requirements(&self) -> ReadinessRequirements {
        match (&self.plan, &self.dynamic) {
            (Some(plan), _) => plan.requirements(),
            (_, Some(policy)) => policy.requirements,
            _ => unreachable!(),
        }
    }
    fn intents(&self) -> &[ConfigurationRecord] {
        self.plan.as_ref().map_or(&[], |p| p.intents())
    }
    pub fn authorize(
        &self,
        scope: GroupIdentity,
        membership: &Membership,
        proposal: &ConfigurationProposal,
    ) -> Result<(), ConfigurationProposalError> {
        if let Some(policy) = &self.dynamic {
            if scope != group()
                || self.target.as_ref() != Some(&proposal.record)
                || proposal.requirements != policy.requirements
            {
                return Err(ConfigurationProposalError::AuthenticationRequired);
            }
            policy
                .placement
                .authorize(scope, membership, &proposal.record)
                .map_err(ConfigurationProposalError::Placement)
        } else {
            self.plan
                .as_ref()
                .ok_or(ConfigurationProposalError::AuthenticationRequired)?
                .authorize(scope, membership, proposal)
        }
    }
    pub fn request_record(
        &mut self,
        text: &str,
        service: &Service,
    ) -> Result<RecordSubmission, String> {
        let policy = self
            .dynamic
            .as_ref()
            .ok_or("client-supplied targets disabled")?;
        if self.requested.is_some() || self.pending.is_some() || self.reply.is_some() {
            return Err("administration busy".into());
        }
        if text.len() > 256 {
            return Err("record too long".into());
        }
        let mut tokens = text.split_whitespace();
        let kind = tokens.next().ok_or("missing record kind")?;
        let record =
            parse_record(kind, tokens, policy.placement.replicas()).map_err(|e| e.to_string())?;
        if record.retained_bytes() > MAX_ADMINISTRATION_BYTES {
            return Err("record retained budget".into());
        }
        let core = service.local().owner.core(group()).ok_or("missing group")?;
        let state = core.state();
        let status = core
            .configuration_status(record.operation)
            .map_err(|e| format!("{e:?}"))?;
        let exact = state.entries.iter().find(|entry| {
            matches!(&entry.payload,
            voteboat::log::EntryPayload::Configuration(previous) if **previous == record)
        });
        if let Some(entry) = exact {
            return Ok(RecordSubmission::Reply(
                if entry.index <= state.commit_index {
                    format!(
                        "OK operation={} committed_index={} term={} duplicate=true",
                        record.operation.get(),
                        entry.index,
                        entry.term
                    )
                } else {
                    "UNKNOWN exact record locally durable but not committed; preserve original record".into()
                },
            ));
        }
        let final_matches = matches!(status.resume_action(), ConfigurationResumeAction::Finalize(ref next) if *next == record);
        if !final_matches && status.resume_action() != ConfigurationResumeAction::NotFoundLocally {
            let retained = state.entries.iter().any(|entry| matches!(&entry.payload,
                voteboat::log::EntryPayload::Configuration(previous) if previous.operation == record.operation));
            return Err(if retained {
                "configuration operation conflicts with retained record"
            } else {
                "configuration comparison history unavailable; inspect configuration-status"
            }
            .into());
        }
        let operation = record.operation;
        self.target = Some(record);
        self.requested = Some(operation);
        self.stopped = false;
        self.next = Instant::now();
        Ok(RecordSubmission::Pending(operation))
    }
    pub fn remote(&self) -> bool {
        self.remote
    }
    pub fn request(&mut self, operation: OperationId) -> Result<(), String> {
        if !self.remote {
            return Err("remote administration disabled".into());
        }
        if self.requested.is_some() || self.pending.is_some() || self.reply.is_some() {
            return Err("administration busy".into());
        }
        if !self.intents().iter().any(|r| r.operation == operation) {
            return Err("operation absent from provisioned plan".into());
        }
        self.requested = Some(operation);
        self.stopped = false;
        self.next = Instant::now();
        Ok(())
    }
    pub fn take_reply(&mut self) -> Option<String> {
        if self.remote
            && self.stopped
            && self.pending.is_none()
            && self.requested.take().is_some()
            && self.reply.is_none()
        {
            self.target = None;
            self.reply = Some("ERR administration blocked; inspect server log".into());
        }
        self.reply.take()
    }
    pub fn cancel_remote(&mut self, service: &mut Service, reason: &str) -> Result<(), Failure> {
        if !self.remote {
            return Ok(());
        }
        if let Some(operation) = self.requested {
            eprintln!(
                "administration observation_cancelled operation={} phase={} reason={reason}",
                operation.get(),
                if self.pending.is_some() {
                    "configuration_queued"
                } else {
                    "preparing"
                }
            );
        }
        if let Some(ticket) = self.pending {
            checked(service.cancel_configuration(ticket))?;
        }
        self.requested = None;
        self.target = None;
        self.reply = None;
        self.proofs.clear();
        self.stopped = true;
        if self.waiting.take().is_some() {
            checked(service.cancel_learner_readiness(group()))?;
        }
        Ok(())
    }
    fn consume_results(&mut self, service: &mut Service) -> Result<(), Failure> {
        while let Some(result) = service.poll_configuration() {
            if self.pending != Some(result.ticket) {
                return Err("unrecognized administration ticket".into());
            }
            self.pending = None;
            self.proofs.clear();
            eprintln!(
                "administration operation={} outcome={:?}",
                result.ticket.operation().get(),
                result.outcome
            );
            if self.remote {
                if self.requested == Some(result.ticket.operation()) {
                    self.reply = Some(match &result.outcome {
                        ConfigurationOutcome::Committed(position) => format!(
                            "OK operation={} committed_index={} term={}",
                            result.ticket.operation().get(),
                            position.index,
                            position.term
                        ),
                        ConfigurationOutcome::NotProposed(RaftError::NotLeader) => {
                            "ERR NOT_LEADER".into()
                        }
                        ConfigurationOutcome::NotProposed(error) => {
                            format!("ERR not_proposed={error:?}")
                        }
                        ConfigurationOutcome::Unknown(reason) => {
                            format!("UNKNOWN {reason:?}; retry the same configuration operation ID and record")
                        }
                    });
                    self.requested = None;
                }
                self.target = None;
                self.stopped = true;
                continue;
            }
            if let ConfigurationOutcome::NotProposed(error) = result.outcome {
                // Scope/readiness can change between enqueue and execution. Recheck
                // exact original intent; do not alter its ID or target on retry.
                let retry = matches!(
                    error,
                    RaftError::NotLeader | RaftError::ReadNotReady | RaftError::Busy
                ) || matches!(&error, RaftError::Configuration(reason) if matches!(reason.as_ref(),
                    ConfigurationProposalError::AuthenticationRequired
                    | ConfigurationProposalError::Membership(MembershipError::StaleConfiguration | MembershipError::JointNotCommitted)
                    | ConfigurationProposalError::Readiness(ReadinessError::Stale | ReadinessError::WrongBinding | ReadinessError::NotCaughtUp)));
                if !retry {
                    self.stopped = true;
                }
            }
        }
        Ok(())
    }
    fn next_proposal(
        &mut self,
        service: &Service,
    ) -> Result<Option<(ConfigurationProposal, Vec<NodeId>)>, Failure> {
        let core = service
            .local()
            .owner
            .core(group())
            .ok_or("missing administration group")?;
        if core.role() != Role::Leader || !core.local_voter() {
            self.proofs.clear();
            return Ok(None);
        }
        let state = core.state();
        if state.commit_index == 0
            || state.term_at(state.commit_index) != Some(state.hard_state.term)
        {
            self.proofs.clear();
            return Ok(None);
        }
        let mut selected = self.target.clone();
        for intent in self
            .intents()
            .iter()
            .filter(|r| !self.remote || self.requested == Some(r.operation))
        {
            match checked(service.configuration_status(group(), intent.operation))?.resume_action()
            {
                ConfigurationResumeAction::Completed => continue,
                ConfigurationResumeAction::WaitForCommit => return Ok(None),
                ConfigurationResumeAction::Finalize(record) => {
                    if !self.intents().contains(&record) {
                        self.stopped = true;
                        eprintln!("administration blocked: final record absent from trusted plan");
                        return Ok(None);
                    }
                    selected = Some(record);
                    break;
                }
                ConfigurationResumeAction::NotFoundLocally => {
                    selected = Some(intent.clone());
                    break;
                }
            }
        }
        let Some(record) = selected else {
            self.stopped = true;
            if let Some(operation) = self.requested.take() {
                self.reply = Some(format!(
                    "OK evidence=local_durable operation={} action=completed",
                    operation.get()
                ));
            }
            eprintln!("administration historical operations completed; inspect configuration-status for local evidence");
            return Ok(None);
        };
        // Validate before spending readiness work. Local absence alone grants no
        // authority: this exact record came from the operator's startup file.
        if record.expected != core.membership().id() {
            self.stopped = true;
            eprintln!(
                "administration blocked: expected configuration differs from local accepted head"
            );
            return Ok(None);
        }
        let proposal = ConfigurationProposal {
            record,
            readiness: Vec::new(),
            requirements: self.requirements(),
        };
        if let Err(error) = self.authorize(group(), core.membership(), &proposal) {
            self.stopped = true;
            eprintln!("administration blocked: {error:?}");
            return Ok(None);
        }
        let required = match &proposal.record.change {
            ConfigurationChange::Joint { next, .. } => next
                .voter_stores()
                .keys()
                .filter(|n| !core.membership().stable().voter_stores().contains_key(n))
                .copied()
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };
        Ok(Some((proposal, required)))
    }
    fn collect_readiness(
        &mut self,
        service: &mut Service,
        proposal: &ConfigurationProposal,
        required: &[NodeId],
    ) -> Result<bool, Failure> {
        let core = service
            .local()
            .owner
            .core(group())
            .ok_or("missing administration group")?;
        let peers = service.peers().ok_or("missing administration peers")?;
        self.proofs.retain(|id, proof| {
            required.contains(id)
                && peers.roster().binding(*id).is_some_and(|b| {
                    core.check_learner_readiness(&proof.ready, b.peer.store)
                        .is_ok()
                })
        });
        if let Some(ready) = core.ready_learner() {
            let id = ready.request().learner.node;
            if required.contains(&id) {
                if let Some(binding) = peers.roster().binding(id) {
                    if core
                        .check_learner_readiness(ready, binding.peer.store)
                        .is_ok()
                    {
                        self.proofs.insert(
                            id,
                            PromotionReadiness {
                                ready: ready.clone(),
                                authenticated: binding.peer.store,
                            },
                        );
                    }
                }
            }
        }
        if let Some(id) = required.iter().find(|id| !self.proofs.contains_key(id)) {
            if let Some((waiting, since)) = self.waiting {
                if (waiting != *id || since.elapsed() >= Duration::from_secs(2))
                    && service.cancel_learner_readiness(group()).is_ok()
                {
                    self.waiting = None;
                }
            } else {
                let admitted = service
                    .request_learner_readiness(group(), *id, self.requirements())
                    .is_ok();
                if self.remote {
                    eprintln!("administration operation={} preparing_learner={} readiness_admitted={admitted}",
                        proposal.record.operation.get(), id.get());
                }
                if admitted {
                    self.waiting = Some((*id, Instant::now()));
                }
            }
            return Ok(false);
        }
        self.waiting = None;
        Ok(true)
    }
    pub fn tick(&mut self, service: &mut Service, shutting_down: bool) -> Result<(), Failure> {
        self.consume_results(service)?;
        if shutting_down || self.stopped || self.pending.is_some() || Instant::now() < self.next {
            return Ok(());
        }
        self.next = Instant::now() + Duration::from_millis(250);
        let Some((mut proposal, required)) = self.next_proposal(service)? else {
            return Ok(());
        };
        if !self.collect_readiness(service, &proposal, &required)? {
            return Ok(());
        }
        proposal.readiness = required.iter().map(|id| self.proofs[id].clone()).collect();
        match service.configure(ConfigurationRequest {
            group: group(),
            proposal,
        }) {
            Ok(ticket) => self.pending = Some(ticket),
            Err(rejected) => {
                self.stopped = !matches!(
                    rejected.reason,
                    ConfigurationRequestError::Overloaded
                        | ConfigurationRequestError::Runtime(RuntimeError::Overloaded)
                );
                eprintln!("administration admission={:?}", rejected.reason);
            }
        }
        Ok(())
    }
}
