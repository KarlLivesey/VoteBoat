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
//! Trusted startup input and bounded owner-driven administration; no ingress API.
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

pub struct Administration {
    pub plan: NativeAdministrationPlan,
    pending: Option<ConfigurationTicket>,
    proofs: BTreeMap<NodeId, PromotionReadiness>,
    waiting: Option<(NodeId, Instant)>,
    next: Instant,
    stopped: bool,
}
fn node(text: &str) -> Result<NodeId, Failure> {
    let number: u64 = text.parse()?;
    if !(1..=MAX_NODE).contains(&number) {
        return Err("invalid administration node".into());
    }
    Ok(NodeId::new(number).unwrap())
}
fn cid(text: &str) -> Result<ConfigurationId, Failure> {
    ConfigurationId::new(text.parse()?).ok_or_else(|| "invalid configuration ID".into())
}
fn tree<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
    depth: usize,
    remaining: &mut usize,
) -> Result<Tree, Failure> {
    if depth > 32 || *remaining == 0 {
        return Err("administration policy exceeds bounds".into());
    }
    *remaining -= 1;
    let token = tokens.next().ok_or("truncated administration policy")?;
    let (kind, value) = token
        .split_once(':')
        .ok_or("expected v:N, m:COUNT or w:COUNT")?;
    if kind == "v" {
        return Ok(Tree::Voter(node(value)?));
    }
    let count: usize = value.parse()?;
    if count == 0 || count > *remaining {
        return Err("invalid administration branch size".into());
    }
    match kind {
        "m" => (0..count)
            .map(|_| tree(tokens, depth + 1, remaining))
            .collect::<Result<Vec<_>, _>>()
            .map(Tree::Majority),
        "w" => (0..count)
            .map(|_| {
                let weight = tokens.next().ok_or("missing policy weight")?.parse()?;
                Ok(WeightedChild {
                    weight,
                    node: tree(tokens, depth + 1, remaining)?,
                })
            })
            .collect::<Result<Vec<_>, Failure>>()
            .map(Tree::Weighted),
        _ => Err("unknown administration policy branch".into()),
    }
}
impl Administration {
    pub fn load(path: &Path, stores: &BTreeMap<NodeId, StoreIdentity>) -> Result<Self, Failure> {
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
                let id = node(tokens.next().ok_or("missing replica node")?)?;
                let domain =
                    FailureDomainId::new(tokens.next().ok_or("missing failure domain")?.parse()?)
                        .ok_or("invalid failure domain")?;
                let store = *stores
                    .get(&id)
                    .ok_or("administration replica missing from deployment")?;
                if replicas
                    .insert(id, ReplicaPlacement { store, domain })
                    .is_some()
                    || tokens.next().is_some()
                {
                    return Err("duplicate replica or trailing fields".into());
                }
                continue;
            }
            if records.len() >= MAX_ADMINISTRATION_INTENTS {
                return Err("too many administration intents".into());
            }
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
            records.push(ConfigurationRecord {
                operation,
                expected,
                change,
            });
        }
        // Vec growth can otherwise exceed the backend's retained-capacity ceiling.
        records.shrink_to_fit();
        let placement = NativePlacementAuthorizer::new(group(), replicas, requirements)
            .map_err(|(e, _)| format!("{e:?}"))?;
        let requirements = application()?.readiness_requirements();
        let plan = NativeAdministrationPlan::new(group(), placement, requirements, records)
            .map_err(|e| format!("{:?}", e.reason))?;
        Ok(Self {
            plan,
            pending: None,
            proofs: BTreeMap::new(),
            waiting: None,
            next: Instant::now(),
            stopped: false,
        })
    }
    pub fn tick(&mut self, service: &mut Service, shutting_down: bool) -> Result<(), Failure> {
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
        if shutting_down || self.stopped || self.pending.is_some() || Instant::now() < self.next {
            return Ok(());
        }
        self.next = Instant::now() + Duration::from_millis(250);
        let core = service
            .local()
            .owner
            .core(group())
            .ok_or("missing administration group")?;
        if core.role() != Role::Leader || !core.local_voter() {
            self.proofs.clear();
            return Ok(());
        }
        let state = core.state();
        if state.commit_index == 0
            || state.term_at(state.commit_index) != Some(state.hard_state.term)
        {
            self.proofs.clear();
            return Ok(());
        }
        let mut selected = None;
        for intent in self.plan.intents() {
            match checked(service.configuration_status(group(), intent.operation))?.resume_action()
            {
                ConfigurationResumeAction::Completed => continue,
                ConfigurationResumeAction::WaitForCommit => return Ok(()),
                ConfigurationResumeAction::Finalize(record) => {
                    if !self.plan.intents().contains(&record) {
                        self.stopped = true;
                        eprintln!("administration blocked: final record absent from trusted plan");
                        return Ok(());
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
            eprintln!("administration historical operations completed; inspect configuration-status for local evidence");
            return Ok(());
        };
        // Validate before spending readiness work. Local absence alone grants no
        // authority: this exact record came from the operator's startup file.
        if record.expected != core.membership().id() {
            self.stopped = true;
            eprintln!(
                "administration blocked: expected configuration differs from local accepted head"
            );
            return Ok(());
        }
        let mut proposal = ConfigurationProposal {
            record,
            readiness: Vec::new(),
            requirements: self.plan.requirements(),
        };
        if let Err(error) = self.plan.authorize(group(), core.membership(), &proposal) {
            self.stopped = true;
            eprintln!("administration blocked: {error:?}");
            return Ok(());
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
            } else if service
                .request_learner_readiness(group(), *id, self.plan.requirements())
                .is_ok()
            {
                self.waiting = Some((*id, Instant::now()));
            }
            return Ok(());
        }
        self.waiting = None;
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
