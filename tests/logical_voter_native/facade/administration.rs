// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original observation lifecycle, not recursive learner enrollment qualification.
use super::*;
use voteboat::{
    identity::{ConfigurationId, OperationId},
    log::EntryPayload,
    membership::{Configuration, ConfigurationChange, ConfigurationProgress, ConfigurationRecord},
    raft::{ConfigurationProposal, RaftError},
    worker::PersistenceWorker,
};

#[test]
fn logical_facade_configuration_original_observer_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn logical_facade_configuration_original_observer_quic() {
    history(NativePeerProtocol::Quic);
}

fn request(node: &FacadeNode, operation: u128) -> ConfigurationRequest {
    let core = node.local().owner.core(setup::mapping().parent).unwrap();
    let current = core.membership().stable();
    ConfigurationRequest {
        group: setup::mapping().parent,
        proposal: ConfigurationProposal {
            record: ConfigurationRecord {
                operation: OperationId::new(operation).unwrap(),
                expected: current.id(),
                change: ConfigurationChange::Learners(
                    Configuration::new(
                        ConfigurationId::new(current.id().get() + 1).unwrap(),
                        current.policy().clone(),
                        current.voter_stores().clone(),
                        current.learners().clone(),
                    )
                    .unwrap(),
                ),
            },
            readiness: Vec::new(),
            requirements: node.local().applications[&setup::mapping().parent]
                .deployment_requirements()
                .unwrap(),
        },
    }
}
fn invalid(node: &FacadeNode, operation: u128) -> ConfigurationRequest {
    let mut request = request(node, operation);
    request.proposal.requirements.application_schema += 1;
    request
}
fn denied(progress: &ReplicaProgress, ticket: ConfigurationTicket) {
    let matching = progress
        .steps
        .iter()
        .filter(|step| step.admission == Some(ticket.admission()))
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1);
    let step = matching[0];
    assert_eq!(step.operation, Some(ticket.operation()));
    assert_eq!(step.visit.group, ticket.admission().group);
    assert_eq!(step.visit.owner, ticket.admission().owner);
    assert_eq!(step.proposed, None);
    assert_eq!(
        step.error,
        Some(RaftError::Admission(ApplicationError::UnsupportedSchema))
    );
}
fn checked_node(image: &VerifiedLogicalImage) -> FacadeNode {
    let parts = build(image);
    let owner = parts.local.owner.identity();
    let binding = parts.persistence.binding();
    let state = parts
        .local
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    let mut limits = fixture::limits();
    limits.configuration.requests = 0;
    let rejected = FacadeNode::from_parts(image, parts, limits, MonoTime(0))
        .err()
        .expect("zero configuration capacity refuses original parts");
    assert_eq!(
        rejected.reason,
        LogicalNodeError::Configuration(ConfigurationRequestError::InvalidLimits)
    );
    assert_eq!(rejected.parts.local.owner.identity(), owner);
    assert_eq!(rejected.parts.persistence.binding(), binding);
    assert_eq!(
        rejected
            .parts
            .local
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state(),
        &state
    );
    assert!(rejected.parts.persistence.is_drained());
    FacadeNode::from_parts(image, *rejected.parts, fixture::limits(), MonoTime(0))
        .unwrap_or_else(|e| panic!("original checked configuration facade {:?}", e.reason))
}
fn refusals(node: &mut FacadeNode) {
    let mut foreign = request(node, 34201);
    foreign.group = support::group(999);
    let proposal = foreign.proposal.clone();
    let rejected = node.configure(foreign).unwrap_err();
    assert_eq!(
        rejected.reason,
        ConfigurationRequestError::Runtime(RuntimeError::UnknownGroup)
    );
    assert_eq!(rejected.request.group, support::group(999));
    assert_eq!(rejected.request.proposal, proposal);
    assert_eq!(
        node.configuration_status(support::group(999), OperationId::new(34201).unwrap()),
        Err(LogicalNodeError::Owner(EffectOwnerError::Runtime(
            RuntimeError::UnknownGroup
        )))
    );
    let ticket = node.configure(invalid(node, 34202)).unwrap();
    assert!(node.poll_configuration().is_none());
    let refused = request(node, 34203);
    let original = refused.proposal.clone();
    let rejected = node.configure(refused).unwrap_err();
    assert_eq!(rejected.reason, ConfigurationRequestError::Overloaded);
    assert_eq!(rejected.request.proposal, original);
    let progress = node
        .poll(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap();
    denied(&progress, ticket);
    let completion = node
        .poll_configuration()
        .expect("original refused observation");
    assert_eq!(completion.ticket, ticket);
    assert_eq!(
        completion.outcome,
        ConfigurationOutcome::NotProposed(RaftError::Admission(
            ApplicationError::UnsupportedSchema
        ))
    );
    assert_eq!(
        node.cancel_configuration(ticket),
        Err(ConfigurationRequestError::StaleTicket)
    );
    assert!(node.configuration_requests_are_drained());
}
fn cancelled(node: &mut FacadeNode) {
    let ticket = node.configure(invalid(node, 34204)).unwrap();
    assert!(!node.local().owner.is_drained());
    node.cancel_configuration(ticket).unwrap();
    assert!(!node.configuration_requests_are_drained());
    let completion = node
        .poll_configuration()
        .expect("original cancelled observation");
    assert_eq!(completion.ticket, ticket);
    assert_eq!(
        completion.outcome,
        ConfigurationOutcome::Unknown(ConfigurationUnknown::CancelledWait)
    );
    assert!(node.configuration_requests_are_drained());
    assert!(!node.local().owner.is_drained());
    let progress = node
        .poll(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap();
    denied(&progress, ticket);
    assert!(node.poll_configuration().is_none());
    assert!(node.local().owner.is_drained());
}
fn committed(bridge: &mut Bridge<'_>, node: &mut FacadeNode, clock: Instant) {
    let request = request(node, 34205);
    let record = request.proposal.record.clone();
    let ticket = node.configure(request).unwrap();
    let progress = node
        .poll(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap();
    let step = progress
        .steps
        .iter()
        .find(|step| step.admission == Some(ticket.admission()))
        .unwrap();
    assert_eq!(step.operation, Some(ticket.operation()));
    assert_eq!(step.error, None);
    let position = step
        .proposed
        .expect("actual original configuration proposal");
    assert!(node.poll_configuration().is_none());
    assert_eq!(node.local().persistence.usage().requests, 1);
    super::step(bridge, node, clock, 0);
    assert!(node.poll_configuration().is_none());
    assert!(
        node.local()
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .commit_index
            < position.index
    );
    until(bridge, node, clock, |node| {
        node.local()
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .commit_index
            >= position.index
            && node.local().persistence.is_drained()
    });
    let state = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state();
    let entry = state
        .entry_at(position.index)
        .expect("original durable configuration entry");
    assert_eq!(entry.term, position.term);
    assert_eq!(entry.payload, EntryPayload::Configuration(Box::new(record)));
    assert!(!node.configuration_requests_are_drained());
    let completion = node
        .poll_configuration()
        .expect("actual committed metadata");
    assert_eq!(completion.ticket, ticket);
    assert_eq!(
        completion.outcome,
        ConfigurationOutcome::Committed(position)
    );
    let status = node
        .configuration_status(setup::mapping().parent, ticket.operation())
        .unwrap();
    assert_eq!(
        status.committed,
        ConfigurationProgress::Learners {
            configuration: ConfigurationId::new(2).unwrap(),
            index: position.index,
            term: position.term
        }
    );
    assert!(node.configuration_requests_are_drained());
}
fn shutdown(node: &mut FacadeNode) {
    let ticket = node.configure(invalid(node, 34206)).unwrap();
    node.close_intake().unwrap();
    assert_eq!(node.state(), NodeState::Quiescing);
    let progress = node
        .poll_shutdown(MonoTime(0), LogicalNodePollBudget::default())
        .unwrap();
    denied(&progress.replica, ticket);
    assert_eq!(node.state(), NodeState::Quiescing);
    assert!(!node.work_is_drained());
    assert!(!node.configuration_requests_are_drained());
    let closed = request(node, 34207);
    let proposal = closed.proposal.clone();
    let rejected = node.configure(closed).unwrap_err();
    assert_eq!(rejected.reason, ConfigurationRequestError::Closed);
    assert_eq!(rejected.request.proposal, proposal);
    let completion = node
        .poll_configuration()
        .expect("original shutdown observation");
    assert_eq!(completion.ticket, ticket);
    assert_eq!(
        completion.outcome,
        ConfigurationOutcome::Unknown(ConfigurationUnknown::Shutdown)
    );
    assert!(node.configuration_requests_are_drained());
}
fn failed_observer(image: &VerifiedLogicalImage) {
    let mut node = checked_node(image);
    let original_owner = node.local().owner.identity();
    let original_state = node
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .clone();
    let ticket = node.configure(invalid(&node, 34208)).unwrap();
    assert!(node.poll_configuration().is_none());
    node.abort();
    assert_eq!(node.state(), NodeState::RecoveryRequired);
    assert_eq!(
        node.configuration_status(setup::mapping().parent, ticket.operation()),
        Err(LogicalNodeError::RecoveryRequired)
    );
    assert!(!node.configuration_requests_are_drained());
    let mut recovery = node
        .into_recovery()
        .unwrap_or_else(|_| panic!("original observer transfer"));
    assert_eq!(recovery.reason, LogicalNodeError::Closed);
    assert_eq!(recovery.parts.local.owner.identity(), original_owner);
    assert_eq!(
        recovery
            .parts
            .local
            .owner
            .core(setup::mapping().parent)
            .unwrap()
            .state(),
        &original_state
    );
    assert!(recovery.parts.local.owner.is_failed());
    assert!(!recovery.configuration.is_drained());
    let completion = recovery
        .configuration
        .poll()
        .expect("same recovered observer");
    assert_eq!(completion.ticket, ticket);
    assert_eq!(
        completion.outcome,
        ConfigurationOutcome::Unknown(ConfigurationUnknown::OwnerFailed)
    );
    assert!(recovery.configuration.is_drained());
    assert!(!recovery.parts.local.owner.is_drained());
    assert!(recovery.parts.persistence.is_drained());
    assert!(recovery.pending.is_empty());
    assert!(recovery.incoming.is_empty());
    assert!(recovery.completions.is_empty());
}
fn history(protocol: NativePeerProtocol) {
    let _serial = serial();
    let mut cohort = Cluster::new_with_application(protocol, fixture::child_application());
    let clock = cohort.clock;
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(1)).unwrap();
    let image = claim_in(&mut bridge, clock, 34200, false);
    let mut node = checked_node(&image);
    restart::elect(&mut bridge, &mut node, clock);
    refusals(&mut node);
    cancelled(&mut node);
    committed(&mut bridge, &mut node, clock);
    until(&mut bridge, &mut node, clock, FacadeNode::work_is_drained);
    shutdown(&mut node);
    until(&mut bridge, &mut node, clock, FacadeNode::is_drained);
    drop(
        node.into_parts()
            .unwrap_or_else(|_| panic!("original configuration observer drain")),
    );
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    let mut bridge =
        LogicalChildBridge::new(&mut cohort.nodes, setup::mapping(), support::node(2)).unwrap();
    let image = claim_in(&mut bridge, clock, 34209, false);
    failed_observer(&image);
    bridge.close();
    assert!(bridge.is_drained());
    drop(bridge);
    cohort.finish();
}
