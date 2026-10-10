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
use voteboat::{
    identity::FailureDomainId,
    membership::{
        Configuration, ConfigurationChange, ConfigurationProgress, ConfigurationRecord,
        ConfigurationResumeAction,
    },
    native::{
        administration::NativeAdministrationPlan, placement::NativePlacementAuthorizer,
        startup::NativeMemberStartup,
    },
    placement::{PlacementRequirements, ReplicaPlacement},
    raft::ConfigurationProposal,
    snapshot_worker::SnapshotWorker,
    worker::PersistenceWorker,
};
type SourceNode = Node<source_fixture::Source>;
#[derive(Clone, Copy, Eq, PartialEq)]
enum FinalCut {
    AfterActivation,
    BeforeImport,
}
#[derive(Clone, Copy)]
enum RecoveryCheckpoint {
    Current,
    Retained,
}
fn cid(id: u64) -> ConfigurationId {
    ConfigurationId::new(id).unwrap()
}
fn record(finalize: bool) -> ConfigurationRecord {
    ConfigurationRecord {
        operation: OperationId::new(500).unwrap(),
        expected: cid(if finalize { 2 } else { 1 }),
        change: if finalize {
            ConfigurationChange::Final { id: cid(3) }
        } else {
            ConfigurationChange::Joint {
                id: cid(2),
                next: Configuration::new(
                    cid(3),
                    Policy::new(
                        Tree::Majority(vec![
                            Tree::Voter(support::node(2)),
                            Tree::Voter(support::node(3)),
                        ]),
                        Limits::default(),
                    )
                    .unwrap(),
                    (2..=3)
                        .map(|n| (support::node(n), support::identity(n.into())))
                        .collect(),
                    [(support::node(1), support::identity(1))].into(),
                )
                .unwrap(),
            }
        },
    }
}
fn plan() -> NativeAdministrationPlan {
    NativeAdministrationPlan::new(
        group(20),
        NativePlacementAuthorizer::new(
            group(20),
            (1..=3)
                .map(|n| {
                    (
                        support::node(n),
                        ReplicaPlacement {
                            store: support::identity(n.into()),
                            domain: FailureDomainId::new(n).unwrap(),
                        },
                    )
                })
                .collect(),
            PlacementRequirements {
                minimum_voting_domains: 2,
                survive_any_single_domain_loss: false,
            },
        )
        .unwrap(),
        source_fixture::fresh().readiness_requirements(),
        vec![record(false), record(true)],
    )
    .unwrap()
}
fn open_source(rig: &Split) -> Vec<SourceNode> {
    configuration(&rig.root, 20, &[1, 2, 3], NativeOpenMode::Recover)
        .into_iter()
        .map(|mut startup| {
            startup.tls = startup.tls.with_wire_version(6).unwrap();
            NativeMemberStartup {
                provisioned_stores: startup.bootstrap.voter_stores.clone(),
                startup,
            }
            .open_with_protocol(
                rig.protocol,
                source_fixture::fresh(),
                Arc::new(ThreadWake::current()),
                MonoTime(rig.clock.elapsed().as_millis() as u64),
            )
            .unwrap_or_else(|e| panic!("member source: {:?}", e.reason))
        })
        .collect()
}
fn background(rig: &mut Split) {
    drive(&mut rig.parent, &rig.clock, |_| true);
    for target in &mut rig.targets {
        drive(target, &rig.clock, |_| true);
    }
}
fn member_drive(nodes: &mut [SourceNode], clock: &Instant, done: impl Fn(&[SourceNode]) -> bool) {
    let allowed = plan();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        for n in &mut *nodes {
            let progress = n
                .poll_with_configuration_authorization(
                    MonoTime(clock.elapsed().as_millis() as u64),
                    NodePollBudget::default(),
                    |core, proposal| allowed.authorize(group(20), core.membership(), proposal),
                )
                .unwrap();
            if let Some(replica) = progress.replica {
                for step in replica.steps {
                    assert!(
                        step.error.is_none()
                            || (n.local().owner.core(group(20)).unwrap().membership().id()
                                != cid(1)
                                && step.error == Some(voteboat::raft::RaftError::WrongIdentity)
                                && step.admission.is_none()
                                && step.operation.is_none()
                                && step.proposed.is_none()
                                && step.read.is_none()),
                        "{step:?}"
                    );
                }
            }
        }
        if done(nodes) {
            return;
        }
        assert!(Instant::now() < deadline, "source configuration stalled");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn committed(nodes: &[SourceNode], id: ConfigurationId) -> bool {
    nodes.iter().all(|n| {
        let core = n.local().owner.core(group(20)).unwrap();
        core.state()
            .membership_at(core.state().commit_index)
            .is_ok_and(|m| m.id() == id)
            && n.local().applications[&group(20)].applied_index() == core.state().commit_index
    })
}
fn configure(nodes: &mut [SourceNode], clock: &Instant, finalize: bool) -> ConfigurationTicket {
    let record = record(finalize);
    let ticket = nodes[0]
        .configure(ConfigurationRequest {
            group: group(20),
            proposal: ConfigurationProposal {
                record,
                readiness: vec![],
                requirements: source_fixture::fresh().readiness_requirements(),
            },
        })
        .unwrap();
    member_drive(nodes, clock, |ns| {
        committed(ns, cid(if finalize { 3 } else { 2 }))
    });
    ticket
}
// Abort retains the unread outcome until this recovery owner is dropped. Drain
// transport cancellation and join native workers before reopening the files.
fn abort_source(mut node: SourceNode, clock: &Instant, expected: Option<ConfigurationTicket>) {
    node.abort();
    let mut recovery = node
        .into_recovery()
        .unwrap_or_else(|_| panic!("source not aborted"));
    if let Some(ticket) =
        expected.filter(|t| t.admission().owner == recovery.local.owner.identity())
    {
        let original = recovery
            .configuration
            .poll()
            .expect("unread original configuration result");
        assert_eq!(original.ticket, ticket);
        assert!(matches!(
            original.outcome,
            ConfigurationOutcome::Committed(_)
        ));
    }
    let mut peers = recovery.peers.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !peers.is_drained() {
        peers
            .drain(
                &mut recovery.local.outbound,
                MonoTime(clock.elapsed().as_millis() as u64),
                PeerDriverBudget::default(),
            )
            .unwrap();
        assert!(Instant::now() < deadline);
    }
    let mut dialer = peers
        .into_parts()
        .unwrap_or_else(|_| panic!("peers retained"))
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("dialer retained"));
    let mut snapshots = recovery.local.snapshots.take().unwrap();
    let (mut log, mut snapshot) = (false, false);
    loop {
        recovery.local.persistence.poll(64);
        recovery.local.persistence.poll_reclaims(64);
        if !log {
            log = recovery.local.persistence.try_reclaim().unwrap().is_some();
        }
        snapshots.worker.poll(64);
        if !snapshot {
            snapshot = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if log && snapshot && dial {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "aborted source workers not joined"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn recover_source(
    rig: &mut Split,
    expected: Option<ConfigurationTicket>,
    checkpoint: RecoveryCheckpoint,
) {
    if rig.checkpoint && matches!(checkpoint, RecoveryCheckpoint::Current) {
        compact(&mut rig.source, &rig.clock, 20);
    }
    for node in std::mem::take(&mut rig.source) {
        abort_source(node, &rig.clock, expected);
    }
    background(rig);
    rig.source = open_source(rig);
    let eligible = rig
        .source
        .iter()
        .position(|n| n.local().owner.core(group(20)).unwrap().local_node() == support::node(2))
        .unwrap();
    rig.source.swap(0, eligible);
    campaign(&mut rig.source, &rig.clock, 20);
}
fn check_imported_children(rig: &mut Split, configuration: ConfigurationId) {
    let state = rig.observed();
    for target in &state.targets {
        let imported = target.imported.as_ref().unwrap();
        assert_eq!(imported.sources.len(), 1);
        assert_eq!(imported.sources[0].configuration, configuration);
        assert_eq!(
            imported.sources[0].fence,
            state.source.as_ref().unwrap().fence
        );
    }
}
fn reopen_children(rig: &mut Split) {
    rig.targets = std::array::from_fn(|i| {
        let g = 21 + i as u128;
        open(
            configuration(&rig.root, g, &[1, 2, 3], NativeOpenMode::Recover),
            &rig.clock,
            rig.protocol,
            || target_fixture::fresh_for(g),
        )
    });
    for (i, key, expected) in [(0, 1u8, 9), (1, 200u8, 13)] {
        let g = 21 + i as u128;
        assert_eq!(
            observe(
                &mut rig.targets[i],
                &rig.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: hint(key),
                    key: vec![key],
                    query: vec![key],
                })
            ),
            TargetRead::Data(expected)
        );
        let TargetOutcome::Applied(retry) = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            101 + i as u128,
            data(key, 2),
        )
        .outcome
        else {
            panic!("target retry rejected");
        };
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, BucketOutcome::Value(expected));
        for node in &rig.targets[i] {
            assert_eq!(
                node.local().applications[&group(g)]
                    .application()
                    .outbox()
                    .count(),
                2
            );
        }
    }
}

fn assert_final_source(rig: &Split, fence: OwnershipFence) {
    for n in &rig.source {
        let core = n.local().owner.core(group(20)).unwrap();
        assert_eq!(core.local_voter(), core.local_node() != support::node(1));
        assert_eq!(n.local().applications[&group(20)].fence(), Some(fence));
    }
}
fn finalize_source(
    rig: &mut Split,
    original: ConfigurationTicket,
) -> voteboat::runtime::ProposalPosition {
    campaign(&mut rig.source, &rig.clock, 20);
    let ConfigurationResumption::Submitted(ticket) = rig.source[0]
        .resume_configuration(
            group(20),
            original.operation(),
            source_fixture::fresh().readiness_requirements(),
        )
        .unwrap()
    else {
        panic!("original final configuration not submitted");
    };
    assert_eq!(ticket.operation(), original.operation());
    member_drive(&mut rig.source, &rig.clock, |nodes| {
        committed(nodes, cid(3))
    });
    let receipt = rig.source[0].poll_configuration().unwrap();
    assert_eq!(receipt.ticket, ticket);
    let ConfigurationOutcome::Committed(position) = receipt.outcome else {
        panic!("original final configuration not committed");
    };
    position
}
fn finalize_before_import(rig: &mut Split, original: ConfigurationTicket, frozen: &Observed) {
    let bases: BTreeMap<_, _> = rig
        .source
        .iter()
        .map(|n| {
            let core = n.local().owner.core(group(20)).unwrap();
            (core.local_node(), core.state().base_index())
        })
        .collect();
    let images = [21, 22].map(|g| {
        rig.source[0].local().applications[&group(20)]
            .export_target(group(g), 65536)
            .unwrap()
    });
    let final_position = finalize_source(rig, original);
    recover_source(rig, None, RecoveryCheckpoint::Retained);
    assert!(committed(&rig.source, cid(3)));
    for n in &rig.source {
        let core = n.local().owner.core(group(20)).unwrap();
        let base = core.state().base_index();
        assert_eq!(base, bases[&core.local_node()]);
        assert_eq!(
            core.state().membership_at(base).unwrap().id(),
            cid(if rig.checkpoint { 2 } else { 1 })
        );
        assert!(base < core.state().commit_index);
        assert_eq!(core.membership().id(), cid(3));
        let status = n
            .configuration_status(group(20), original.operation())
            .unwrap();
        let expected = ConfigurationProgress::Final {
            configuration: cid(3),
            index: final_position.index,
            term: final_position.term,
        };
        assert_eq!(status.accepted, expected);
        assert_eq!(status.committed, expected);
        assert_eq!(status.resume_action(), ConfigurationResumeAction::Completed);
    }
    assert_final_source(rig, frozen.source.as_ref().unwrap().fence);
    assert_eq!(rig.observed(), *frozen);
    for (g, image) in [21, 22].into_iter().zip(images) {
        assert_eq!(
            rig.source[0].local().applications[&group(20)]
                .export_target(group(g), 65536)
                .unwrap(),
            image
        );
    }
    rig.serving(frozen);
}
fn history(protocol: NativePeerProtocol, checkpoint: bool, final_cut: FinalCut) {
    let _guard = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Split::new(protocol, checkpoint);
    for phase in [Phase::Intent, Phase::Stage(21), Phase::Stage(22)] {
        assert_eq!(rig.resume_one(), phase);
    }
    let source = std::mem::take(&mut rig.source);
    let clock = rig.clock;
    close(source, &clock, 20, || background(&mut rig));
    rig.source = open_source(&rig);
    rig.source.swap(0, 1);
    campaign(&mut rig.source, &rig.clock, 20);
    let old_ticket = configure(&mut rig.source, &rig.clock, false);
    eprintln!("joint_split protocol={protocol:?} checkpoint={checkpoint} phase=joint");
    assert_eq!(rig.resume_one(), Phase::Fence);
    let frozen = rig.observed();
    let images = [21, 22].map(|g| {
        rig.source[0].local().applications[&group(20)]
            .export_target(group(g), 65536)
            .unwrap()
    });
    eprintln!("joint_split protocol={protocol:?} checkpoint={checkpoint} phase=fenced");
    recover_source(&mut rig, Some(old_ticket), RecoveryCheckpoint::Current);
    eprintln!("joint_split protocol={protocol:?} checkpoint={checkpoint} phase=joint_recovered");
    assert!(committed(&rig.source, cid(2)));
    assert_eq!(
        rig.source[0].cancel_configuration(old_ticket),
        Err(ConfigurationRequestError::StaleTicket)
    );
    assert_eq!(rig.observed(), frozen);
    for (g, image) in [21, 22].into_iter().zip(images) {
        assert_eq!(
            rig.source[0].local().applications[&group(20)]
                .export_target(group(g), 65536)
                .unwrap(),
            image
        );
    }
    rig.serving(&frozen);
    // Recover the original operation via its durable status, rather than
    // submitting an obsolete expected-configuration record as a fresh change.
    let status = rig.source[0]
        .configuration_status(group(20), old_ticket.operation())
        .unwrap();
    assert_eq!(
        status.resume_action(),
        ConfigurationResumeAction::Finalize(record(true))
    );
    if final_cut == FinalCut::BeforeImport {
        finalize_before_import(&mut rig, old_ticket, &frozen);
    }
    for phase in [
        Phase::Import(21),
        Phase::Import(22),
        Phase::Publish,
        Phase::Activate(21),
        Phase::Activate(22),
    ] {
        assert_eq!(rig.resume_one(), phase);
        let state = rig.observed();
        rig.serving(&state);
    }
    // Import provenance comes from the fresh quorum observation, while the
    // immutable source fence/export boundary remains the original Joint cut.
    check_imported_children(
        &mut rig,
        cid(if final_cut == FinalCut::BeforeImport {
            3
        } else {
            2
        }),
    );
    eprintln!("joint_split protocol={protocol:?} checkpoint={checkpoint} phase=targets_active");
    if final_cut == FinalCut::AfterActivation {
        finalize_source(&mut rig, old_ticket);
        recover_source(&mut rig, None, RecoveryCheckpoint::Current);
    }
    eprintln!("joint_split protocol={protocol:?} checkpoint={checkpoint} phase=final_recovered");
    assert!(committed(&rig.source, cid(3)));
    assert_final_source(&rig, frozen.source.as_ref().unwrap().fence);
    let state = rig.observed();
    rig.serving(&state);
    write_independent_children(&mut rig);
    rig.close_all();
    reopen_children(&mut rig);
    eprintln!("joint_split protocol={protocol:?} checkpoint={checkpoint} phase=children_recovered");
    rig.close_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_split_fence_survives_joint_membership_and_unread_source_abort() {
    history(NativePeerProtocol::TcpTls, false, FinalCut::AfterActivation);
}
#[test]
fn tcp_split_fence_survives_joint_checkpoint_and_unread_source_abort() {
    history(NativePeerProtocol::TcpTls, true, FinalCut::AfterActivation);
}
#[cfg(feature = "quic")]
#[test]
fn quic_split_fence_survives_joint_membership_and_unread_source_abort() {
    history(NativePeerProtocol::Quic, false, FinalCut::AfterActivation);
}
#[cfg(feature = "quic")]
#[test]
fn quic_split_fence_survives_joint_checkpoint_and_unread_source_abort() {
    history(NativePeerProtocol::Quic, true, FinalCut::AfterActivation);
}

#[test]
fn tcp_split_import_preserves_joint_fence_after_final_wal_recovery() {
    history(NativePeerProtocol::TcpTls, false, FinalCut::BeforeImport);
}
#[test]
fn tcp_split_import_recovers_final_suffix_over_retained_joint_checkpoint() {
    history(NativePeerProtocol::TcpTls, true, FinalCut::BeforeImport);
}
#[cfg(feature = "quic")]
#[test]
fn quic_split_import_preserves_joint_fence_after_final_wal_recovery() {
    history(NativePeerProtocol::Quic, false, FinalCut::BeforeImport);
}
#[cfg(feature = "quic")]
#[test]
fn quic_split_import_recovers_final_suffix_over_retained_joint_checkpoint() {
    history(NativePeerProtocol::Quic, true, FinalCut::BeforeImport);
}
