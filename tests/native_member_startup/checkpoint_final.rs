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

//! Force pre-election checkpoint repair, then abandon volatile observation.
use super::*;
use voteboat::{snapshot_worker::SnapshotWorker, worker::PersistenceWorker};
type Service = NativeNode<Counter, NativeServiceConnector>;

fn seed_checkpoint(path: &Path, local: u64, joint_checkpoint: bool) {
    let target = Configuration::new(
        cid(12),
        Policy::new(
            Tree::Majority(vec![
                Tree::Voter(node(2)),
                Tree::Majority(vec![Tree::Voter(node(3))]),
            ]),
            Limits::default(),
        )
        .unwrap(),
        [(node(2), identity(2)), (node(3), identity(3))].into(),
        BTreeMap::new(),
    )
    .unwrap();
    let mut history = entries(3);
    let EntryPayload::Configuration(joint) = &mut history[1].payload else {
        unreachable!()
    };
    joint.change = ConfigurationChange::Joint {
        id: cid(11),
        next: target,
    };
    history[1].index = 3;
    history[2].index = 4;
    history.insert(
        1,
        LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(901).unwrap(),
                bytes: 11i64.to_le_bytes().to_vec(),
            },
        },
    );
    let count = if local == 3 { 4 } else { 1 };
    let committed = if local == 3 {
        if joint_checkpoint {
            3
        } else {
            4
        }
    } else {
        1
    };
    let mut log = NativeLogStore::create(
        FileLogIo::create(path).unwrap(),
        identity(local),
        LogLimits::default(),
    )
    .unwrap();
    let tickets = log
        .append_batch(vec![LogMutation::Create(bootstrap())])
        .unwrap();
    log.barrier(&tickets).unwrap();
    let state = log.state(group()).unwrap();
    let tickets = log
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group: group(),
            expected_revision: state.revision,
            hard_state: HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: committed,
            suffix: Some(Suffix {
                from: 1,
                entries: history[..count].to_vec(),
            }),
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    log.barrier(&tickets).unwrap();
    let mut images = NativeSnapshotStore::create(
        FileSnapshotIo::create(path.join("snapshots")).unwrap(),
        SnapshotIdentity {
            store: identity(local),
            group: group(),
        },
        SnapshotLimits::default(),
    )
    .unwrap();
    if local == 3 {
        let mut app = Counter::new(100).unwrap();
        let (mut core, _) =
            recover_member_replica(node(local), group(), &log, &mut images, &mut app).unwrap();
        let reference = checkpoint_application(&core, &app, &mut images)
            .unwrap()
            .reference();
        compact_replica(&mut core, &mut log, &mut images, &app, reference).unwrap();
        assert_eq!(reference.index, committed);
        assert_eq!(core.state().base_index(), committed);
        assert!(
            core.state().entry_at(3).is_none(),
            "joint must require checkpoint recovery"
        );
        assert_eq!(app.read_applied(committed), Ok(11));
    }
}

fn restart(root: &Path, addresses: &[SocketAddr; 3], protocol: NativePeerProtocol) -> Vec<Service> {
    [3, 2]
        .into_iter()
        .map(|local| {
            let (mut config, _unused) = startup(&root.join(local.to_string()), local, &[1, 2, 3]);
            config.startup.tls = config.startup.tls.with_wire_version(7).unwrap();
            config.startup.listen = addresses[local as usize - 1];
            for (peer, hint) in &mut config.startup.peers {
                hint.address = addresses[peer.get() as usize - 1];
            }
            open(config, protocol).unwrap()
        })
        .collect()
}

// Stop protocol polling immediately; accepted native I/O may finish. Discard
// terminal worker observations without applying them to a core, then join and
// release the actual selected stores before reopening. This is an owner abort,
// not a power-loss model or an assertion that accepted writes rolled back.
fn abandon(mut service: Service) {
    service.abort();
    let mut recovery = service
        .into_recovery()
        .unwrap_or_else(|_| panic!("aborted"));
    drop(recovery.peers.take());
    let mut snapshots = recovery.local.snapshots.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut log_done, mut snapshot_done) = (false, false);
    loop {
        if !log_done {
            let _ = recovery.local.persistence.poll(64);
            let _ = recovery.local.persistence.poll_reclaims(64);
            log_done = recovery.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshot_done {
            let _ = snapshots.worker.poll(64);
            snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if log_done && snapshot_done {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "aborted workers must return actual stores"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn apply(
    nodes: &mut [Service],
    clock: &Instant,
    operation: u128,
    delta: i64,
    expected: i64,
    duplicate: bool,
) {
    let ticket = nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(operation).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        })
        .unwrap_or_else(|r| panic!("{:?}", r.reason));
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut applied = None;
    loop {
        recursive::drive(nodes, clock, |_| true);
        while let Some(output) = nodes[0].poll_client() {
            assert_eq!(output.ticket(), ticket);
            match nodes[0]
                .complete_client(output)
                .unwrap_or_else(|r| panic!("{:?}", r.reason))
            {
                ClientOutcome::Applied { position, receipt } => {
                    assert_eq!(receipt.duplicate, duplicate);
                    assert_eq!(
                        receipt.outcome,
                        CounterOutcome::Value(if duplicate { 11 } else { expected })
                    );
                    applied = Some(position.index);
                }
                other => panic!("{other:?}"),
            }
        }
        if applied.is_some_and(|index| {
            nodes
                .iter()
                .all(|n| n.local().applications[&group()].read_applied(index) == Ok(expected))
        }) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "post-repair write did not replicate/apply"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn history(protocol: NativePeerProtocol, joint_checkpoint: bool) {
    let root = root();
    std::fs::create_dir(&root).unwrap();
    for local in [2, 3] {
        seed_checkpoint(&root.join(local.to_string()), local, joint_checkpoint);
    }
    let reservations = [reservation(), reservation(), reservation()];
    let addresses = reservations.each_ref().map(|endpoint| endpoint.0);
    drop(reservations); // voter1 is absent throughout the history
    let mut nodes = restart(&root, &addresses, protocol);
    let checkpoint = if joint_checkpoint { 3 } else { 4 };
    let clock = Instant::now();
    assert!(!nodes[1].local().owner.core(group()).unwrap().local_voter());
    assert_eq!(
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .base_index(),
        checkpoint
    );
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    // Poll candidate first: stop immediately after the learner restores, before
    // the candidate can observe that round's repair reply or request its ballot.
    recursive::drive(&mut nodes, &clock, |ns| {
        let source = ns[0].local().owner.core(group()).unwrap();
        let learner = ns[1].local().owner.core(group()).unwrap();
        if source.role() == Role::Leader {
            assert_eq!(
                ns[1].local().applications[&group()].read_applied(checkpoint),
                Ok(11)
            );
        }
        learner.state().base_index() == checkpoint
            && ns[1].local().applications[&group()].read_applied(checkpoint) == Ok(11)
    });
    assert_eq!(
        nodes[0].local().owner.core(group()).unwrap().role(),
        Role::Candidate
    );
    assert_eq!(
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .commit_index,
        checkpoint
    );
    assert_eq!(
        nodes[1]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .commit_index,
        checkpoint
    );
    for service in nodes {
        abandon(service);
    }
    let mut nodes = restart(&root, &addresses, protocol);
    let clock = Instant::now();
    for n in &nodes {
        let core = n.local().owner.core(group()).unwrap();
        assert_eq!(core.role(), Role::Follower);
        assert!(core.local_voter());
        assert_eq!(core.state().commit_index, checkpoint);
        assert_eq!(
            n.local().applications[&group()].read_applied(checkpoint),
            Ok(11)
        );
    }
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    recursive::drive(&mut nodes, &clock, |ns| {
        ns[0].local().owner.core(group()).unwrap().role() == Role::Leader
            && ns.iter().all(|n| {
                n.local().owner.core(group()).unwrap().state().commit_index >= 5
                    && n.local().applications[&group()].applied_index() >= 5
            })
    });
    apply(&mut nodes, &clock, 902, 7, 18, false);
    apply(&mut nodes, &clock, 901, 11, 18, true);
    recursive::shutdown(nodes, &clock);
    for local in [2, 3] {
        let path = root.join(local.to_string());
        let log = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let mut images = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut app = Counter::new(100).unwrap();
        let (core, _) =
            recover_member_replica(node(local), group(), &log, &mut images, &mut app).unwrap();
        assert_eq!(core.membership().id(), cid(12));
        assert!(core.membership().joint().is_none());
        assert_eq!(app.read_applied(core.state().commit_index), Ok(18));
        assert_eq!(core.state().base_index(), checkpoint);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn tcp_final_head_recovers_from_committed_joint_checkpoint_after_lost_reply() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_final_head_recovers_from_committed_joint_checkpoint_after_lost_reply() {
    history(NativePeerProtocol::Quic, true);
}
#[test]
fn tcp_final_head_recovers_from_committed_final_checkpoint_after_lost_reply() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_final_head_recovers_from_committed_final_checkpoint_after_lost_reply() {
    history(NativePeerProtocol::Quic, false);
}
