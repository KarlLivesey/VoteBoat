// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original actual WAL/snapshot owners and independent nonvoting helper.
use super::*;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    application::StateMachine,
    connect::*,
    identity::*,
    native::{connect::NativePeerProtocol, node::*, relay::*, startup::*, worker::ThreadWake},
    raft::Role,
    runtime::*,
    secure::*,
    transport::*,
};
#[path = "cluster/providers.rs"]
mod providers;
pub struct Cluster {
    pub root: PathBuf,
    pub clock: Instant,
    pub boats: Vec<Boat>,
    pub helper: RelayForwarder<channels::Gated, voteboat::native::wire::NativeWireCodec>,
}
impl Cluster {
    pub fn open(
        root: PathBuf,
        protocol: NativePeerProtocol,
        mode: NativeOpenMode,
        clock: Instant,
    ) -> Self {
        let helper = LocalIdentity {
            node: support::node(9),
            store: StoreBinding {
                identity: support::identity(9),
                session: StoreSession::new(900 + u64::from(mode == NativeOpenMode::Recover))
                    .unwrap(),
            },
        };
        let tree = RelayTreePlan::new(
            setup::child().voter_stores,
            [(helper.node, helper.store.identity)].into(),
            Default::default(),
            (1..=3).map(|id| (support::node(id), helper.node)).collect(),
            8,
            1,
        )
        .unwrap();
        let mut boats = Vec::new();
        let mut hops = Vec::new();
        for id in 1..=3 {
            let local = providers::prepare(&root, clock, id, protocol, mode);
            let physical = LocalIdentity {
                node: support::node(id),
                store: local.owner.identity().store,
            };
            let plan = NativeRelayPlan::for_tree(physical, tree.clone()).unwrap();
            let (voter, relay, _gate) = channels::pair(protocol, physical, helper, 1, clock);
            hops.push(
                RelayHop::for_helper(
                    relay,
                    RelayHelperPlan::for_tree(helper, tree.clone()).unwrap(),
                    Default::default(),
                )
                .unwrap_or_else(|e| panic!("actual helper hop {:?}", e.reason)),
            );
            let hop = RelayHop::for_voter(voter, plan.clone(), Default::default())
                .unwrap_or_else(|e| panic!("actual voter hop {:?}", e.reason));
            let boat = Boat::from_parts(
                NativeNetworkNodeParts {
                    local,
                    peers: Some(NativeRelayParts {
                        plan,
                        hop,
                        codec: codec(),
                        limits: Default::default(),
                        routes: None,
                    }),
                },
                NodeLimits::default(),
                MonoTime(clock.elapsed().as_millis() as u64),
            )
            .unwrap_or_else(|e| panic!("actual selected relay child {:?}", e.reason));
            assert!(boat
                .activation_peer_binding(support::node(if id == 1 { 2 } else { 1 }))
                .is_none());
            boats.push(boat);
        }
        let helper = RelayForwarder::build(RelayForwarderParts {
            plan: RelayHelperPlan::for_tree(helper, tree).unwrap(),
            codec: codec(),
            limits: Default::default(),
            hops,
        })
        .unwrap_or_else(|e| panic!("actual relay helper {:?}", e.reason));
        Self {
            root,
            clock,
            boats,
            helper,
        }
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
    }
    pub fn helper(&mut self) {
        let p = self
            .helper
            .poll(self.now(), 3, TransportPollBudget::default())
            .unwrap();
        for (_, error) in p.source_failures {
            assert!(
                self.boats.iter().all(|b| b.state() != NodeState::Running)
                    && matches!(
                        error,
                        RelayNetworkError::Truncated
                            | RelayNetworkError::Session(
                                SessionError::Closed | SessionError::Truncated
                            )
                            | RelayNetworkError::Session(SessionError::Io(
                                std::io::ErrorKind::ConnectionReset
                            ))
                    ),
                "original helper closure {error:?}, node states {:?}",
                self.boats.iter().map(Boat::state).collect::<Vec<_>>()
            );
        }
    }
    pub fn poll(&mut self) {
        self.helper();
        let now = self.now();
        for boat in &mut self.boats {
            let progress = boat.poll(now, NodePollBudget::default()).unwrap();
            if let Some(replica) = progress.replica {
                for step in replica.steps {
                    if step.operation == OperationId::new(371002) {
                        assert_eq!(
                            step.error,
                            Some(
                                voteboat::raft::ConfigurationProposalError::AuthenticationRequired
                                    .into()
                            )
                        );
                    } else {
                        assert!(step.error.is_none(), "original native step {step:?}");
                    }
                }
            }
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn elect(&mut self) {
        self.boats[0]
            .control(setup::child().group, NodeControl::Campaign)
            .unwrap();
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            self.poll();
            let core = self.boats[0]
                .local()
                .owner
                .core(setup::child().group)
                .unwrap();
            if core.role() == Role::Leader
                && core.state().commit_index > 0
                && !core.has_pending_dependency()
                && self.boats.iter().all(|b| {
                    b.local().applications[&setup::child().group].applied_index()
                        >= core.state().commit_index
                })
            {
                break;
            }
            assert!(
                Instant::now() < end,
                "actual relay native election: helper {:?}; nodes {:?}",
                self.helper,
                self.boats
                    .iter()
                    .map(|b| (
                        b.state(),
                        b.peers(),
                        b.local().owner.usage(),
                        b.local()
                            .owner
                            .core(setup::child().group)
                            .unwrap()
                            .state()
                            .clone()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }
    pub fn finish(mut self) -> PathBuf {
        for boat in &mut self.boats {
            boat.begin_shutdown();
        }
        self.helper.close();
        let end = Instant::now() + Duration::from_secs(15);
        while !self.boats.iter().all(Boat::is_drained) || !self.helper.is_drained() {
            self.poll();
            assert!(
                Instant::now() < end,
                "actual native relay child and helper close"
            );
        }
        for boat in self.boats {
            providers::join(boat);
        }
        self.helper
            .into_parts()
            .unwrap_or_else(|_| panic!("original helper really drained"));
        self.root
    }
}
pub fn codec() -> voteboat::native::wire::NativeWireCodec {
    voteboat::native::wire::NativeWireCodec::with_leadership_transfer(Default::default()).unwrap()
}
