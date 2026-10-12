// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real parent Raft input controls; not authenticated parent network assembly.
use super::parent_snapshots::ParentSnapshots;
use super::{application::App, outbound::Outbound, setup};
use std::collections::BTreeMap;
use voteboat::{
    application::*, identity::*, log::*, logical_voter::*, native::runtime::*, outbound::*,
    raft::*, runtime::*, worker::*,
};
pub type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
pub struct Parent {
    pub owner: Owner,
    pub worker: LogicalPersistenceWorker,
    pub port: LogicalPersistencePort,
    pub apps: BTreeMap<GroupIdentity, App>,
    pub routers: LogicalRouterParts<CounterReceipt, (), i64>,
    pub outbound: Outbound,
    pub driver: LogicalReplicaDriver<CounterReceipt, i64>,
    pub snapshots: Option<ParentSnapshots>,
}
impl Parent {
    pub fn new(image: &VerifiedLogicalImage) -> Self {
        Self::build(image, false)
    }
    pub fn with_snapshots(image: &VerifiedLogicalImage) -> Self {
        Self::build(image, true)
    }
    fn build(image: &VerifiedLogicalImage, snapshots: bool) -> Self {
        let generation = StorageWorkerGeneration::new(1).unwrap();
        let (worker, port) = logical_persistence(
            image,
            generation,
            WorkerLimits {
                max_requests: 8,
                max_units: 8,
                max_bytes: 64 * 1024,
                control_requests: 2,
                control_units: 2,
                control_bytes: 8192,
                batch_units: 1,
                batch_bytes: 8192,
            },
        )
        .unwrap();
        let mut app = App::new();
        let (core, _) = image.recover_parent(&mut app).unwrap();
        let owner = owner(image, worker.binding(), core);
        let routers = image
            .routers(&owner, LogicalRouterLimits::default())
            .unwrap();
        let mut snapshots = snapshots.then(|| ParentSnapshots::new(image, &port, &owner));
        let mut apps = BTreeMap::from([(setup::mapping().parent, app)]);
        let mut owner = owner;
        let mut worker = worker;
        let mut routers = routers;
        let mut outbound = Outbound::new(OutboundBinding {
            node: setup::mapping().voter,
            store: image.binding(),
            generation: OutboundGeneration::new(1).unwrap(),
        });
        let parts = ReplicaParts {
            owner: &mut owner,
            persistence: &mut worker,
            applications: &mut apps,
            results: &mut routers.results,
            clients: &mut routers.clients,
            reads: &mut routers.reads,
            outbound: &mut outbound,
            snapshots: snapshots.as_mut().map(ParentSnapshots::parts),
        };
        let driver = LogicalReplicaDriver::new(
            &parts,
            ReplicaDriverLimits {
                persistence_batch_units: 1,
                ..ReplicaDriverLimits::default()
            },
            LogicalReleaseLimits {
                pending: 16,
                bytes: 1024 * 1024,
                capsule_bytes: 8192,
                control_pending: 4,
                control_bytes: 128 * 1024,
            },
            MonoTime(0),
        )
        .unwrap();
        Self {
            owner,
            worker,
            port,
            apps,
            routers,
            outbound,
            driver,
            snapshots,
        }
    }
    pub fn poll(&mut self) -> ReplicaProgress {
        let mut parts = ReplicaParts {
            owner: &mut self.owner,
            persistence: &mut self.worker,
            applications: &mut self.apps,
            results: &mut self.routers.results,
            clients: &mut self.routers.clients,
            reads: &mut self.routers.reads,
            outbound: &mut self.outbound,
            snapshots: self.snapshots.as_mut().map(ParentSnapshots::parts),
        };
        let progress = self
            .driver
            .poll(&mut parts, MonoTime(0), ReplicaPollBudget::default())
            .unwrap();
        assert!(progress.steps.iter().all(|step| step.error.is_none()));
        progress
    }
    pub fn is_drained(&self) -> bool {
        self.owner.is_drained()
            && self.worker.is_drained()
            && self.port.is_drained()
            && self.driver.is_drained()
            && self.routers.results.is_drained()
            && self.routers.clients.is_drained()
            && self.routers.reads.is_drained()
            && self.outbound.is_drained()
            && self
                .snapshots
                .as_ref()
                .is_none_or(ParentSnapshots::is_drained)
    }
    pub fn take_sends(&mut self) -> Vec<Message> {
        let mut messages = Vec::new();
        for batch in self.outbound.poll(8) {
            let proof = self.outbound.proof(batch.ticket);
            assert_eq!(proof.mapping(), setup::mapping());
            assert_eq!(proof.receipt().outcome, LogicalOutcome::CapsuleAuthorized);
            assert!(!proof.command_bytes().is_empty());
            assert!(proof.receipt().first_index > 0);
            assert_ne!(proof.child_owner().store.identity, setup::mapping().store);
            messages.extend(batch.messages.iter().cloned());
            self.outbound
                .complete(batch, LocalSendResult::Sent)
                .unwrap();
        }
        messages
    }
    pub fn close(mut self) -> GroupLog {
        let mut parts = ReplicaParts {
            owner: &mut self.owner,
            persistence: &mut self.worker,
            applications: &mut self.apps,
            results: &mut self.routers.results,
            clients: &mut self.routers.clients,
            reads: &mut self.routers.reads,
            outbound: &mut self.outbound,
            snapshots: self.snapshots.as_mut().map(ParentSnapshots::parts),
        };
        self.driver.close_intake(&mut parts).unwrap();
        assert!(self.driver.is_drained());
        assert!(self.owner.is_drained());
        self.worker.close();
        assert!(self.worker.is_drained());
        assert!(self.port.is_drained());
        if let Some(snapshots) = &mut self.snapshots {
            snapshots.close();
        }
        self.outbound.close();
        assert!(self.outbound.is_drained());
        self.owner
            .core(setup::mapping().parent)
            .unwrap()
            .state()
            .clone()
    }
}
fn owner(image: &VerifiedLogicalImage, binding: WorkerBinding, core: Raft) -> Owner {
    let id = RuntimeOwner {
        store: image.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        id,
        ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        },
        FairScheduler::new(1).unwrap(),
    )
    .unwrap();
    shard.register(core).unwrap();
    let runtime = TimedShard::new(
        shard,
        DeadlineQueue::new(id, 2).unwrap(),
        JitterEntropy::new(17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    image
        .into_owner(runtime, binding, EffectOwnerLimits::default())
        .unwrap_or_else(|error| panic!("actual verified logical owner: {:?}", error.reason))
}
