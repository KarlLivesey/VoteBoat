// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{parent::Owner, setup};
use voteboat::{identity::*, logical_voter::*, runtime::*, snapshot_worker::*};

pub struct ParentSnapshots {
    pub worker: LogicalSnapshotWorker,
    pub port: LogicalSnapshotPort,
    pub router: SnapshotRouter,
}
impl ParentSnapshots {
    pub fn new(
        image: &VerifiedLogicalImage,
        persistence: &LogicalPersistencePort,
        owner: &Owner,
    ) -> Self {
        let (worker, port) = logical_snapshots(
            image,
            persistence,
            SnapshotWorkerGeneration::new(1).unwrap(),
            SnapshotWorkLimits {
                max_groups: 1,
                max_requests: 4,
                max_bytes: 128 * 1024,
                control_requests: 1,
                control_bytes: 32 * 1024,
            },
        )
        .unwrap();
        let router = SnapshotRouter::new(
            owner.identity(),
            worker.binding(),
            SnapshotRouterLimits {
                requests: 4,
                max_image_bytes: snapshot_load_reservation(setup::limits().parent_snapshots)
                    .unwrap(),
            },
        )
        .unwrap();
        Self {
            worker,
            port,
            router,
        }
    }
    pub fn parts(&mut self) -> ReplicaSnapshots<'_> {
        ReplicaSnapshots {
            router: &mut self.router,
            worker: &mut self.worker,
        }
    }
    pub fn is_drained(&self) -> bool {
        self.worker.is_drained() && self.port.is_drained() && self.router.is_drained()
    }
    pub fn close(&mut self) {
        assert!(self.is_drained());
        self.worker.close();
        assert!(self.is_drained());
    }
}
