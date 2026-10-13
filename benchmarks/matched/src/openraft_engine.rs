// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    Envelopes, Failure, Maintenance, Settings, CHANNEL_CAPACITY, COMMON_ENTRY_BATCH_CAPACITY,
};
use bench_minimal::{network::Router, store::ClientRequest};
use futures::TryStreamExt;
use openraft::rt::watch::WatchReceiver;
use openraft::{Config, SnapshotPolicy};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

pub(super) struct Cluster {
    router: Router,
    capacities: (usize, usize, usize),
}
#[derive(Clone)]
pub(super) struct Client(bench_minimal::network::BenchRaft);
impl Cluster {
    pub async fn new(settings: Settings) -> Result<Self, Failure> {
        let mut config = Config {
            election_timeout_min: 200,
            election_timeout_max: 2000,
            purge_batch_size: 1024,
            max_payload_entries: 1024,
            api_channel_size: Some(CHANNEL_CAPACITY as u64),
            snapshot_policy: match settings.maintenance {
                Maintenance::DefaultRetention => SnapshotPolicy::LogsSinceLast(5000),
                Maintenance::RetainAll => SnapshotPolicy::Never,
            },
            max_in_snapshot_log_to_keep: 1000,
            ..Config::default()
        };
        if settings.envelopes == Envelopes::Common64 {
            config.max_payload_entries = COMMON_ENTRY_BATCH_CAPACITY as u64;
            config.max_append_entries = Some(COMMON_ENTRY_BATCH_CAPACITY as u64);
            config.api_batch_capacity = COMMON_ENTRY_BATCH_CAPACITY as u64;
        }
        let config = config.validate()?;
        let capacities = (
            usize::try_from(config.api_batch_capacity)?,
            usize::try_from(config.max_append_entries.unwrap_or(4096))?,
            usize::try_from(config.max_payload_entries)?,
        );
        let mut router = Router::new();
        router
            .new_cluster(Arc::new(config), BTreeSet::from([0, 1, 2]))
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self { router, capacities })
    }
    pub fn capacities(&self) -> (usize, usize, usize) {
        self.capacities
    }
    pub fn client(&self) -> Client {
        Client(self.router.get_raft(0))
    }
    pub async fn verify(&self, operations: usize, warmup: usize) -> Result<Vec<u64>, Failure> {
        // OpenRaft startup applies initial membership0 and leader blank1.
        let expected = (operations + warmup + 1) as u64;
        let mut frontiers = Vec::new();
        for node in 0..3 {
            let raft = self.router.get_raft(node);
            raft.wait(Some(Duration::from_secs(10)))
                .applied_index(Some(expected), "complete benchmark prefix")
                .await?;
            let frontier = raft
                .metrics()
                .borrow_watched()
                .last_applied
                .map(|id| id.index);
            if frontier != Some(expected) {
                return Err("OpenRaft final applied prefix differs".into());
            }
            frontiers.push(expected);
        }
        Ok(frontiers)
    }
    pub async fn shutdown(&self) -> Result<(), Failure> {
        for node in 0..3 {
            self.router.get_raft(node).shutdown().await?;
        }
        Ok(())
    }
}
impl Client {
    pub async fn write(&self, originals: &[u128]) -> Result<usize, Failure> {
        if originals.len() == 1 {
            self.0.client_write(ClientRequest {}).await?;
            return Ok(1);
        }
        let entries: Vec<_> = originals.iter().map(|_| ClientRequest {}).collect();
        let mut results = self.0.client_write_many(entries).await?;
        let mut count = 0;
        while let Some(result) = results.try_next().await? {
            result?;
            count += 1;
        }
        if count != originals.len() {
            return Err("OpenRaft missing/extra original result".into());
        }
        Ok(count)
    }
}
