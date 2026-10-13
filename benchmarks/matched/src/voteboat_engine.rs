// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
mod actor;
mod stats;
use super::{application::Noop, snapshots::MemorySnapshots, store::MemoryStore, Failure, Settings};
use crate::CHANNEL_CAPACITY;
pub(crate) use stats::Diagnostics;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use voteboat::{identity::*, log::*, quorum::*, raft::*};

type Reply = oneshot::Sender<OperationId>;
struct Request {
    proposals: Vec<Proposal>,
    replies: Vec<Reply>,
}
#[derive(Clone)]
pub(super) struct Client(mpsc::Sender<Request>);
pub(super) struct Cluster {
    senders: Vec<mpsc::Sender<Request>>,
    actors: Vec<JoinHandle<Result<(), Failure>>>,
    frontiers: Arc<[AtomicU64; 3]>,
    stopping: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
    stats: Arc<[stats::Stats; 3]>,
    #[cfg(test)]
    paused: Arc<[AtomicBool; 3]>,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        for actor in &self.actors {
            actor.abort();
        }
    }
}
impl Cluster {
    pub async fn new(settings: Settings) -> Result<Self, Failure> {
        let bootstrap = bootstrap()?;
        let group = bootstrap.group;
        let limits = LogLimits {
            max_entries_per_group: settings.retained_budget(),
            ..LogLimits::default()
        }
        .validate()?;
        let frontiers = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
        let (senders, receivers): (Vec<_>, Vec<_>) =
            (0..3).map(|_| mpsc::channel(CHANNEL_CAPACITY)).unzip();
        let (peer_senders, peer_receivers): (Vec<_>, Vec<_>) =
            (0..3).map(|_| mpsc::channel(CHANNEL_CAPACITY)).unzip();
        let (stop, stopped) = watch::channel(false);
        let stopping = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(std::array::from_fn(|_| stats::Stats::default()));
        #[cfg(test)]
        let paused = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
        let mut parts = Vec::new();
        for (slot, (receiver, peer_receiver)) in
            receivers.into_iter().zip(peer_receivers).enumerate()
        {
            let node = NodeId::new(slot as u64 + 1).unwrap();
            let binding = StoreBinding {
                identity: bootstrap.voter_stores[&node],
                session: StoreSession::new(1).unwrap(),
            };
            let store = MemoryStore::new(binding, limits, bootstrap.clone())?;
            let core =
                Raft::recover(node, binding, store.state(group)?, limits).map_err(debug_error)?;
            parts.push(actor::Parts {
                slot,
                core,
                store,
                snapshots: MemorySnapshots::new(binding, group),
                application: Noop { group, applied: 0 },
                receiver,
                peer_receiver,
                peers: peer_senders.clone(),
                frontiers: frontiers.clone(),
                settings,
                stopped: stopped.clone(),
                stopping: stopping.clone(),
                stats: stats.clone(),
                #[cfg(test)]
                paused: paused.clone(),
            });
        }
        // Validate all fallible replica construction before starting any actor.
        let actors = parts
            .into_iter()
            .map(|parts| tokio::spawn(actor::Actor::new(parts).run()))
            .collect();
        let cluster = Self {
            senders,
            actors,
            frontiers,
            stopping,
            stop,
            stats,
            #[cfg(test)]
            paused,
        };
        cluster.wait_frontier(1).await?;
        Ok(cluster)
    }
    #[cfg(test)]
    fn pause_follower(&self, slot: usize, paused: bool) {
        assert!((1..3).contains(&slot));
        self.paused[slot].store(paused, Ordering::Release);
    }
    pub fn diagnostics(&self) -> Vec<Diagnostics> {
        self.stats.iter().map(stats::Stats::read).collect()
    }
    pub fn client(&self) -> Client {
        Client(self.senders[0].clone())
    }
    async fn wait_frontier(&self, expected: u64) -> Result<(), Failure> {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self
                .frontiers
                .iter()
                .any(|f| f.load(Ordering::Acquire) < expected)
            {
                if self.actors.iter().any(JoinHandle::is_finished) {
                    return Err("VoteBoat actor exited before expected frontier".into());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok::<_, Failure>(())
        })
        .await?
    }
    pub async fn verify(&self, operations: usize, warmup: usize) -> Result<Vec<u64>, Failure> {
        let expected = (operations + warmup + 1) as u64;
        self.wait_frontier(expected).await?;
        let frontiers: Vec<_> = self
            .frontiers
            .iter()
            .map(|f| f.load(Ordering::Acquire))
            .collect();
        if frontiers.iter().any(|f| *f != expected) {
            return Err("VoteBoat final applied prefix differs".into());
        }
        Ok(frontiers)
    }
    pub async fn shutdown(&mut self) -> Result<(), Failure> {
        self.stopping.store(true, Ordering::Release);
        let _ = self.stop.send(true);
        let mut first = None;
        for actor in self.actors.drain(..) {
            if let Err(error) = actor.await.map_err(Failure::from).and_then(|r| r) {
                first.get_or_insert(error);
            }
        }
        first.map_or(Ok(()), Err)
    }
}
impl Client {
    pub async fn write(&self, originals: &[u128]) -> Result<usize, Failure> {
        let mut proposals = Vec::with_capacity(originals.len());
        let mut replies = Vec::with_capacity(originals.len());
        let mut waits = Vec::with_capacity(originals.len());
        for &original in originals {
            let operation = OperationId::new(original).ok_or("zero original identity")?;
            let (reply, wait) = oneshot::channel();
            proposals.push(Proposal {
                operation,
                bytes: Vec::new(),
            });
            replies.push(reply);
            waits.push((operation, wait));
        }
        self.0.send(Request { proposals, replies }).await?;
        for (operation, wait) in waits {
            if wait.await? != operation {
                return Err("VoteBoat original applied result differs".into());
            }
        }
        Ok(originals.len())
    }
}
fn debug_error(error: impl std::fmt::Debug) -> Failure {
    format!("{error:?}").into()
}
fn bootstrap() -> Result<Bootstrap, Failure> {
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let voter_stores: BTreeMap<_, _> = (1..=3)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                StoreIdentity {
                    id: StoreId::new(u128::from(n)).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect();
    let policy = Policy::new(
        Tree::Majority(voter_stores.keys().copied().map(Tree::Voter).collect()),
        Limits::default(),
    )
    .map_err(debug_error)?;
    Ok(Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        voter_stores,
        policy,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn compacted_prefix_reaches_a_lagging_follower_through_actual_snapshot_install() {
        let settings = Settings {
            clients: 1,
            batch: 4,
            requested_operations: 12_000,
            operations: 12_000,
            warmup: 0,
            maintenance: crate::Maintenance::DefaultRetention,
            envelopes: crate::Envelopes::UpstreamDefault,
        }
        .validate()
        .unwrap();
        let mut cluster = Cluster::new(settings).await.unwrap();
        cluster.pause_follower(2, true);
        let client = cluster.client();
        for start in (1..=12_000).step_by(4) {
            let originals: Vec<_> = (start..start + 4).map(|n| n as u128).collect();
            assert_eq!(client.write(&originals).await.unwrap(), 4);
        }
        assert!(cluster.frontiers[2].load(Ordering::Acquire) < 4001);
        cluster.pause_follower(2, false);
        let result = cluster.verify(12_000, 0).await;
        let installed = cluster.stats[2].snapshot_installs.load(Ordering::Relaxed);
        let cleanup = cluster.shutdown().await;
        assert_eq!(result.unwrap(), vec![12_001; 3]);
        assert!(
            installed > 0,
            "follower must install a verified anchored image"
        );
        cleanup.unwrap();
    }
}
