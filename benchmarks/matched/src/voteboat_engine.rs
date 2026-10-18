// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
mod actor;
mod stats;
use super::{
    application::Noop, snapshots::MemorySnapshots, store::VolatileStore, Failure, Settings,
};
use crate::{attribution::Attribution, CHANNEL_CAPACITY};
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
struct Original {
    operation: OperationId,
    bytes: Vec<u8>,
    reply: Reply,
}
struct Request {
    originals: Vec<Original>,
}
#[derive(Clone)]
pub(super) struct Client(mpsc::Sender<Request>);
pub(super) struct Cluster {
    senders: Vec<mpsc::Sender<Request>>,
    actors: Vec<JoinHandle<Result<Option<Attribution>, Failure>>>,
    attribution: Vec<Attribution>,
    frontiers: Arc<[AtomicU64; 3]>,
    stopping: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
    stats: Arc<[stats::Stats; 3]>,
    batch_limits: BatchLimits,
    #[cfg(test)]
    paused: Arc<[AtomicBool; 3]>,
    #[cfg(test)]
    paused_proposals: Arc<AtomicBool>,
    #[cfg(test)]
    proposals_pause_observed: Arc<[AtomicBool; 3]>,
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
        if settings.attribution {
            Self::create::<true>(settings).await
        } else {
            Self::create::<false>(settings).await
        }
    }
    async fn create<const TRACE: bool>(settings: Settings) -> Result<Self, Failure> {
        let bootstrap = bootstrap()?;
        let group = bootstrap.group;
        let limits = settings.log_limits()?;
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
        #[cfg(test)]
        let paused_proposals = Arc::new(AtomicBool::new(false));
        #[cfg(test)]
        let proposals_pause_observed = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
        let batch_limits = settings.envelopes.voteboat_limits(settings.batch)?;
        let mut parts = Vec::new();
        for (slot, (receiver, peer_receiver)) in
            receivers.into_iter().zip(peer_receivers).enumerate()
        {
            let node = NodeId::new(slot as u64 + 1).unwrap();
            let binding = StoreBinding {
                identity: bootstrap.voter_stores[&node],
                session: StoreSession::new(1).unwrap(),
            };
            let store = VolatileStore::<TRACE>::new(binding, limits)?;
            let state = store.log.bootstrap(&bootstrap)?;
            let mut core = Raft::recover_volatile(node, &store.log, state).map_err(debug_error)?;
            core.configure_batch_limits(batch_limits)
                .map_err(debug_error)?;
            if core.batch_limits() != batch_limits {
                return Err("Core batch limit readback differs from requested profile".into());
            }
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
                #[cfg(test)]
                paused_proposals: paused_proposals.clone(),
                #[cfg(test)]
                proposals_pause_observed: proposals_pause_observed.clone(),
            });
        }
        // Validate all fallible replica construction before starting any actor.
        let actors = parts
            .into_iter()
            .map(|parts| tokio::spawn(actor::Actor::<TRACE>::new(parts).run()))
            .collect();
        let cluster = Self {
            senders,
            actors,
            attribution: Vec::new(),
            frontiers,
            stopping,
            stop,
            stats,
            batch_limits,
            #[cfg(test)]
            paused,
            #[cfg(test)]
            paused_proposals,
            #[cfg(test)]
            proposals_pause_observed,
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
    pub fn attribution(&self) -> &[Attribution] {
        &self.attribution
    }
    pub fn batch_limits(&self) -> BatchLimits {
        self.batch_limits
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
        while let Some(actor) = self.actors.first_mut() {
            // A canceled cleanup must leave each unfinished handle owned for Drop.
            let joined = actor.await.map_err(Failure::from).and_then(|r| r);
            self.actors.remove(0);
            match joined {
                Ok(Some(attribution)) => self.attribution.push(attribution),
                Ok(None) => {}
                Err(error) => {
                    first.get_or_insert(error);
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}
impl Client {
    pub async fn write(&self, originals: &[u128]) -> Result<usize, Failure> {
        let mut entries = Vec::with_capacity(originals.len());
        let mut waits = Vec::with_capacity(originals.len());
        for &original in originals {
            let operation = OperationId::new(original).ok_or("zero original identity")?;
            let (reply, wait) = oneshot::channel();
            entries.push(Original {
                operation,
                bytes: Vec::new(),
                reply,
            });
            waits.push(wait);
        }
        self.0.send(Request { originals: entries }).await?;
        let mut expected = originals.iter();
        for wait in waits {
            let applied = wait.await?;
            if expected.next().copied() != Some(applied.get()) {
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
    fn preload_originals(cluster: &Cluster) -> Vec<oneshot::Receiver<OperationId>> {
        let mut waits = Vec::new();
        for start in (1..=16384).step_by(4) {
            let mut originals = Vec::new();
            for original in start..start + 4 {
                let operation = OperationId::new(original as u128).unwrap();
                let (reply, wait) = oneshot::channel();
                originals.push(Original {
                    operation,
                    bytes: Vec::new(),
                    reply,
                });
                waits.push(wait);
            }
            assert!(
                cluster.senders[0].try_send(Request { originals }).is_ok(),
                "preloaded requests must fit the bounded admission queue"
            );
        }
        waits
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn queued_three_voter_originals_keep_every_receipt_and_respect_the_bound() {
        for (envelopes, attribution) in [
            crate::Envelopes::UpstreamDefault,
            crate::Envelopes::Common64,
        ]
        .into_iter()
        .flat_map(|envelopes| [false, true].map(|attribution| (envelopes, attribution)))
        {
            let settings = Settings {
                clients: 4096,
                batch: 4,
                requested_operations: 16384,
                operations: 16384,
                warmup: 0,
                maintenance: crate::Maintenance::DefaultRetention,
                envelopes,
                attribution,
                campaign_timeout_secs: crate::DEFAULT_CAMPAIGN_TIMEOUT_SECS,
            }
            .validate()
            .unwrap();
            let mut cluster = Cluster::new(settings).await.unwrap();
            cluster.paused_proposals.store(true, Ordering::Release);
            tokio::time::timeout(Duration::from_secs(10), async {
                while !cluster.proposals_pause_observed[0].load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            let waits = preload_originals(&cluster);
            cluster.paused_proposals.store(false, Ordering::Release);
            let checked = tokio::time::timeout(Duration::from_secs(10), async {
                for (offset, wait) in waits.into_iter().enumerate() {
                    assert_eq!(wait.await.unwrap().get(), offset as u128 + 1);
                }
                cluster.verify(16384, 0).await.unwrap()
            })
            .await;
            let diagnostics = cluster.diagnostics();
            cluster.shutdown().await.unwrap();
            assert_eq!(checked.unwrap(), vec![16385; 3]);
            let leader = serde_json::to_value(&diagnostics[0]).unwrap();
            assert_eq!(leader["proposed_originals"], 16384);
            let width = leader["maximum_proposal_width"].as_u64().unwrap() as usize;
            assert!(
                width
                    <= settings
                        .envelopes
                        .voteboat_limits(settings.batch)
                        .unwrap()
                        .max_proposal_entries
            );
            for follower in &diagnostics[1..] {
                let follower = serde_json::to_value(follower).unwrap();
                assert_eq!(follower["proposed_originals"], 0);
                assert_eq!(follower["proposal_widths"], serde_json::json!([]));
            }
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn compacted_prefix_reaches_a_lagging_follower_through_actual_snapshot_install() {
        for attribution in [false, true] {
            let settings = Settings {
                clients: 1,
                batch: 4,
                requested_operations: 12_000,
                operations: 12_000,
                warmup: 0,
                maintenance: crate::Maintenance::DefaultRetention,
                envelopes: crate::Envelopes::UpstreamDefault,
                attribution,
                campaign_timeout_secs: crate::DEFAULT_CAMPAIGN_TIMEOUT_SECS,
            }
            .validate()
            .unwrap();
            let mut cluster = Cluster::new(settings).await.unwrap();
            cluster.pause_follower(2, true);
            let client = cluster.client();
            for start in (1..=12_000).step_by(4) {
                let originals: Vec<_> = (start..start + 4).map(|n| n as u128).collect();
                let completed = match tokio::time::timeout(
                    Duration::from_secs(10),
                    client.write(&originals),
                )
                .await
                {
                    Ok(Ok(completed)) => completed,
                    result => {
                        let frontiers: Vec<_> = cluster
                            .frontiers
                            .iter()
                            .map(|value| value.load(Ordering::Acquire))
                            .collect();
                        let cleanup = cluster.shutdown().await;
                        panic!(
                            "snapshot test client write failed attribution={attribution} originals={originals:?} result={result:?} frontiers={frontiers:?}; actor cleanup={cleanup:?}"
                        );
                    }
                };
                assert_eq!(completed, 4);
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
            if attribution {
                let follower = serde_json::to_value(
                    cluster
                        .attribution()
                        .iter()
                        .find(|value| serde_json::to_value(value).unwrap()["replica_slot"] == 2)
                        .unwrap(),
                )
                .unwrap();
                let helpers = follower["phases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|phase| phase["phase"] == "snapshot_helpers_inclusive")
                    .unwrap();
                assert!(helpers["calls"].as_u64().unwrap() >= installed as u64);
            }
        }
    }
}

#[cfg(test)]
mod cancellation_tests {
    use super::*;
    struct Released(Option<oneshot::Sender<()>>);
    impl Drop for Released {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    #[tokio::test]
    async fn canceled_cluster_shutdown_keeps_handles_owned_and_drop_aborts_every_actor() {
        let (_, settings) = Settings::parse_from(
            ["--clients", "1", "--operations", "4", "--warmup", "0"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();
        let mut cluster = Cluster::new(settings).await.unwrap();
        cluster.shutdown().await.unwrap();
        assert!(cluster.actors.is_empty());
        // Refill the same actual Cluster owner with three controlled stalled actors.
        // Normal Raft shutdown has finished; no protocol task is detached by this fixture.
        let mut releases = Vec::new();
        for _ in 0..3 {
            let (started, observed) = oneshot::channel();
            let (released, release_observed) = oneshot::channel();
            cluster.actors.push(tokio::spawn(async move {
                let _owned = Released(Some(released));
                started.send(()).unwrap();
                std::future::pending::<Result<Option<Attribution>, Failure>>().await
            }));
            observed.await.unwrap();
            releases.push(release_observed);
        }
        let ids: Vec<_> = cluster.actors.iter().map(JoinHandle::id).collect();
        let cleanup = crate::guard::bounded("cleanup", Duration::ZERO, cluster.shutdown()).await;
        assert!(cleanup.is_err());
        assert_eq!(
            cluster
                .actors
                .iter()
                .map(JoinHandle::id)
                .collect::<Vec<_>>(),
            ids
        );
        assert!(cluster.actors.iter().all(|actor| !actor.is_finished()));
        assert!(crate::guard::finish(Ok(()), cleanup).is_err());
        drop(cluster);
        crate::guard::bounded("release observation", Duration::from_secs(1), async {
            for released in releases {
                released.await?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod client_requests;

#[cfg(test)]
mod client_outcomes;
