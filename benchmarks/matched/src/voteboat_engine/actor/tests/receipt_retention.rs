// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

struct Fixture {
    actors: Vec<Actor<true>>,
    _clients: Vec<mpsc::Sender<Request>>,
    bases: [u64; 3],
    checkpoints: [u64; 3],
}
impl Fixture {
    fn new() -> Self {
        let settings = Settings {
            clients: 4096,
            server_workers: crate::DEFAULT_SERVER_WORKERS,
            batch: 4,
            requested_operations: 16384,
            operations: 16384,
            warmup: 0,
            maintenance: Maintenance::DefaultRetention,
            envelopes: crate::Envelopes::UpstreamDefault,
            attribution: true,
            campaign_timeout_secs: crate::DEFAULT_CAMPAIGN_TIMEOUT_SECS,
        }
        .validate()
        .unwrap();
        let bootstrap = bootstrap().unwrap();
        let (clients, receivers): (Vec<_>, Vec<_>) =
            (0..3).map(|_| mpsc::channel(CHANNEL_CAPACITY)).unzip();
        let (peers, peer_receivers): (Vec<_>, Vec<_>) =
            (0..3).map(|_| mpsc::channel(CHANNEL_CAPACITY)).unzip();
        let stats = Arc::new(std::array::from_fn(|_| stats::Stats::default()));
        let mut actors = Vec::new();
        for (slot, (receiver, peer_receiver)) in
            receivers.into_iter().zip(peer_receivers).enumerate()
        {
            let node = NodeId::new(slot as u64 + 1).unwrap();
            let binding = StoreBinding {
                identity: bootstrap.voter_stores[&node],
                session: StoreSession::new(1).unwrap(),
            };
            let store =
                VolatileStore::<true>::new(binding, settings.log_limits().unwrap()).unwrap();
            let state = store.log.bootstrap(&bootstrap).unwrap();
            let mut core = Raft::recover_volatile(node, &store.log, state).unwrap();
            core.configure_batch_limits(settings.envelopes.voteboat_limits(4).unwrap())
                .unwrap();
            actors.push(Actor::new(Parts {
                slot,
                core,
                store,
                receiver,
                peer_receiver,
                peers: peers.clone(),
                snapshots: MemorySnapshots::new(binding, bootstrap.group),
                application: Noop {
                    group: bootstrap.group,
                    applied: 0,
                },
                stopped: watch::channel(false).1,
                stopping: Arc::new(AtomicBool::new(false)),
                stats: stats.clone(),
                frontiers: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
                settings,
                paused: Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
                paused_proposals: Arc::new(AtomicBool::new(false)),
                proposals_pause_observed: Arc::new(std::array::from_fn(|_| AtomicBool::new(false))),
            }));
        }
        let mut fixture = Self {
            actors,
            _clients: clients,
            bases: [0; 3],
            checkpoints: [0; 3],
        };
        fixture.actors[0].step(Event::Campaign).unwrap();
        fixture.pump(false);
        assert!(fixture
            .actors
            .iter()
            .all(|a| a.parts.application.applied == 1));
        fixture
    }
    fn maintain(&mut self, slot: usize) {
        let actor = &mut self.actors[slot];
        let previous = actor.parts.stats[slot]
            .checkpoint_count
            .load(Ordering::Relaxed);
        actor.maintain().unwrap();
        let count = actor.parts.stats[slot]
            .checkpoint_count
            .load(Ordering::Relaxed);
        if count > previous {
            let applied = actor.parts.application.applied;
            let base = actor.parts.core.state().base_index();
            assert!(base > self.bases[slot]);
            assert_eq!(base, applied - 1000);
            assert_eq!(actor.parts.core.state().entries.len(), 1000);
            assert_eq!(actor.parts.core.state().commit_index, applied);
            let reference = actor.parts.snapshots.latest_reference().unwrap().unwrap();
            assert_eq!(reference.index, base);
            assert_eq!(actor.parts.snapshots.pins.len(), 1);
            let image = actor.parts.snapshots.load_pinned(reference).unwrap();
            assert_eq!(image.application, base.to_le_bytes());
            assert!(reference.matches(&image));
            self.bases[slot] = base;
            self.checkpoints[slot] += 1;
            println!("checkpoint slot={slot} count={count} applied={applied} base={base} suffix=1000 pins=1 receipt_capacity={}", actor.receipts.capacity());
        }
    }
    fn pump(&mut self, lagging: bool) {
        for _ in 0..100_000 {
            let mut progress = false;
            for slot in 0..3 {
                if lagging && slot == 2 {
                    continue;
                }
                while let Ok(message) = self.actors[slot].parts.peer_receiver.try_recv() {
                    self.actors[slot].step(Event::Receive(message)).unwrap();
                    self.maintain(slot);
                    progress = true;
                }
            }
            if !progress {
                return;
            }
        }
        panic!("bounded synchronous pump did not quiesce");
    }
    fn write(&mut self, start: usize, width: usize, lagging: bool) {
        let mut originals = Vec::new();
        let mut waits = Vec::new();
        for id in start..start + width {
            let operation = OperationId::new((1 << 127) + id as u128 * 7919).unwrap();
            let (reply, wait) = oneshot::channel();
            originals.push(Original {
                operation,
                bytes: Vec::new(),
                reply,
            });
            waits.push((operation, wait));
        }
        self.actors[0].requests(Request { originals }).unwrap();
        self.maintain(0);
        self.pump(lagging);
        for (id, mut wait) in waits {
            assert_eq!(wait.try_recv().unwrap(), id);
        }
        let expected = (start + width) as u64;
        for (slot, actor) in self.actors.iter().enumerate() {
            if lagging && slot == 2 {
                continue;
            }
            assert_eq!(actor.parts.application.applied, expected);
            assert_eq!(actor.parts.core.state().commit_index, expected);
            assert!(actor.receipts.is_empty());
            assert!(actor.receipts.capacity() >= width);
        }
    }
    fn application_receipts(&self, slot: usize) -> u64 {
        let actor = &self.actors[slot];
        let report = actor
            .attribution
            .report(
                slot,
                &actor.parts.store.attribution,
                &actor.parts.snapshots.attribution,
            )
            .unwrap()
            .unwrap();
        let value = serde_json::to_value(report).unwrap();
        let phases = value["phases"].as_array().unwrap();
        let phase = phases.iter().find(|p| p["phase"] == "application").unwrap();
        phase["items"].as_u64().unwrap()
    }
}
#[test]
fn receipt_reuse_all_three_default_retention_two_checkpoints_each_width() {
    for width in [1, 231, 515] {
        let mut fixture = Fixture::new();
        let rounds = 12_000usize.div_ceil(width);
        for round in 0..rounds {
            fixture.write(1 + round * width, width, false);
        }
        let originals = rounds * width;
        for slot in 0..3 {
            assert_eq!(fixture.checkpoints[slot], 2);
            assert_eq!(fixture.application_receipts(slot), originals as u64);
            assert_eq!(
                fixture.actors[slot].parts.application.applied,
                originals as u64 + 1
            );
        }
        println!("width={width} originals={originals} all3 actual receipts/frontiers match; two real checkpoints each");
    }
}
#[test]
fn receipt_reuse_actual_lagging_snapshot_install_then_resumed_each_width() {
    for width in [1, 231, 515] {
        let mut fixture = Fixture::new();
        let rounds = 12_000usize.div_ceil(width);
        for round in 0..rounds {
            fixture.write(1 + round * width, width, true);
        }
        let originals = rounds * width;
        fixture.pump(false);
        assert!(
            fixture.actors[2].parts.stats[2]
                .snapshot_installs
                .load(Ordering::Relaxed)
                > 0
        );
        assert_eq!(
            fixture.actors[2].parts.application.applied,
            originals as u64 + 1
        );
        let before = fixture.application_receipts(2);
        fixture.write(originals + 1, width, false);
        assert_eq!(fixture.application_receipts(2) - before, width as u64);
        assert!(fixture
            .actors
            .iter()
            .all(|a| a.parts.application.applied == (originals + width + 1) as u64));
        println!("width={width} actual follower snapshot installed; resumed receipts={width}");
    }
}
