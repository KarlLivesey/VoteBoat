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
//! Clear transport buffers, then require each stale group's own snapshot repair.
use super::*;
const GROUPS: usize = 8;
#[path = "shared_recovery/maintenance.rs"]
mod maintenance;
struct Harness {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    bootstraps: Vec<Bootstrap>,
    addresses: BTreeMap<u64, std::net::SocketAddr>,
    recovery: Option<SnapshotRecoveryLimits>,
}
impl Harness {
    fn new(protocol: NativePeerProtocol) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-shared-repair-{}-{protocol:?}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let reservations = (0..3)
            .map(|_| {
                let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = UdpSocket::bind(tcp.local_addr().unwrap()).unwrap();
                (tcp, udp)
            })
            .collect::<Vec<_>>();
        let addresses = (1..=3)
            .zip(reservations.iter().map(|(s, _)| s.local_addr().unwrap()))
            .collect();
        let policy = Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap();
        let bootstraps = (1..=GROUPS)
            .map(|g| Bootstrap {
                group: group_id(g),
                configuration: ConfigurationId::new(1).unwrap(),
                policy: policy.clone(),
                voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
            })
            .collect();
        Self {
            root,
            clock: Instant::now(),
            protocol,
            bootstraps,
            addresses,
            recovery: None,
        }
    }
    fn open(&self, n: u64, mode: NativeOpenMode) -> Replica<SharedLog> {
        ClusterSetup {
            root: &self.root,
            mode,
            protocol: self.protocol,
            capacity: 128,
            clock: &self.clock,
            bootstraps: &self.bootstraps,
            addresses: &self.addresses,
            lane: 1,
        }
        .open_replica_with_recovery(n, self.recovery)
        .unwrap()
        .0
    }
    fn all(&self, mode: NativeOpenMode) -> Vec<Replica<SharedLog>> {
        (1..=3).map(|n| self.open(n, mode)).collect()
    }
}

fn write_round(h: &Harness, replicas: &mut [Replica<SharedLog>], first: usize) -> Measurement {
    workload(
        replicas,
        &h.clock,
        Workload {
            first,
            count: GROUPS,
            window: GROUPS,
            groups: GROUPS,
            diagnostic: None,
        },
    )
    .unwrap()
}

fn abort_follower(mut replica: Replica<SharedLog>) -> BTreeMap<GroupIdentity, u64> {
    replica.abort();
    let mut recovery = replica
        .into_recovery()
        .unwrap_or_else(|_| panic!("aborted owner"));
    drop(recovery.peers.take());
    let mut snapshots = recovery.local.snapshots.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saved = None;
    let mut images_returned = false;
    loop {
        if saved.is_none() {
            recovery.local.persistence.poll(64);
            recovery.local.persistence.poll_reclaims(64);
            if let Some(log) = recovery.local.persistence.try_reclaim().unwrap() {
                saved = Some(
                    (1..=GROUPS)
                        .map(|g| {
                            let group = group_id(g);
                            (group, log.state(group).unwrap().last_index())
                        })
                        .collect(),
                );
            }
        }
        if !images_returned {
            snapshots.worker.poll(64);
            images_returned = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if images_returned {
            if let Some(saved) = saved.take() {
                return saved;
            }
        }
        assert!(
            Instant::now() < deadline,
            "aborted stores were not returned"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn checkpoint_survivors(
    h: &Harness,
    replicas: &mut [Replica<SharedLog>],
    old: &BTreeMap<GroupIdentity, u64>,
) -> BTreeMap<GroupIdentity, u64> {
    for replica in replicas.iter_mut() {
        for g in 1..=GROUPS {
            replica
                .control(group_id(g), NodeControl::Checkpoint)
                .unwrap();
        }
    }
    until(replicas, &h.clock, |ns| {
        Ok(ns.iter().all(|n| {
            let local = n.local();
            local
                .snapshots
                .as_ref()
                .is_some_and(|s| s.router.is_drained() && s.worker.is_drained())
                && (1..=GROUPS).all(|g| {
                    local.owner.core(group_id(g)).unwrap().state().base_index() > old[&group_id(g)]
                })
        }))
    })
    .unwrap();
    (1..=GROUPS)
        .map(|g| {
            let group = group_id(g);
            let base = replicas
                .iter()
                .map(|n| n.local().owner.core(group).unwrap().state().base_index())
                .min()
                .unwrap();
            (group, base)
        })
        .collect()
}

fn repair(h: &Harness, replicas: &mut [Replica<SharedLog>], forced: &BTreeMap<GroupIdentity, u64>) {
    let mut totals = PollTotals::default();
    for g in 1..=GROUPS {
        replicas[0]
            .control(group_id(g), NodeControl::Campaign)
            .unwrap();
    }
    let mut foreground = None;
    let mut applied = false;
    let mut peak = 0;
    drive(replicas, &h.clock, &mut totals, |ns| {
        let mut active = false;
        for n in ns.iter() {
            let router = &n.local().snapshots.as_ref().unwrap().router;
            let usage = router.recovery_usage();
            let limits = router.recovery_limits();
            assert!(usage.requests <= limits.requests);
            assert!(usage.image_bytes <= limits.image_bytes);
            peak = peak.max(usage.requests);
            active |= usage.requests > 0;
        }
        if h.recovery.is_some() && foreground.is_none() && active {
            if let Ok(n) = leader(ns, group_id(GROUPS)) {
                let ticket = ns[n]
                    .propose(ClientRequest {
                        group: group_id(GROUPS),
                        operation: OperationId::new(900_001).unwrap(),
                        bytes: 0i64.to_le_bytes().to_vec(),
                    })
                    .map_err(|e| format!("foreground refused: {:?}", e.reason))?;
                foreground = Some((n, ticket));
            }
        }
        if let Some((n, ticket)) = &foreground {
            if let Some(output) = ns[*n].poll_client() {
                assert_eq!(output.ticket(), *ticket);
                let result = checked(ns[*n].complete_client(output).map_err(|e| e.reason))?;
                assert!(matches!(result, ClientOutcome::Applied { receipt, .. }
                    if receipt.outcome == CounterOutcome::Value(2) && !receipt.duplicate));
                applied = true;
            }
        }
        let local = ns[2].local();
        let repaired = forced.iter().all(|(g, bound)| {
            local.owner.core(*g).unwrap().state().base_index() >= *bound
                && local.applications[g].applied_index() >= *bound
                && local.applications[g].read_applied(*bound) == Ok(2)
        });
        Ok(repaired && (h.recovery.is_none() || applied))
    })
    .unwrap();
    assert!(totals.snapshot_installs[2] >= GROUPS);
    if h.recovery.is_some() {
        assert_eq!(peak, 1);
        assert!(applied);
    }
}

fn history(protocol: NativePeerProtocol, bounded: bool) {
    let mut h = Harness::new(protocol);
    if bounded {
        h.recovery = Some(SnapshotRecoveryLimits {
            requests: 1,
            image_bytes: 128 * 1024 * 1024,
        });
    }
    let mut replicas = h.all(NativeOpenMode::Create);
    campaign(&mut replicas, &h.clock, GROUPS).unwrap();
    let first = write_round(&h, &mut replicas, 1);
    let first_bounds = boundaries(first.samples.iter().map(|s| (s.group, s.index)));
    verify(&mut replicas, &h.clock, GROUPS, GROUPS, &first_bounds).unwrap();
    let old = abort_follower(replicas.pop().unwrap());
    let second = write_round(&h, &mut replicas, GROUPS + 1);
    let last = boundaries(second.samples.iter().map(|s| (s.group, s.index)));
    verify(&mut replicas, &h.clock, 2 * GROUPS, GROUPS, &last).unwrap();
    let forced = checkpoint_survivors(&h, &mut replicas, &old);
    // Close every surviving transport so no retained Append can satisfy repair.
    close(replicas, &h.clock).unwrap();
    let mut replicas = h.all(NativeOpenMode::Recover);
    for (group, bound) in &forced {
        assert!(
            replicas[2]
                .local()
                .owner
                .core(*group)
                .unwrap()
                .state()
                .last_index()
                < *bound
        );
    }
    repair(&h, &mut replicas, &forced);
    verify(&mut replicas, &h.clock, 2 * GROUPS, GROUPS, &last).unwrap();
    for g in 1..=GROUPS {
        assert_eq!(retry(&mut replicas, &h.clock, g, 1, GROUPS).unwrap(), 0);
    }
    for (g, bound) in &forced {
        println!(
            "protocol={protocol:?} group={} prior_last={} required_base={bound} recovered_base={}",
            g.id.get(),
            old[g],
            replicas[2]
                .local()
                .owner
                .core(*g)
                .unwrap()
                .state()
                .base_index()
        );
    }
    close(replicas, &h.clock).unwrap();
    let mut replicas = h.all(NativeOpenMode::Recover);
    for (g, bound) in &forced {
        assert!(
            replicas[2]
                .local()
                .owner
                .core(*g)
                .unwrap()
                .state()
                .base_index()
                >= *bound
        );
    }
    campaign(&mut replicas, &h.clock, GROUPS).unwrap();
    verify(&mut replicas, &h.clock, 2 * GROUPS, GROUPS, &last).unwrap();
    for g in 1..=GROUPS {
        assert_eq!(retry(&mut replicas, &h.clock, g, 1, GROUPS).unwrap(), 0);
    }
    close(replicas, &h.clock).unwrap();
    std::fs::remove_dir_all(&h.root).unwrap();
}

#[test]
fn tcp_each_stale_group_requires_snapshot_after_transport_restart() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_each_stale_group_requires_snapshot_after_transport_restart() {
    history(NativePeerProtocol::Quic, false);
}

#[test]
fn tcp_recovery_quota_drains_eight_groups_with_foreground_writes() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recovery_quota_drains_eight_groups_with_foreground_writes() {
    history(NativePeerProtocol::Quic, true);
}
