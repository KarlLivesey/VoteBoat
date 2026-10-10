// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Native bounded receive pressure with real quorum work and forced snapshots.
use super::*;

#[derive(Default)]
struct Observations {
    rounds: usize,
    refused: [usize; 3],
    installs: [usize; 3],
    peak_frames: usize,
    peak_recovery: usize,
    active_stale_recovery: bool,
    writes_during_catchup: usize,
    forced: BTreeMap<GroupIdentity, u64>,
}
impl Observations {
    fn bounded(&mut self, nodes: &[Replica<SharedLog>]) {
        for n in nodes {
            let peers = n.peers().unwrap();
            let usage = peers.ingress().usage();
            let limits = peers.ingress().limits();
            assert!(usage.batches <= limits.batches);
            assert!(usage.messages <= limits.messages);
            assert!(usage.bytes <= limits.bytes);
            self.peak_frames = self.peak_frames.max(usage.batches);
            let router = &n.local().snapshots.as_ref().unwrap().router;
            let recovery = router.recovery_usage();
            assert!(recovery.requests <= 1);
            assert!(recovery.image_bytes <= router.recovery_limits().image_bytes);
            self.peak_recovery = self.peak_recovery.max(recovery.requests);
            let outbound = &n.local().outbound;
            assert!(outbound
                .usage()
                .fits(OutboundUsage::default(), outbound.limits().node.max));
            let owner = &n.local().owner;
            assert!(owner.usage().reserved_bytes <= owner.limits().reserved_bytes);
        }
        self.active_stale_recovery = nodes[2]
            .local()
            .snapshots
            .as_ref()
            .unwrap()
            .router
            .recovery_usage()
            .requests
            > 0;
    }
    fn repaired(&self, nodes: &[Replica<SharedLog>]) -> bool {
        self.forced.iter().all(|(g, base)| {
            nodes[2]
                .local()
                .owner
                .core(*g)
                .unwrap()
                .state()
                .base_index()
                >= *base
                && nodes[2].local().applications[g].applied_index() >= *base
        })
    }
}
fn poll_pressure(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    seen: &mut Observations,
    hold: bool,
) {
    seen.rounds += 1;
    for (i, n) in nodes.iter_mut().enumerate() {
        // Keep the stale receiver scheduled, but give its recovery owner one
        // bounded turn per four rounds while the healthy majority serves work.
        if !hold && i == 2 && !seen.rounds.is_multiple_of(4) {
            continue;
        }
        let mut budget = NodePollBudget::default();
        budget.peers.peer_visits = 2;
        budget.peers.ingress = usize::from(!hold || i != 2);
        if i == 2 {
            budget.replica = ReplicaPollBudget {
                worker_events: 1,
                snapshot_events: 1,
                steps: 1,
                effects: 1,
                retries: 1,
                reconcile: 1,
            };
        }
        let progress = n
            .poll(MonoTime(h.clock.elapsed().as_millis() as u64), budget)
            .unwrap();
        if let Some(p) = progress.peers {
            seen.refused[i] += p.ingress_blocked;
        }
        if let Some(p) = progress.replica {
            seen.installs[i] += p.snapshot_installs;
            assert!(p.steps.iter().all(|step| step.error.is_none()));
        }
    }
    seen.bounded(nodes);
}
fn drive_pressure(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    seen: &mut Observations,
    hold: bool,
    mut done: impl FnMut(&mut [Replica<SharedLog>], &mut Observations) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        poll_pressure(h, nodes, seen, hold);
        if done(nodes, seen) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "receive pressure stalled: refused={:?} installs={:?}",
            seen.refused,
            seen.installs
        );
        std::thread::park_timeout(Duration::from_micros(100));
    }
}
fn prepare(h: &Harness) -> (Vec<Replica<SharedLog>>, Observations) {
    let mut nodes = h.all(NativeOpenMode::Create);
    campaign(&mut nodes, &h.clock, GROUPS).unwrap();
    let first = write_round(h, &mut nodes, 1);
    verify(
        &mut nodes,
        &h.clock,
        GROUPS,
        GROUPS,
        &boundaries(first.samples.iter().map(|s| (s.group, s.index))),
    )
    .unwrap();
    let old = abort_follower(nodes.pop().unwrap());
    let second = write_round(h, &mut nodes, GROUPS + 1);
    verify(
        &mut nodes,
        &h.clock,
        2 * GROUPS,
        GROUPS,
        &boundaries(second.samples.iter().map(|s| (s.group, s.index))),
    )
    .unwrap();
    let forced = checkpoint_survivors(h, &mut nodes, &old);
    close(nodes, &h.clock).unwrap();
    let mut nodes = h.all(NativeOpenMode::Recover);
    for (g, base) in &forced {
        assert!(
            nodes[2]
                .local()
                .owner
                .core(*g)
                .unwrap()
                .state()
                .last_index()
                < *base
        );
        nodes[0].control(*g, NodeControl::Campaign).unwrap();
    }
    (
        nodes,
        Observations {
            forced,
            ..Default::default()
        },
    )
}
fn write_wave(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    seen: &mut Observations,
    hold: bool,
    first: usize,
    expected: i64,
) -> BTreeMap<GroupIdentity, u64> {
    let mut pending = BTreeMap::new();
    let mut last = BTreeMap::new();
    for g in 1..=GROUPS {
        let n = leader(nodes, group_id(g)).unwrap();
        assert!(n < 2);
        let ticket = nodes[n]
            .propose(ClientRequest {
                group: group_id(g),
                operation: OperationId::new((first + g - 1) as u128).unwrap(),
                bytes: 1i64.to_le_bytes().to_vec(),
            })
            .unwrap();
        pending.insert((n, ticket.sequence), ticket);
    }
    drive_pressure(h, nodes, seen, hold, |nodes, seen| {
        let catching_up = !seen.repaired(nodes);
        for (n, replica) in nodes.iter_mut().enumerate() {
            while let Some(output) = replica.poll_client() {
                let ticket = pending.remove(&(n, output.ticket().sequence)).unwrap();
                assert_eq!(output.ticket(), ticket);
                let result = replica.complete_client(output).unwrap();
                let ClientOutcome::Applied { position, receipt } = result else {
                    panic!("foreground write did not apply: {result:?}");
                };
                assert_eq!(receipt.operation, ticket.operation);
                assert_eq!(receipt.outcome, CounterOutcome::Value(expected));
                assert!(!receipt.duplicate);
                last.insert(ticket.group, position.index);
                seen.writes_during_catchup += usize::from(!hold && catching_up);
            }
        }
        pending.is_empty()
    });
    assert_eq!(last.len(), GROUPS);
    last
}
fn read_wave(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    seen: &mut Observations,
    hold: bool,
    expected: i64,
) {
    let mut pending = BTreeMap::new();
    for g in 1..=GROUPS {
        let n = leader(nodes, group_id(g)).unwrap();
        let ticket = nodes[n].read(group_id(g), ()).unwrap();
        pending.insert((n, ticket.sequence), ticket);
    }
    drive_pressure(h, nodes, seen, hold, |nodes, _| {
        for (n, replica) in nodes.iter_mut().enumerate() {
            while let Some(output) = replica.poll_read() {
                let ticket = pending.remove(&(n, output.ticket().sequence)).unwrap();
                assert_eq!(output.ticket(), ticket);
                assert!(matches!(replica.complete_read(output).unwrap(),
                    ReadOutcome::Read { result: Ok(value), .. } if value == expected));
            }
        }
        pending.is_empty()
    });
}
fn history(protocol: NativePeerProtocol) {
    let mut h = Harness::new(protocol);
    eprintln!("receive pressure files: {}", h.root.display());
    h.ingress = IngressLimits {
        batches: 3,
        control_batches: 1,
        background_batches: 1,
        ..IngressLimits::default()
    };
    h.recovery = Some(SnapshotRecoveryLimits {
        requests: 1,
        image_bytes: 128 * 1024 * 1024,
    });
    let (mut nodes, mut seen) = prepare(&h);
    drive_pressure(&h, &mut nodes, &mut seen, true, |nodes, seen| {
        seen.refused[2] > 0 && (1..=GROUPS).all(|g| leader(nodes, group_id(g)).is_ok_and(|n| n < 2))
    });
    assert_eq!(seen.peak_frames, 3);
    write_wave(&h, &mut nodes, &mut seen, true, 2 * GROUPS + 1, 3);
    read_wave(&h, &mut nodes, &mut seen, true, 3);
    assert_eq!(
        seen.installs[2], 0,
        "held receive cannot install recovery data"
    );
    drive_pressure(&h, &mut nodes, &mut seen, false, |_, seen| {
        seen.active_stale_recovery
    });
    let last = write_wave(&h, &mut nodes, &mut seen, false, 3 * GROUPS + 1, 4);
    read_wave(&h, &mut nodes, &mut seen, false, 4);
    drive_pressure(&h, &mut nodes, &mut seen, false, |nodes, seen| {
        seen.repaired(nodes)
    });
    assert!(seen.writes_during_catchup > 0);
    assert!(seen.installs[2] >= GROUPS, "installs={:?}", seen.installs);
    assert_eq!(seen.peak_recovery, 1);
    verify(&mut nodes, &h.clock, 4 * GROUPS, GROUPS, &last).unwrap();
    println!("protocol={protocol:?} receive_refusals={:?} peak_frames={} peak_recovery={} installs={} foreground_during_catchup={} writes=16 reads=16", seen.refused, seen.peak_frames, seen.peak_recovery, seen.installs[2], seen.writes_during_catchup);
    close(nodes, &h.clock).unwrap();
    let mut nodes = h.all(NativeOpenMode::Recover);
    campaign(&mut nodes, &h.clock, GROUPS).unwrap();
    verify(&mut nodes, &h.clock, 4 * GROUPS, GROUPS, &last).unwrap();
    for operation in 1..=4 * GROUPS {
        let expected = (operation - 1) / GROUPS + 1;
        assert_eq!(
            retry(&mut nodes, &h.clock, operation, expected as i64, GROUPS).unwrap(),
            0
        );
    }
    close(nodes, &h.clock).unwrap();
    std::fs::remove_dir_all(&h.root).unwrap();
}
#[test]
fn tcp_receive_pressure_keeps_quorum_work_and_recovers_every_group() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_receive_pressure_keeps_quorum_work_and_recovers_every_group() {
    history(NativePeerProtocol::Quic);
}
