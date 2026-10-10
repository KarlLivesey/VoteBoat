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
//! Real shared-WAL maintenance while a snapshot recovery receipt is held.
use super::*;

fn enable(nodes: &mut [Replica<SharedLog>]) {
    for n in nodes {
        n.configure_checkpoints(Some(CheckpointPolicy {
            min_entries: 1,
            interval_ms: 1,
            scan_groups: 2,
            max_in_flight: 2,
        }))
        .unwrap();
        n.configure_wal_maintenance(Some(WalMaintenancePolicy {
            interval_ms: 1,
            retry_ms: 1,
            max_bytes: LogLimits::default().max_wal_bytes,
        }))
        .unwrap();
    }
}

#[derive(Default)]
struct Observations {
    tickets: [Option<ReclaimTicket>; 3],
    completions: [usize; 3],
    reclaimed: usize,
    peak_recovery: usize,
}
impl Observations {
    fn record(&mut self, nodes: &[Replica<SharedLog>]) {
        for (i, n) in nodes.iter().enumerate() {
            assert!(n.checkpoints().pending.len() <= 2);
            let router = &n.local().snapshots.as_ref().unwrap().router;
            let usage = router.recovery_usage();
            assert!(usage.requests <= 1);
            assert!(usage.image_bytes <= router.recovery_limits().image_bytes);
            self.peak_recovery = self.peak_recovery.max(usage.requests);
            if let Some(event) = &n.wal_maintenance().last_completion {
                if self.tickets[i] != Some(event.request) {
                    self.tickets[i] = Some(event.request);
                    let report = event.result.as_ref().unwrap();
                    self.reclaimed += report.before_bytes - report.after_bytes;
                    self.completions[i] += 1;
                }
            }
        }
    }
}

fn prepare(h: &Harness) -> (Vec<Replica<SharedLog>>, BTreeMap<GroupIdentity, u64>) {
    let mut nodes = h.all(NativeOpenMode::Create);
    campaign(&mut nodes, &h.clock, GROUPS).unwrap();
    eprintln!("protocol={:?} phase=initial-write", h.protocol);
    let first = write_round(h, &mut nodes, 1);
    let first_bounds = boundaries(first.samples.iter().map(|s| (s.group, s.index)));
    verify(&mut nodes, &h.clock, GROUPS, GROUPS, &first_bounds).unwrap();
    let old = abort_follower(nodes.pop().unwrap());
    eprintln!("protocol={:?} phase=survivor-write", h.protocol);
    let second = write_round(h, &mut nodes, GROUPS + 1);
    let last = boundaries(second.samples.iter().map(|s| (s.group, s.index)));
    verify(&mut nodes, &h.clock, 2 * GROUPS, GROUPS, &last).unwrap();
    enable(&mut nodes);
    eprintln!("protocol={:?} phase=automatic-checkpoint", h.protocol);
    let mut observations = Observations::default();
    until(&mut nodes, &h.clock, |ns| {
        observations.record(ns);
        Ok(observations.reclaimed > 0
            && ns.iter().all(|n| {
                old.iter()
                    .all(|(g, last)| n.local().owner.core(*g).unwrap().state().base_index() > *last)
            }))
    })
    .unwrap_or_else(|error| {
        diagnose(&nodes);
        panic!("automatic checkpoint failed: {error}");
    });
    let forced = old
        .keys()
        .map(|&g| {
            let base = nodes
                .iter()
                .map(|n| n.local().owner.core(g).unwrap().state().base_index())
                .min()
                .unwrap();
            (g, base)
        })
        .collect();
    // Join workers and close every transport: buffered Append cannot do repair.
    close(nodes, &h.clock).unwrap();
    eprintln!("protocol={:?} phase=reopen", h.protocol);
    let mut nodes = h.all(NativeOpenMode::Recover);
    for (&g, &last) in &old {
        assert_eq!(
            nodes[2].local().owner.core(g).unwrap().state().last_index(),
            last
        );
    }
    enable(&mut nodes);
    eprintln!("protocol={:?} phase=arm-recovery", h.protocol);
    (nodes, forced)
}

fn arm_recovery(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    totals: &mut PollTotals,
    observations: &mut Observations,
) {
    for g in 1..=GROUPS {
        nodes[0]
            .control(group_id(g), NodeControl::Campaign)
            .unwrap();
    }
    drive(nodes, &h.clock, totals, |ns| {
        observations.record(ns);
        Ok(ns[2]
            .local()
            .snapshots
            .as_ref()
            .unwrap()
            .router
            .recovery_usage()
            .requests
            == 1
            && leader(ns, group_id(GROUPS)).is_ok_and(|n| n < 2))
    })
    .unwrap_or_else(|error| {
        diagnose(nodes);
        panic!("arming recovery failed: {error}");
    });
}

fn foreground(
    nodes: &mut [Replica<SharedLog>],
    pending: &mut Option<(usize, ClientTicket)>,
    applied: &mut Option<(usize, u64)>,
) -> Result<(), Failure> {
    if pending.is_none() && applied.is_none() {
        let Ok(n) = leader(nodes, group_id(GROUPS)) else {
            return Ok(());
        };
        assert!(n < 2);
        let ticket = nodes[n]
            .propose(ClientRequest {
                group: group_id(GROUPS),
                operation: OperationId::new(900_001).unwrap(),
                bytes: 0i64.to_le_bytes().to_vec(),
            })
            .map_err(|e| format!("foreground refused: {:?}", e.reason))?;
        *pending = Some((n, ticket));
    }
    if let Some(&(n, ticket)) = pending.as_ref() {
        if let Some(output) = nodes[n].poll_client() {
            assert_eq!(output.ticket(), ticket);
            let result = checked(nodes[n].complete_client(output).map_err(|e| e.reason))?;
            let ClientOutcome::Applied { position, receipt } = result else {
                return Err(format!("foreground failed: {result:?}").into());
            };
            assert_eq!(receipt.outcome, CounterOutcome::Value(2));
            assert!(!receipt.duplicate);
            *applied = Some((n, position.index));
            *pending = None;
        }
    }
    Ok(())
}

fn maintenance_while_held(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    totals: &mut PollTotals,
    observations: &mut Observations,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let (mut pending, mut applied, mut checkpoint_reclaims) = (None, None, None);
    loop {
        // The stale router owns an actual accepted request throughout this phase.
        // Its worker may finish, but its receipt cannot be consumed until resume.
        poll_selected(nodes, &h.clock, totals, Some(2)).unwrap();
        observations.record(nodes);
        assert_eq!(
            nodes[2]
                .local()
                .snapshots
                .as_ref()
                .unwrap()
                .router
                .recovery_usage()
                .requests,
            1
        );
        foreground(nodes, &mut pending, &mut applied).unwrap();
        if let Some((n, index)) = applied {
            if nodes[n]
                .local()
                .owner
                .core(group_id(GROUPS))
                .unwrap()
                .state()
                .base_index()
                >= index
            {
                let count = checkpoint_reclaims.get_or_insert(observations.completions[n]);
                let refusal_observed = match h.protocol {
                    NativePeerProtocol::TcpTls => true,
                    #[cfg(feature = "quic")]
                    NativePeerProtocol::Quic => {
                        totals.snapshot_send_refusals.iter().sum::<usize>() > 0
                    }
                };
                if observations.completions[n] > *count && refusal_observed {
                    println!("held_recovery=1 foreground_index={index} checkpointed=true post_checkpoint_reclaim=true");
                    return;
                }
            }
        }
        if Instant::now() >= deadline {
            diagnose(nodes);
            panic!("held recovery blocked foreground maintenance: pending={pending:?} applied={applied:?} reclaim_count_at_checkpoint={checkpoint_reclaims:?}");
        }
        std::thread::park_timeout(Duration::from_micros(100));
    }
}

fn diagnose(nodes: &[Replica<SharedLog>]) {
    for (i, n) in nodes.iter().enumerate() {
        eprintln!("replica={i} owner={:?} driver={:?} outbound={:?} peer1={:?} peer2={:?} peer3={:?} connections={:?}",
            n.local().owner.usage(), n.replica_usage(), n.local().outbound.usage(),
            n.local().outbound.peer_usage(node(1)), n.local().outbound.peer_usage(node(2)),
            n.local().outbound.peer_usage(node(3)), n.peers().unwrap().usage());
        eprintln!(
            "replica={i} checkpoints={:?} wal={:?} recovery={:?}",
            n.checkpoints(),
            n.wal_maintenance(),
            n.local()
                .snapshots
                .as_ref()
                .unwrap()
                .router
                .recovery_usage()
        );
        for g in 1..=GROUPS {
            let core = n.local().owner.core(group_id(g)).unwrap();
            eprintln!("replica={i} group={g} role={:?} term={} base={} commit={} applied={} dependency={}",
                core.role(), core.state().hard_state.term, core.state().base_index(), core.state().commit_index,
                n.local().applications[&group_id(g)].applied_index(), core.has_pending_dependency());
        }
    }
}

fn resume_recovery(
    h: &Harness,
    nodes: &mut [Replica<SharedLog>],
    forced: &BTreeMap<GroupIdentity, u64>,
    totals: &mut PollTotals,
    observations: &mut Observations,
) {
    drive(nodes, &h.clock, totals, |ns| {
        observations.record(ns);
        let local = ns[2].local();
        Ok(forced.iter().all(|(g, bound)| {
            local.owner.core(*g).unwrap().state().base_index() >= *bound
                && local.applications[g].read_applied(*bound) == Ok(2)
        }))
    })
    .unwrap();
    assert!(totals.snapshot_installs[2] >= GROUPS);
    assert_eq!(observations.peak_recovery, 1);
    assert!(observations.reclaimed > 0);
    println!(
        "protocol={:?} groups={GROUPS} snapshot_installs={} peak_recovery={} reclaimed_bytes={} snapshot_send_refusals={}",
        h.protocol, totals.snapshot_installs[2], observations.peak_recovery, observations.reclaimed,
        totals.snapshot_send_refusals.iter().sum::<usize>()
    );
}

fn retry_foreground(h: &Harness, nodes: &mut [Replica<SharedLog>]) {
    let n = leader(nodes, group_id(GROUPS)).unwrap();
    let ticket = nodes[n]
        .propose(ClientRequest {
            group: group_id(GROUPS),
            operation: OperationId::new(900_001).unwrap(),
            bytes: 0i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    until(nodes, &h.clock, |ns| {
        let Some(output) = ns[n].poll_client() else {
            return Ok(false);
        };
        assert_eq!(output.ticket(), ticket);
        let result = checked(ns[n].complete_client(output).map_err(|e| e.reason))?;
        assert!(matches!(result, ClientOutcome::Applied { receipt, .. }
            if receipt.duplicate && receipt.outcome == CounterOutcome::Value(2)));
        Ok(true)
    })
    .unwrap();
}

fn history(protocol: NativePeerProtocol) {
    let mut h = Harness::new(protocol);
    eprintln!("maintenance recovery files: {}", h.root.display());
    h.recovery = Some(SnapshotRecoveryLimits {
        requests: 1,
        image_bytes: 128 * 1024 * 1024,
    });
    let (mut nodes, forced) = prepare(&h);
    let mut totals = PollTotals::default();
    let mut observations = Observations::default();
    arm_recovery(&h, &mut nodes, &mut totals, &mut observations);
    maintenance_while_held(&h, &mut nodes, &mut totals, &mut observations);
    resume_recovery(&h, &mut nodes, &forced, &mut totals, &mut observations);
    close(nodes, &h.clock).unwrap();
    let mut nodes = h.all(NativeOpenMode::Recover);
    for (&g, &base) in &forced {
        assert!(nodes[2].local().owner.core(g).unwrap().state().base_index() >= base);
    }
    campaign(&mut nodes, &h.clock, GROUPS).unwrap();
    verify(&mut nodes, &h.clock, 2 * GROUPS, GROUPS, &forced).unwrap();
    for operation in 1..=2 * GROUPS {
        let expected = (operation - 1) / GROUPS + 1;
        assert_eq!(
            retry(&mut nodes, &h.clock, operation, expected as i64, GROUPS).unwrap(),
            0
        );
    }
    retry_foreground(&h, &mut nodes);
    close(nodes, &h.clock).unwrap();
    std::fs::remove_dir_all(&h.root).unwrap();
}

#[test]
fn tcp_automatic_maintenance_progresses_with_held_snapshot_recovery() {
    history(NativePeerProtocol::TcpTls);
}

#[cfg(feature = "quic")]
#[test]
fn quic_automatic_maintenance_progresses_with_held_snapshot_recovery() {
    history(NativePeerProtocol::Quic);
}
