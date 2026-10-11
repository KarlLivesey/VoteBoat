// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{snapshot_worker::SnapshotWorker, worker::PersistenceWorker};

pub fn close(mut node: HostNode, clock: Instant) -> Result<(), Failure> {
    if node.state() == NodeState::RecoveryRequired {
        return recover(node, clock);
    }
    let normal = shutdown(&mut node, clock);
    if let Err(first) = normal {
        // An explicit recovery transfer preserves unknown accepted work. It
        // never declares a rollback or claims that a failed core was drained.
        node.abort();
        if let Err(error) = recover(node, clock) {
            eprintln!("recovery cleanup failed: {error}");
        }
        return Err(first);
    }
    let mut parts = node.into_parts().map_err(|_| "actual Node not drained")?;
    let mut dialer = parts
        .peers
        .take()
        .ok_or("missing peers")?
        .connector
        .into_dialer()
        .map_err(|_| "connector not drained")?
        .ok_or("missing TCP dialer")?;
    let mut snapshots = parts.local.snapshots.take().ok_or("missing snapshots")?;
    let end = Instant::now() + Duration::from_secs(10);
    let (mut wal, mut snapshot) = (false, false);
    loop {
        let dial = check(dialer.try_finish())?;
        if !wal {
            wal = parts.local.persistence.try_reclaim()?.is_some();
        }
        if !snapshot {
            snapshot = snapshots.worker.try_reclaim()?.is_some();
        }
        if dial && wal && snapshot {
            println!("workers_joined=true; authoritative files retained");
            return Ok(());
        }
        if Instant::now() > end {
            return Err("worker join timed out; files retained".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn shutdown(node: &mut HostNode, clock: Instant) -> Result<(), Failure> {
    if node.state() == NodeState::Running {
        for group in groups() {
            check(node.control(group, NodeControl::Checkpoint))?;
        }
    }
    node.begin_shutdown();
    drive(node, clock, |n| Ok(n.is_drained()))
}
fn recover(node: HostNode, clock: Instant) -> Result<(), Failure> {
    let mut recovery = node
        .into_recovery()
        .map_err(|_| "actual recovery transfer refused")?;
    let mut peers = recovery.peers.take().ok_or("missing recovery peers")?;
    let end = Instant::now() + Duration::from_secs(15);
    while !peers.is_drained() {
        // This path accesses network ownership only, never the failed core.
        check(peers.drain(
            &mut recovery.local.outbound,
            now(clock),
            PeerDriverBudget::default(),
        ))?;
        if Instant::now() > end {
            return Err("recovery network drain timed out; files retained".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut dialer = peers
        .into_parts()
        .map_err(|_| "actual recovery network not drained")?
        .connector
        .into_dialer()
        .map_err(|_| "recovery connector not drained")?
        .ok_or("missing TCP dialer")?;
    let mut snapshots = recovery
        .local
        .snapshots
        .take()
        .ok_or("missing recovery snapshots")?;
    let end = Instant::now() + Duration::from_secs(10);
    let (mut wal, mut snapshot) = (false, false);
    loop {
        let dial = check(dialer.try_finish())?;
        if !wal {
            // Accepted I/O may finish after abort; discard only observations,
            // then return the actual possibly fenced store for cold recovery.
            recovery.local.persistence.poll(64);
            recovery.local.persistence.poll_reclaims(64);
            wal = recovery.local.persistence.try_reclaim()?.is_some();
        }
        if !snapshot {
            snapshots.worker.poll(64);
            snapshot = snapshots.worker.try_reclaim()?.is_some();
        }
        if dial && wal && snapshot {
            println!("recovery_workers_joined=true; authoritative files retained");
            return Ok(());
        }
        if Instant::now() > end {
            return Err("recovery worker join timed out; files retained".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
