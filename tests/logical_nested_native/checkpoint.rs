// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{aliases::Middle, host::Host, lower::Lower, setup};
use std::time::{Duration, Instant};
use voteboat::{log::GroupLog, snapshot_worker::SnapshotWorker};
pub fn upper(host: &mut Host<'_, '_>) {
    for n in &mut host.nodes {
        n.checkpoint().unwrap();
    }
    host.until(|h| {
        h.drained()
            && h.nodes.iter().all(|n| {
                n.local()
                    .owner
                    .core(setup::top().group)
                    .unwrap()
                    .state()
                    .base_index()
                    > 0
            })
    });
    for n in &host.nodes {
        assert!(n.local().snapshots.as_ref().unwrap().worker.is_drained());
    }
}
pub fn middle(lower: &mut Lower<'_>, middle: &mut [Vec<Middle>]) {
    for n in middle.iter_mut().flatten() {
        n.checkpoint().unwrap();
    }
    let end = Instant::now() + Duration::from_secs(15);
    while !middle.iter().enumerate().all(|(i, ns)| {
        ns.iter().all(|n| {
            n.work_is_drained()
                && n.local()
                    .owner
                    .core(setup::middle(i).group)
                    .unwrap()
                    .state()
                    .base_index()
                    > 0
        })
    }) || !lower.drained()
    {
        lower.poll_middle(middle);
        assert!(
            Instant::now() < end,
            "actual nested middle Published/anchor/Reconcile timeout"
        );
    }
}
pub fn states(host: &Host<'_, '_>) -> Vec<GroupLog> {
    host.nodes
        .iter()
        .map(|n| {
            n.local()
                .owner
                .core(setup::top().group)
                .unwrap()
                .state()
                .clone()
        })
        .collect()
}
pub fn compare(host: &Host<'_, '_>, expected: &[GroupLog], value: i64) {
    use voteboat::application::ReadableStateMachine;
    for (n, state) in host.nodes.iter().zip(expected) {
        assert_eq!(
            n.local().owner.core(setup::top().group).unwrap().state(),
            state
        );
        assert_eq!(
            n.local().applications[&setup::top().group]
                .read_at(state.commit_index, ())
                .unwrap(),
            value
        );
    }
}
