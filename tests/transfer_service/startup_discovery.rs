// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) fn leader(rig: &Cluster, group: u128, nodes: &[u16]) -> u16 {
    wait(
        Instant::now() + Duration::from_secs(15),
        || probe(rig, group, nodes),
        Instant::now,
        || std::thread::sleep(Duration::from_millis(10)),
    )
    .unwrap_or_else(|error| panic!("group {group} leader discovery: {error}"))
}
pub(super) fn probe(rig: &Cluster, group: u128, nodes: &[u16]) -> Result<Option<u16>, String> {
    for node in nodes {
        let out = rig.request(*node, rig.admin, group, &["status"]);
        if !out.status.success() {
            return Err(format!(
                "node {node}: {} {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        if String::from_utf8_lossy(&out.stdout)
            .split_whitespace()
            .any(|w| w == "role=Leader")
        {
            return Ok(Some(*node));
        }
    }
    Ok(None)
}
pub(super) fn wait(
    deadline: Instant,
    mut probe: impl FnMut() -> Result<Option<u16>, String>,
    mut now: impl FnMut() -> Instant,
    mut pause: impl FnMut(),
) -> Result<u16, String> {
    loop {
        if now() >= deadline {
            return Err("leader discovery deadline".into());
        }
        if let Some(node) = probe()? {
            return Ok(node);
        }
        pause();
    }
}

#[test]
fn delayed_election_is_observed_after_the_initial_no_leader_scan() {
    let start = Instant::now();
    let mut calls = 0;
    let result = wait(
        start + Duration::from_secs(15),
        || {
            calls += 1;
            Ok(if calls == 1 { None } else { Some(2) })
        },
        || start,
        || (),
    );
    assert_eq!(result.unwrap(), 2);
    assert_eq!(calls, 2);
}

#[test]
fn discovery_deadline_is_not_renewed_and_other_probe_failures_are_terminal() {
    let start = Instant::now();
    let now = std::cell::Cell::new(start);
    let mut calls = 0;
    let result = wait(
        start + Duration::from_secs(15),
        || {
            calls += 1;
            Ok(None)
        },
        || now.get(),
        || now.set(start + Duration::from_secs(15)),
    );
    assert!(result.is_err());
    assert_eq!(calls, 1);
    let mut calls = 0;
    assert!(wait(
        start + Duration::from_secs(15),
        || {
            calls += 1;
            Err("original status failure".into())
        },
        || start,
        || panic!("failed probe must not be repeated")
    )
    .is_err());
    assert_eq!(calls, 1);
}
