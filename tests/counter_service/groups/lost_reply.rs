// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn unread_write(c: &mut Cluster, group: &str, incarnation: &str, value: &str) {
    let end = Instant::now() + Duration::from_secs(20);
    let command = format!("group {group} {incarnation} add 42 {value}");
    let mut pending = Vec::new();
    for _ in 0..8 {
        let node = leader(c, group, incarnation);
        pending.push(UnobservedCommand::send(c, node, &command));
        let output = c.target("auto", &["group", group, incarnation, "read"]);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() && text == format!("OK value={value}\n") {
            for unread in pending {
                unread.disconnect();
            }
            return;
        }
        assert!(
            text == "OK value=0\n" || text.starts_with("ERR "),
            "unexpected independent observation: {text}"
        );
        assert!(
            Instant::now() < end,
            "unread write never became visible: {text}"
        );
    }
    panic!("unread original write exhausted attempts: {command}");
}

fn checkpoint(c: &mut Cluster) -> Vec<(usize, u128, u64, u64)> {
    [("1", "1"), ("7", "3"), ("8", "2")]
        .into_iter()
        .map(|(group, incarnation)| {
            let node = leader(c, group, incarnation);
            let status = c.ok(node, &["group", group, incarnation, "status"]);
            let through = status
                .split_whitespace()
                .find_map(|w| w.strip_prefix("committed="))
                .unwrap()
                .parse()
                .unwrap();
            c.ok(node, &["group", group, incarnation, "checkpoint"]);
            (
                node,
                group.parse().unwrap(),
                incarnation.parse().unwrap(),
                through,
            )
        })
        .collect()
}

fn verify_checkpoints(c: &Cluster, cuts: &[(usize, u128, u64, u64)]) {
    use voteboat::{identity::*, log::*, native::log_store::*};
    let _guard = fixture_gate();
    for &(node, group, incarnation, through) in cuts {
        let log = NativeLogStore::recover(
            FileLogIo::open(c.root.join(node.to_string())).unwrap(),
            StoreIdentity {
                id: StoreId::new(node as u128).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            LogLimits::default(),
        )
        .unwrap();
        let state = log
            .state(GroupIdentity {
                id: GroupId::new(group).unwrap(),
                incarnation: GroupIncarnation::new(incarnation).unwrap(),
            })
            .unwrap();
        assert!(through > 0);
        assert!(
            state.base_index() >= through,
            "group={group} base={} through={through}",
            state.base_index()
        );
    }
}

fn history(quic: bool) {
    let mut c = setup(quic);
    for node in 1..=3 {
        c.start(node, "create");
    }
    for (group, incarnation, value) in [("1", "1", "3"), ("7", "3", "5"), ("8", "2", "9")] {
        unread_write(&mut c, group, incarnation, value);
    }
    // First observed successes must allow retained receipts after unread writes.
    data(&mut c, false);
    for (group, incarnation, value) in [("1", "1", "3"), ("7", "3", "5"), ("8", "2", "9")] {
        let conflict = authenticated_write(&c, &["group", group, incarnation, "add", "42", "1"]);
        assert_eq!(conflict, "OK outcome=OperationConflict duplicate=true\n");
        assert_eq!(
            c.routed(&["group", group, incarnation, "read"]),
            format!("OK value={value}\n")
        );
    }
    let cuts = quic.then(|| checkpoint(&mut c));
    c.stop();
    if let Some(cuts) = cuts {
        verify_checkpoints(&c, &cuts);
    }
    for node in 1..=3 {
        c.start(node, "recover");
    }
    data(&mut c, true);
    c.stop();
}

#[test]
fn first_observation_after_unread_group_writes_recovers_original_receipts_tcp() {
    history(false);
}

#[test]
#[cfg(feature = "quic")]
fn first_observation_after_unread_group_writes_recovers_original_receipts_quic() {
    history(true);
}
