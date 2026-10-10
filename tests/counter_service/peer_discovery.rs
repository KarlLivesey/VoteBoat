// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn setup(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    let access = c.root.join("access");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    let source = c.root.join("source");
    let rows = (1..=3)
        .map(|id| {
            format!(
                "{id} {id} 1 127.0.0.1:{} node{id}.voteboat.test\n",
                c.base + id
            )
        })
        .collect::<String>();
    fs::write(&source, format!("voteboat-peer-discovery-v1 10\n{rows}")).unwrap();
    c.discovery_peers = Some(source);
    let consumer = c.root.join("consumer");
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    fs::write(&consumer, format!("voteboat-peer-discovery-client-v1\nsource 3 127.0.0.1:{} node3.voteboat.test\nprincipal 3\ntls {}\n", c.base + 103, tls.display())).unwrap();
    c.peer_discovery = Some(consumer);
    c
}
fn start(c: &mut Cluster, id: usize, mode: &str) {
    let path = c.root.join(format!("peers-{id}"));
    // Only outgoing Dial routes are stale. Fixed incoming QUIC routes and the
    // local listener remain provisioned; discovery cannot authorize new peers.
    let rows = (1..=3)
        .map(|peer| {
            let port = c.base
                + if peer > id {
                    200 + peer as u16
                } else {
                    peer as u16
                };
            format!("{peer} 127.0.0.1:{port} node{peer}.voteboat.test\n")
        })
        .collect::<String>();
    fs::write(&path, rows).unwrap();
    c.endpoints = Some(path);
    c.start(id, mode);
}
fn history(quic: bool) {
    let mut c = setup(quic);
    for id in [3, 2, 1] {
        start(&mut c, id, "create");
    }
    c.leader();
    assert!(authenticated_write(&c, &["add", "20921", "7"]).contains("Value(7)"));
    // Discovery releases the command socket after each lookup, so ordinary
    // administration on the source remains available while replicas connect.
    assert!(c.ok(3, &["discovery-status"]).contains("generation=10"));
    let mut child = c.children[2].take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    c.leader();
    assert!(authenticated_write(&c, &["add", "20922", "2"]).contains("Value(9)"));
    start(&mut c, 3, "recover");
    c.leader();
    if quic {
        checkpoint_stop(&mut c);
    } else {
        c.stop();
    }
    for id in [3, 2, 1] {
        start(&mut c, id, "recover");
    }
    c.leader();
    assert!(authenticated_write(&c, &["add", "20921", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "20922", "2"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=9\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn executable_consumes_peer_discovery_and_recovers_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn executable_consumes_peer_discovery_and_recovers_quic() {
    history(true);
}

#[test]
fn unavailable_peer_source_does_not_block_commands_or_shutdown() {
    let mut c = setup(false);
    // No source process is started: node1 has discovery-bound outgoing peers.
    start(&mut c, 1, "create");
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let reply = c.request(1, &["status"]);
        if reply.status.success() {
            break;
        }
        assert!(Instant::now() < until, "{}", c.service_log(1));
        std::thread::park_timeout(Duration::from_millis(10));
    }
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}

fn rotated_data(c: &Cluster, duplicate: bool) {
    let groups = if c.groups.is_some() {
        vec![("1", "1"), ("7", "3"), ("8", "2")]
    } else {
        vec![("1", "1")]
    };
    for (group, inc) in groups {
        let reply = authenticated_write(c, &["group", group, inc, "add", "20923", "4"]);
        assert!(reply.contains("Value(4)"), "{reply}");
        if duplicate {
            assert!(reply.contains("duplicate=true"), "{reply}");
        }
        assert_eq!(c.routed(&["group", group, inc, "read"]), "OK value=4\n");
    }
}
fn profile_history(quic: bool, multi: bool) {
    let mut c = if multi {
        groups::setup(quic)
    } else {
        setup(quic)
    };
    let mut source = setup(false);
    // A separate authenticated service supplies exact data-peer hints, also
    // when the consumers own several groups in a shared WAL.
    fs::write(
        source.discovery_peers.as_ref().unwrap(),
        format!(
            "voteboat-peer-discovery-v1 10\n{}",
            (1..=3)
                .map(|id| format!(
                    "{id} {id} 1 127.0.0.1:{} node{id}.voteboat.test\n",
                    c.base + id
                ))
                .collect::<String>()
        ),
    )
    .unwrap();
    source.peer_discovery = None;
    source.start(3, "create");
    c.discovery_peers = None;
    let consumer = c.root.join("consumer");
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    fs::write(&consumer, format!("voteboat-peer-discovery-client-v1\nsource 3 127.0.0.1:{} node3.voteboat.test\nprincipal 3\ntls {}\n", source.base + 103, fixtures.join("tls").display())).unwrap();
    c.peer_discovery = Some(consumer);
    let keys = c.root.join("keys");
    fs::write(
        &keys,
        format!(
            "voteboat-peer-credentials-v1 1\ntls {}\n",
            fixtures.join("tls").display()
        ),
    )
    .unwrap();
    c.peer_credentials = Some(keys.clone());
    for id in [3, 2, 1] {
        start(&mut c, id, "create");
    }
    rotated_data(&c, false);
    c.stop();
    let mode = if multi { "recover" } else { "recover-member" };
    for id in [3, 2, 1] {
        start(&mut c, id, mode);
    }
    rotated_data(&c, true);
    fs::write(
        &keys,
        format!(
            "voteboat-peer-credentials-v1 2\ntls {}\n",
            fixtures.join("four-node-tls").display()
        ),
    )
    .unwrap();
    for id in 1..=3 {
        assert!(c
            .ok(id, &["reload-peers", "1", "1", "2"])
            .contains("queued=true"));
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if c.ok(id, &["peer-credential-status", "1"])
                .contains("state=recorded")
            {
                break;
            }
            assert!(Instant::now() < until, "{}", c.service_log(id));
            std::thread::park_timeout(Duration::from_millis(5));
        }
    }
    rotated_data(&c, true);
    if quic {
        checkpoint_stop(&mut c);
    } else {
        c.stop();
    }
    for id in [3, 2, 1] {
        start(&mut c, id, mode);
    }
    rotated_data(&c, true);
    c.stop();
    source.stop();
    fs::remove_dir_all(&c.root).unwrap();
    fs::remove_dir_all(&source.root).unwrap();
}

pub(super) fn checkpoint_stop(c: &mut Cluster) {
    use voteboat::{identity::*, log::*, native::log_store::*};
    let selected = if c.groups.is_some() {
        vec![(1, 1), (7, 3), (8, 2)]
    } else {
        vec![(1, 1)]
    };
    let mut bounds = Vec::new();
    for (id, inc) in selected {
        let group = id.to_string();
        let incarnation = inc.to_string();
        let leader = groups::leader(c, &group, &incarnation);
        let committed = |node| {
            c.ok(node, &["group", &group, &incarnation, "status"])
                .split("committed=")
                .nth(1)
                .unwrap()
                .trim()
                .parse::<u64>()
                .unwrap()
        };
        let through = committed(leader);
        assert!(through > 0);
        let until = Instant::now() + Duration::from_secs(10);
        while !(1..=3).all(|node| committed(node) >= through) {
            assert!(Instant::now() < until, "group {id} did not replicate");
            std::thread::park_timeout(Duration::from_millis(5));
        }
        for node in 1..=3 {
            c.ok(node, &["group", &group, &incarnation, "checkpoint"]);
        }
        bounds.push((
            GroupIdentity {
                id: GroupId::new(id).unwrap(),
                incarnation: GroupIncarnation::new(inc).unwrap(),
            },
            through,
        ));
    }
    c.stop();
    let _gate = fixture_gate();
    for node in 1..=3 {
        let store = StoreIdentity {
            id: StoreId::new(node as u128).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        };
        let log = NativeLogStore::recover(
            FileLogIo::open(c.root.join(node.to_string())).unwrap(),
            store,
            LogLimits::default(),
        )
        .unwrap();
        for (group, through) in &bounds {
            assert!(log.state(*group).unwrap().snapshot.unwrap().index >= *through);
        }
    }
}
#[test]
fn discovered_member_recovery_and_peer_rotation_tcp() {
    profile_history(false, false);
}
#[cfg(feature = "quic")]
#[test]
fn discovered_member_recovery_and_peer_rotation_quic() {
    profile_history(true, false);
}
#[test]
fn discovered_shared_groups_and_peer_rotation_tcp() {
    profile_history(false, true);
}
#[cfg(feature = "quic")]
#[test]
fn discovered_shared_groups_and_peer_rotation_quic() {
    profile_history(true, true);
}

#[test]
fn malformed_peer_discovery_client_refuses_before_store_creation() {
    let mut c = setup(false);
    let path = c.peer_discovery.clone().unwrap();
    let original = fs::read_to_string(&path).unwrap();
    for invalid in [
        original.replace("client-v1", "client-v9"),
        original.replace("principal 3", "principal 0"),
        original.replace("source 3", "source 0"),
        format!("{original}unexpected\n"),
        "x".repeat(4097),
    ] {
        fs::write(&path, invalid).unwrap();
        c.start(1, "create");
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = c.children[0].as_mut().unwrap().try_wait().unwrap() {
                assert!(!status.success(), "{}", c.service_log(1));
                c.children[0] = None;
                break;
            }
            assert!(Instant::now() < until, "{}", c.service_log(1));
            std::thread::park_timeout(Duration::from_millis(5));
        }
        assert!(!c.root.join("1").exists());
    }
    fs::remove_dir_all(&c.root).unwrap();
}
