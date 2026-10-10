// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    discovery::*,
    identity::*,
    native::{remote_discovery::*, tls::NativeTlsSession},
    runtime::MonoTime,
    secure::*,
};

fn peer(node: u64, store: u128, incarnation: u64) -> PeerIdentity {
    PeerIdentity {
        node: NodeId::new(node).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(store).unwrap(),
            incarnation: StoreIncarnation::new(incarnation).unwrap(),
        },
    }
}
fn source_identity() -> PeerIdentity {
    let number = (1u64 << 62) | 1;
    peer(number, (1u128 << 127) | u128::from(number), 1)
}
fn configuration(c: &Cluster, generation: u64, updated: bool) -> String {
    format!(
        "voteboat-peer-discovery-v1 {generation}\n1 401 7 127.0.0.1:{} node1.voteboat.test\n2 402 9 127.0.0.1:{} node2.voteboat.test\n",
        c.base + 1,
        c.base + if updated { 202 } else { 2 },
    )
}
type Consumer = NativeRemotePeerDiscovery<NativeTlsSession<std::net::TcpStream>>;
fn connect(c: &Cluster) -> (Consumer, Instant) {
    let UnobservedCommand {
        mut session,
        socket,
        start,
    } = UnobservedCommand::send(c, 1, "discover");
    drop(socket);
    let mut ack = [0; 16];
    let mut read = 0;
    let expected = b"OK discovery-v1\n";
    while read < expected.len() {
        let now = MonoTime(start.elapsed().as_millis() as u64);
        session.poll(now, SessionPollBudget::default()).unwrap();
        match session.read_plaintext(&mut ack[read..expected.len()]) {
            Ok(0) => panic!("discovery source closed before upgrade"),
            Ok(n) => read += n,
            Err(SessionError::WouldBlock) => (),
            other => panic!("source upgrade: {other:?}"),
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(&ack[..read], expected);
    let now = MonoTime(start.elapsed().as_millis() as u64);
    let remote = NativeRemotePeerDiscovery::new(
        session,
        source_identity(),
        RemoteDiscoveryConfig::default(),
        now,
    )
    .ok()
    .unwrap();
    (remote, start)
}
fn resolve(
    remote: &mut Consumer,
    start: Instant,
    peer: PeerIdentity,
) -> Result<PeerEndpointHint, DiscoveryError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let now = MonoTime(start.elapsed().as_millis() as u64);
        match remote.resolve(peer, now) {
            Err(DiscoveryError::Unavailable) => (),
            result => return result,
        }
        if let Some(reply) = remote.poll(now, SessionPollBudget::default()).unwrap() {
            return reply.result.map_err(|error| match error {
                RemoteDiscoveryError::Discovery(error) => error,
                other => panic!("source reply: {other:?}"),
            });
        }
        assert!(Instant::now() < deadline, "peer source did not reply");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn probe(c: &Cluster, generation: u64, updated: bool) {
    let (mut remote, start) = connect(c);
    for (target, port) in [
        (peer(1, 401, 7), c.base + 1),
        (peer(2, 402, 9), c.base + if updated { 202 } else { 2 }),
    ] {
        let hint = resolve(&mut remote, start, target).unwrap();
        assert_eq!(hint.peer, target);
        assert_eq!(hint.generation.get(), generation);
        assert_eq!(hint.endpoint.port(), port);
    }
    for target in [
        peer(1, 401, 8),
        peer(1, 402, 7),
        source_identity(),
        peer(1, 1, 1),
    ] {
        assert_eq!(
            resolve(&mut remote, start, target),
            Err(DiscoveryError::Missing)
        );
    }
    remote.close();
}
fn history(quic: bool) {
    let mut c = prepared(quic);
    c.remote_commands = false;
    c.command_peers = None;
    c.discover_via = None;
    let path = c.discovery_peers.clone().unwrap();
    let original = configuration(&c, 10, false);
    fs::write(&path, &original).unwrap();
    for id in 1..=3 {
        c.start(id, "create");
    }
    wait_for_source(&mut c);
    probe(&c, 10, false);
    let endpoint = format!("127.0.0.1:{}", c.base + 202);
    c.command_principal = Some(1);
    let denied = c.request(1, &["discovery-update", "10", "11", "2", &endpoint]);
    assert_eq!(
        String::from_utf8(denied.stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    c.command_principal = Some(3);
    assert_eq!(
        c.ok(1, &["discovery-update", "10", "11", "2", &endpoint]),
        "OK generation=11 duplicate=false durable=false\n"
    );
    assert_eq!(
        c.ok(1, &["discovery-update", "10", "11", "2", &endpoint]),
        "OK generation=11 duplicate=true durable=false\n"
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
    probe(&c, 11, true);
    assert!(authenticated_write(&c, &["add", "20911", "9"]).contains("Value(9)"));
    let leader = c.leader();
    c.ok(leader, &["checkpoint"]);
    c.stop();
    fs::write(&path, configuration(&c, 11, true)).unwrap();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    wait_for_source(&mut c);
    probe(&c, 11, true);
    assert!(authenticated_write(&c, &["add", "20911", "9"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=9\n");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn executable_peer_source_uses_exact_identities_and_explicit_restart_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn executable_peer_source_uses_exact_identities_and_explicit_restart_quic() {
    history(true);
}

#[test]
fn invalid_peer_source_refuses_before_opening_store() {
    let c = prepared(false);
    fs::write(
        c.discovery_peers.as_ref().unwrap(),
        "voteboat-peer-discovery-v1 10\n1 0 7 127.0.0.1:4011 node1.voteboat.test\n",
    )
    .unwrap();
    let root = c.root.join("never-created");
    let out = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&root)
        .args([
            "1",
            &c.base.to_string(),
            "/missing-tls",
            "--service-access",
            "/missing-access",
            "--discovery-peers",
        ])
        .arg(c.discovery_peers.as_ref().unwrap()));
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid peer store"));
    assert!(!root.exists());
    fs::remove_dir_all(&c.root).unwrap();
}
