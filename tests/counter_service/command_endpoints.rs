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
use super::*;

fn routes(cluster: &Cluster, offset: u16) -> String {
    let mut text = String::from("voteboat-command-peers-v1\n");
    for id in [3, 1, 2] {
        text.push_str(&format!(
            "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
            cluster.base + offset + id
        ));
    }
    text
}

fn history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    cluster.remote_commands = true;
    let access = cluster.root.join("access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    let peers = cluster.root.join("commands.txt");
    fs::write(&peers, routes(&cluster, 110)).unwrap();
    cluster.command_peers = Some(peers);
    cluster.command_access = Some(access);
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    for id in 1..=3 {
        assert!(cluster.service_log(id).contains(&format!(
            "command=0.0.0.0:{}",
            cluster.base + 110 + id as u16
        )));
    }
    refuse_wrong_server_name(&cluster);
    cluster.command_principal = Some(1);
    assert_eq!(cluster.routed(&["read"]), "OK value=0\n");
    assert_eq!(
        String::from_utf8(cluster.request(leader, &["add", "18701", "7"]).stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    cluster.command_principal = Some(2);
    assert!(authenticated_write(&cluster, &["add", "18701", "7"]).contains("Value(7)"));
    cluster.command_principal = Some(3);
    cluster.ok(leader, &["checkpoint"]);
    let mut lost = cluster.children[leader - 1].take().unwrap();
    lost.kill().unwrap();
    lost.wait().unwrap();
    assert_ne!(cluster.leader(), leader);
    assert!(authenticated_write(&cluster, &["add", "18701", "7"]).contains("duplicate=true"));
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    cluster.stop();
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    cluster.leader();
    assert!(authenticated_write(&cluster, &["add", "18701", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&cluster, &["add", "18702", "3"]).contains("Value(10)"));
    assert_eq!(cluster.routed(&["read"]), "OK value=10\n");
    cluster.stop();
    for id in 1..=3 {
        assert!(cluster.service_log(id).contains("workers_joined=true"));
    }
    fs::remove_dir_all(&cluster.root).unwrap();
}

fn refuse_wrong_server_name(cluster: &Cluster) {
    let path = cluster.command_peers.as_ref().unwrap();
    let valid = routes(cluster, 110);
    fs::write(
        path,
        valid.replace("node1.voteboat.test", "wrong.voteboat.test"),
    )
    .unwrap();
    let output = cluster.target("auto", &["read"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("authentication failed"));
    fs::write(path, valid).unwrap();
    assert_eq!(cluster.routed(&["read"]), "OK value=0\n");
}

#[test]
fn configured_remote_command_endpoints_retry_and_recover_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn configured_remote_command_endpoints_retry_and_recover_quic() {
    history(true);
}

#[test]
fn command_listener_requires_authentication_before_opening_files_or_sockets() {
    let cluster = Cluster::new();
    let root = cluster.root.join("never-created");
    let output = run(Command::new(BIN)
        .args(["serve", "create"])
        .arg(&root)
        .args([
            "1",
            &cluster.base.to_string(),
            "/missing-tls",
            "--command-listen",
            "0.0.0.0:5000",
        ]));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("requires --service-access"));
    assert!(!root.exists());
    for address in ["127.0.0.1:0", "224.0.0.1:5000"] {
        let output = run(Command::new(BIN)
            .args(["serve", "create"])
            .arg(&root)
            .args([
                "1",
                &cluster.base.to_string(),
                "/missing-tls",
                "--service-access",
                "/missing-access",
                "--command-listen",
                address,
            ]));
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("invalid command listener address")
        );
        assert!(!root.exists());
    }
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn command_route_pins_have_an_aggregate_budget_before_connecting() {
    let mut cluster = Cluster::new();
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let tls = cluster.root.join("client-tls");
    fs::create_dir(&tls).unwrap();
    for name in ["ca.der", "node3.der", "node3-key.der"] {
        fs::copy(fixtures.join(name), tls.join(name)).unwrap();
    }
    let mut entries = String::from("voteboat-command-peers-v1\n");
    for id in 1..=64 {
        if id != 3 {
            fs::write(tls.join(format!("node{id}.der")), vec![0; 65536]).unwrap();
        }
        entries.push_str(&format!(
            "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
            30000 + id
        ));
    }
    let path = cluster.root.join("commands.txt");
    fs::write(&path, entries).unwrap();
    cluster.command_peers = Some(path);
    cluster.command_principal = Some(3);
    cluster.tls = Some(tls);
    let output = cluster.target("auto", &["read"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("service client pin budget"));
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn malformed_command_routes_and_missing_targets_fail_before_connecting() {
    let mut cluster = Cluster::new();
    cluster.command_principal = Some(3);
    let path = cluster.root.join("commands.txt");
    cluster.command_peers = Some(path.clone());
    let address = format!("127.0.0.1:{}", cluster.base + 101);
    let valid = format!("1 {address} node1.voteboat.test\n");
    let bodies = [
        String::new(),
        "1 0.0.0.0:1234 node1.voteboat.test\n".into(),
        "1 224.0.0.1:1234 node1.voteboat.test\n".into(),
        "1 [::]:1234 node1.voteboat.test\n".into(),
        "1 127.0.0.1:0 node1.voteboat.test\n".into(),
        "1 127.0.0.1:1234 !\n".into(),
        "0 127.0.0.1:1234 node1.voteboat.test\n".into(),
        "4097 127.0.0.1:1234 node1.voteboat.test\n".into(),
        format!("{valid}{valid}"),
        format!("{valid}2 {address} node2.voteboat.test\n"),
        "1 127.0.0.1:1234 node1.voteboat.test extra\n".into(),
        "2 127.0.0.1:1234 node2.voteboat.test\n".into(), // Explicit node1 is absent.
    ];
    let listener = cluster.take_listener(101);
    listener.set_nonblocking(true).unwrap();
    for body in bodies {
        fs::write(&path, format!("voteboat-command-peers-v1\n{body}")).unwrap();
        assert!(!cluster.request(1, &["read"]).status.success(), "{body}");
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    for text in [
        "bad-header\n".to_owned(),
        "x".repeat(16 * 1024 + 1),
        format!(
            "voteboat-command-peers-v1\n{}",
            (1..=65)
                .map(|id| format!("{id} 127.0.0.1:{} node{id}.voteboat.test\n", 30000 + id))
                .collect::<String>()
        ),
    ] {
        fs::write(&path, text).unwrap();
        assert!(!cluster.request(1, &["read"]).status.success());
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
    cluster.command_principal = None;
    fs::write(&path, format!("voteboat-command-peers-v1\n{valid}")).unwrap();
    let output = cluster.request(1, &["read"]);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires --service-tls and --principal")
    );
    fs::remove_dir_all(&cluster.root).unwrap();
}

#[test]
fn command_route_tls_interruption_never_reroutes_an_unknown_write() {
    let mut cluster = Cluster::new();
    cluster.command_principal = Some(3);
    let path = cluster.root.join("commands.txt");
    fs::write(&path, routes(&cluster, 100)).unwrap();
    cluster.command_peers = Some(path);
    let listener = cluster.take_listener(101);
    let first = std::thread::spawn(move || {
        use std::io::Read;
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(error) => panic!("{error}"),
            }
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        // Read exactly the selector: ClientHello may already follow in TCP.
        let mut selector = [0; 2];
        stream.read_exact(&mut selector).unwrap();
        selector
    });
    let second = cluster.take_listener(102);
    second.set_nonblocking(true).unwrap();
    let output = cluster.target("auto", &["add", "18701", "7"]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("UNKNOWN "));
    assert_eq!(first.join().unwrap(), *b"3\n");
    assert_eq!(
        second.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    fs::remove_dir_all(&cluster.root).unwrap();
}
