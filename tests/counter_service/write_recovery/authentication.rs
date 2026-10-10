// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
const ORIGINAL: &[&str] = &["group", "7", "3", "add", "42", "5"];
const TIMEOUT: &str =
    "UNKNOWN authentication deadline expired; retry the same operation ID and delta\n";
const RECEIPT: &str = "OK outcome=Value(5) duplicate=true\n";

fn rows(base: u16, fault: bool) -> String {
    let mut text = String::from("voteboat-command-peers-v1\n");
    for id in 1..=3 {
        let offset = if id == 1 && !fault { 110 } else { 100 };
        text.push_str(&format!(
            "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
            base + offset + id
        ));
    }
    text
}
fn start(c: &mut Cluster, mode: &str) {
    for id in 1..=3 {
        // All data voters remain live; only node1's command endpoint differs.
        c.remote_commands = id == 1;
        c.start(id, mode);
    }
    c.remote_commands = false;
}
fn recover(c: &Cluster, listener: TcpListener) -> String {
    let mut fault = Some(read_failover::peer(listener, None));
    let mut calls = Vec::new();
    let result = retry(
        ORIGINAL,
        Instant::now() + Duration::from_secs(15),
        |words| {
            calls.push(
                words
                    .iter()
                    .map(|word| (*word).to_owned())
                    .collect::<Vec<_>>(),
            );
            let output = c.target("auto", words);
            let text = String::from_utf8(output.stdout).unwrap();
            let error = String::from_utf8(output.stderr).unwrap();
            if let Some(peer) = fault.take() {
                assert!(!output.status.success());
                assert_eq!(text, TIMEOUT);
                assert_eq!(error.trim_end(), INTERRUPTED);
                assert_eq!(peer.join().unwrap(), Some(b"3\n".to_vec()));
                // Publish the healthy original endpoints completely before the
                // retry sees uncertainty. Request identity never changes.
                fs::write(c.command_peers.as_ref().unwrap(), rows(c.base, false)).unwrap();
            }
            if output.status.success() {
                Ok(text)
            } else {
                Err((text, error))
            }
        },
        Instant::now,
        || std::thread::park_timeout(Duration::from_millis(10)),
    );
    let receipt =
        result.unwrap_or_else(|e| panic!("{e}\n{}", failure_diagnostics::snapshot(c, ORIGINAL)));
    assert_eq!(calls, [ORIGINAL, ORIGINAL]);
    assert_eq!(receipt, RECEIPT);
    receipt
}
fn values(c: &Cluster, seven: &str) {
    for (group, inc, value) in [("1", "1", "3"), ("7", "3", seven), ("8", "2", "9")] {
        assert_eq!(
            c.routed(&["group", group, inc, "read"]),
            format!("OK value={value}\n")
        );
    }
}
fn history(quic: bool) {
    let mut c = groups::setup(quic);
    let listener = c.take_listener(101);
    let endpoints = c.root.join("original-command-peers");
    fs::write(&endpoints, rows(c.base, false)).unwrap();
    c.command_peers = Some(endpoints);
    start(&mut c, "create");
    for (group, inc, value) in [("1", "1", "3"), ("7", "3", "5"), ("8", "2", "9")] {
        groups::leader(&mut c, group, inc);
        assert!(invoke(
            &c,
            &["group", group, inc, "add", "42", value],
            Duration::from_secs(10)
        )
        .contains(&format!("Value({value})")));
    }
    values(&c, "5");
    fs::write(c.command_peers.as_ref().unwrap(), rows(c.base, true)).unwrap();
    let original = recover(&c, listener);
    values(&c, "5");
    assert!(invoke(
        &c,
        &["group", "7", "3", "add", "24200", "2"],
        Duration::from_secs(10)
    )
    .contains("Value(7)"));
    values(&c, "7");
    if quic {
        peer_discovery::checkpoint_stop(&mut c);
    } else {
        c.stop();
    }
    start(&mut c, "recover");
    assert_eq!(invoke(&c, ORIGINAL, Duration::from_secs(15)), original);
    assert!(invoke(
        &c,
        &["group", "7", "3", "add", "24200", "2"],
        Duration::from_secs(10)
    )
    .contains("duplicate=true"));
    for (group, inc, value) in [("1", "1", "3"), ("8", "2", "9")] {
        assert!(invoke(
            &c,
            &["group", group, inc, "add", "42", value],
            Duration::from_secs(10)
        )
        .contains("duplicate=true"));
    }
    values(&c, "7");
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn original_scoped_write_recovers_authentication_timeout_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn original_scoped_write_recovers_authentication_timeout_quic_checkpoint() {
    history(true);
}
