// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn command(c: &Cluster, operation: &str, principal: &str) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.args([
        "group-drain-run",
        &c.base.to_string(),
        "3",
        "1",
        operation,
        "--service-tls",
    ])
    .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"))
    .args(["--principal", principal]);
    if let Some(path) = &c.command_peers {
        cmd.arg("--command-peers").arg(path);
    }
    cmd
}
fn finish(c: &mut Cluster) {
    let output = run(&mut command(c, "19701", "3"));
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("groups=3")
            && text.contains("shutdown_requested=true")
            && text.contains("multi=true"),
        "{text}"
    );
    drain::joined(c, 3);
    assert!(
        authenticated_write(c, &["group", "7", "3", "add", "42", "5"]).contains("duplicate=true")
    );
    assert!(authenticated_write(c, &["group", "8", "2", "add", "43", "1"]).contains("Value(9)"));
    assert_eq!(c.routed(&["group", "1", "1", "read"]), "OK value=3\n");
    c.stop();
}
#[test]
fn multi_group_runner_drives_distinct_targets_and_stops_source_tcp() {
    let mut c = group_drain::setup(false);
    finish(&mut c);
}
#[test]
#[cfg(feature = "quic")]
fn multi_group_runner_drives_distinct_targets_and_stops_source_quic() {
    let mut c = group_drain::setup(true);
    let path = c.root.join("runner-peers.txt");
    fs::write(&path, format!("voteboat-command-peers-v1\n3 127.0.0.1:{} node3.voteboat.test\n1 127.0.0.1:{} node1.voteboat.test\n2 127.0.0.1:{} node2.voteboat.test\n", c.base+103, c.base+101, c.base+102)).unwrap();
    c.command_peers = Some(path);
    finish(&mut c);
}
#[test]
fn multi_group_runner_refuses_wrong_scope_identity_and_cancelled_intent() {
    let mut c = group_drain::setup(false);
    for (operation, principal) in [("19701", "1"), ("999", "3"), ("0", "3")] {
        let result = run(&mut command(&c, operation, principal));
        assert!(!result.status.success());
        assert!(!c
            .request(3, &["drain-status", "1", "19701"])
            .status
            .success());
    }
    let path = c.root.join("source-only-peers.txt");
    fs::write(
        &path,
        format!(
            "voteboat-command-peers-v1\n3 127.0.0.1:{} node3.voteboat.test\n",
            c.base + 103
        ),
    )
    .unwrap();
    c.command_peers = Some(path);
    let missing = run(&mut command(&c, "19701", "3"));
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("handoff target absent"));
    group_drain::status(&c, "phase=Active");
    c.command_peers = None;
    c.ok(3, &["drain-node", "1", "19701"]);
    c.ok(3, &["cancel-drain", "1", "19701"]);
    group_drain::status(&c, "phase=Cancelled");
    let result = run(&mut command(&c, "19701", "3"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("active original multi-group drain"));
    group_drain::status(&c, "phase=Cancelled");
    c.ok(3, &["status"]);
    c.stop();
}

fn interrupted(quic: bool) {
    let mut c = group_drain::setup(quic);
    drain::kill(&mut c, 2);
    let log = fs::File::create(c.root.join("group-runner-first.log")).unwrap();
    let mut runner = {
        let _gate = fixture_gate();
        command(&c, "19701", "3")
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap()
    };
    group_drain::status(&c, "phase=Active");
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let state = c.ok(3, &["group", "7", "3", "configuration-status", "7001"]);
        if state.contains("accepted=Joint") {
            assert!(state.contains("committed=NotFoundLocally"), "{state}");
            break;
        }
        assert!(Instant::now() < deadline, "{state}; {}", c.service_log(1));
        std::thread::sleep(Duration::from_millis(10));
    }
    runner.kill().unwrap();
    runner.wait().unwrap();
    if quic {
        c.ok(3, &["group", "7", "3", "checkpoint"]);
    }
    drain::kill(&mut c, 3);
    c.start(2, "recover");
    c.start(3, "recover");
    group_drain::status(&c, "phase=Active");
    finish(&mut c);
}
#[test]
fn multi_group_runner_resumes_partial_membership_after_source_loss_tcp() {
    interrupted(false);
}
#[test]
#[cfg(feature = "quic")]
fn multi_group_runner_resumes_partial_membership_after_source_checkpoint_quic() {
    interrupted(true);
}
