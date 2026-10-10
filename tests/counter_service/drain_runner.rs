// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "drain_runner/confirmation.rs"]
mod confirmation;
#[path = "drain_runner/follower.rs"]
mod follower;
#[path = "drain_runner/historical.rs"]
mod historical;
pub(super) use confirmation::confirmed;

pub(super) fn command(c: &Cluster, source: usize, operation: &str, principal: u64) -> Command {
    let mut command = Command::new(BIN);
    command
        .args([
            "drain-run",
            &c.base.to_string(),
            &source.to_string(),
            "1",
            operation,
        ])
        .arg("--service-tls")
        .arg(c.tls.clone().unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls")
        }))
        .arg("--principal")
        .arg(principal.to_string());
    if let Some(path) = &c.command_peers {
        command.arg("--command-peers").arg(path);
    }
    command
}
fn finish(c: &mut Cluster, source: usize) {
    confirmed(c, source, 19701);
    drain::joined(c, source);
    assert!(authenticated_write(c, &["add", "19760", "3"]).contains("Value(10)"));
    assert!(authenticated_write(c, &["add", "19700", "7"]).contains("duplicate=true"));
    c.stop();
}
#[test]
fn authenticated_runner_drives_membership_and_stops_source_tcp() {
    let (mut c, source, _) = membership_drain::prepare(false);
    finish(&mut c, source);
}
#[cfg(feature = "quic")]
#[test]
fn authenticated_runner_drives_membership_and_stops_source_quic() {
    let (mut c, source, _) = membership_drain::prepare(true);
    let path = c.root.join("runner-peers.txt");
    let mut peers = String::from("voteboat-command-peers-v1\n");
    for id in [3, 1, 2] {
        peers.push_str(&format!(
            "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
            c.base + 100 + id
        ));
    }
    fs::write(&path, peers).unwrap();
    c.command_peers = Some(path);
    finish(&mut c, source);
}
#[test]
fn killed_runner_and_source_resume_original_durable_plan() {
    interrupted(false);
}
#[cfg(feature = "quic")]
#[test]
fn killed_quic_runner_and_source_resume_original_durable_plan() {
    interrupted(true);
}
fn interrupted(quic: bool) {
    let (mut c, source, target) = membership_drain::prepare(quic);
    let profile = c.membership_drain.as_ref().unwrap().1.clone();
    let prepared = fs::read(&profile).unwrap();
    let journal = c.root.join(source.to_string()).join("drain.record");
    drain::kill(&mut c, target);
    let log = fs::File::create(c.root.join("runner-first.log")).unwrap();
    let mut runner = {
        let _gate = fixture_gate();
        command(&c, source, "19701", 3)
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap()
    };
    drain::wait_status(&c, source, "phase=Active");
    let original = fs::read(&journal).unwrap();
    runner.kill().unwrap();
    runner.wait().unwrap();
    drain::kill(&mut c, source);
    c.start(target, "recover-member");
    c.start(source, "recover-member");
    drain::wait_status(&c, source, "phase=Active");
    assert_eq!(fs::read(&journal).unwrap(), original);
    assert_eq!(fs::read(&profile).unwrap(), prepared);
    finish(&mut c, source);
    assert_eq!(fs::read(&journal).unwrap(), original);
    assert_eq!(fs::read(&profile).unwrap(), prepared);
}
#[test]
fn runner_rejects_unauthorized_and_wrong_identity_without_drain() {
    let (mut c, source, _) = membership_drain::prepare(false);
    for (operation, principal) in [("19701", 1), ("999", 3), ("0", 3)] {
        let output = run(&mut command(&c, source, operation, principal));
        assert!(!output.status.success());
        assert!(!c
            .request(source, &["drain-status", "1", "19701"])
            .status
            .success());
    }
    let output = run(Command::new(BIN).args([
        "drain-run",
        &c.base.to_string(),
        &source.to_string(),
        "1",
        "19701",
    ]));
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("requires --service-tls and --principal")
    );
    let peers = c.root.join("missing-source.txt");
    let other = source % 3 + 1;
    fs::write(
        &peers,
        format!(
            "voteboat-command-peers-v1\n{other} 127.0.0.1:{} node{other}.voteboat.test\n",
            c.base + 100 + other as u16
        ),
    )
    .unwrap();
    c.command_peers = Some(peers);
    let output = run(&mut command(&c, source, "19701", 3));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("source missing from command peers"));
    c.command_peers = None;
    c.stop();
}
