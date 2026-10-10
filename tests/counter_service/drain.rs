// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) fn cluster(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    c.leadership_maintenance = true;
    c.node_drain = true;
    let access = c.root.join("access.txt");
    fs::write(
        &access,
        "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n",
    )
    .unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    for id in 1..=3 {
        c.start(id, "create");
    }
    c.leader();
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("Value(7)"));
    c
}
fn begin(c: &Cluster, source: usize, target: usize) -> String {
    c.ok(
        source,
        &[
            "drain-node",
            "1",
            "19701",
            "1",
            &target.to_string(),
            &target.to_string(),
            "1",
        ],
    )
}
pub(super) fn wait_status(c: &Cluster, source: usize, expected: &str) -> String {
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let output = c.request(source, &["drain-status", "1", "19701"]);
        let text = String::from_utf8_lossy(&output.stdout);
        if output.status.success() && text.contains(expected) {
            return text.into_owned();
        }
        assert!(
            Instant::now() < end,
            "expected {expected}: {text}; {}",
            c.service_log(source)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
pub(super) fn kill(c: &mut Cluster, id: usize) {
    let mut child = c.children[id - 1].take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
}
pub(super) fn joined(c: &mut Cluster, id: usize) {
    assert!(exited(c, id).success(), "{}", c.service_log(id));
    assert!(c.service_log(id).contains("workers_joined=true"));
}
fn exited(c: &mut Cluster, id: usize) -> std::process::ExitStatus {
    let mut child = c.children[id - 1].take().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < end, "shutdown: {}", c.service_log(id));
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn retained_drain_handoff_stop_recover_and_cancel_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn retained_drain_handoff_stop_recover_and_cancel_quic() {
    history(true);
}
fn history(quic: bool) {
    let mut c = cluster(quic);
    let source = c.leader();
    let target = source % 3 + 1;
    let first = begin(&c, source, target);
    assert!(
        first.contains("phase=Active") && first.contains("evidence=local_durable"),
        "{first}"
    );
    let ready = wait_status(&c, source, "ready=true");
    assert!(ready.contains("handoff=Some(Completed"), "{ready}");
    assert!(begin(&c, source, target).contains("phase=Active"));
    let changed = c.request(
        source,
        &[
            "drain-node",
            "1",
            "19701",
            "1",
            &(target % 3 + 1).to_string(),
            "99",
            "1",
        ],
    );
    assert!(!changed.status.success());
    let blocked = c.request(source, &["add", "19702", "3"]);
    assert!(!blocked.status.success());
    assert!(String::from_utf8_lossy(&blocked.stdout).contains("Draining"));
    if quic {
        c.ok(source, &["checkpoint"]);
        wait_manual_checkpoint(&c, source);
        wait_status(&c, source, "ready=true");
    }
    assert!(c
        .ok(source, &["drain-stop", "1", "19701"])
        .contains("stopping=true"));
    joined(&mut c, source);
    assert!(authenticated_write(&c, &["add", "19702", "3"]).contains("Value(10)"));
    c.start(source, "recover");
    wait_status(&c, source, "phase=Active");
    let blocked = c.request(source, &["read"]);
    assert!(!blocked.status.success());
    assert!(c
        .ok(source, &["cancel-drain", "1", "19701"])
        .contains("phase=Cancelled"));
    wait_status(&c, source, "resuming=false");
    kill(&mut c, source);
    c.start(source, "recover");
    wait_status(&c, source, "phase=Cancelled");
    wait_status(&c, source, "resuming=false");
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    c.stop();
}
pub(super) fn wait_manual_checkpoint(c: &Cluster, source: usize) {
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let text = c.ok(source, &["maintenance"]);
        let base = text
            .split_whitespace()
            .find_map(|word| word.strip_prefix("checkpoint_base="))
            .unwrap()
            .parse::<u64>()
            .unwrap();
        if base > 0 {
            return;
        }
        assert!(Instant::now() < end, "manual checkpoint: {text}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn drain_requires_admin_and_never_stops_an_incomplete_handoff() {
    let mut c = cluster(false);
    let source = c.leader();
    let target = source % 3 + 1;
    for principal in [1, 2] {
        c.command_principal = Some(principal);
        let denied = c.request(
            source,
            &[
                "drain-node",
                "1",
                "19701",
                "1",
                &target.to_string(),
                &target.to_string(),
                "1",
            ],
        );
        assert!(!denied.status.success());
        assert!(String::from_utf8_lossy(&denied.stdout).contains("AUTHORIZATION"));
    }
    c.command_principal = Some(3);
    kill(&mut c, target);
    assert!(begin(&c, source, target).contains("phase=Active"));
    let status = wait_status(&c, source, "ready=false");
    assert!(status.contains("handoff=Some(Pending)"), "{status}");
    assert!(!c
        .request(source, &["drain-stop", "1", "19701"])
        .status
        .success());
    kill(&mut c, source);
    c.start(source, "recover");
    wait_status(&c, source, "ready=false");
    c.start(target, "recover");
    c.ok(source, &["resume-drain", "1", "19701"]);
    wait_status(&c, source, "ready=true");
    c.ok(source, &["cancel-drain", "1", "19701"]);
    wait_status(&c, source, "resuming=false");
    c.stop();
}

#[test]
fn required_empty_drain_journal_cannot_be_silently_recreated_on_recovery() {
    let mut c = cluster(false);
    let source = c.leader();
    kill(&mut c, source);
    fs::remove_file(c.root.join(source.to_string()).join("drain.record")).unwrap();
    let output = run(Command::new(BIN)
        .args(["serve", "recover"])
        .arg(c.root.join(source.to_string()))
        .arg(source.to_string())
        .arg(c.base.to_string())
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"))
        .arg("--service-access")
        .arg(c.command_access.as_ref().unwrap())
        .args([
            "--leadership-maintenance",
            "enabled",
            "--node-drain",
            "enabled",
        ]));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("MissingRecord"));
    c.stop();
}

#[test]
fn lost_drain_reply_keeps_original_identity_and_local_cancel_is_durable() {
    let mut c = cluster(false);
    let source = c.leader();
    let target = source % 3 + 1;
    kill(&mut c, target);
    let lost = UnobservedCommand::send(
        &c,
        source,
        &format!("drain-node 1 19701 1 {target} {target} 1"),
    );
    lost.disconnect();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        let output = c.request(
            source,
            &[
                "drain-node",
                "1",
                "19701",
                "1",
                &target.to_string(),
                &target.to_string(),
                "1",
            ],
        );
        if output.status.success() {
            assert!(String::from_utf8_lossy(&output.stdout).contains("phase=Active"));
            break;
        }
        assert!(
            Instant::now() < end,
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    for identity in [["2", "19701"], ["1", "19702"]] {
        assert!(!c
            .request(source, &["cancel-drain", identity[0], identity[1]])
            .status
            .success());
    }
    assert!(c
        .ok(source, &["cancel-drain", "1", "19701"])
        .contains("cancellation_scope=local_gate"));
    wait_status(&c, source, "resuming=false");
    // Gate cancellation is separate from the already replicated handoff.
    assert!(c
        .ok(source, &["cancel-leadership", "19701"])
        .contains("phase=Cancelled"));
    assert!(c.ok(source, &["add", "19703", "2"]).contains("Value(9)"));
    kill(&mut c, source);
    c.start(source, "recover");
    wait_status(&c, source, "phase=Cancelled");
    assert!(!c
        .request(source, &["drain-stop", "1", "19701"])
        .status
        .success());
    c.start(target, "recover");
    c.leader();
    assert_eq!(c.routed(&["read"]), "OK value=9\n");
    c.stop();
}

#[test]
fn recovery_refuses_to_omit_the_existing_drain_profile() {
    let mut c = cluster(false);
    let source = c.leader();
    kill(&mut c, source);
    let output = run(Command::new(BIN)
        .args(["serve", "recover"])
        .arg(c.root.join(source.to_string()))
        .arg(source.to_string())
        .arg(c.base.to_string())
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"))
        .arg("--service-access")
        .arg(c.command_access.as_ref().unwrap())
        .args(["--leadership-maintenance", "enabled"]));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("existing drain journal requires --node-drain"));
    c.stop();
}

#[test]
fn failed_journal_publication_stops_service_and_preserves_recovery_evidence() {
    let mut c = cluster(false);
    let source = c.leader();
    let target = source % 3 + 1;
    let directory = c.root.join(source.to_string());
    let original = fs::read(directory.join("drain.record")).unwrap();
    let staging = directory.join("drain.drain-staging");
    fs::create_dir(&staging).unwrap();
    let output = c.request(
        source,
        &[
            "drain-node",
            "1",
            "19701",
            "1",
            &target.to_string(),
            &target.to_string(),
            "1",
        ],
    );
    assert!(!output.status.success());
    assert!(!exited(&mut c, source).success());
    assert!(
        c.service_log(source).contains("uncertain: true"),
        "{}",
        c.service_log(source)
    );
    assert_eq!(fs::read(directory.join("drain.record")).unwrap(), original);
    fs::remove_dir(staging).unwrap();
    c.start(source, "recover");
    c.leader();
    // No successful drain receipt escaped. The initialized journal remains empty.
    assert!(!c
        .request(source, &["drain-status", "1", "19701"])
        .status
        .success());
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    c.stop();
}

#[test]
fn retained_profile_refuses_to_interpret_a_membership_drain_journal() {
    use voteboat::{
        drain::*, identity::*, native::drain_journal::*, runtime::*, secure::PeerIdentity,
    };
    let mut c = cluster(false);
    let source = c.leader();
    kill(&mut c, source);
    let owner = PeerIdentity {
        node: NodeId::new(source as u64).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(source as u128).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    };
    let mut journal = NativeDrainJournal::recover(
        FileDrainRecord::new(c.root.join(source.to_string()).join("drain.record")),
        owner,
    )
    .ok()
    .unwrap();
    journal
        .publish(DrainRecord {
            owner,
            sequence: 1,
            request: LocalDrainRequest {
                operation: OperationId::new(19799).unwrap(),
                groups: vec![DrainGroup {
                    group: GroupIdentity {
                        id: GroupId::new(1).unwrap(),
                        incarnation: GroupIncarnation::new(1).unwrap(),
                    },
                    configuration: ConfigurationId::new(1).unwrap(),
                }],
            },
            phase: DrainPhase::Active,
            plan: Some(DrainPlanDigest::from_bytes([7; 32])),
        })
        .unwrap();
    let output = run(Command::new(BIN)
        .args(["serve", "recover"])
        .arg(c.root.join(source.to_string()))
        .arg(source.to_string())
        .arg(c.base.to_string())
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls"))
        .arg("--service-access")
        .arg(c.command_access.as_ref().unwrap())
        .args([
            "--leadership-maintenance",
            "enabled",
            "--node-drain",
            "enabled",
        ]));
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("membership drain journal requires its original host plan"));
    c.stop();
}
