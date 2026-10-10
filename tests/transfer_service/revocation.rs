// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn replacement() -> String {
    let mut access = "voteboat-service-access-v1 2\n".to_owned();
    for group in GROUPS {
        access.push_str(&format!("3 reader {group} 1\n2 admin {group} 1\n"));
    }
    access
}

fn rotate(rig: &mut Cluster) {
    fs::write(rig.root.join("access"), replacement()).unwrap();
    for group in GROUPS {
        for node in 1..=3 {
            // Publication can revoke the response channel. Observe the original
            // sequence through the replacement principal instead of replaying
            // the split or interpreting an unread reply as rollback.
            let submitted = rig.request(node, 3, group, &["reload-access", "1", "1", "2"]);
            let end = Instant::now() + Duration::from_secs(10);
            loop {
                let output = rig.request(node, 2, group, &["credential-status", "1"]);
                let text = String::from_utf8(output.stdout).unwrap();
                if output.status.success()
                    && text == "OK generation=2 reload=1 state=recorded recorded_generation=2\n"
                {
                    break;
                }
                assert!(
                    Instant::now() < end,
                    "reload {group}/{node}: {text}; submitted={} {}",
                    String::from_utf8_lossy(&submitted.stdout),
                    String::from_utf8_lossy(&submitted.stderr)
                );
                std::thread::park_timeout(Duration::from_millis(5));
            }
            let retry = rig.request(node, 2, group, &["reload-access", "1", "1", "2"]);
            assert!(retry.status.success());
            assert_eq!(retry.stdout, b"OK reload=1 already_recorded=true\n");
            let conflict = rig.request(node, 2, group, &["reload-access", "2", "1", "3"]);
            assert!(!conflict.status.success());
            assert!(
                String::from_utf8_lossy(&conflict.stderr).contains("generation changed"),
                "{}",
                String::from_utf8_lossy(&conflict.stderr)
            );
        }
    }
    rig.admin = 2;
}

fn revoked(rig: &Cluster) {
    for group in GROUPS {
        for node in 1..=3 {
            let denied = rig.request(node, 3, group, &["transfer-step", "intent"]);
            assert!(!denied.status.success(), "revoked {group}/{node}");
            assert!(String::from_utf8_lossy(&denied.stderr).contains("ERR AUTHORIZATION"));
            let denied = rig.request(node, 3, group, &["reload-access", "2", "2", "3"]);
            assert!(!denied.status.success());
            assert!(String::from_utf8_lossy(&denied.stderr).contains("ERR AUTHORIZATION"));
            assert!(rig.request(node, 3, group, &["status"]).status.success());
            let status = rig.request(node, 2, group, &["credential-status", "1"]);
            assert!(status.status.success());
            assert_eq!(
                status.stdout,
                b"OK generation=2 reload=1 state=recorded recorded_generation=2\n"
            );
        }
    }
}

fn refuse_rollback(rig: &Cluster, original: &[u8]) {
    for obsolete in [
        original.to_vec(),
        replacement()
            .replacen("3 reader", "3 writer", 1)
            .into_bytes(),
    ] {
        fs::write(rig.root.join("access"), obsolete).unwrap();
        for (slot, group) in GROUPS.into_iter().enumerate() {
            let mut child = spawn(
                Command::new(BIN)
                    .args(["serve", "recover"])
                    .arg(rig.root.join(format!("{group}/1")))
                    .arg("1")
                    .arg((rig.base + slot as u16 * 128).to_string())
                    .arg(rig.tls())
                    .arg(rig.root.join("profile"))
                    .arg(group.to_string())
                    .arg(rig.root.join("access"))
                    .arg(if rig.quic { "quic" } else { "tcp" })
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped()),
            );
            let end = Instant::now() + Duration::from_secs(5);
            while child.try_wait().unwrap().is_none() {
                if Instant::now() >= end {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("group {group} accepted stale credential material");
                }
                std::thread::park_timeout(Duration::from_millis(5));
            }
            let output = child.wait_with_output().unwrap();
            assert!(!output.status.success());
            assert!(String::from_utf8_lossy(&output.stderr)
                .contains("credential files conflict with durable reload record"));
        }
    }
    fs::write(rig.root.join("access"), replacement()).unwrap();
}

fn history(quic: bool) {
    let mut rig = Cluster::new(quic);
    initialize(&rig);
    let original = fs::read(rig.root.join("profile")).unwrap();
    let original_access = fs::read(rig.root.join("access")).unwrap();
    for _ in 0..3 {
        rig.operate("step");
    }
    assert!(rig.operate("status").starts_with("OK next=Fence("));
    cuts::support::interrupt_command(&mut rig, 20, &["transfer-step".into(), "fence".into()]);
    let interrupted = rig.operate("status");
    rotate(&mut rig);
    revoked(&rig);
    assert_eq!(rig.operate("status"), interrupted);
    assert!(rig.operate("resume").contains("OK complete"));
    if quic {
        cuts::support::checkpoint(&rig);
    }
    rig.crash();
    refuse_rollback(&rig, &original_access);
    rig.start("recover");
    revoked(&rig);
    assert!(rig.operate("resume").contains("OK complete"));
    assert_eq!(fs::read(rig.root.join("profile")).unwrap(), original);
    finish(rig);
}

#[test]
fn interrupted_split_resumes_under_replacement_administrator_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn interrupted_split_resumes_under_replacement_administrator_quic() {
    history(true);
}
