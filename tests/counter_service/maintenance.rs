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
#[test]
fn invalid_schedule_rejected_before_files_or_listeners() {
    let cluster = Cluster::new();
    let target = cluster.root.join("invalid");
    for value in ["0", "-1", "text", "18446744073709551616"] {
        let out = run(Command::new(BIN)
            .args(["serve", "create"])
            .arg(&target)
            .arg("1")
            .arg(cluster.base.to_string())
            .arg("missing-tls")
            .args(["--wal-reclaim-ms", value]));
        assert!(!out.status.success());
        assert!(!target.exists());
    }
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn automatic_reclaim_service_tcp_recovers_and_retries() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn automatic_reclaim_service_quic_recovers_and_retries() {
    history(true);
}
fn history(quic: bool) {
    let mut c = Cluster::new();
    c.quic = quic;
    c.wal_reclaim_ms = Some(20);
    for id in 1..=3 {
        c.start(id, "create");
    }
    c.leader();
    assert!(c.routed(&["add", "91001", "7"]).contains("Value(7)"));
    assert_eq!(c.routed(&["read"]), "OK value=7\n");
    for id in 1..=3 {
        c.ok(id, &["checkpoint"]);
    }
    let first = wait_maintenance(&c, 1, 0);
    // Further progress proves the scheduler, rather than a one-off
    // startup/manual request, continues to drive physical maintenance.
    wait_maintenance(&c, 1, first);
    c.stop();
    for id in 1..=3 {
        c.start(id, "recover");
    }
    c.leader();
    let retry = c.routed(&["add", "91001", "7"]);
    assert!(
        retry.contains("Value(7)") && retry.contains("duplicate=true"),
        "{retry}"
    );
    assert!(c.routed(&["add", "91002", "3"]).contains("Value(10)"));
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    wait_maintenance(&c, 1, 0);
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
fn wait_maintenance(c: &Cluster, id: usize, after: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let out = c.request(id, &["maintenance"]);
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout);
            assert!(
                text.contains("enabled=true") && text.contains("completion_error=None"),
                "{text}"
            );
            let field = |name: &str| {
                text.split_whitespace()
                    .find_map(|s| s.strip_prefix(name))
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            };
            // Each interval either rewrites (a new worker sequence) or, with
            // too little to reclaim, moves on (`skipped`): both show the
            // schedule is still running.
            let progress = field("last_sequence=") + field("skipped=");
            if progress > after {
                return progress;
            }
        }
        assert!(Instant::now() < deadline, "maintenance did not complete");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn automatic_checkpoints_and_reclaim_tcp_survive_restart() {
    checkpoint_history(false);
}
#[cfg(feature = "quic")]
#[test]
fn automatic_checkpoints_and_reclaim_quic_survive_restart() {
    checkpoint_history(true);
}
fn checkpoint_history(quic: bool) {
    let mut c = Cluster::new();
    c.quic = quic;
    c.checkpoint_entries = Some(2);
    c.wal_reclaim_ms = Some(20);
    for id in 1..=3 {
        c.start(id, "create");
    }
    c.leader();
    assert!(authenticated_write(&c, &["add", "92001", "7"]).contains("Value(7)"));
    for id in 1..=3 {
        wait_checkpoint(&c, id, 2);
    }
    assert!(authenticated_write(&c, &["add", "92002", "3"]).contains("Value(10)"));
    assert!(authenticated_write(&c, &["add", "92003", "4"]).contains("Value(14)"));
    for id in 1..=3 {
        wait_checkpoint(&c, id, 4);
    }
    c.stop();
    for id in 1..=3 {
        check_drained_checkpoint(&c, id);
    }
    for id in 1..=3 {
        c.start(id, "recover");
    }
    c.leader();
    let retry = authenticated_write(&c, &["add", "92001", "7"]);
    assert!(
        retry.contains("Value(7)") && retry.contains("duplicate=true"),
        "{retry}"
    );
    assert_eq!(c.routed(&["read"]), "OK value=14\n");
    assert!(authenticated_write(&c, &["add", "92004", "5"]).contains("Value(19)"));
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
fn wait_checkpoint(c: &Cluster, id: usize, target: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let out = c.request(id, &["maintenance"]);
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout);
            assert!(text.contains("checkpoint_enabled=true"), "{text}");
            let base = text
                .split_whitespace()
                .find_map(|s| s.strip_prefix("checkpoint_base="))
                .unwrap()
                .parse::<u64>()
                .unwrap();
            if base >= target {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "node {id} checkpoint did not reach {target}: stdout={} stderr={} log={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
            c.service_log(id)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
#[test]
fn invalid_checkpoint_schedule_precedes_resource_creation() {
    let c = Cluster::new();
    let target = c.root.join("invalid-checkpoint");
    for value in ["0", "-1", "NaN", "18446744073709551616"] {
        let out = run(Command::new(BIN)
            .args(["serve", "create"])
            .arg(&target)
            .arg("1")
            .arg(c.base.to_string())
            .arg("missing-tls")
            .args(["--checkpoint-entries", value]));
        assert!(!out.status.success());
        assert!(!target.exists());
    }
    fs::remove_dir_all(&c.root).unwrap();
}

/// ST2: with no flags the service checkpoints and reclaims by default, and the
/// explicit opt-out is recorded at startup and in the status.
#[test]
fn maintenance_defaults_and_opt_out_are_operator_visible() {
    let mut c = Cluster::new();
    c.start(1, "create");
    let text = wait_status(&c, 1);
    for field in [
        "retention=maintained",
        "enabled=true",
        "checkpoint_enabled=true",
        "checkpoint_entries=8192",
        "failures=0",
        "wal_capacity=66060288",
        "high_water=false",
        "refused_proposals=0",
        "wal_parked=0",
    ] {
        assert!(text.contains(field), "{field}: {text}");
    }
    c.stop();
    let log = c.service_log(1);
    assert!(
        log.contains("maintenance retention=maintained checkpoint_entries=8192 checkpoint_scan_ms=10 wal_reclaim_ms=1000 wal_retry_ms=100"),
        "{log}"
    );
    c.no_maintenance = true;
    c.start(1, "recover");
    let text = wait_status(&c, 1);
    for field in [
        "retention=lifetime-bounded",
        "enabled=false",
        "checkpoint_enabled=false",
    ] {
        assert!(text.contains(field), "{field}: {text}");
    }
    c.stop();
    let log = c.service_log(1);
    assert!(
        log.contains("maintenance retention=lifetime-bounded checkpoint_entries=off checkpoint_scan_ms=off wal_reclaim_ms=off wal_retry_ms=off"),
        "{log}"
    );
    fs::remove_dir_all(&c.root).unwrap();
}
fn wait_status(c: &Cluster, id: usize) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let out = c.request(id, &["maintenance"]);
        if out.status.success() {
            return String::from_utf8_lossy(&out.stdout).into_owned();
        }
        assert!(
            Instant::now() < deadline,
            "maintenance status unavailable: {}",
            c.service_log(id)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
/// U5: a maintenance selection that would be refused or ignored once serving
/// is refused at startup, before any file or listener exists.
#[test]
fn maintenance_selection_is_validated_before_files_or_listeners() {
    let c = Cluster::new();
    let target = c.root.join("invalid-maintenance");
    let ack = "I-accept-a-bounded-log";
    for (args, message) in [
        (
            vec!["--no-maintenance", "yes"],
            "Pass --no-maintenance I-accept-a-bounded-log",
        ),
        (
            vec!["--no-maintenance", "i-accept-a-bounded-log"],
            "Pass --no-maintenance",
        ),
        (
            vec!["--no-maintenance", ack, "--checkpoint-entries", "5"],
            "cannot be combined",
        ),
        (
            vec!["--no-maintenance", ack, "--wal-reclaim-ms", "5"],
            "cannot be combined",
        ),
        (
            vec!["--no-maintenance", ack, "--no-maintenance", ack],
            "duplicate",
        ),
        (vec!["--checkpoint-entries", "8193"], "at most 8192"),
    ] {
        let out = run(Command::new(BIN)
            .args(["serve", "create"])
            .arg(&target)
            .arg("1")
            .arg(c.base.to_string())
            .arg("missing-tls")
            .args(&args));
        assert!(!out.status.success(), "{args:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains(message), "{args:?}: {stderr}");
        assert!(!target.exists(), "{args:?}");
    }
    fs::remove_dir_all(&c.root).unwrap();
}
