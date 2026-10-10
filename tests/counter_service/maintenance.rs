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
    // A second matched worker sequence proves the scheduler, rather than a
    // one-off startup/manual request, continues to drive physical maintenance.
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
            let text = String::from_utf8(out.stdout).unwrap();
            assert!(
                text.contains("enabled=true") && text.contains("completion_error=None"),
                "{text}"
            );
            let sequence = text
                .split_whitespace()
                .find_map(|s| s.strip_prefix("last_sequence="))
                .unwrap()
                .parse::<u64>()
                .unwrap();
            if sequence > after {
                return sequence;
            }
        }
        assert!(Instant::now() < deadline, "maintenance did not complete");
        std::thread::sleep(Duration::from_millis(10));
    }
}
