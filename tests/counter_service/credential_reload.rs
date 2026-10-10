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
const ORIGINAL: &str = "voteboat-service-access-v1 1\n1 reader 1 1\n2 writer 1 1\n3 admin 1 1\n";
const REPLACEMENT: &str = "voteboat-service-access-v1 2\n1 reader 1 1\n2 reader 1 1\n3 admin 1 1\n";

fn wait_status(cluster: &Cluster, id: usize, sequence: &str, expected: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = cluster.request(id, &["credential-status", sequence]);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() && text.contains(expected) {
            return text;
        }
        assert!(
            Instant::now() < deadline,
            "reload status: {text}; {}",
            cluster.service_log(id)
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}
fn publish_without_observing_reply(cluster: &Cluster, id: usize) {
    let unread = UnobservedCommand::send(cluster, id, "reload-access 1 1 2");
    let deadline = Instant::now() + Duration::from_secs(10);
    let record = cluster.root.join(id.to_string()).join("CREDENTIAL-RELOAD");
    while !record.exists() {
        assert!(
            Instant::now() < deadline,
            "reload never recorded: {}",
            cluster.service_log(id)
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    unread.disconnect();
    let status = wait_status(cluster, id, "1", "state=recorded");
    assert!(status.starts_with("OK generation=2 "), "{status}");
    assert!(cluster
        .ok(id, &["reload-access", "1", "1", "2"])
        .contains("already_recorded=true"));
    let stale = cluster.request(id, &["reload-access", "1", "2", "3"]);
    assert!(!stale.status.success());
    assert!(String::from_utf8(stale.stdout)
        .unwrap()
        .contains("stale credential reload sequence"));
}
fn verify_revocation(cluster: &mut Cluster) {
    cluster.command_principal = Some(2);
    for id in 1..=3 {
        let denied = cluster.request(id, &["add", "18001", "7"]);
        assert!(!denied.status.success());
        assert_eq!(
            String::from_utf8(denied.stdout).unwrap(),
            "ERR AUTHORIZATION\n"
        );
    }
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    cluster.command_principal = Some(3);
    assert!(authenticated_write(cluster, &["add", "18001", "7"]).contains("duplicate=true"));
}
fn failed_preparation(cluster: &Cluster, path: &std::path::Path) {
    fs::write(path, "voteboat-service-access-v1 3\n3 invalid-role 1 1\n").unwrap();
    assert!(cluster
        .ok(1, &["reload-access", "2", "2", "3"])
        .contains("queued=true"));
    let status = wait_status(cluster, 1, "2", "state=failed");
    assert!(status.starts_with("OK generation=2 "), "{status}");
    assert_eq!(cluster.routed(&["read"]), "OK value=7\n");
    let changed = cluster.request(1, &["reload-access", "3", "1", "3"]);
    assert!(!changed.status.success());
    assert!(String::from_utf8(changed.stdout)
        .unwrap()
        .contains("credential generation changed"));
}
fn refuse_rollback(cluster: &Cluster, access: &std::path::Path) {
    for text in [
        ORIGINAL.to_owned(),
        REPLACEMENT.replace("2 reader", "2 writer"),
    ] {
        fs::write(access, text).unwrap();
        refuse_conflicting_startup(cluster, access);
    }
}
fn refuse_conflicting_startup(cluster: &Cluster, access: &std::path::Path) {
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let output = run(Command::new(BIN)
        .args(["serve", "recover"])
        .arg(cluster.root.join("1"))
        .arg("1")
        .arg(cluster.base.to_string())
        .arg(tls)
        .arg("--service-access")
        .arg(access));
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("credential files conflict with durable reload record"));
}
fn history(quic: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    let access = cluster.root.join("service-access.txt");
    fs::write(&access, ORIGINAL).unwrap();
    cluster.command_access = Some(access.clone());
    cluster.command_principal = Some(3);
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    let leader = cluster.leader();
    cluster.command_principal = Some(2);
    assert!(authenticated_write(&cluster, &["add", "18001", "7"]).contains("Value(7)"));
    let denied = cluster.request(leader, &["reload-access", "1", "1", "2"]);
    assert_eq!(
        String::from_utf8(denied.stdout).unwrap(),
        "ERR AUTHORIZATION\n"
    );
    cluster.command_principal = Some(3);
    fs::write(&access, REPLACEMENT).unwrap();
    for id in 1..=3 {
        publish_without_observing_reply(&cluster, id);
    }
    verify_revocation(&mut cluster);
    failed_preparation(&cluster, &access);
    fs::write(&access, REPLACEMENT).unwrap();
    cluster.stop();
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    cluster.leader();
    for id in 1..=3 {
        wait_status(&cluster, id, "1", "state=recorded");
    }
    verify_revocation(&mut cluster);
    cluster.stop();
    refuse_rollback(&cluster, &access);
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn live_reload_revokes_writer_and_recovers_after_lost_reply_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn live_reload_revokes_writer_and_recovers_after_lost_reply_quic() {
    history(true);
}
