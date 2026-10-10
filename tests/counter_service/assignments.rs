// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn field<'a>(text: &'a str, key: &str) -> &'a str {
    let prefix = format!("{key}=");
    text.split_whitespace()
        .find_map(|w| w.strip_prefix(&prefix))
        .unwrap()
}
fn page(c: &Cluster, cursor: &str, limit: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = c.request(1, &["list-assigned-groups", cursor, limit]);
        if output.status.success() {
            return String::from_utf8(output.stdout).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn original_retry(c: &Cluster) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // This write was acknowledged before restart. Retry only its original
        // identity/payload; an interrupted observation is not a new operation.
        let output = c.target("auto", &["group", "7", "3", "add", "42", "5"]);
        let text = String::from_utf8(output.stdout).unwrap();
        if output.status.success() {
            assert!(text.contains("duplicate=true"), "{text}");
            return;
        }
        assert!(
            text.starts_with("UNKNOWN LeadershipChanged;")
                || text
                    == "UNKNOWN authenticated read failed; retry the same operation ID and delta\n",
            "{text}"
        );
        assert!(Instant::now() < deadline, "{text}");
    }
}
fn history(quic: bool) {
    let mut c = group_admin::setup(quic);
    let access = c.command_access.as_ref().unwrap();
    let policy = fs::read_to_string(access)
        .unwrap()
        .replace("1 reader 7 3", "1 reader 1 1")
        .replace("2 admin 7 3", "2 reader 7 3\n2 reader 1 1\n2 reader 8 2");
    fs::write(access, policy).unwrap();
    for n in 1..=3 {
        c.start(n, "create");
    }
    let first = page(&c, "-", "1");
    assert!(first.contains("evidence=local_accepted_configuration"));
    assert_eq!(field(&first, "assignments"), "1:1:1:1:-");
    let cursor = field(&first, "next").to_owned();
    authenticated_write(&c, &["group", "7", "3", "add", "42", "5"]);
    let second = page(&c, &cursor, "1");
    assert_eq!(field(&second, "assignments"), "7:3:9:9:-");
    let third = page(&c, field(&second, "next"), "1");
    assert_eq!(field(&third, "assignments"), "8:2:11:11:-");
    assert!(third.contains("more=false next=-"));
    c.command_principal = Some(1);
    let denied = c.request(1, &["list-assigned-groups", "-", "1"]);
    assert!(!denied.status.success());
    assert!(String::from_utf8_lossy(&denied.stdout).contains("AUTHORIZATION"));
    c.command_principal = Some(2);
    assert!(page(&c, "-", "8").contains("groups=3 rows=3"));
    c.command_principal = Some(3);
    for args in [
        vec!["list-assigned-groups", "bad", "1"],
        vec!["list-assigned-groups", "-", "0"],
        vec!["list-assigned-groups", "-", "9"],
        vec!["group", "7", "3", "list-assigned-groups", "-", "1"],
    ] {
        assert!(!c.request(1, &args).status.success());
    }
    group_admin::configure(&mut c, "7", "3");
    group_admin::configure(&mut c, "7", "3");
    let deadline = Instant::now() + Duration::from_secs(15);
    while !page(&c, "-", "8").contains("7:3:11:11:-") {
        assert!(Instant::now() < deadline);
    }
    let stale = c.request(1, &["list-assigned-groups", &cursor, "1"]);
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stdout).contains("stale assignment cursor"));
    let before = page(&c, "-", "1");
    let cursor = field(&before, "next").to_owned();
    for (g, inc) in [("1", "1"), ("7", "3"), ("8", "2")] {
        c.ok(1, &["group", g, inc, "checkpoint"]);
    }
    drain::kill(&mut c, 1);
    c.start(1, "recover");
    let after = page(&c, "-", "8");
    assert_ne!(
        field(&before, "store_session"),
        field(&after, "store_session")
    );
    let stale = c.request(1, &["list-assigned-groups", &cursor, "1"]);
    assert!(!stale.status.success());
    assert!(String::from_utf8_lossy(&stale.stdout).contains("stale assignment cursor"));
    original_retry(&c);
    c.stop();
}
#[test]
fn assignment_pages_tcp_bind_permissions_membership_and_reopen() {
    history(false);
}
#[test]
#[cfg(feature = "quic")]
fn assignment_pages_quic_bind_permissions_membership_and_reopen() {
    history(true);
}
#[test]
fn single_group_inventory_does_not_require_a_leader() {
    let mut c = Cluster::new();
    c.start(1, "create");
    let text = page(&c, "-", "8");
    assert!(text.contains("groups=1 rows=1 more=false next=- assignments=1:1:1:1:-"));
    c.stop();
}
