// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) fn command(c: &Cluster) -> Command {
    let mut command = Command::new(BIN);
    command
        .args(["lookup", &c.base.to_string(), "auto"])
        .arg(c.tls())
        .args(["1", "42", "1", "10", "1"]);
    command
}
fn history(quic: bool) {
    let mut c = Cluster::new(quic);
    let plan = fs::read(c.root.join("plan")).unwrap();
    let old = initialize(&mut c);
    // Initialization's sampled leader is not a promise of a fresh quorum read.
    // Seed through the bounded read client before forcing that source to fail.
    let baseline = command(&c).fixture_output().unwrap();
    assert!(baseline.status.success(), "{:?}", baseline);
    c.kill(old);
    let new = c.leader();
    assert_ne!(new, old);
    c.start(old, "recover");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let out = c.request(old, 3, &["status"]);
        if out.status.success() && String::from_utf8_lossy(&out.stdout).contains("role=Follower") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "old source did not become a follower"
        );
        std::thread::park_timeout(Duration::from_millis(5));
    }
    let pinned = c.lookup(old).fixture_output().unwrap();
    println!(
        "old_source={old} replacement={new} pinned_status={} stdout={:?} stderr={:?}",
        pinned.status,
        String::from_utf8_lossy(&pinned.stdout),
        String::from_utf8_lossy(&pinned.stderr)
    );
    assert!(!pinned.status.success());
    assert!(pinned.stdout.is_empty());
    assert!(String::from_utf8_lossy(&pinned.stderr).contains("manifest lookup deadline expired"));
    // Build the actual CLI mode, preserving the exact original manifest query.
    let found = command(&c).fixture_output().unwrap();
    assert!(
        found.status.success(),
        "stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&found.stdout),
        String::from_utf8_lossy(&found.stderr)
    );
    assert_eq!(found.stdout, baseline.stdout);
    let map = c.root.join("command-peers");
    let mut rows = "voteboat-command-peers-v1\n".to_owned();
    for node in 1..=3 {
        rows.push_str(&format!(
            "{node} 127.0.0.1:{} node{node}.voteboat.test\n",
            c.base + 100 + node
        ));
    }
    fs::write(&map, rows).unwrap();
    let mapped = command(&c)
        .arg("--command-peers")
        .arg(&map)
        .fixture_output()
        .unwrap();
    assert!(mapped.status.success(), "{:?}", mapped);
    assert_eq!(mapped.stdout, baseline.stdout);
    assert_eq!(fs::read(c.root.join("plan")).unwrap(), plan);
    c.stop();
}
#[test]
fn automatic_lookup_preserves_original_manifest_after_sampled_leader_loss_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn automatic_lookup_preserves_original_manifest_after_sampled_leader_loss_quic() {
    history(true);
}
#[test]
fn automatic_lookup_never_returns_a_hint_without_a_live_authority() {
    let c = Cluster::new(false); // held listeners, no authenticated replica
    let start = Instant::now();
    let out = command(&c).fixture_output().unwrap();
    println!(
        "unavailable lookup elapsed={:?} stderr={:?}",
        start.elapsed(),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success());
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("deadline expired"));
    // Includes process/setup overhead; the shared discovery deadline stays 10s.
    assert!(start.elapsed() < Duration::from_secs(12));
    assert!(c.children.iter().all(Option::is_none));
}
