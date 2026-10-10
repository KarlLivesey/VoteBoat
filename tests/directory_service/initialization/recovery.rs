// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

struct WaitingClient(Child);
impl Drop for WaitingClient {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn interrupt(c: &mut Cluster, args: &[&str], operation: &str) {
    let leader = c.leader();
    for node in 1..=3 {
        if node != leader {
            c.kill(node);
        }
    }
    let log = c.root.join(format!("{leader}.log"));
    let offset = fs::read_to_string(&log).unwrap().len();
    let mut client = WaitingClient(
        c.client(leader, 3, args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .fixture_spawn()
            .unwrap(),
    );
    let event = format!("directory proposal admitted operation={operation}\n");
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let text = fs::read_to_string(&log).unwrap();
        if text[offset..].contains(&event) {
            break;
        }
        assert!(
            client.0.try_wait().unwrap().is_none(),
            "command ended before admission: {text}"
        );
        assert!(Instant::now() < end, "missing {event}: {text}");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    // No live majority can complete the admitted write. Neither the CLI nor
    // the caller can observe success before this interruption.
    assert!(client.0.try_wait().unwrap().is_none());
    drop(client);
    c.kill(leader);
    for node in 1..=3 {
        c.start(node, "recover");
    }
}

fn checkpoint(c: &Cluster, leader: usize) {
    let status = c.ok(leader, &["status"]);
    let field = |text: &str, prefix: &str| {
        text.split_whitespace()
            .find_map(|part| part.strip_prefix(prefix))
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    let through = field(&status, "committed=");
    c.ok(leader, &["checkpoint"]);
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let status = c.ok(leader, &["status"]);
        if field(&status, "checkpoint_index=") >= through {
            return;
        }
        assert!(Instant::now() < end, "checkpoint below {through}: {status}");
        std::thread::park_timeout(Duration::from_millis(5));
    }
}

fn history(quic: bool) {
    let mut c = Cluster::new(quic);
    let original_plan = fs::read(c.root.join("plan")).unwrap();
    for node in 1..=3 {
        c.start(node, "create");
    }
    interrupt(&mut c, &["initialize"], "100");
    command(&mut c, &["initialize"]);
    interrupt(&mut c, &["publish", "101"], "101");
    command(&mut c, &["publish", "101"]);
    assert!(command(&mut c, &["initialize"])
        .1
        .contains("duplicate=true"));
    let (leader, receipt) = command(&mut c, &["publish", "101"]);
    assert!(receipt.contains("duplicate=true"));
    let before = c.lookup(leader).fixture_output().unwrap();
    assert!(
        before.status.success(),
        "{}",
        String::from_utf8_lossy(&before.stderr)
    );
    assert!(String::from_utf8_lossy(&before.stdout).contains("authority=42 "));
    if quic {
        checkpoint(&c, leader);
    }
    c.stop();
    for node in 1..=3 {
        c.start(node, "recover");
    }
    assert!(command(&mut c, &["initialize"])
        .1
        .contains("duplicate=true"));
    let (leader, receipt) = command(&mut c, &["publish", "101"]);
    assert!(receipt.contains("duplicate=true"));
    let after = c.lookup(leader).fixture_output().unwrap();
    assert!(
        after.status.success(),
        "{}",
        String::from_utf8_lossy(&after.stderr)
    );
    assert_eq!(after.stdout, before.stdout);
    assert_eq!(fs::read(c.root.join("plan")).unwrap(), original_plan);
    c.stop();
}

#[test]
fn original_directory_operations_recover_after_admitted_client_loss_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn original_directory_operations_recover_after_admitted_client_loss_quic() {
    history(true);
}
