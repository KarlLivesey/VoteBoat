// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) fn read(rig: &mut Cluster) {
    let leader = (1..=3)
        .find(|node| {
            let out = rig.request(*node, 3, 1, &["status"]);
            out.status.success() && String::from_utf8_lossy(&out.stdout).contains("role=Leader")
        })
        .expect("metadata leader");
    let stopped = (1..=3).filter(|n| *n != leader).collect::<Vec<_>>();
    for n in &stopped {
        let at = rig
            .children
            .iter()
            .position(|(g, node, _)| *g == 1 && node == n)
            .unwrap();
        let (_, _, mut child) = rig.children.remove(at);
        child.kill().unwrap();
        child.wait().unwrap();
    }
    let log_path = rig.root.join(format!("1-{leader}.log"));
    let before = fs::read_to_string(&log_path).unwrap().len();
    let mut pending = spawn(
        rig.client(leader, 3, "client")
            .arg("status")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let log = fs::read_to_string(&log_path).unwrap();
        if log[before..].contains("transfer_read accepted") {
            break;
        }
        assert!(
            pending.try_wait().unwrap().is_none(),
            "read ended before interruption"
        );
        assert!(Instant::now() < deadline, "read was not admitted");
        std::thread::sleep(Duration::from_millis(1));
    }
    pending.kill().unwrap();
    pending.wait().unwrap();
    let out = rig.request(leader, 3, 1, &["status"]);
    assert!(
        out.status.success(),
        "status after disconnect: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("pending_reads=0"));
    for n in stopped {
        rig.start_one(1, 0, n, "recover");
    }
}
