// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[path = "interrupt/observation.rs"]
mod observation;
#[path = "interrupt/recovery.rs"]
mod recovery;

fn prepared(
    rig: &Cluster,
    mut probe: impl FnMut() -> Result<Option<u16>, String>,
) -> Result<u16, String> {
    startup_discovery::wait(
        Instant::now() + Duration::from_secs(15),
        || match probe()? {
            Some(node) => observation::ready(
                node,
                initialization::response(rig.request(
                    node,
                    rig.admin,
                    1,
                    &["transfer-read", "intent"],
                )),
            ),
            None => Ok(None),
        },
        Instant::now,
        || std::thread::sleep(Duration::from_millis(10)),
    )
}

fn released(
    mut status: impl FnMut() -> Result<String, (String, String)>,
    deadline: Instant,
    now: impl FnMut() -> Instant,
    pause: impl FnMut(),
) -> Result<(), String> {
    startup_discovery::wait(deadline, || observation::released(status()), now, pause)
}

pub(super) fn read(rig: &mut Cluster) {
    // The selected role alone cannot authorize the read cut. Complete a fresh
    // quorum read at that endpoint before deliberately removing its quorum.
    let leader = prepared(rig, || startup_discovery::probe(rig, 1, &[1, 2, 3])).unwrap();
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
    released(
        || initialization::response(rig.request(leader, 3, 1, &["status"])),
        Instant::now() + Duration::from_secs(5),
        Instant::now,
        || std::thread::sleep(Duration::from_millis(1)),
    )
    .unwrap();
    for n in stopped {
        rig.start_one(1, 0, n, "recover");
    }
}
