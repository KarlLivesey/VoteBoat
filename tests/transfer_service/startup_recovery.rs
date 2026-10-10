// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

struct Paused(Option<u32>);
fn signal(pid: u32, action: &str) -> bool {
    Command::new("kill")
        .args([action, &pid.to_string()])
        .status()
        .is_ok_and(|s| s.success())
}
impl Paused {
    fn new(pid: u32) -> Self {
        assert!(signal(pid, "-STOP"));
        Self(Some(pid))
    }
    fn resume(mut self) {
        assert!(signal(self.0.unwrap(), "-CONT"));
        self.0.take();
    }
}
impl Drop for Paused {
    fn drop(&mut self) {
        if let Some(pid) = self.0.take() {
            signal(pid, "-CONT");
        }
    }
}
struct Waiting(Option<Child>);
impl Drop for Waiting {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn stop(rig: &mut Cluster, group: u128, node: u16) {
    let at = rig
        .children
        .iter()
        .position(|(g, n, _)| (*g, *n) == (group, node))
        .unwrap();
    let (_, _, mut child) = rig.children.remove(at);
    child.kill().unwrap();
    child.wait().unwrap();
}

fn leadership_loss(rig: &mut Cluster, words: &[&str]) -> Result<String, (String, String)> {
    let leader = startup_discovery::leader(rig, 20, &[1, 2, 3]);
    let others = (1..=3).filter(|n| *n != leader).collect::<Vec<_>>();
    for node in &others {
        stop(rig, 20, *node);
    }
    let log = rig.root.join(format!("20-{leader}.log"));
    let offset = fs::read_to_string(&log).unwrap().len();
    let mut client = Waiting(Some(spawn(
        rig.client(leader, rig.admin, "command")
            .arg("20")
            .args(words)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    )));
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if fs::read_to_string(&log).unwrap()[offset..].contains("transfer_proposal accepted") {
            break;
        }
        assert!(client.0.as_mut().unwrap().try_wait().unwrap().is_none());
        assert!(
            Instant::now() < deadline,
            "original data proposal not admitted"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let pid = rig
        .children
        .iter()
        .find(|(g, n, _)| (*g, *n) == (20, leader))
        .unwrap()
        .2
        .id();
    let paused = Paused::new(pid);
    for node in &others {
        rig.start_one(20, 1, *node, "recover");
    }
    let replacement = startup_discovery::leader(rig, 20, &others);
    assert_ne!(replacement, leader);
    paused.resume();
    let output = client.0.take().unwrap().wait_with_output().unwrap();
    assert!(
        !output.status.success(),
        "uncommitted old-leader request completed unexpectedly"
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim_end(),
        "UNKNOWN Unknown(LeadershipChanged)"
    );
    eprintln!(
        "original source reply retained: {}",
        String::from_utf8_lossy(&output.stdout).trim_end()
    );
    initialization::response(output)
}

fn delayed_metadata(rig: &mut Cluster) {
    for node in 1..=3 {
        stop(rig, 1, node);
    }
    rig.start_one(1, 0, 1, "recover");
    let mut scans = 0;
    let deadline = Instant::now() + Duration::from_secs(15);
    let result = startup_discovery::wait(
        deadline,
        || {
            scans += 1;
            if scans == 1 {
                let first = startup_discovery::probe(rig, 1, &[1])?;
                assert_eq!(
                    first, None,
                    "one recovered metadata voter cannot elect alone"
                );
                // The established child still serves its original data without a
                // metadata quorum. This receipt is not metadata leadership evidence.
                assert!(startup_data::add(rig, "1", "1", "7", 7).contains("duplicate: true"));
                for node in 2..=3 {
                    rig.start_one(1, 0, node, "recover");
                }
                Ok(first)
            } else {
                startup_discovery::probe(rig, 1, &[1, 2, 3])
            }
        },
        Instant::now,
        || std::thread::sleep(Duration::from_millis(10)),
    );
    let leader = result.unwrap();
    assert!((1..=3).contains(&leader));
    assert!(scans > 1);
    interrupt::read(rig);
}

fn history(quic: bool) {
    let mut rig = Cluster::new(quic);
    initialization::command(&rig, 1, "initialize");
    initialization::command(&rig, 1, "grant");
    initialization::command(&rig, 20, "initialize");
    let profile = fs::read(rig.root.join("profile")).unwrap();
    let words = ["add", "1", "1", "7"];
    let mut attempts = 0;
    let receipt = startup_data::invoke(&words, 7, false, |original| {
        assert_eq!(original, words);
        attempts += 1;
        if attempts == 1 {
            leadership_loss(&mut rig, original)
        } else {
            initialization::response(rig.request(0, rig.admin, 20, original))
        }
    })
    .unwrap();
    assert!((2..=4).contains(&attempts));
    assert!(receipt.contains("Value(7)"));
    assert!(startup_data::add(&rig, "1", "1", "7", 7).contains("duplicate: true"));
    let mut conflicting_attempts = 0;
    let conflict = startup_data::invoke(&["add", "1", "1", "8"], 8, false, |original| {
        conflicting_attempts += 1;
        initialization::response(rig.request(0, rig.admin, 20, original))
    });
    assert!(conflict
        .unwrap_err()
        .contains("ERR Application(InvalidCommand)"));
    assert_eq!(conflicting_attempts, 1);
    assert!(rig.ok(20, &["read", "1"]).contains("value=7"));
    startup_data::add(&rig, "2", "200", "11", 11);
    delayed_metadata(&mut rig);
    if quic {
        cuts::support::checkpoint(&rig);
    }
    rig.crash();
    rig.start("recover");
    assert_eq!(fs::read(rig.root.join("profile")).unwrap(), profile);
    assert!(startup_data::add(&rig, "1", "1", "7", 7).contains("duplicate: true"));
    assert!(startup_data::add(&rig, "2", "200", "11", 11).contains("duplicate: true"));
    assert!(rig.operate("start").contains("OK complete"));
    finish(rig);
}
#[test]
fn tcp_original_source_unknown_and_delayed_metadata_election_recover() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_original_source_unknown_and_delayed_metadata_election_recover() {
    history(true);
}
