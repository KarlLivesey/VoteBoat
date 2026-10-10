// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn field(text: &str, name: &str) -> u64 {
    text.split_whitespace()
        .find_map(|w| w.strip_prefix(name))
        .unwrap_or_else(|| panic!("missing {name}: {text}"))
        .parse()
        .unwrap()
}
fn status(rig: &Cluster, g: u128, node: u16) -> String {
    let out = rig.request(node, 3, g, &["status"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
fn wait_prefix(rig: &Cluster, g: u128, node: u16, name: &str, prefix: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s = status(rig, g, node);
        if field(&s, name) >= prefix {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{g}/{node} awaiting {name}{prefix}: {s}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
pub(in super::super) fn checkpoint(rig: &Cluster) {
    for g in GROUPS {
        let prefix = (1..=3)
            .map(|n| field(&status(rig, g, n), "committed="))
            .max()
            .unwrap();
        assert!(prefix > 0);
        for n in 1..=3 {
            wait_prefix(rig, g, n, "committed=", prefix);
            let out = rig.request(n, 3, g, &["checkpoint"]);
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            wait_prefix(rig, g, n, "checkpoint_index=", prefix);
        }
    }
}
struct WaitingClient(Child);
impl Drop for WaitingClient {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(super) fn lost_proposal(rig: &mut Cluster, g: u128, verb: &str, before: &str) {
    let payload = (verb == "publish").then(|| publication(rig));
    let mut words = vec!["transfer-step".to_owned(), verb.to_owned()];
    words.extend(payload);
    lost_command(rig, g, &words, before);
}
pub(in super::super) fn lost_command(rig: &mut Cluster, g: u128, words: &[String], before: &str) {
    let leader = (1..=3)
        .find(|n| status(rig, g, *n).contains("role=Leader"))
        .unwrap();
    for n in (1..=3).filter(|n| *n != leader) {
        let at = rig
            .children
            .iter()
            .position(|(id, node, _)| *id == g && *node == n)
            .unwrap();
        let (_, _, mut child) = rig.children.remove(at);
        child.kill().unwrap();
        child.wait().unwrap();
    }
    let log = rig.root.join(format!("{g}-{leader}.log"));
    let offset = fs::read_to_string(&log).unwrap().len();
    let mut command = rig.client(leader, 3, "command");
    command.arg(g.to_string()).args(words);
    let mut client = WaitingClient(
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let text = fs::read_to_string(&log).unwrap();
        if text[offset..].contains("transfer_proposal accepted") {
            break;
        }
        assert!(
            client.0.try_wait().unwrap().is_none(),
            "client ended before accepted {}",
            words[1]
        );
        assert!(Instant::now() < deadline, "{} not admitted", words[1]);
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(client);
    rig.crash();
    rig.start("recover");
    let next = rig.operate("status");
    if next.starts_with(&format!("OK next={before}")) {
        rig.operate("step");
    }
    eprintln!("lost {} reply recovered next={next}", words[1]);
}
fn unhex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}
fn publication(rig: &Cluster) -> String {
    use voteboat::{identity::OperationId, transfer::*};
    let profile = fs::read_to_string(rig.root.join("profile")).unwrap();
    let hex = profile
        .lines()
        .nth(1)
        .unwrap()
        .strip_prefix("intent ")
        .unwrap();
    let intent = TransferIntent::decode(&unhex(hex)).unwrap();
    let plan = TransferOperation::new(
        intent,
        OperationId::new(200).unwrap(),
        OperationId::new(201).unwrap(),
    )
    .unwrap();
    let observations = [
        (1, "intent"),
        (1, "publication"),
        (20, "source"),
        (21, "target"),
        (22, "target"),
    ]
    .into_iter()
    .map(|(g, kind)| {
        let response = rig.ok(g, &["transfer-read", kind]);
        let hex = response.trim().strip_prefix("OK observation ").unwrap();
        TransferObservation::decode_authenticated(&unhex(hex)).unwrap()
    })
    .collect::<Vec<_>>();
    let TransferAction::Publish(publication) = plan.next(&observations, &[]).unwrap() else {
        panic!("expected publication after both imports");
    };
    publication
        .encode(64 * 1024)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
