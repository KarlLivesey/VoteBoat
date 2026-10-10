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
use super::history_checker::{self as checker, Action, Call, Limits, Reply};
use super::*;
use std::io::Write;

struct Tape {
    calls: Vec<Call>,
    events: Vec<String>,
    clock: u64,
    path: PathBuf,
}
impl Tape {
    fn new(cluster: &Cluster) -> Self {
        Self {
            calls: Vec::new(),
            events: Vec::new(),
            clock: 0,
            path: cluster.root.join("history.txt"),
        }
    }
    fn event(&mut self, text: String) -> u64 {
        self.clock += 1;
        self.events.push(format!("{} {text}", self.clock));
        self.save();
        self.clock
    }
    fn save(&self) {
        fs::write(
            &self.path,
            format!("{}\n{:#?}\n", self.events.join("\n"), self.calls),
        )
        .unwrap();
    }
    fn begin(&mut self, action: Action) -> usize {
        assert!(
            self.calls.len() < 48,
            "history budget exceeded: {:?}",
            self.path
        );
        let invoked = self.event(format!("invoke {action:?}"));
        self.calls.push(Call {
            invoked,
            completed: 0,
            action,
            reply: Reply::Unknown,
        });
        self.save();
        self.calls.len() - 1
    }
    fn finish(&mut self, index: usize, text: &str) -> Reply {
        let reply = parse(text);
        self.complete(index, reply, format!("raw={text:?}"));
        reply
    }
    fn unobserved(&mut self, index: usize) {
        self.complete(
            index,
            Reply::Unknown,
            "client disconnected; no reply observed".into(),
        );
    }
    fn complete(&mut self, index: usize, reply: Reply, observation: String) {
        let completed = self.event(format!("complete call={index} {observation}"));
        self.calls[index].completed = completed;
        self.calls[index].reply = reply;
        self.save();
    }
    fn invoke(&mut self, cluster: &Cluster, leader: usize, action: Action) -> Reply {
        let index = self.begin(action);
        let args = match action {
            Action::Read => vec!["read".to_string()],
            Action::Write { operation, delta } => {
                vec!["add".into(), operation.to_string(), delta.to_string()]
            }
        };
        let args = args.iter().map(String::as_str).collect::<Vec<_>>();
        let output = cluster.request(leader, &args);
        self.finish(index, &String::from_utf8(output.stdout).unwrap())
    }
    fn retry(&mut self, cluster: &mut Cluster, action: Action) -> i64 {
        for _ in 0..8 {
            let leader = cluster.leader();
            match self.invoke(cluster, leader, action) {
                Reply::Value(value) => return value,
                Reply::Unknown | Reply::NotAdmitted => {
                    std::thread::park_timeout(Duration::from_millis(10))
                }
                result => panic!("unexpected {result:?}: {:?}", self.path),
            }
        }
        panic!("explicit bounded retry failed: {:?}", self.path);
    }
    fn check(&self) {
        let witness = checker::check(
            &self.calls,
            Limits {
                calls: 48,
                states: 100_000,
            },
        );
        println!(
            "history {:?}\n{}\n{:#?}\nwitness={witness:?}",
            self.path,
            self.events.join("\n"),
            self.calls
        );
        assert!(
            witness.is_ok(),
            "history failed: {:?}: {witness:?}",
            self.path
        );
        let mut corrupted = self.calls.clone();
        assert_eq!(corrupted.last().unwrap().action, Action::Read);
        corrupted.last_mut().unwrap().reply = Reply::Value(-1);
        assert_eq!(
            checker::check(
                &corrupted,
                Limits {
                    calls: 48,
                    states: 100_000
                }
            ),
            Err(checker::CheckError::NotLinearizable),
            "checker accepted a corrupted observed read"
        );
    }
}
fn parse(text: &str) -> Reply {
    if let Some(value) = text.strip_prefix("OK value=") {
        return Reply::Value(value.trim().parse().unwrap());
    }
    if let Some(value) = text.strip_prefix("OK outcome=Value(") {
        return Reply::Value(value.split(')').next().unwrap().parse().unwrap());
    }
    if text.starts_with("OK outcome=OperationConflict") {
        return Reply::Conflict;
    }
    if text.starts_with("OK outcome=Overflow") {
        return Reply::Overflow;
    }
    if text == "ERR NOT_LEADER\n" || text == "ERR NotRead(ReadNotReady)\n" {
        return Reply::NotAdmitted;
    }
    if text.starts_with("UNKNOWN ") {
        return Reply::Unknown;
    }
    panic!("unrecognized observed reply {text:?}");
}
fn write(operation: u128, delta: i64) -> Action {
    Action::Write { operation, delta }
}
fn kill(cluster: &mut Cluster, id: usize, tape: &mut Tape) {
    tape.event(format!("kill node={id}"));
    let mut child = cluster.children[id - 1].take().unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
}
fn unknown(
    cluster: &Cluster,
    id: usize,
    tape: &mut Tape,
    operation: u128,
    delta: i64,
) -> (usize, std::net::TcpStream) {
    let call = tape.begin(write(operation, delta));
    let mut stream =
        std::net::TcpStream::connect((Ipv4Addr::LOCALHOST, cluster.base + 100 + id as u16))
            .unwrap();
    writeln!(stream, "add {operation} {delta}").unwrap();
    (call, stream)
}
fn overlapping(cluster: &mut Cluster, tape: &mut Tape) {
    let leader = cluster.leader();
    let pending = [(2, 3), (3, 5)].map(|(operation, delta)| {
        let index = tape.begin(write(operation, delta));
        let child = {
            let _guard = fixture_gate();
            Command::new(BIN)
                .args([
                    "client",
                    &cluster.base.to_string(),
                    &leader.to_string(),
                    "add",
                    &operation.to_string(),
                    &delta.to_string(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap()
        };
        (index, child)
    });
    for (index, child) in pending {
        let result = child.wait_with_output().unwrap();
        tape.finish(index, &String::from_utf8(result.stdout).unwrap());
    }
    for (id, delta) in [(2, 3), (3, 5)] {
        tape.retry(cluster, write(id, delta));
    }
    assert_eq!(tape.retry(cluster, Action::Read), 15);
}
fn lost_reply(cluster: &mut Cluster, tape: &mut Tape) {
    let leader = cluster.leader();
    let (index, stream) = unknown(cluster, leader, tape, 4, 11);
    let mut committed = false;
    for _ in 0..8 {
        if tape.invoke(cluster, leader, Action::Read) == Reply::Value(26) {
            committed = true;
            break;
        }
        std::thread::park_timeout(Duration::from_millis(10));
    }
    assert!(
        committed,
        "unread operation did not become observable: {:?}",
        tape.path
    );
    drop(stream);
    tape.unobserved(index);
    kill(cluster, leader, tape);
    assert_eq!(tape.retry(cluster, Action::Read), 26);
    assert_eq!(tape.retry(cluster, write(4, 11)), 26);
    cluster.start(leader, "recover");
}
fn quorum_loss(cluster: &mut Cluster, tape: &mut Tape) {
    let leader = cluster.leader();
    for id in 1..=3 {
        if id != leader {
            kill(cluster, id, tape);
        }
    }
    let (index, stream) = unknown(cluster, leader, tape, 5, 13);
    drop(stream);
    tape.unobserved(index);
    kill(cluster, leader, tape);
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    let value = tape.retry(cluster, Action::Read);
    assert!(value == 26 || value == 39);
    assert_eq!(tape.retry(cluster, write(5, 13)), 39);
    assert_eq!(tape.retry(cluster, Action::Read), 39);
}
fn history(quic: bool, checkpoint: bool) {
    let mut cluster = Cluster::new();
    cluster.quic = quic;
    if quic {
        let path = cluster.root.join("quic.txt");
        fs::write(
            &path,
            (1..=3)
                .map(|id| {
                    format!(
                        "{id} 127.0.0.1:{} node{id}.voteboat.test\n",
                        cluster.base + 10 + id
                    )
                })
                .collect::<String>(),
        )
        .unwrap();
        cluster.endpoints = Some(path);
    }
    let mut tape = Tape::new(&cluster);
    tape.event(format!("scenario quic={quic} checkpoint={checkpoint}"));
    for id in 1..=3 {
        cluster.start(id, "create");
    }
    assert_eq!(tape.retry(&mut cluster, write(1, 7)), 7);
    overlapping(&mut cluster, &mut tape);
    lost_reply(&mut cluster, &mut tape);
    quorum_loss(&mut cluster, &mut tape);
    let checkpoint_node = if checkpoint {
        let leader = cluster.leader();
        cluster.ok(leader, &["checkpoint"]);
        tape.event(format!("checkpoint node={leader}"));
        Some(leader)
    } else {
        None
    };
    cluster.stop();
    if let Some(id) = checkpoint_node {
        check_drained_checkpoint(&cluster, id);
    }
    tape.event("stop all; reopen original files".into());
    for id in 1..=3 {
        cluster.start(id, "recover");
    }
    assert_eq!(tape.retry(&mut cluster, write(1, 7)), 7);
    assert_eq!(tape.retry(&mut cluster, write(4, 11)), 26);
    assert_eq!(tape.retry(&mut cluster, Action::Read), 39);
    tape.check();
    cluster.stop();
    fs::remove_dir_all(&cluster.root).unwrap();
}
#[test]
fn tcp_faulted_client_histories_are_linearizable() {
    for checkpoint in [false, true] {
        history(false, checkpoint);
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_faulted_client_histories_are_linearizable() {
    for checkpoint in [false, true] {
        history(true, checkpoint);
    }
}
