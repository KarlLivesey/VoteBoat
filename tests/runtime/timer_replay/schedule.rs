// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{fmt::Write, str::FromStr};
const MAX_ACTIONS: usize = 512;
const MAX_BYTES: usize = 65536;
#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    At(u64),
    Hold(usize),
    Partition(usize),
    Sync,
    Complete,
    Heal,
    Old,
    Write(u128, u128, i64),
    Read(u128, u64),
    CrashLeader(bool),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Trace {
    seed: u64,
    actions: Vec<Action>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Journal {
    written: Vec<u8>,
    synced: Vec<u8>,
    manifest: Option<Vec<u8>>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Point {
    now: MonoTime,
    states: Vec<GroupLog>,
    roles: Vec<Role>,
    owners: Vec<RuntimeOwner>,
    deadlines: Vec<Option<TimerToken>>,
    usage: Vec<Usage>,
    values: Vec<i64>,
    reads: Vec<Vec<i64>>,
    held: Option<(usize, VisitTicket, LogTicket, Option<Vec<LogTicket>>)>,
    messages: Vec<Vec<Message>>,
    delayed: Vec<Message>,
    journals: Vec<Journal>,
}
impl Replay {
    fn point(&self) -> Point {
        let groups = |n: &NativeNode| {
            (1..=2)
                .map(|g| n.runtime.core(group(g)).unwrap().state().clone())
                .collect::<Vec<_>>()
        };
        Point {
            now: self.now,
            states: self.nodes.iter().flat_map(groups).collect(),
            roles: self
                .nodes
                .iter()
                .flat_map(|n| (1..=2).map(|g| n.runtime.core(group(g)).unwrap().role()))
                .collect(),
            owners: self.nodes.iter().map(|n| n.runtime.owner()).collect(),
            deadlines: self
                .nodes
                .iter()
                .flat_map(|n| (1..=2).map(|g| n.runtime.deadline(group(g))))
                .collect(),
            usage: self.nodes.iter().map(|n| n.runtime.usage()).collect(),
            values: self
                .nodes
                .iter()
                .flat_map(|n| {
                    n.apps
                        .values()
                        .map(|app| app.read_applied(app.applied_index()).unwrap())
                })
                .collect(),
            reads: self.nodes.iter().map(|n| n.reads.clone()).collect(),
            held: self.held.as_ref().map(|h| {
                (
                    h.node,
                    h.visit,
                    h.ticket,
                    h.completion.as_ref().map(|c| c.tickets.clone()),
                )
            }),
            messages: self
                .nodes
                .iter()
                .map(|n| n.messages.iter().cloned().collect())
                .collect(),
            delayed: self.delayed.clone(),
            journals: self
                .images
                .iter()
                .map(|io| {
                    let d = io.0.borrow();
                    Journal {
                        written: d.log.clone(),
                        synced: d.synced.clone(),
                        manifest: d.manifest.clone(),
                    }
                })
                .collect(),
        }
    }
    fn leader(&self, g: u128) -> usize {
        self.nodes
            .iter()
            .position(|n| n.runtime.core(group(g)).unwrap().role() == Role::Leader)
            .unwrap()
    }
    fn old_messages(&mut self) {
        let old: Vec<_> = self.delayed.drain(..).take(8).collect();
        for message in old {
            for _ in 0..2 {
                self.nodes[message.to.get() as usize - 1]
                    .runtime
                    .admit(message.group, Event::Receive(message.clone()))
                    .unwrap();
            }
            self.advance(self.now.0);
        }
    }
    fn action(&mut self, action: &Action) {
        match *action {
            Action::At(ms) => self.advance(ms),
            Action::Hold(id) => self.hold_election(id),
            Action::Partition(id) => self.partition = Some(id),
            Action::Sync => self.sync(),
            Action::Complete => self.release(),
            Action::Heal => self.partition = None,
            Action::Old => self.old_messages(),
            Action::Write(g, operation, delta) => {
                let id = self.leader(g);
                self.nodes[id]
                    .runtime
                    .admit(
                        group(g),
                        Event::Propose {
                            operation: OperationId::new(operation).unwrap(),
                            bytes: delta.to_le_bytes().to_vec(),
                        },
                    )
                    .unwrap();
            }
            Action::Read(g, request) => {
                let id = self.leader(g);
                self.nodes[id]
                    .runtime
                    .admit(
                        group(g),
                        Event::Read {
                            request: ReadRequestId::new(request).unwrap(),
                        },
                    )
                    .unwrap();
            }
            Action::CrashLeader(power_loss) => {
                assert!(self.crash(self.leader(1), power_loss).is_none());
            }
        }
        for n in &self.nodes {
            let usage = n.runtime.usage();
            let limits = n.runtime.limits();
            assert!(usage.items <= limits.max_items && usage.bytes <= limits.max_bytes);
        }
    }
}
impl Trace {
    fn generated(seed: u64) -> Self {
        let stride = seed as usize % 3 + 1;
        let mut actions = vec![Action::Hold(0), Action::Partition(0)];
        actions.extend((40..=120).step_by(stride).map(Action::At));
        actions.extend([
            Action::At(120),
            Action::Write(2, 9911, 7),
            Action::At(121),
            Action::Read(2, seed),
            Action::At(122),
            Action::Sync,
            Action::Complete,
            Action::Heal,
            Action::Old,
        ]);
        actions.extend((123..=200).step_by(stride).map(Action::At));
        actions.extend([
            Action::At(200),
            Action::Write(1, 9911, 7),
            Action::At(201),
            Action::CrashLeader(seed.is_multiple_of(2)),
        ]);
        actions.extend((202..=300).step_by(stride).map(Action::At));
        actions.extend([
            Action::At(300),
            Action::Write(1, 9911, 7),
            Action::At(301),
            Action::Read(1, seed + 1000),
            Action::At(302),
        ]);
        Self { seed, actions }
    }
    fn encode(&self) -> String {
        assert!(self.seed != 0 && self.actions.len() <= MAX_ACTIONS);
        let mut text = format!("VBTIMER 1 {}\n", self.seed);
        for action in &self.actions {
            match *action {
                Action::At(ms) => writeln!(text, "at {ms}"),
                Action::Hold(id) => writeln!(text, "hold {}", id + 1),
                Action::Partition(id) => writeln!(text, "partition {}", id + 1),
                Action::Sync => writeln!(text, "sync"),
                Action::Complete => writeln!(text, "complete"),
                Action::Heal => writeln!(text, "heal"),
                Action::Old => writeln!(text, "old"),
                Action::Write(g, op, delta) => writeln!(text, "write {g} {op} {delta}"),
                Action::Read(g, request) => writeln!(text, "read {g} {request}"),
                Action::CrashLeader(power_loss) => writeln!(text, "crash {}", u8::from(power_loss)),
            }
            .unwrap();
        }
        assert!(text.len() <= MAX_BYTES);
        text
    }
    fn decode(text: &str) -> Result<Self, ()> {
        if text.len() > MAX_BYTES {
            return Err(());
        }
        let mut lines = text.lines();
        let header: Vec<_> = lines.next().ok_or(())?.split_whitespace().collect();
        let ["VBTIMER", "1", seed] = header.as_slice() else {
            return Err(());
        };
        let seed = nonzero(seed)?;
        let mut actions = Vec::new();
        for line in lines {
            if actions.len() == MAX_ACTIONS {
                return Err(());
            }
            let words: Vec<_> = line.split_whitespace().collect();
            let action = match words.as_slice() {
                ["at", ms] => {
                    let ms = number(ms)?;
                    if ms > 10000 {
                        return Err(());
                    }
                    Action::At(ms)
                }
                ["hold", id] => Action::Hold(actor(id)?),
                ["partition", id] => Action::Partition(actor(id)?),
                ["sync"] => Action::Sync,
                ["complete"] => Action::Complete,
                ["heal"] => Action::Heal,
                ["old"] => Action::Old,
                ["write", g, op, delta] => {
                    Action::Write(responsibility(g)?, nonzero(op)?, number(delta)?)
                }
                ["read", g, request] => Action::Read(responsibility(g)?, nonzero(request)?),
                ["crash", "0"] => Action::CrashLeader(false),
                ["crash", "1"] => Action::CrashLeader(true),
                _ => return Err(()),
            };
            actions.push(action);
        }
        if actions.is_empty() {
            return Err(());
        }
        Ok(Self { seed, actions })
    }
}
fn number<T: FromStr>(text: &str) -> Result<T, ()> {
    text.parse().map_err(|_| ())
}
fn nonzero<T: FromStr + Default + PartialEq>(text: &str) -> Result<T, ()> {
    let n = number(text)?;
    if n == T::default() {
        Err(())
    } else {
        Ok(n)
    }
}
fn actor(text: &str) -> Result<usize, ()> {
    let n: usize = number(text)?;
    if (1..=3).contains(&n) {
        Ok(n - 1)
    } else {
        Err(())
    }
}
fn responsibility(text: &str) -> Result<u128, ()> {
    let g: u128 = number(text)?;
    if (1..=2).contains(&g) {
        Ok(g)
    } else {
        Err(())
    }
}
fn replay(trace: &Trace) -> Vec<Point> {
    let mut run = Replay::new();
    let mut points = vec![run.point()];
    for action in &trace.actions {
        run.action(action);
        points.push(run.point());
    }
    let expected_reads = trace
        .actions
        .iter()
        .filter(|a| matches!(a, Action::Read(..)))
        .count();
    assert_eq!(
        run.nodes.iter().map(|n| n.reads.len()).sum::<usize>(),
        expected_reads
    );
    assert!(run
        .nodes
        .iter()
        .flat_map(|n| &n.reads)
        .all(|value| *value == 7));
    for n in &run.nodes {
        for app in n.apps.values() {
            assert_eq!(app.read_applied(app.applied_index()), Ok(7));
            assert_eq!(app.remaining_operations(), 9);
        }
    }
    points
}
#[test]
fn recorded_timed_owner_schedules_replay_exact_native_states_and_counter_retries() {
    for seed in 1..=32 {
        let trace = Trace::generated(seed);
        let encoded = trace.encode();
        let decoded = Trace::decode(&encoded).unwrap();
        assert_eq!(trace, decoded);
        assert_eq!(replay(&trace), replay(&decoded), "seed {seed}\n{encoded}");
        if seed == 1 {
            eprintln!("{encoded}");
        }
    }
}
#[test]
fn timed_schedule_codec_rejects_unknown_domains_and_unbounded_inputs() {
    for text in [
        "VBTIMER 2 1\nat 1\n",
        "VBTIMER 1 0\nat 1\n",
        "VBTIMER 1 1\nhold 0\n",
        "VBTIMER 1 1\npartition 4\n",
        "VBTIMER 1 1\nat 10001\n",
        "VBTIMER 1 1\nwrite 3 9911 7\n",
        "VBTIMER 1 1\nread 1 0\n",
        "VBTIMER 1 1\ncrash 2\n",
        "VBTIMER 1 1\nheal extra\n",
    ] {
        assert!(Trace::decode(text).is_err());
    }
    assert!(Trace::decode(&format!(
        "VBTIMER 1 1\n{}",
        "heal\n".repeat(MAX_ACTIONS + 1)
    ))
    .is_err());
    assert!(Trace::decode(&"x".repeat(MAX_BYTES + 1)).is_err());
}

#[test]
fn bounded_recorded_timer_file_replays_through_the_actual_owner() {
    use std::io::Read;
    let text = if let Some(path) = std::env::var_os("VOTEBOAT_TIMED_TRACE") {
        let mut text = String::new();
        std::fs::File::open(path)
            .unwrap()
            .take(MAX_BYTES as u64 + 1)
            .read_to_string(&mut text)
            .unwrap();
        text
    } else {
        Trace::generated(1).encode()
    };
    let trace = Trace::decode(&text).unwrap();
    let points = replay(&trace);
    assert_eq!(points, replay(&Trace::decode(&trace.encode()).unwrap()));
    eprintln!(
        "replayed seed={} actions={} states={}",
        trace.seed,
        trace.actions.len(),
        points.len()
    );
}
