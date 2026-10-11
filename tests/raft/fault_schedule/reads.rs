// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::application::{read_at_barrier, Counter, ReadError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Ready {
    group: GroupIdentity,
    configuration: ConfigurationId,
    term: u64,
    request: u64,
    index: u64,
}
impl From<&ReadBarrier> for Ready {
    fn from(b: &ReadBarrier) -> Self {
        Self {
            group: b.group(),
            configuration: b.configuration(),
            term: b.term(),
            request: b.request().get(),
            index: b.index(),
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    Begin,
    Cancel,
    Serve,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Accepted,
    Value(i64),
    NoBarrier,
    Refused(RaftError),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Attempt {
    node: u64,
    request: u64,
    kind: Kind,
    outcome: Outcome,
}
impl Replay {
    pub(super) fn read_action(&mut self, id: u64, request: u64, kind: Kind) {
        let request_id = ReadRequestId::new(request).unwrap();
        let event = match kind {
            Kind::Begin => Event::Read {
                request: request_id,
            },
            Kind::Cancel => Event::CancelRead {
                request: request_id,
            },
            Kind::Serve => unreachable!("serve uses the actual application barrier"),
        };
        let before = self.point();
        let outcome = match self.cluster.replicas.get_mut(&id).unwrap().core.step(event) {
            Ok(effects) => {
                self.effects(id, effects);
                Outcome::Accepted
            }
            Err(error) => {
                match error {
                    RaftError::NotLeader if kind == Kind::Begin => {
                        assert_ne!(self.cluster.replicas[&id].core.role(), Role::Leader);
                    }
                    RaftError::Busy => assert!(self.delayed.phases().contains_key(&id)),
                    RaftError::Fenced => assert!(self.cluster.replicas[&id].core.is_fenced()),
                    RaftError::ReadInFlight | RaftError::ReadNotReady if kind == Kind::Begin => (),
                    RaftError::StaleRead => (),
                    _ => panic!("unexpected read event refusal: {error:?}"),
                }
                assert_eq!(self.point(), before);
                Outcome::Refused(error)
            }
        };
        self.read_outcomes.push(Attempt {
            node: id,
            request,
            kind,
            outcome,
        });
    }
    pub(super) fn serve_counter(&mut self, id: u64, request: u64) {
        let barrier = self.cluster.reads.get(&id).and_then(|barriers| {
            barriers
                .iter()
                .rev()
                .find(|b| b.request().get() == request)
                .copied()
        });
        let outcome = if let Some(barrier) = barrier {
            let application = crash::counter(self, id).0;
            let before = self.point();
            match read_at_barrier(
                &mut self.cluster.replicas.get_mut(&id).unwrap().core,
                &barrier,
                &application,
                (),
            ) {
                Ok(value) => Outcome::Value(value),
                Err(ReadError::Consensus(error)) => {
                    assert!(matches!(
                        error,
                        RaftError::NotLeader
                            | RaftError::Fenced
                            | RaftError::StaleRead
                            | RaftError::NotApplied
                    ));
                    assert_eq!(self.point(), before);
                    Outcome::Refused(error)
                }
                Err(error) => panic!("unexpected counter read error: {error:?}"),
            }
        } else {
            Outcome::NoBarrier
        };
        self.read_outcomes.push(Attempt {
            node: id,
            request,
            kind: Kind::Serve,
            outcome,
        });
    }
}
fn prepared() -> Replay {
    let mut run = Replay::new();
    for action in [
        Action::Campaign(1),
        Action::Pump,
        Action::TryCounter(1, 9911, 9),
        Action::Pump,
    ] {
        run.action(&action).unwrap();
    }
    run
}
fn last(run: &Replay) -> &Outcome {
    &run.read_outcomes.last().unwrap().outcome
}

#[test]
fn cached_leadership_cannot_replace_fresh_read_quorum_or_one_use() {
    let mut run = prepared();
    run.action(&Action::Read(1, 1)).unwrap();
    assert!(run.cluster.reads.is_empty());
    run.action(&Action::Pump).unwrap();
    run.action(&Action::ServeCounter(1, 1)).unwrap();
    assert_eq!(last(&run), &Outcome::Value(9));
    run.action(&Action::ServeCounter(1, 1)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::StaleRead));
    run.action(&Action::Isolate(1)).unwrap();
    run.action(&Action::Read(1, 2)).unwrap();
    run.action(&Action::Pump).unwrap();
    assert_eq!(run.cluster.reads[&1].len(), 1);
    run.action(&Action::ServeCounter(1, 2)).unwrap();
    assert_eq!(last(&run), &Outcome::NoBarrier);
    run.action(&Action::Campaign(2)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::TryCounter(2, 9912, 3)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::Heartbeat(1)).unwrap();
    run.action(&Action::Pump).unwrap();
    assert_eq!(run.cluster.reads[&1].len(), 1);
    run.action(&Action::Heal).unwrap();
    run.action(&Action::Heartbeat(2)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::Read(1, 3)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::NotLeader));
    run.action(&Action::Read(2, 1)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::ServeCounter(2, 1)).unwrap();
    assert_eq!(last(&run), &Outcome::Value(12));
}

#[test]
fn cancelled_pending_and_ready_reads_reject_old_replies_and_request_reuse() {
    let mut run = prepared();
    run.action(&Action::Read(1, 1)).unwrap();
    run.action(&Action::DeliverNewest).unwrap();
    let old = run.cluster.messages.pop_back().unwrap();
    assert!(matches!(old.rpc, Rpc::ReadAck));
    run.action(&Action::CancelRead(1, 1)).unwrap();
    run.action(&Action::Read(1, 2)).unwrap();
    run.deliver(old.clone());
    assert!(run.cluster.reads.is_empty());
    run.cluster.messages.push_back(old);
    run.action(&Action::Pump).unwrap();
    assert_eq!(run.cluster.reads[&1].len(), 1);
    assert_eq!(run.cluster.reads[&1][0].request().get(), 2);
    run.action(&Action::Read(1, 3)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::ReadInFlight));
    run.action(&Action::CancelRead(1, 2)).unwrap();
    run.action(&Action::ServeCounter(1, 2)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::StaleRead));
    run.action(&Action::Read(1, 2)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::StaleRead));
    run.action(&Action::Read(1, 3)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::ServeCounter(1, 3)).unwrap();
    assert_eq!(last(&run), &Outcome::Value(9));
}

#[test]
fn lagging_application_restart_and_fencing_cannot_reuse_read_authority() {
    let mut run = prepared();
    run.action(&Action::Read(1, 1)).unwrap();
    run.action(&Action::Pump).unwrap();
    let token = run.cluster.reads[&1][0];
    let lagging = Counter::new(4).unwrap();
    assert_eq!(
        read_at_barrier(
            &mut run.cluster.replicas.get_mut(&1).unwrap().core,
            &token,
            &lagging,
            ()
        ),
        Err(ReadError::Consensus(RaftError::NotApplied))
    );
    run.action(&Action::ServeCounter(1, 1)).unwrap();
    assert_eq!(last(&run), &Outcome::Value(9));
    run.action(&Action::Read(1, 2)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::ProcessCrash(1)).unwrap();
    run.action(&Action::ServeCounter(1, 2)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::NotLeader));
    run.action(&Action::Campaign(1)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::Read(1, 3)).unwrap();
    run.action(&Action::Pump).unwrap();
    run.action(&Action::ServeCounter(1, 2)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::StaleRead));
    run.action(&Action::ServeCounter(1, 3)).unwrap();
    assert_eq!(last(&run), &Outcome::Value(9));
    run.action(&Action::Delay(1)).unwrap();
    run.action(&Action::TryCounter(1, 9912, 1)).unwrap();
    run.action(&Action::FailSync(1)).unwrap();
    run.action(&Action::ServeCounter(1, 3)).unwrap();
    assert_eq!(last(&run), &Outcome::Refused(RaftError::Fenced));
}

#[test]
fn read_cancel_crash_and_partition_schedule_replays_exact_authority() {
    let trace = Trace {
        seed: 267,
        actions: vec![
            Action::Campaign(1),
            Action::Pump,
            Action::TryCounter(1, 9911, 9),
            Action::Pump,
            Action::Read(1, 1),
            Action::DeliverNewest,
            Action::CancelRead(1, 1),
            Action::Read(1, 2),
            Action::Pump,
            Action::ServeCounter(1, 2),
            Action::ServeCounter(1, 2),
            Action::Read(1, 3),
            Action::Pump,
            Action::ProcessCrash(1),
            Action::ServeCounter(1, 3),
            Action::Campaign(1),
            Action::Pump,
            Action::ServeCounter(1, 3),
            Action::Read(1, 4),
            Action::Isolate(1),
            Action::Pump,
            Action::ServeCounter(1, 4),
            Action::CancelRead(1, 4),
            Action::Heal,
            Action::Read(1, 5),
            Action::Pump,
            Action::ServeCounter(1, 5),
        ],
    };
    let saved = trace.encode();
    let decoded = Trace::decode(&saved).unwrap();
    let points = replay(&trace).unwrap();
    assert_eq!(points, replay(&decoded).unwrap(), "{saved}");
    let last = points.last().unwrap();
    assert_eq!(
        last.read_outcomes
            .iter()
            .filter(|a| a.outcome == Outcome::Value(9))
            .count(),
        2
    );
    assert_eq!(
        last.reads[&1].iter().map(|r| r.request).collect::<Vec<_>>(),
        vec![2, 3, 5]
    );
    eprintln!("composed read/cancel trace:\n{saved}");
}
