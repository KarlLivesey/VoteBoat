// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "reference/codec.rs"]
mod codec;
#[path = "reference/histories.rs"]
mod histories;
#[path = "reference/host.rs"]
mod host;
#[path = "reference/native_host.rs"]
mod native_host;
#[path = "reference/persistence.rs"]
mod persistence;
use host::Reference;
use voteboat::application::{read_at_barrier, Counter, StateMachine};

const MAX_ACTIONS: usize = 512;
#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Campaign(u64),
    Write(u64, u128, i64),
    Isolate(u64),
    Heal,
    Pump,
    Heartbeat(u64),
    Read(u64, u64),
    Check,
    CorruptReference(u64),
    Persistence(u64, Persistence),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Accepted,
    Written,
    Synced,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Persistence {
    Hold,
    Write,
    Sync,
    Complete,
    ProcessCrash,
    PowerLoss,
    Stale,
    LieReferenceSync,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Trace {
    seed: u64,
    actions: Vec<Action>,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Evidence {
    campaigns: usize,
    writes: usize,
    reads: usize,
    unavailable_reads: usize,
    checks: usize,
    partitions: usize,
    holds: usize,
    storage_writes: usize,
    barriers: usize,
    completions: usize,
    restarts: usize,
    stale_checks: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Mismatch {
    Leader(u64),
    Commands(u64),
    Application(u64),
    Read(u64, u64),
    Action(u64),
    NativeRecovery(u64, u128),
    ReferenceRecovery(u64, u128),
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observation {
    commands: BTreeMap<u64, Vec<(u128, i64)>>,
    values: BTreeMap<u64, i64>,
    evidence: Evidence,
}
struct Comparison {
    native: native_host::Native,
    reference: Reference,
    evidence: Evidence,
}
impl Comparison {
    fn new() -> Self {
        Self {
            native: native_host::Native::new(),
            reference: Reference::new(),
            evidence: Evidence::default(),
        }
    }
    fn pump(&mut self) -> Result<(), Mismatch> {
        self.native.pump()?;
        self.reference.pump();
        Ok(())
    }
    fn action(&mut self, action: &Action) -> Result<(), Mismatch> {
        match *action {
            Action::Campaign(id) => {
                self.native.act(id, Event::Campaign)?;
                self.reference.campaign(id);
                self.evidence.campaigns += 1;
            }
            Action::Write(id, operation, delta) => {
                // The controlled trace addresses an actual leader, including an
                // isolated old leader. Follower forwarding is not equivalent.
                if self.native.cluster.replicas[&id].core.role() != Role::Leader
                    || !self.reference.is_leader(id)
                {
                    return Err(Mismatch::Leader(id));
                }
                self.native.act(
                    id,
                    Event::Propose {
                        operation: OperationId::new(operation).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                )?;
                self.reference.write(id, operation, delta);
                self.evidence.writes += 1;
            }
            Action::Isolate(id) => {
                self.native.cluster.partition(&[id]);
                self.reference.isolate(id);
                self.evidence.partitions += 1;
            }
            Action::Heal => {
                self.native.cluster.heal();
                self.reference.heal();
            }
            Action::Pump => self.pump()?,
            Action::Heartbeat(id) => {
                self.native.act(id, Event::Heartbeat)?;
                self.reference.heartbeat(id);
            }
            Action::Read(id, request) => self.read(id, request)?,
            Action::Check => {
                self.check()?;
                self.evidence.checks += 1;
            }
            Action::CorruptReference(id) => self.reference.corrupt(id),
            Action::Persistence(id, operation) => self.persistence(id, operation)?,
        }
        self.native.cluster.assert_safety();
        self.reference.assert_safety();
        Ok(())
    }
    fn check(&self) -> Result<(), Mismatch> {
        for id in 1..=3 {
            if self.commands(id) != self.reference.commands(id) {
                return Err(Mismatch::Commands(id));
            }
            if self.counter(id).read_applied(0).unwrap() != self.reference.value(id, u64::MAX) {
                return Err(Mismatch::Application(id));
            }
        }
        Ok(())
    }
    fn commands(&self, id: u64) -> Vec<(u128, i64)> {
        self.native.cluster.applied[&id]
            .iter()
            .filter_map(|entry| match &entry.payload {
                EntryPayload::Command { operation, bytes } => Some((
                    operation.get(),
                    i64::from_le_bytes(bytes.as_slice().try_into().unwrap()),
                )),
                _ => None,
            })
            .collect()
    }
    fn counter(&self, id: u64) -> Counter {
        let mut app = Counter::new(MAX_ACTIONS).unwrap();
        app.apply_batch(&self.native.cluster.applied[&id]).unwrap();
        app
    }
    fn read(&mut self, id: u64, request: u64) -> Result<(), Mismatch> {
        if self.native.cluster.replicas[&id].core.role() != Role::Leader
            || !self.reference.is_leader(id)
        {
            return Err(Mismatch::Leader(id));
        }
        self.native.act(id, read_request(request))?;
        self.reference.read(id, request);
        self.pump()?;
        let barrier = self
            .native
            .cluster
            .reads
            .get(&id)
            .and_then(|ready| {
                ready
                    .iter()
                    .find(|barrier| barrier.request().get() == request)
            })
            .copied();
        let app = self.counter(id);
        let value = barrier.map(|barrier| {
            read_at_barrier(
                &mut self.native.cluster.replicas.get_mut(&id).unwrap().core,
                &barrier,
                &app,
                (),
            )
            .unwrap()
        });
        let other = self.reference.read_value(id, request);
        if value != other {
            return Err(Mismatch::Read(id, request));
        }
        if value.is_some() {
            self.evidence.reads += 1;
        } else {
            self.evidence.unavailable_reads += 1;
            self.native.act(
                id,
                Event::CancelRead {
                    request: ReadRequestId::new(request).unwrap(),
                },
            )?;
        }
        Ok(())
    }
    fn observation(&self) -> Observation {
        Observation {
            commands: (1..=3).map(|id| (id, self.commands(id))).collect(),
            values: (1..=3)
                .map(|id| (id, self.counter(id).read_applied(0).unwrap()))
                .collect(),
            evidence: self.evidence.clone(),
        }
    }
}
fn replay(trace: &Trace) -> Result<Observation, Mismatch> {
    assert!(trace.actions.len() <= MAX_ACTIONS);
    let mut comparison = Comparison::new();
    for action in &trace.actions {
        comparison.action(action)?;
    }
    Ok(comparison.observation())
}
