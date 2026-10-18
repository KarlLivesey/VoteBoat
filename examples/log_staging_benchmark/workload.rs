// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{checked, history, journal::*, Failure, History, Workload, WARMUP};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Instant};
use voteboat::{
    contracts::{HardState, StorageError},
    identity::*,
    log::*,
    native::log_store::NativeLogStore,
    quorum::{Limits, Policy, Tree},
};

type Store = NativeLogStore<VolatileIo>;

pub(super) struct Sample {
    pub iteration: usize,
    pub append_ns: u128,
    pub barrier_ns: u128,
    pub total_ns: u128,
    pub io: Counts,
}

fn identity() -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}

fn group(number: usize) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(number as u128).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}

pub(super) fn entry(index: usize, operation: u128) -> LogEntry {
    LogEntry {
        index: index as u64,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: operation.to_le_bytes()[..8].to_vec().into(),
        },
    }
}

fn limits(config: Workload) -> LogLimits {
    LogLimits {
        max_groups: config.unrelated + 1,
        max_entries_per_group: config.history
            + config.kind.base()
            + if config.append {
                config.iterations + WARMUP
            } else {
                0
            },
        max_wal_bytes: 16 * 1024 * 1024,
        ..LogLimits::default()
    }
}

fn bootstrap(number: usize) -> Result<LogMutation, Failure> {
    let node = NodeId::new(1).unwrap();
    Ok(LogMutation::Create(Bootstrap {
        group: group(number),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: checked(Policy::new(Tree::Voter(node), Limits::default()))?,
        voter_stores: [(node, identity())].into(),
    }))
}

fn update(state: &GroupLog, suffix: Suffix) -> LogMutation {
    LogMutation::Update(LogUpdate {
        group: state.bootstrap.group,
        expected_revision: state.revision,
        hard_state: HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: state.commit_index,
        snapshot: None,
        snapshot_membership: None,
        suffix: Some(suffix),
    })
}

fn initialize(
    store: &mut Store,
    config: Workload,
) -> Result<BTreeMap<GroupIdentity, GroupLog>, Failure> {
    let creates = (1..=config.unrelated + 1)
        .map(bootstrap)
        .collect::<Result<Vec<_>, _>>()?;
    let mut expected = BTreeMap::new();
    checked(apply_batch(&mut expected, &creates, limits(config)))?;
    let tickets = checked(store.append_batch(creates))?;
    checked(store.barrier(&tickets))?;
    for number in 1..=config.unrelated + 1 {
        let mut initial = expected[&group(number)].clone();
        initial.commit_index = (config.history + config.kind.base()) as u64 - 1;
        let mutation = update(
            &initial,
            Suffix {
                from: 1,
                entries: history::entries(&initial, config)?.into(),
            },
        );
        let mutations = vec![mutation];
        checked(apply_batch(&mut expected, &mutations, limits(config)))?;
        let tickets = checked(store.append_batch(mutations))?;
        checked(store.barrier(&tickets))?;
        if config.kind == History::Checkpoint {
            let mutations = vec![history::checkpoint(&expected[&group(number)])?];
            checked(apply_batch(&mut expected, &mutations, limits(config)))?;
            let tickets = checked(store.append_batch(mutations))?;
            checked(store.barrier(&tickets))?;
        }
        let state = checked(store.state(group(number)))?;
        if state.entries.len() != config.history
            || state.commit_index != (config.history + config.kind.base()) as u64 - 1
            || state.base_index() != config.kind.base() as u64
            || state != expected[&group(number)]
        {
            return Err("initial retained shape mismatch".into());
        }
    }
    Ok(expected)
}

fn replacement(original: &GroupLog, config: Workload, step: usize) -> LogMutation {
    let index = config.history + config.kind.base() + if config.append { step } else { 0 };
    let mut mutation = update(
        original,
        Suffix {
            from: index as u64,
            entries: (vec![entry(index, config.history as u128 + step as u128)]).into(),
        },
    );
    let LogMutation::Update(value) = &mut mutation else {
        unreachable!()
    };
    value.expected_revision = LogRevision::new(original.revision.get() + step as u64 - 1).unwrap();
    if config.commit {
        value.commit_index = index as u64;
    }
    mutation
}

fn measure(
    store: &mut Store,
    device: &Rc<RefCell<Device>>,
    mutation: LogMutation,
    iteration: usize,
) -> Result<(Sample, LogTicket), Failure> {
    let mutations = vec![mutation];
    device.borrow_mut().counts = Counts::default();
    let start = Instant::now();
    let result = store.append_batch(mutations);
    let append_ns = start.elapsed().as_nanos();
    let tickets = checked(result)?;
    let barrier_start = Instant::now();
    let result = store.barrier(&tickets);
    let barrier_ns = barrier_start.elapsed().as_nanos();
    let total_ns = start.elapsed().as_nanos();
    let durable = checked(result)?;
    let io = device.borrow().counts;
    if tickets.len() != 1
        || durable.tickets != tickets
        || io.appends != 1
        || io.syncs != 1
        || io.publications != 1
        || io.wal_bytes == 0
        || io.manifest_bytes != 52
    {
        return Err("encoded append/simulated barrier scope mismatch".into());
    }
    Ok((
        Sample {
            iteration,
            append_ns,
            barrier_ns,
            total_ns,
            io,
        },
        tickets[0],
    ))
}

fn verify_states(
    store: &Store,
    expected: &BTreeMap<GroupIdentity, GroupLog>,
) -> Result<(), Failure> {
    for (group, state) in expected {
        if checked(store.state(*group))? != *state {
            return Err("exact retained group state mismatch".into());
        }
    }
    Ok(())
}

fn verify_recovery(
    device: Rc<RefCell<Device>>,
    config: Workload,
    old_binding: StoreBinding,
    old_ticket: LogTicket,
    expected: &BTreeMap<GroupIdentity, GroupLog>,
) -> Result<(), Failure> {
    device.borrow_mut().power_loss();
    let mut recovered = checked(NativeLogStore::recover(
        VolatileIo(device),
        identity(),
        limits(config),
    ))?;
    let binding = recovered.binding();
    if binding.identity != old_binding.identity
        || binding.session.get() != old_binding.session.get() + 1
        || recovered.barrier(&[old_ticket]) != Err(StorageError::StaleTicket)
    {
        return Err("restart identity/session/stale-ticket contract mismatch".into());
    }
    verify_states(&recovered, expected)
}

pub(super) fn run(config: Workload) -> Result<Vec<Sample>, Failure> {
    let config = config.validate()?;
    let capacity = (config.unrelated + 1) * (config.history * 64 + 512)
        + (config.iterations + WARMUP) * 512
        + 4096;
    let device = Rc::new(RefCell::new(Device::new(
        limits(config).max_wal_bytes,
        capacity,
    )));
    let mut store = checked(NativeLogStore::create(
        VolatileIo(device.clone()),
        identity(),
        limits(config),
    ))?;
    let binding = store.binding();
    let mut expected = initialize(&mut store, config)?;
    let original = &expected[&group(1)];
    let mut samples = Vec::with_capacity(config.iterations);
    let mut last_ticket = None;
    for step in 1..=config.iterations + WARMUP {
        let mutation = replacement(original, config, step);
        let (sample, ticket) = measure(&mut store, &device, mutation, step - 1)?;
        if ticket.binding != binding
            || ticket.group != group(1)
            || ticket.revision.get() != original.revision.get() + step as u64
            || ticket.generation.get()
                != original.generation.get() + if config.append { 0 } else { step as u64 }
            || ticket.last_index
                != original.last_index() + if config.append { step as u64 } else { 0 }
            || ticket.term != 1
        {
            return Err("original scoped transition ticket mismatch".into());
        }
        if step > WARMUP {
            samples.push(Sample {
                iteration: step - WARMUP - 1,
                ..sample
            });
        }
        last_ticket = Some(ticket);
    }
    let steps = config.iterations + WARMUP;
    // Full public validator is an independent oracle; exclude its history work
    // from timed provider transitions and compare the complete final state.
    let original = original.clone();
    for step in 1..=steps {
        let mutation = replacement(&original, config, step);
        checked(apply_batch(&mut expected, &[mutation], limits(config)))?;
    }
    verify_states(&store, &expected)?;
    drop(store);
    verify_recovery(device, config, binding, last_ticket.unwrap(), &expected)?;
    Ok(samples)
}
