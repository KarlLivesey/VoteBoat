// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::Cluster, Failure, Settings};
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};
use voteboat::identity::OperationId;

pub(super) struct Report {
    pub completed: usize,
    pub peak_pending: usize,
    pub frontiers: [u64; 3],
    elapsed: Duration,
    latencies: Vec<u64>,
    retained: [usize; 3],
    messages: usize,
    peak_messages: usize,
    persistence_units: usize,
}
impl Report {
    pub fn json(&self, settings: Settings) -> String {
        let p50 = self.latencies[(self.latencies.len() - 1) * 50 / 100];
        let p99 = self.latencies[(self.latencies.len() - 1) * 99 / 100];
        format!(concat!("{{\"kind\":\"public_core_scaffold\",\"comparison_gate\":\"unmatched\",",
            "\"durability\":\"volatile_exact_ticket_no_disk\",\"transport\":\"in_process_messages\",",
            "\"scheduler\":\"single_cooperative_driver\",\"latency_origin\":\"client_request_queued\",\"members\":3,\"payload_bytes\":0,",
            "\"clients\":{},\"batch\":1,\"warmup\":{},\"completed\":{},\"seconds\":{},",
            "\"operations_per_second\":{},\"request_p50_ns\":{},\"request_p99_ns\":{},",
            "\"peak_pending\":{},\"applied_frontiers\":{:?},\"retained_entries\":{:?},",
            "\"delivered_messages\":{},\"peak_message_queue\":{},\"persistence_units\":{},",
            "\"mismatches\":[\"upstream_server_workers_16\",\"no_checkpoint_purge\",\"batch_many_unsupported\"]}}"),
            settings.clients, settings.warmup, self.completed, self.elapsed.as_secs_f64(),
            self.completed as f64 / self.elapsed.as_secs_f64(), p50, p99,
            self.peak_pending, self.frontiers, self.retained, self.messages, self.peak_messages, self.persistence_units)
    }
}
struct Clients {
    pending: BTreeMap<OperationId, (usize, Instant)>,
    ready: VecDeque<(usize, Instant)>,
    per_client_completed: Vec<usize>,
    submitted: usize,
    completed: usize,
    peak_pending: usize,
    latencies: Vec<u64>,
}
impl Clients {
    fn submit(
        &mut self,
        cluster: &mut Cluster,
        settings: Settings,
        offset: usize,
    ) -> Result<(), Failure> {
        let Some((client, started)) = self.ready.pop_front() else {
            return Ok(());
        };
        if self.submitted == settings.operations {
            return Ok(());
        }
        let operation = OperationId::new((offset + self.submitted + 1) as u128).unwrap();
        cluster.propose(operation)?;
        self.pending.insert(operation, (client, started));
        self.submitted += 1;
        self.peak_pending = self.peak_pending.max(self.pending.len());
        Ok(())
    }
    fn receive(&mut self, cluster: &mut Cluster, settings: Settings) -> Result<(), Failure> {
        while let Some(operation) = cluster.take_completed() {
            let (client, started) = self
                .pending
                .remove(&operation)
                .ok_or("missing or duplicate original result")?;
            self.completed += 1;
            self.latencies
                .push(u64::try_from(started.elapsed().as_nanos())?);
            self.per_client_completed[client] += 1;
            if self.per_client_completed[client] < settings.operations / settings.clients {
                self.ready.push_back((client, Instant::now()));
            }
        }
        Ok(())
    }
}
pub(super) fn run(settings: Settings) -> Result<Report, Failure> {
    let settings = settings.validate()?;
    let mut cluster = Cluster::new(settings)?;
    for index in 0..settings.warmup {
        let operation = OperationId::new(index as u128 + 1).unwrap();
        cluster.propose(operation)?;
        cluster.drain()?;
        if cluster.take_completed() != Some(operation) || cluster.take_completed().is_some() {
            return Err("warmup did not yield its exact original result".into());
        }
    }
    let started = Instant::now();
    let mut clients = Clients {
        pending: BTreeMap::new(),
        ready: (0..settings.clients)
            .map(|client| (client, started))
            .collect(),
        per_client_completed: vec![0; settings.clients],
        submitted: 0,
        completed: 0,
        peak_pending: 0,
        latencies: Vec::with_capacity(settings.operations),
    };
    let messages = cluster.delivered;
    let persistence_units = cluster.persistence_units;
    cluster.peak_messages = 0;
    while clients.completed < settings.operations {
        clients.submit(&mut cluster, settings, settings.warmup)?;
        let progress = cluster.deliver_one()?;
        clients.receive(&mut cluster, settings)?;
        if !progress && clients.ready.is_empty() {
            return Err("outstanding original requests made no progress".into());
        }
    }
    let elapsed = started.elapsed();
    let measured_messages = cluster.delivered - messages;
    let measured_persistence_units = cluster.persistence_units - persistence_units;
    let peak_messages = cluster.peak_messages;
    cluster.drain()?;
    if !clients.pending.is_empty() || cluster.take_completed().is_some() {
        return Err("unresolved or extra original result".into());
    }
    if clients
        .per_client_completed
        .iter()
        .any(|count| *count != settings.operations / settings.clients)
    {
        return Err("client counts differ from the original closed-loop workload".into());
    }
    let frontiers = cluster.frontiers();
    let expected = (settings.warmup + settings.operations + 1) as u64;
    if frontiers != [expected; 3] {
        return Err(format!("followers did not apply full original prefix: {frontiers:?}").into());
    }
    clients.latencies.sort_unstable();
    Ok(Report {
        completed: clients.completed,
        peak_pending: clients.peak_pending,
        frontiers,
        elapsed,
        latencies: clients.latencies,
        retained: cluster.retained_entries(),
        messages: measured_messages,
        peak_messages,
        persistence_units: measured_persistence_units,
    })
}
