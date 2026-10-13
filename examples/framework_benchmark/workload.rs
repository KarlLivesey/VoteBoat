// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cluster::Cluster, Failure, Settings};
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};
use voteboat::{identity::OperationId, raft::Proposal};

pub(super) struct Report {
    pub completed: usize,
    pub peak_pending: usize,
    pub frontiers: [u64; 3],
    pub completed_batches: usize,
    elapsed: Duration,
    latencies: Vec<u64>,
    retained: [usize; 3],
    messages: usize,
    peak_messages: usize,
    persistence_units: usize,
}
fn percentile(sorted: &[u64], percent: usize) -> u64 {
    let rank = (sorted.len() / 100) * percent + ((sorted.len() % 100) * percent).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}
impl Report {
    pub fn json(&self, settings: Settings) -> String {
        let p50 = percentile(&self.latencies, 50);
        let p99 = percentile(&self.latencies, 99);
        format!(concat!("{{\"kind\":\"public_core_scaffold\",\"comparison_gate\":\"unmatched\",",
            "\"durability\":\"volatile_exact_ticket_no_disk\",\"transport\":\"in_process_messages\",",
            "\"scheduler\":\"single_cooperative_driver\",\"latency_origin\":\"client_request_queued\",",
            "\"latency_unit\":\"all_original_results_in_client_batch\",\"percentile\":\"nearest_rank\",\"members\":3,\"payload_bytes\":0,",
            "\"clients\":{},\"batch\":{},\"warmup_single_writes\":{},\"completed\":{},\"completed_batches\":{},\"seconds\":{},",
            "\"operations_per_second\":{},\"request_p50_ns\":{},\"request_p99_ns\":{},",
            "\"peak_pending_operations\":{},\"applied_frontiers\":{:?},\"retained_entries\":{:?},",
            "\"delivered_messages\":{},\"peak_message_queue\":{},\"persistence_units\":{},",
            "\"mismatches\":[\"upstream_server_workers_16\",\"no_checkpoint_purge\"]}}"),
            settings.clients, settings.batch, settings.warmup, self.completed, self.completed_batches, self.elapsed.as_secs_f64(),
            self.completed as f64 / self.elapsed.as_secs_f64(), p50, p99,
            self.peak_pending, self.frontiers, self.retained, self.messages, self.peak_messages, self.persistence_units)
    }
}
struct PendingBatch {
    started: Instant,
    remaining: usize,
}
struct Clients {
    pending: BTreeMap<OperationId, usize>,
    batches: Vec<Option<PendingBatch>>,
    ready: VecDeque<(usize, Instant)>,
    per_client_completed: Vec<usize>,
    submitted: usize,
    completed: usize,
    peak_pending: usize,
    latencies: Vec<u64>,
}
impl Clients {
    fn new(settings: Settings, started: Instant) -> Self {
        Self {
            pending: BTreeMap::new(),
            batches: (0..settings.clients).map(|_| None).collect(),
            ready: (0..settings.clients)
                .map(|client| (client, started))
                .collect(),
            per_client_completed: vec![0; settings.clients],
            submitted: 0,
            completed: 0,
            peak_pending: 0,
            latencies: Vec::with_capacity(settings.operations / settings.batch),
        }
    }
    fn submit(
        &mut self,
        cluster: &mut Cluster,
        settings: Settings,
        offset: usize,
    ) -> Result<(), Failure> {
        let Some((client, started)) = self.ready.pop_front() else {
            return Ok(());
        };
        if self.batches[client].is_some() || self.submitted + settings.batch > settings.operations {
            return Err("client batch admission accounting mismatch".into());
        }
        let mut proposals = Vec::with_capacity(settings.batch);
        for slot in 0..settings.batch {
            let operation = OperationId::new((offset + self.submitted + slot + 1) as u128).unwrap();
            self.pending.insert(operation, client);
            proposals.push(Proposal {
                operation,
                bytes: Vec::new(),
            });
        }
        self.batches[client] = Some(PendingBatch {
            started,
            remaining: settings.batch,
        });
        if settings.batch == 1 {
            cluster.propose(proposals.pop().unwrap().operation)?;
        } else {
            cluster.propose_batch(proposals)?;
        }
        self.submitted += settings.batch;
        self.peak_pending = self.peak_pending.max(self.pending.len());
        Ok(())
    }
    fn receive(&mut self, cluster: &mut Cluster, settings: Settings) -> Result<(), Failure> {
        while let Some(operation) = cluster.take_completed() {
            self.complete(operation, settings)?;
        }
        Ok(())
    }
    fn complete(&mut self, operation: OperationId, settings: Settings) -> Result<(), Failure> {
        let client = self
            .pending
            .remove(&operation)
            .ok_or("missing or duplicate original result")?;
        let batch = self.batches[client]
            .as_mut()
            .ok_or("original result lacks live client batch")?;
        batch.remaining = batch
            .remaining
            .checked_sub(1)
            .ok_or("too many original results")?;
        self.completed += 1;
        self.per_client_completed[client] += 1;
        if batch.remaining == 0 {
            self.latencies
                .push(u64::try_from(batch.started.elapsed().as_nanos())?);
            self.batches[client] = None;
            if self.per_client_completed[client] < settings.operations / settings.clients {
                self.ready.push_back((client, Instant::now()));
            }
        }
        Ok(())
    }
}
fn warm_up(cluster: &mut Cluster, count: usize) -> Result<(), Failure> {
    for index in 0..count {
        let operation = OperationId::new(index as u128 + 1).unwrap();
        cluster.propose(operation)?;
        cluster.drain()?;
        if cluster.take_completed() != Some(operation) || cluster.take_completed().is_some() {
            return Err("warmup did not yield its exact original result".into());
        }
    }
    Ok(())
}
pub(super) fn run(settings: Settings) -> Result<Report, Failure> {
    let settings = settings.validate()?;
    let mut cluster = Cluster::new(settings)?;
    warm_up(&mut cluster, settings.warmup)?;
    let started = Instant::now();
    let mut clients = Clients::new(settings, started);
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
    verify_finished(&clients, &mut cluster, settings)?;
    clients.latencies.sort_unstable();
    Ok(Report {
        completed: clients.completed,
        completed_batches: clients.latencies.len(),
        peak_pending: clients.peak_pending,
        frontiers: cluster.frontiers(),
        elapsed,
        latencies: clients.latencies,
        retained: cluster.retained_entries(),
        messages: measured_messages,
        peak_messages,
        persistence_units: measured_persistence_units,
    })
}
fn verify_finished(
    clients: &Clients,
    cluster: &mut Cluster,
    settings: Settings,
) -> Result<(), Failure> {
    if !clients.pending.is_empty()
        || clients.batches.iter().any(Option::is_some)
        || cluster.take_completed().is_some()
    {
        return Err("unresolved or extra original result".into());
    }
    if clients
        .per_client_completed
        .iter()
        .any(|count| *count != settings.operations / settings.clients)
    {
        return Err("client counts differ from the original closed-loop workload".into());
    }
    let expected = (settings.warmup + settings.operations + 1) as u64;
    if cluster.frontiers() != [expected; 3] {
        return Err("followers did not apply full original prefix".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nearest_rank_keeps_small_sample_tail() {
        assert_eq!(percentile(&[10], 99), 10);
        assert_eq!(percentile(&[10, 20, 30, 40], 50), 20);
        assert_eq!(percentile(&[10, 20, 30, 40], 99), 40);
    }
    #[test]
    fn partial_or_duplicate_batch_result_never_releases_client() {
        let settings = Settings {
            clients: 1,
            batch: 4,
            operations: 8,
            warmup: 0,
            ..Settings::default()
        };
        let mut clients = Clients::new(settings, Instant::now());
        clients.ready.clear();
        clients.batches[0] = Some(PendingBatch {
            started: Instant::now(),
            remaining: 4,
        });
        for id in 1..=4 {
            clients.pending.insert(OperationId::new(id).unwrap(), 0);
        }
        for id in 1..=3 {
            clients
                .complete(OperationId::new(id).unwrap(), settings)
                .unwrap();
        }
        assert!(clients.ready.is_empty());
        assert!(clients.latencies.is_empty());
        assert!(clients
            .complete(OperationId::new(1).unwrap(), settings)
            .is_err());
        assert!(clients.ready.is_empty());
        clients
            .complete(OperationId::new(4).unwrap(), settings)
            .unwrap();
        assert_eq!(clients.ready.len(), 1);
        assert_eq!(clients.latencies.len(), 1);
    }
}
