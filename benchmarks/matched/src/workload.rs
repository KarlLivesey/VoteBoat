// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{openraft_engine, voteboat_engine, Failure, Settings, UPSTREAM};
use serde::Serialize;
use std::{sync::Arc, time::Instant};
use tokio::sync::Barrier;

pub(super) enum Cluster {
    Openraft(openraft_engine::Cluster),
    Voteboat(voteboat_engine::Cluster),
}
#[derive(Clone)]
enum Client {
    Openraft(openraft_engine::Client),
    Voteboat(voteboat_engine::Client),
}
impl Cluster {
    pub async fn new(engine: &str, settings: Settings) -> Result<Self, Failure> {
        if engine == "openraft" && settings.attribution {
            return Err("elapsed phase attribution is available only for voteboat".into());
        }
        match engine {
            "openraft" => Ok(Self::Openraft(
                openraft_engine::Cluster::new(settings).await?,
            )),
            "voteboat" => Ok(Self::Voteboat(
                voteboat_engine::Cluster::new(settings).await?,
            )),
            _ => Err("unknown engine".into()),
        }
    }
    fn client(&self) -> Client {
        match self {
            Self::Openraft(c) => Client::Openraft(c.client()),
            Self::Voteboat(c) => Client::Voteboat(c.client()),
        }
    }
    async fn verify(&self, settings: Settings) -> Result<Vec<u64>, Failure> {
        match self {
            Self::Openraft(c) => c.verify(settings.operations, settings.warmup).await,
            Self::Voteboat(c) => c.verify(settings.operations, settings.warmup).await,
        }
    }
    pub async fn shutdown(&mut self) -> Result<(), Failure> {
        match self {
            Self::Openraft(c) => c.shutdown().await,
            Self::Voteboat(c) => c.shutdown().await,
        }
    }
}
impl Client {
    async fn write(&self, originals: &[u128]) -> Result<usize, Failure> {
        match self {
            Self::Openraft(c) => c.write(originals).await,
            Self::Voteboat(c) => c.write(originals).await,
        }
    }
}
#[derive(Serialize)]
pub(super) struct Report {
    engine: &'static str,
    comparison_gate: &'static str,
    upstream_commit: &'static str,
    settings: Settings,
    client_workers: usize,
    server_workers: usize,
    members: usize,
    payload_bytes: usize,
    completion: &'static str,
    durability: &'static str,
    transport: &'static str,
    api_entry_batch_capacity: usize,
    append_entry_batch_capacity: usize,
    replication_entry_capacity: usize,
    primary_upstream_envelopes_preserved: bool,
    snapshot_model: &'static str,
    capacity_contract: crate::envelopes::CapacityContract,
    diagnostics: Option<Vec<voteboat_engine::Diagnostics>>,
    upstream_internal_queues: &'static str,
    completed_originals: usize,
    completed_requests: usize,
    seconds: f64,
    operations_per_second: f64,
    request_p50_ns: u64,
    request_p99_ns: u64,
    applied_frontiers: Vec<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attribution: Option<Vec<crate::attribution::Attribution>>,
}
impl Report {
    pub fn attach_attribution(&mut self, cluster: &Cluster) {
        if self.settings.attribution {
            if let Cluster::Voteboat(cluster) = cluster {
                self.attribution = Some(cluster.attribution().to_vec());
            }
        }
    }
}
struct Completed {
    originals: usize,
    latencies: Vec<u64>,
}
async fn client_loop(
    client: Client,
    id: usize,
    settings: Settings,
    start: Arc<Barrier>,
) -> Result<Completed, Failure> {
    let requests = settings.operations / settings.clients / settings.batch;
    let mut latencies = Vec::with_capacity(requests);
    let mut completed = 0;
    start.wait().await;
    for request in 0..requests {
        let first = settings.warmup + (request * settings.clients + id) * settings.batch + 1;
        let originals: Vec<_> = (first..first + settings.batch).map(|n| n as u128).collect();
        let now = Instant::now();
        completed += client.write(&originals).await?;
        latencies.push(u64::try_from(now.elapsed().as_nanos())?);
    }
    if completed != settings.operations / settings.clients {
        return Err("client completion count differs".into());
    }
    Ok(Completed {
        originals: completed,
        latencies,
    })
}
pub(super) async fn run(cluster: &mut Cluster, settings: Settings) -> Result<Report, Failure> {
    let client = cluster.client();
    for original in 1..=settings.warmup {
        if client.write(&[original as u128]).await? != 1 {
            return Err("warmup original completion differs".into());
        }
    }
    let start = Arc::new(Barrier::new(settings.clients + 1));
    let mut tasks = tokio::task::JoinSet::new();
    for id in 0..settings.clients {
        tasks.spawn(client_loop(client.clone(), id, settings, start.clone()));
    }
    let now = Instant::now();
    start.wait().await;
    let mut completed = 0;
    let mut latencies = Vec::with_capacity(settings.operations / settings.batch);
    while let Some(result) = tasks.join_next().await {
        let result = result??;
        completed += result.originals;
        latencies.extend(result.latencies);
    }
    let seconds = now.elapsed().as_secs_f64();
    if completed != settings.operations || latencies.len() != completed / settings.batch {
        return Err("global original completion count differs".into());
    }
    let frontiers = cluster.verify(settings).await?;
    latencies.sort_unstable();
    let (engine, diagnostics, capacities) = match cluster {
        Cluster::Openraft(c) => ("openraft", None, c.capacities()),
        Cluster::Voteboat(c) => {
            let limits = c.batch_limits();
            (
                "voteboat",
                Some(c.diagnostics()),
                (
                    limits.max_proposal_entries,
                    limits.max_proposal_entries,
                    limits.max_replication_entries,
                ),
            )
        }
    };
    Ok(Report {
        capacity_contract: crate::envelopes::CapacityContract::new(engine, settings, capacities)?,
        engine,
        comparison_gate: "pending_independent_validation_and_policy_review",
        upstream_commit: UPSTREAM, settings, client_workers: 1, server_workers: 16,
        members: 3, payload_bytes: 0, completion: "every_original_locally_applied_after_quorum_commit",
        durability: "volatile_no_disk", transport: "in_process_actor_messages",
        api_entry_batch_capacity: capacities.0,
        append_entry_batch_capacity: capacities.1,
        replication_entry_capacity: capacities.2,
        primary_upstream_envelopes_preserved: settings.envelopes == crate::Envelopes::UpstreamDefault,
        snapshot_model: match settings.maintenance {
            super::Maintenance::DefaultRetention => "openraft_current_snapshot_keep1000_voteboat_historical_snapshot_retained_suffix1000",
            super::Maintenance::RetainAll => "snapshots_disabled_both_retain_whole_run",
        },
        diagnostics, upstream_internal_queues: "not_exposed_by_pinned_minimal_provider",
        completed_originals: completed, completed_requests: latencies.len(), seconds,
        operations_per_second: completed as f64 / seconds,
        request_p50_ns: percentile(&latencies, 50), request_p99_ns: percentile(&latencies, 99),
        applied_frontiers: frontiers,
        attribution: None,
    })
}
fn percentile(sorted: &[u64], percent: usize) -> u64 {
    let rank = (sorted.len() / 100) * percent + ((sorted.len() % 100) * percent).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    fn settings(batch: usize, operations: usize) -> Settings {
        Settings {
            clients: 8,
            batch,
            requested_operations: operations,
            operations: 0,
            warmup: 4,
            maintenance: crate::Maintenance::DefaultRetention,
            envelopes: crate::Envelopes::UpstreamDefault,
            attribution: false,
        }
        .validate()
        .unwrap()
    }
    async fn checked_run(engine: &str, settings: Settings) -> Report {
        let mut cluster = Cluster::new(engine, settings).await.unwrap();
        let result = tokio::time::timeout(crate::TIMEOUT, run(&mut cluster, settings)).await;
        cluster.shutdown().await.unwrap();
        let mut report = result.unwrap().unwrap();
        report.attach_attribution(&cluster);
        report
    }
    fn check_attribution(report: &Report) {
        let Some(records) = &report.attribution else {
            assert!(!report.settings.attribution);
            assert!(serde_json::to_value(report)
                .unwrap()
                .get("attribution")
                .is_none());
            return;
        };
        assert!(report.settings.attribution);
        assert_eq!(records.len(), 3);
        for record in records {
            let value = serde_json::to_value(record).unwrap();
            assert!(value["scope"]
                .as_str()
                .unwrap()
                .contains("warmup_follower_drain"));
            assert!(value["clock"]
                .as_str()
                .unwrap()
                .contains("not_cpu_or_critical_path"));
            assert!(value["nesting"].as_str().unwrap().contains("do_not_sum"));
            let phases = value["phases"].as_array().unwrap();
            assert_eq!(phases.len(), 27);
            assert_eq!(
                value["checkpoint_provider_scopes"]
                    .as_array()
                    .unwrap()
                    .len(),
                4
            );
            let get = |name| phases.iter().find(|p| p["phase"] == name).unwrap();
            assert_eq!(get("actor_lifetime_inclusive")["calls"], 1);
            assert!(get("core_direct")["calls"].as_u64().unwrap() > 0);
            let append = get("store_append_nested")["calls"].as_u64().unwrap();
            assert!(append > 0);
            assert_eq!(
                append,
                get("store_barrier_nested")["calls"].as_u64().unwrap()
            );
            for phase in phases {
                assert_eq!(phase["failures"], 0);
                assert!(
                    phase["total_elapsed_ns"].as_u64().unwrap()
                        >= phase["maximum_elapsed_ns"].as_u64().unwrap()
                );
            }
            if value["replica_slot"] == 0 {
                let expected = (report.settings.operations + report.settings.warmup) as u64;
                for name in ["host_admission", "core_direct", "application", "results"] {
                    assert_eq!(get(name)["items"], expected, "{name}");
                }
            }
        }
    }
    #[tokio::test]
    async fn unavailable_upstream_attribution_is_rejected_explicitly() {
        assert!(Cluster::new(
            "openraft",
            Settings {
                attribution: true,
                ..settings(1, 128)
            }
        )
        .await
        .is_err());
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn both_engines_finish_every_original_for_single_and_four_entry_requests() {
        for engine in ["voteboat", "openraft"] {
            for attribution in [false, true] {
                if engine == "openraft" && attribution {
                    continue;
                }
                for batch in [1, 4] {
                    for envelopes in [
                        crate::Envelopes::UpstreamDefault,
                        crate::Envelopes::Common64,
                    ] {
                        let report = checked_run(
                            engine,
                            Settings {
                                envelopes,
                                attribution,
                                ..settings(batch, 128)
                            },
                        )
                        .await;
                        assert_eq!(report.completed_originals, 128);
                        assert_eq!(report.completed_requests, 128 / batch);
                        assert_eq!(report.applied_frontiers, vec![133; 3]);
                        let limits = envelopes.voteboat_limits(batch).unwrap();
                        let api = if engine == "openraft" {
                            envelopes.request_capacity()
                        } else {
                            limits.max_proposal_entries
                        };
                        assert_eq!(report.api_entry_batch_capacity, api);
                        assert_eq!(report.append_entry_batch_capacity, api);
                        let contract = serde_json::to_value(&report.capacity_contract).unwrap();
                        assert_eq!(
                            contract["api_merge_original_bound_for_fixed_request_batch"],
                            limits.max_proposal_entries
                        );
                        assert_eq!(
                            report.replication_entry_capacity,
                            limits.max_replication_entries
                        );
                        check_attribution(&report);
                        if let Some(diagnostics) = report.diagnostics {
                            let leader = serde_json::to_value(&diagnostics[0]).unwrap();
                            assert_eq!(leader["proposed_originals"], 132);
                            let widths = leader["proposal_widths"].as_array().unwrap();
                            let originals: u64 = widths
                                .iter()
                                .map(|w| {
                                    w["original_entries"].as_u64().unwrap()
                                        * w["batches"].as_u64().unwrap()
                                })
                                .sum();
                            assert_eq!(originals, 132);
                        }
                    }
                }
            }
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 16)]
    async fn actual_maintenance_keeps_original_results_and_all_follower_prefixes() {
        let settings = settings(4, 12_000);
        for engine in ["voteboat", "openraft"] {
            for attribution in [false, true] {
                if engine == "openraft" && attribution {
                    continue;
                }
                let report = checked_run(
                    engine,
                    Settings {
                        attribution,
                        ..settings
                    },
                )
                .await;
                assert_eq!(report.completed_originals, 12_000);
                assert_eq!(report.applied_frontiers, vec![12_005; 3]);
                check_attribution(&report);
                if let Some(stats) = report.diagnostics {
                    for value in stats {
                        let value = serde_json::to_value(value).unwrap();
                        assert!(value["checkpoint_count"].as_u64().unwrap() >= 2);
                        assert!(value["retained_entries"].as_u64().unwrap() < 5000);
                    }
                }
            }
        }
    }
}
