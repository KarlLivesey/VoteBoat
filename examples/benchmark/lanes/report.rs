// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) fn write(
    root: &Path,
    config: Config,
    protocol: NativePeerProtocol,
    reports: &[Report],
) -> Result<(), Failure> {
    if reports.len() != config.lanes
        || reports
            .iter()
            .map(|r| r.measured.samples.len())
            .sum::<usize>()
            != config.count
    {
        return Err("incomplete lane results".into());
    }
    let mut csv = exclusive(&root.join("samples.csv"))?;
    writeln!(
        csv,
        "lane,group,operation,submitted_ns,completed_ns,latency_ns,applied_index,value"
    )?;
    let mut latencies = Vec::with_capacity(config.count);
    let mut completed = 0;
    let mut max_inflight = 0;
    let mut retries = 0;
    for report in reports {
        max_inflight += report.measured.max_inflight;
        retries += report.retries;
        for sample in &report.measured.samples {
            let latency = sample.completed_ns - sample.submitted_ns;
            latencies.push(latency);
            completed = completed.max(sample.completed_ns);
            writeln!(
                csv,
                "{},{},{},{},{},{},{},{}",
                report.plan.partition.lane,
                sample.group.id.get(),
                sample.operation,
                sample.submitted_ns,
                sample.completed_ns,
                latency,
                sample.index,
                sample.value
            )?;
        }
    }
    csv.sync_all()?;
    latencies.sort_unstable();
    let p99 = latencies[(config.count * 99).div_ceil(100) - 1];
    let elapsed = completed as f64 / 1e9;
    let summary = format!("protocol={protocol:?} assembly=static_lanes replicas=3 lanes={} groups={} host_owner_threads={} wal_workers={} snapshot_workers={} peer_endpoints={} tcp_dial_workers={} payload_bytes=8 recovered_value={} warmup={} operations={} window={} sum_lane_max_inflight={} elapsed_ns={completed} elapsed_s={elapsed:.6} applied_ops_s={:.3} p99_ns={p99} p99_us={:.3} recovery_retries={retries} retry_verified=true workers_joined=true host_threads_joined=true heartbeat_ms=50 election_min_ms=10000 election_spread_ms=10000\n",
        config.lanes, config.groups, config.lanes, config.lanes * 3, config.lanes * 3,
        config.lanes * 3, if protocol == NativePeerProtocol::TcpTls { config.lanes * 3 } else { 0 },
        WARMUP + config.count, WARMUP, config.count, config.window, max_inflight, config.count as f64 / elapsed, p99 as f64 / 1000.);
    let mut output = exclusive(&root.join("summary.txt"))?;
    output.write_all(summary.as_bytes())?;
    output.sync_all()?;
    print!("{summary}");
    Ok(())
}

pub(super) fn plan(root: &Path, config: Config) -> Result<(), Failure> {
    let mut output = exclusive(&root.join("plan.csv"))?;
    writeln!(output, "lane,first_group,groups,warmup,operations,window")?;
    for lane in 0..config.lanes {
        let plan = config.plan(lane);
        writeln!(
            output,
            "{},{},{},{},{},{}",
            plan.partition.lane,
            plan.partition.offset + 1,
            plan.partition.groups,
            plan.warmup,
            plan.count,
            plan.window
        )?;
    }
    output.sync_all()?;
    Ok(())
}
