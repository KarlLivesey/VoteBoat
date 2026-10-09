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
//! Scheduled offered load: refused offers remain visible; no client retry queue.
use super::*;
#[path = "maintenance.rs"]
mod maintenance;
use maintenance::Maintenance;
pub(super) use maintenance::{config as maintenance_config, Config as MaintenanceConfig};

pub(super) fn validate(count: usize, rate: usize) -> Result<(), Failure> {
    if !(1..=100000).contains(&rate) || count == 0 || count > 50000 {
        return Err("offered count/rate out of bounds".into());
    }
    if count as u128 * 1_000_000_000 / rate as u128 > 300_000_000_000 {
        return Err("offering horizon exceeds 300 seconds".into());
    }
    Ok(())
}
struct Row {
    operation: usize,
    group: GroupIdentity,
    intended: u128,
    dispatched: u128,
    completed: u128,
    replica: usize,
    sequence: u64,
    status: &'static str,
    reason: String,
    index: u64,
    value: i64,
}
type History = (BTreeMap<GroupIdentity, i64>, BTreeMap<GroupIdentity, u64>);
struct Ledger {
    rows: Vec<Row>,
    pending: BTreeMap<(usize, u64), (ClientTicket, usize)>,
    group_pending: BTreeMap<GroupIdentity, usize>,
    max_pending: usize,
    count: usize,
    rate: usize,
    window: usize,
    groups: usize,
}
impl Ledger {
    fn new(count: usize, rate: usize, window: usize, groups: usize) -> Self {
        Self {
            rows: Vec::with_capacity(count),
            pending: BTreeMap::new(),
            group_pending: BTreeMap::new(),
            max_pending: 0,
            count,
            rate,
            window,
            groups,
        }
    }
    fn intended(&self, offset: usize) -> u128 {
        offset as u128 * 1_000_000_000 / self.rate as u128
    }
    fn horizon(&self) -> u128 {
        self.intended(self.count)
    }
    fn due(&self, now: u128) -> bool {
        self.rows.len() < self.count && self.intended(self.rows.len()) <= now
    }
    fn begin(&mut self, now: u128) -> usize {
        let offset = self.rows.len();
        let operation = WARMUP + offset + 1;
        self.rows.push(Row {
            operation,
            group: operation_group(operation, self.groups),
            intended: self.intended(offset),
            dispatched: now,
            completed: now,
            replica: 0,
            sequence: 0,
            status: "pending",
            reason: String::new(),
            index: 0,
            value: 0,
        });
        offset
    }
    fn full(&self, group: GroupIdentity) -> bool {
        self.pending.len() >= self.window
            || self.group_pending.get(&group).copied().unwrap_or(0)
                >= self.window.div_ceil(self.groups)
    }
    fn admit(&mut self, row: usize, replica: usize, ticket: ClientTicket) -> Result<(), Failure> {
        if ticket.operation.get() != self.rows[row].operation as u128
            || ticket.group != self.rows[row].group
            || self.pending.contains_key(&(replica, ticket.sequence))
            || self.full(ticket.group)
        {
            return Err("offered ticket admission scope/limit mismatch".into());
        }
        self.rows[row].replica = replica + 1;
        self.rows[row].sequence = ticket.sequence;
        self.rows[row].completed = 0;
        self.pending
            .insert((replica, ticket.sequence), (ticket, row));
        *self.group_pending.entry(ticket.group).or_default() += 1;
        self.max_pending = self.max_pending.max(self.pending.len());
        Ok(())
    }
    fn finish(
        &mut self,
        replica: usize,
        ticket: ClientTicket,
        outcome: ClientOutcome<CounterReceipt>,
        now: u128,
    ) -> Result<(), Failure> {
        let &(original, row) = self
            .pending
            .get(&(replica, ticket.sequence))
            .ok_or("untracked offered receipt")?;
        if ticket != original {
            return Err("offered receipt binding/identity mismatch".into());
        }
        self.pending.remove(&(replica, ticket.sequence));
        *self
            .group_pending
            .get_mut(&ticket.group)
            .ok_or("untracked offered group")? -= 1;
        let row = &mut self.rows[row];
        row.completed = now;
        match outcome {
            ClientOutcome::Applied { receipt, .. } => {
                let CounterOutcome::Value(value) = receipt.outcome else {
                    row.status = "invalid";
                    return Err("offered non-value receipt".into());
                };
                if receipt.operation != ticket.operation || receipt.duplicate || receipt.index == 0
                {
                    row.status = "invalid";
                    return Err("offered invalid useful receipt".into());
                }
                row.status = "applied";
                row.index = receipt.index;
                row.value = value;
            }
            ClientOutcome::NotProposed(error) => {
                row.status = "not_proposed";
                row.reason = format!("{error:?}");
            }
            ClientOutcome::Unknown(error) => {
                row.status = "unknown";
                row.reason = format!("{error:?}");
            }
        }
        Ok(())
    }
    fn count_status(&self, status: &str) -> usize {
        self.rows.iter().filter(|r| r.status == status).count()
    }
    fn write(&self, file: &mut File) -> Result<(), Failure> {
        writeln!(file, "operation,group,intended_ns,dispatched_ns,completed_ns,replica,sequence,status,reason,applied_index,value")?;
        for r in &self.rows {
            let reason = r.reason.replace([',', '\n', '\r'], "|");
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{},{},{}",
                r.operation,
                r.group.id.get(),
                r.intended,
                r.dispatched,
                r.completed,
                r.replica,
                r.sequence,
                r.status,
                reason,
                r.index,
                r.value
            )?;
        }
        file.sync_all()?;
        Ok(())
    }
    fn history(&self, warmup: &Measurement) -> Result<History, Failure> {
        if self.rows.len() != self.count
            || !self.pending.is_empty()
            || self.rows.iter().any(|r| {
                !matches!(
                    r.status,
                    "applied"
                        | "window_refused"
                        | "no_leader"
                        | "admission_refused"
                        | "not_proposed"
                )
            })
        {
            return Err(
                "unknown/pending/invalid offered outcomes invalidate successful measurement".into(),
            );
        }
        let mut expected = BTreeMap::new();
        let mut last = boundaries(warmup.samples.iter().map(|s| (s.group, s.index)));
        for sample in &warmup.samples {
            expected
                .entry(sample.group)
                .and_modify(|v: &mut i64| *v = (*v).max(sample.value))
                .or_insert(sample.value);
        }
        let mut applied = self
            .rows
            .iter()
            .filter(|r| r.status == "applied")
            .collect::<Vec<_>>();
        applied.sort_unstable_by_key(|r| (r.group, r.index));
        for r in applied {
            let value = expected
                .get_mut(&r.group)
                .ok_or("missing offered warmup group")?;
            *value += 1;
            if r.value != *value || r.index <= last[&r.group] {
                return Err("offered applied log/value history mismatch".into());
            }
            last.insert(r.group, r.index);
        }
        Ok((expected, last))
    }
}
fn outputs<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    start: &Instant,
    ledger: &mut Ledger,
) -> Result<(), Failure> {
    for (replica, n) in replicas.iter_mut().enumerate() {
        while let Some(output) = n.poll_client() {
            let now = start.elapsed().as_nanos();
            let ticket = output.ticket();
            let outcome = checked(n.complete_client(output).map_err(|e| e.reason))?;
            ledger.finish(replica, ticket, outcome, now)?;
        }
    }
    Ok(())
}
fn measure<L: LogStore + Send + 'static>(
    replicas: &mut [Replica<L>],
    clock: &Instant,
    ledger: &mut Ledger,
    totals: &mut PollTotals,
    start: &Instant,
    maintenance: &mut Maintenance,
) -> Result<(), Failure> {
    loop {
        let paused = maintenance.paused(start.elapsed().as_nanos(), replicas, totals)?;
        poll_selected(replicas, clock, totals, paused)?;
        outputs(replicas, start, ledger)?;
        for _ in 0..64 {
            let now = start.elapsed().as_nanos();
            if !ledger.due(now) {
                break;
            }
            let row = ledger.begin(now);
            let group = ledger.rows[row].group;
            if ledger.full(group) {
                ledger.rows[row].status = "window_refused";
                continue;
            }
            let Ok(replica) = leader(replicas, group) else {
                ledger.rows[row].status = "no_leader";
                continue;
            };
            let request = ClientRequest {
                group,
                operation: OperationId::new(ledger.rows[row].operation as u128).unwrap(),
                bytes: 1i64.to_le_bytes().to_vec(),
            };
            match replicas[replica].propose(request) {
                Ok(ticket) => ledger.admit(row, replica, ticket)?,
                Err(error) => {
                    ledger.rows[row].status = "admission_refused";
                    ledger.rows[row].reason = format!("{:?}", error.reason);
                }
            }
        }
        let now = start.elapsed().as_nanos();
        maintenance.step(replicas, now)?;
        if maintenance.finished()
            && ledger.rows.len() == ledger.count
            && ledger.pending.is_empty()
            && now >= ledger.horizon()
        {
            return Ok(());
        }
        if now > ledger.horizon() + 120_000_000_000 {
            return Err("offered drain timed out; pending outcomes remain uncertain".into());
        }
        std::thread::park_timeout(Duration::from_micros(100));
    }
}
fn percentile(values: &mut [u128], p: usize) -> String {
    values.sort_unstable();
    if values.is_empty() {
        "NA".into()
    } else {
        format!(
            "{:.3}",
            values[(values.len() * p).div_ceil(100) - 1] as f64 / 1000.0
        )
    }
}
pub(super) fn run<L: LogStore + Send + 'static>(
    config: RunConfig<'_>,
    rate: usize,
    clock: &Instant,
    open_cluster: impl Fn(NativeOpenMode) -> Result<(Vec<Replica<L>>, Vec<observe::Trace>), Failure>,
) -> Result<(), Failure> {
    let RunConfig {
        root,
        protocol,
        count,
        window,
        groups,
        maintenance: maintenance_config,
        ..
    } = config;
    std::fs::create_dir(root)?;
    std::fs::write(root.join("run.txt"), format!("mode=offered protocol={protocol:?} offers={count} offered_rate={rate} window={window} groups={groups} warmup={WARMUP}\n"))?;
    let mut raw = exclusive(&root.join("offers.csv"))?;
    let mut storage = Vec::new();
    let (mut replicas, traces) = open_cluster(NativeOpenMode::Create)?;
    // Any error after construction still attempts explicit worker shutdown/join.
    let preparation = (|| {
        campaign(&mut replicas, clock, groups)?;
        eprintln!("phase=warmup protocol={protocol:?} mode=offered");
        let warmup = workload(
            &mut replicas,
            clock,
            Workload {
                first: 1,
                count: WARMUP,
                window,
                groups,
                diagnostic: Some((root, "warmup")),
            },
        )?;
        let last = boundaries(warmup.samples.iter().map(|s| (s.group, s.index)));
        verify(&mut replicas, clock, WARMUP, groups, &last)?;
        let placement = (1..=groups)
            .map(|g| leader(&replicas, group_id(g)).map(|i| format!("{g}:{}", i + 1)))
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        Ok::<_, Failure>((warmup, placement))
    })();
    let (warmup, placement) = match preparation {
        Ok(w) => w,
        Err(error) => {
            let cleanup = close(replicas, clock);
            std::fs::write(
                root.join("failure.txt"),
                format!("preparation={error}; cleanup={cleanup:?}\n"),
            )?;
            return Err(error);
        }
    };
    let mut ledger = Ledger::new(count, rate, window, groups);
    let mut maintenance = Maintenance::new(maintenance_config, ledger.horizon(), groups);
    if maintenance_config.is_some() {
        std::fs::write(
            root.join("maintenance-config.txt"),
            maintenance.summary(&PollTotals::default()),
        )?;
    }
    let mut totals = PollTotals::default();
    observe::capture(&mut storage, "before_measurement", &traces);
    eprintln!("phase=offering operations={count} rate={rate} groups={groups} leaders={placement}");
    let start = Instant::now();
    let result = measure(
        &mut replicas,
        clock,
        &mut ledger,
        &mut totals,
        &start,
        &mut maintenance,
    );
    let elapsed = start.elapsed();
    observe::capture(&mut storage, "after_measurement", &traces);
    // Preserve all observed rows before validation or cleanup can fail.
    let persisted = ledger.write(&mut raw);
    let maintenance_raw = if maintenance_config.is_some() {
        maintenance.write(&mut exclusive(&root.join("maintenance.csv"))?)
    } else {
        Ok(())
    };
    let history = result
        .and(persisted)
        .and(maintenance_raw)
        .and_then(|()| maintenance.validate(&replicas, &totals))
        .and_then(|()| ledger.history(&warmup));
    let (expected, last) = match history {
        Ok(h) => h,
        Err(error) => {
            let mut cancellation_errors = Vec::new();
            for ((replica, _), (ticket, _)) in &ledger.pending {
                // Cancellation ends observation only; it never proves non-commit.
                if let Err(e) = replicas[*replica].cancel_client(*ticket) {
                    cancellation_errors.push(format!("{e:?}"));
                }
            }
            for n in &mut replicas {
                while let Some(output) = n.poll_client() {
                    if let Err(e) = n.complete_client(output) {
                        cancellation_errors.push(format!("{:?}", e.reason));
                    }
                }
            }
            let mut reclaimed = Vec::new();
            let reclaim_cleanup = until(&mut replicas, clock, |ns| {
                for n in ns.iter_mut() {
                    while let Some(e) = n.poll_reclaim() {
                        reclaimed.push(format!("{e:?}"));
                    }
                }
                Ok(ns.iter().all(|n| n.replica_usage().reclaims == 0))
            })
            .map(|_| ());
            let cleanup = close(replicas, clock);
            std::fs::write(
                root.join("failure.txt"),
                format!("measurement={error}; cancellation_errors={cancellation_errors:?}; reclaim_cleanup={reclaim_cleanup:?}; reclaim_events={reclaimed:?}; cleanup={cleanup:?}\n"),
            )?;
            return Err(error);
        }
    };
    let compacted_bases = maintenance.bases(&replicas);
    let verified = verify_expected(&mut replicas, clock, &expected, &last);
    let joined = close(replicas, clock);
    checked_stage(root, "verification", verified, joined)?;
    observe::capture(&mut storage, "after_create_join", &traces);
    eprintln!("phase=recovery-verification mode=offered");
    let (mut replicas, recovered_traces) = open_cluster(NativeOpenMode::Recover)?;
    let recovered_bases = maintenance.bases(&replicas);
    let recovered = (|| {
        maintenance.verify_recovery(&replicas, &compacted_bases)?;
        campaign(&mut replicas, clock, groups)?;
        verify_expected(&mut replicas, clock, &expected, &last)?;
        let first = warmup.samples.iter().find(|s| s.operation == 1).unwrap();
        let last_success = ledger.rows.iter().rev().find(|r| r.status == "applied");
        let (operation, value) =
            last_success
                .map(|r| (r.operation, r.value))
                .unwrap_or_else(|| {
                    let s = warmup
                        .samples
                        .iter()
                        .find(|s| s.operation == WARMUP as u128)
                        .unwrap();
                    (WARMUP, s.value)
                });
        let retries = retry(&mut replicas, clock, 1, first.value, groups)?
            + retry(&mut replicas, clock, operation, value, groups)?;
        let retry_index = boundaries(replicas.iter().flat_map(|n| {
            n.local()
                .applications
                .iter()
                .map(|(g, a)| (*g, a.applied_index()))
        }));
        verify_expected(&mut replicas, clock, &expected, &retry_index)?;
        Ok::<_, Failure>(retries)
    })();
    let joined = close(replicas, clock);
    let retries = checked_stage(root, "recovery", recovered, joined)?;
    if maintenance_config.is_some() {
        let mut bases = exclusive(&root.join("bases.csv"))?;
        writeln!(bases, "stage,replica,group,base_index")?;
        for (stage, values) in [
            ("before_close", &compacted_bases),
            ("after_recover", &recovered_bases),
        ] {
            for ((replica, group), base) in values {
                writeln!(bases, "{stage},{},{},{base}", replica + 1, group.id.get())?;
            }
        }
        bases.sync_all()?;
    }
    observe::capture(&mut storage, "after_recover_join", &recovered_traces);
    observe::write_csv(&mut exclusive(&root.join("storage.csv"))?, &storage)?;
    let horizon = ledger.horizon();
    let applied = ledger.count_status("applied");
    let during = ledger
        .rows
        .iter()
        .filter(|r| r.status == "applied" && r.completed < horizon)
        .count();
    let admitted = ledger.rows.iter().filter(|r| r.sequence > 0).count();
    let backlog = ledger
        .rows
        .iter()
        .filter(|r| r.sequence > 0 && r.dispatched < horizon && r.completed >= horizon)
        .count();
    let mut end_to_end = ledger
        .rows
        .iter()
        .filter(|r| r.status == "applied")
        .map(|r| r.completed - r.intended)
        .collect::<Vec<_>>();
    let mut service = ledger
        .rows
        .iter()
        .filter(|r| r.status == "applied")
        .map(|r| r.completed - r.dispatched)
        .collect::<Vec<_>>();
    let p99 = percentile(&mut end_to_end, 99);
    let service_p99 = percentile(&mut service, 99);
    let recovered_value: i64 = expected.values().sum();
    let admitted_during = ledger
        .rows
        .iter()
        .filter(|r| r.sequence > 0 && r.dispatched < horizon)
        .count();
    let late_decisions = ledger
        .rows
        .iter()
        .filter(|r| r.dispatched >= horizon)
        .count();
    let mut dispatch_lag = ledger
        .rows
        .iter()
        .map(|r| r.dispatched - r.intended)
        .collect::<Vec<_>>();
    let dispatch_p99 = percentile(&mut dispatch_lag, 99);
    let maintenance_summary = maintenance.summary(&totals);
    let summary = format!("mode=offered protocol={protocol:?} replicas=3 groups={groups} assembly=shared leader_placement={placement} wal_workers_per_replica=1 snapshot_workers_per_replica=1 peer_endpoints_per_replica=1 heartbeat_ms=50 election_min_ms=10000 election_spread_ms=10000 payload_bytes=8 warmup={WARMUP} offers={count} offered_rate={rate} window={window} max_inflight={} admitted={admitted} admitted_during={admitted_during} late_decisions={late_decisions} dispatch_p99_us={dispatch_p99} applied={applied} applied_during={during} applied_drain={} window_refused={} no_leader={} admission_refused={} not_proposed={} unknown=0 pending=0 horizon_ns={horizon} elapsed_ns={} drain_ns={} backlog_at_horizon={backlog} offered_ops_s={:.3} admitted_ops_s={:.3} applied_during_ops_s={:.3} applied_total_ops_s={:.3} p99_us={p99} service_p99_us={service_p99} recovered_value={recovered_value} recovery_retries={retries} retry_verified=true workers_joined=true poll_rounds={} host_poll_ms={:.3} max_host_poll_ms={:.3}{maintenance_summary}\n", ledger.max_pending, applied-during, ledger.count_status("window_refused"), ledger.count_status("no_leader"), ledger.count_status("admission_refused"), ledger.count_status("not_proposed"), elapsed.as_nanos(), elapsed.as_nanos().saturating_sub(horizon), count as f64 * 1e9 / horizon as f64, admitted as f64 / elapsed.as_secs_f64(), during as f64 * 1e9 / horizon as f64, applied as f64 / elapsed.as_secs_f64(), totals.rounds, totals.host_ns as f64/1e6, totals.max_host_ns as f64/1e6);
    let mut file = exclusive(&root.join("summary.txt"))?;
    file.write_all(summary.as_bytes())?;
    file.sync_all()?;
    print!("{summary}");
    Ok(())
}

fn checked_stage<T>(
    root: &Path,
    stage: &str,
    work: Result<T, Failure>,
    cleanup: Result<(), Failure>,
) -> Result<T, Failure> {
    if work.is_err() || cleanup.is_err() {
        std::fs::write(
            root.join("failure.txt"),
            format!(
                "stage={stage}; work_error={:?}; cleanup_error={:?}\n",
                work.as_ref().err().map(ToString::to_string),
                cleanup.as_ref().err().map(ToString::to_string)
            ),
        )?;
    }
    work.and_then(|value| cleanup.map(|()| value))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ticket(l: &Ledger, row: usize, sequence: u64) -> ClientTicket {
        ClientTicket {
            binding: ClientRouterBinding {
                owner: RuntimeOwner {
                    store: StoreBinding {
                        identity: store(1),
                        session: StoreSession::new(1).unwrap(),
                    },
                    lane: ExecutionLaneId::new(1).unwrap(),
                    generation: RuntimeGeneration::new(1).unwrap(),
                },
                generation: ClientRouterGeneration::new(1).unwrap(),
            },
            sequence,
            group: l.rows[row].group,
            operation: OperationId::new(l.rows[row].operation as u128).unwrap(),
        }
    }
    fn applied(ticket: ClientTicket, index: u64, value: i64) -> ClientOutcome<CounterReceipt> {
        ClientOutcome::Applied {
            position: ProposalPosition { index, term: 1 },
            receipt: CounterReceipt {
                operation: ticket.operation,
                index,
                outcome: CounterOutcome::Value(value),
                duplicate: false,
            },
        }
    }
    fn warmup() -> Measurement {
        Measurement {
            elapsed: Duration::ZERO,
            max_inflight: 0,
            polls: PollTotals::default(),
            samples: (1..=2)
                .map(|g| Sample {
                    group: group_id(g),
                    operation: 62 + g as u128,
                    submitted_ns: 0,
                    completed_ns: 1,
                    index: 33,
                    value: 32,
                })
                .collect(),
        }
    }
    #[test]
    fn missed_schedule_does_not_shift_intended_start_or_drop_due_ids() {
        assert!(validate(1, 0).is_err());
        assert!(validate(301, 1).is_err());
        assert!(validate(300, 1).is_ok());
        let mut l = Ledger::new(3, 3, 1, 1);
        assert_eq!(l.horizon(), 1_000_000_000);
        assert!(l.due(0));
        l.begin(0);
        assert!(!l.due(333_333_332));
        assert!(l.due(1_000_000_000));
        let second = l.begin(1_000_000_000);
        assert_eq!(l.rows[second].intended, 333_333_333);
        assert!(l.due(1_000_000_000));
        let third = l.begin(1_000_000_001);
        assert_eq!(l.rows[third].intended, 666_666_666);
        assert!(!l.due(u128::MAX));
    }
    #[test]
    fn windows_are_global_and_per_group_and_unknown_only_releases_its_exact_slot() {
        let mut l = Ledger::new(4, 4, 3, 2);
        let a = l.begin(0);
        let ta = ticket(&l, a, 1);
        l.admit(a, 0, ta).unwrap();
        let b = l.begin(250_000_000);
        let tb = ticket(&l, b, 1);
        l.admit(b, 1, tb).unwrap();
        let c = l.begin(500_000_000);
        let tc = ticket(&l, c, 2);
        l.admit(c, 0, tc).unwrap();
        assert!(l.full(group_id(1)));
        assert!(l.full(group_id(2)));
        let d = l.begin(750_000_000);
        l.rows[d].status = "window_refused";
        assert_eq!(l.max_pending, 3);
        let mut stale = ta;
        stale.binding.generation = ClientRouterGeneration::new(2).unwrap();
        assert!(l
            .finish(
                0,
                stale,
                ClientOutcome::Unknown(ClientUnknown::LeadershipChanged),
                800_000_000
            )
            .is_err());
        assert_eq!(l.pending.len(), 3);
        assert!(l
            .finish(
                2,
                ta,
                ClientOutcome::Unknown(ClientUnknown::LeadershipChanged),
                800_000_000
            )
            .is_err());
        l.finish(
            0,
            ta,
            ClientOutcome::Unknown(ClientUnknown::LeadershipChanged),
            800_000_000,
        )
        .unwrap();
        assert_eq!(l.pending.len(), 2);
        assert_eq!(l.count_status("unknown"), 1);
        assert!(!l.full(group_id(1)));
        assert!(!l.full(group_id(2)));
        assert!(l.history(&warmup()).is_err());
    }
    #[test]
    fn group_window_can_refuse_before_global_window_is_full() {
        let mut l = Ledger::new(3, 3, 4, 2);
        let a = l.begin(0);
        let ta = ticket(&l, a, 1);
        l.admit(a, 0, ta).unwrap();
        let b = l.begin(1);
        l.rows[b].status = "no_leader";
        let c = l.begin(2);
        let tc = ticket(&l, c, 2);
        l.admit(c, 0, tc).unwrap();
        assert!(l.full(group_id(1)));
        assert!(!l.full(group_id(2)));
        assert_eq!(l.pending.len(), 2);
    }
    #[test]
    fn refused_id_holes_preserve_applied_value_history_and_reject_wrong_receipts() {
        let mut l = Ledger::new(4, 4, 4, 2);
        let a = l.begin(0);
        let ta = ticket(&l, a, 1);
        l.admit(a, 0, ta).unwrap();
        let b = l.begin(1);
        l.rows[b].status = "window_refused";
        let c = l.begin(2);
        let tc = ticket(&l, c, 2);
        l.admit(c, 0, tc).unwrap();
        let d = l.begin(3);
        l.rows[d].status = "admission_refused";
        // Arrival order differs from log order; history is checked by group/index.
        l.finish(0, tc, applied(tc, 35, 34), 5).unwrap();
        l.finish(0, ta, applied(ta, 34, 33), 6).unwrap();
        let (values, last) = l.history(&warmup()).unwrap();
        assert_eq!(values[&group_id(1)], 34);
        assert_eq!(values[&group_id(2)], 32);
        assert_eq!(last[&group_id(1)], 35);
        assert_eq!(last[&group_id(2)], 33);
        l.rows[c].value = 35;
        assert!(l.history(&warmup()).is_err());
    }
    #[test]
    fn no_applied_outcomes_have_no_latency_percentile_and_not_proposed_is_known() {
        let mut l = Ledger::new(2, 2, 1, 2);
        let a = l.begin(0);
        let ta = ticket(&l, a, 1);
        l.admit(a, 0, ta).unwrap();
        l.finish(
            0,
            ta,
            ClientOutcome::NotProposed(voteboat::raft::RaftError::NotLeader),
            1,
        )
        .unwrap();
        let b = l.begin(2);
        l.rows[b].status = "no_leader";
        let (values, _) = l.history(&warmup()).unwrap();
        assert_eq!(values.values().sum::<i64>(), 64);
        assert_eq!(percentile(&mut [], 99), "NA");
        assert_eq!(percentile(&mut [1000, 5000, 3000], 99), "5.000");
    }
    #[test]
    fn stage_failure_retains_both_work_and_cleanup_errors() {
        let root =
            std::env::temp_dir().join(format!("voteboat-offered-stage-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let work: Result<(), Failure> = Err("work failed".into());
        let cleanup: Result<(), Failure> = Err("cleanup failed".into());
        assert!(checked_stage(&root, "test", work, cleanup).is_err());
        let diagnostic = std::fs::read_to_string(root.join("failure.txt")).unwrap();
        assert!(diagnostic.contains("work failed"));
        assert!(diagnostic.contains("cleanup failed"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
