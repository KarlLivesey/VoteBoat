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
//! Benchmark-only bounded checkpoint/reclaim waves and follower host-poll pause.
use super::*;
use voteboat::{
    snapshot::SnapshotRef,
    snapshot_worker::SnapshotWorker,
    worker::{ReclaimEvent, ReclaimTicket},
};

#[derive(Clone, Copy)]
pub(crate) struct Config {
    period: u128,
    pause: Option<(u128, u128)>,
}
pub(crate) fn config(
    count: usize,
    rate: usize,
    seconds: usize,
    pause: Option<&str>,
) -> Result<Config, Failure> {
    validate(count, rate)?;
    if !(1..=60).contains(&seconds) {
        return Err("maintenance period out of bounds".into());
    }
    let horizon = count as u128 * 1_000_000_000 / rate as u128;
    let period = seconds as u128 * 1_000_000_000;
    if period >= horizon {
        return Err("maintenance period must precede offering horizon".into());
    }
    let pause = if let Some(p) = pause {
        let (start, duration) = p
            .split_once(':')
            .ok_or("pause requires START:DURATION seconds")?;
        let start = start.parse::<u128>()?;
        let duration = duration.parse::<u128>()?;
        if start == 0 || start > 300 || !(1..=5).contains(&duration) {
            return Err("pause start/duration out of bounds".into());
        }
        let end = (start + duration) * 1_000_000_000;
        let start = start * 1_000_000_000;
        if end >= horizon || (end - 1) / period < start.div_ceil(period) {
            return Err(
                "pause must end before horizon and include a maintenance opportunity".into(),
            );
        }
        Some((start, end))
    } else {
        None
    };
    Ok(Config { period, pause })
}
struct Row {
    wave: usize,
    kind: &'static str,
    group: GroupIdentity,
    replica: usize,
    intended: u128,
    submitted: u128,
    completed: u128,
    status: &'static str,
    previous: u64,
    requested: u64,
    observed: u64,
    snapshot: u128,
    sequence: u64,
    before: usize,
    after: usize,
    reason: String,
}
impl Row {
    fn new(
        wave: usize,
        kind: &'static str,
        group: GroupIdentity,
        replica: usize,
        intended: u128,
        submitted: u128,
    ) -> Self {
        Self {
            wave,
            kind,
            group,
            replica,
            intended,
            submitted,
            completed: 0,
            status: "pending",
            previous: 0,
            requested: 0,
            observed: 0,
            snapshot: 0,
            sequence: 0,
            before: 0,
            after: 0,
            reason: String::new(),
        }
    }
}
struct Checkpoint {
    row: usize,
    leader: usize,
    group: GroupIdentity,
    binding: StoreBinding,
    term: u64,
}
#[derive(Clone, Copy)]
struct Forced {
    group: GroupIdentity,
    index: u64,
    leader: usize,
    binding: StoreBinding,
    term: u64,
}
#[derive(Clone, Copy)]
struct FollowerProgress {
    base: u64,
    last: u64,
    committed: u64,
    applied: u64,
    value: Option<i64>,
}
impl std::fmt::Debug for FollowerProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FollowerProgress")
            .field("base", &self.base)
            .field("last", &self.last)
            .field("committed", &self.committed)
            .field("applied", &self.applied)
            .field("value", &self.value)
            .finish()
    }
}
impl FollowerProgress {
    fn capture<L: LogStore + Send + 'static>(replica: &Replica<L>, group: GroupIdentity) -> Self {
        let local = replica.local();
        let state = local.owner.core(group).unwrap().state();
        let application = &local.applications[&group];
        Self {
            base: state.base_index(),
            last: state.last_index(),
            committed: state.commit_index,
            applied: application.applied_index(),
            value: application.read_applied(application.applied_index()).ok(),
        }
    }
}
fn follower_progress<L: LogStore + Send + 'static>(
    replicas: &[Replica<L>],
) -> BTreeMap<GroupIdentity, FollowerProgress> {
    replicas.get(2).map_or_else(BTreeMap::new, |replica| {
        replica
            .local()
            .applications
            .keys()
            .map(|&group| (group, FollowerProgress::capture(replica, group)))
            .collect()
    })
}
enum Active {
    Checkpoint(Checkpoint),
    Reclaims(BTreeMap<usize, (ReclaimTicket, usize)>),
}
pub(super) struct Maintenance {
    config: Option<Config>,
    horizon: u128,
    groups: usize,
    next: usize,
    rows: Vec<Row>,
    max_rows: usize,
    active: Option<Active>,
    pause_prefix: BTreeMap<GroupIdentity, u64>,
    actual_pause: Option<u128>,
    actual_resume: Option<u128>,
    installs_at_resume: usize,
    forced: Option<Forced>,
    follower_at_pause: BTreeMap<GroupIdentity, FollowerProgress>,
    follower_at_resume: BTreeMap<GroupIdentity, FollowerProgress>,
}
impl Maintenance {
    pub(super) fn new(config: Option<Config>, horizon: u128, groups: usize) -> Self {
        let max_rows = config.map_or(0, |c| ((horizon / c.period + 1) * 4 + 2) as usize);
        Self {
            config,
            horizon,
            groups,
            next: 1,
            rows: Vec::with_capacity(max_rows),
            max_rows,
            active: None,
            pause_prefix: BTreeMap::new(),
            actual_pause: None,
            actual_resume: None,
            installs_at_resume: 0,
            forced: None,
            follower_at_pause: BTreeMap::new(),
            follower_at_resume: BTreeMap::new(),
        }
    }
    fn push(&mut self, row: Row) -> Result<usize, Failure> {
        if self.rows.len() >= self.max_rows {
            return Err("maintenance event bound exceeded".into());
        }
        let index = self.rows.len();
        self.rows.push(row);
        Ok(index)
    }
    pub(super) fn paused<L: LogStore + Send + 'static>(
        &mut self,
        now: u128,
        replicas: &[Replica<L>],
        totals: &PollTotals,
    ) -> Result<Option<usize>, Failure> {
        let Some((start, end)) = self.config.and_then(|c| c.pause) else {
            return Ok(None);
        };
        if self.actual_pause.is_none() && now >= start {
            if now >= end || now + (end - start) >= self.horizon {
                return Err("follower pause schedule missed".into());
            }
            for g in 1..=self.groups {
                let group = group_id(g);
                let i = leader(replicas, group)?;
                if i == 2 {
                    return Err("replica 3 is not a follower in every group".into());
                }
                self.pause_prefix.insert(
                    group,
                    replicas[i]
                        .local()
                        .owner
                        .core(group)
                        .unwrap()
                        .state()
                        .last_index(),
                );
            }
            self.actual_pause = Some(now);
            self.follower_at_pause = follower_progress(replicas);
            let mut row = Row::new(0, "pause", group_id(1), 2, start, now);
            row.completed = now;
            row.status = "completed";
            self.push(row)?;
        }
        let actual_end = self.actual_pause.map(|pause| pause + (end - start));
        if self.actual_resume.is_none() && actual_end.is_some_and(|end| now >= end) {
            self.actual_resume = Some(now);
            self.follower_at_resume = follower_progress(replicas);
            self.installs_at_resume = totals.snapshot_installs[2];
            let mut row = Row::new(0, "resume", group_id(1), 2, end, now);
            row.completed = now;
            row.status = "completed";
            self.push(row)?;
        }
        Ok(
            if self.actual_pause.is_some() && self.actual_resume.is_none() {
                Some(2)
            } else {
                None
            },
        )
    }
    pub(super) fn idle(&self) -> bool {
        self.active.is_none()
    }
    pub(super) fn finished(&self) -> bool {
        self.idle()
            && self
                .config
                .is_none_or(|c| self.next as u128 * c.period >= self.horizon)
    }
    pub(super) fn step<L: LogStore + Send + 'static>(
        &mut self,
        replicas: &mut [Replica<L>],
        now: u128,
    ) -> Result<(), Failure> {
        let Some(config) = self.config else {
            return Ok(());
        };
        self.complete_active(replicas, now, config)?;
        // At most one periodic opportunity per reactor turn; never queue missed work.
        let intended = self.next as u128 * config.period;
        if intended >= self.horizon || now < intended {
            return Ok(());
        }
        let wave = self.next;
        self.next += 1;
        let mut group = group_id((wave - 1) % self.groups + 1);
        let paused = self.actual_pause.is_some() && self.actual_resume.is_none();
        if paused && self.forced.is_none() {
            if let Some(g) = (1..=self.groups).map(group_id).find(|g| {
                leader(replicas, *g).is_ok_and(|i| {
                    replicas[i].local().applications[g].applied_index() > self.pause_prefix[g]
                })
            }) {
                group = g;
            } else {
                self.skip(wave, group, intended, now, "awaiting_new_prefix")?;
                return Ok(());
            }
        }
        if self.active.is_some() {
            self.skip(wave, group, intended, now, "busy")?;
            return Ok(());
        }
        if now >= self.horizon {
            self.skip(wave, group, intended, now, "late")?;
            return Ok(());
        }
        let i = leader(replicas, group)?;
        let local = replicas[i].local();
        let core = local.owner.core(group).unwrap();
        let previous = core.state().base_index();
        let requested = local.applications[&group].applied_index();
        if requested <= previous {
            self.skip(wave, group, intended, now, "no_new_applied_prefix")?;
            return Ok(());
        }
        let (binding, term) = (core.storage_binding(), core.state().hard_state.term);
        let mut row = Row::new(wave, "checkpoint", group, i, intended, now);
        row.previous = previous;
        row.requested = requested;
        checked(replicas[i].control(group, NodeControl::Checkpoint))?;
        let index = self.push(row)?;
        self.active = Some(Active::Checkpoint(Checkpoint {
            row: index,
            leader: i,
            group,
            binding,
            term,
        }));
        Ok(())
    }
    fn complete_active<L: LogStore + Send + 'static>(
        &mut self,
        replicas: &mut [Replica<L>],
        now: u128,
        config: Config,
    ) -> Result<(), Failure> {
        if let Some(active) = self.active.take() {
            match active {
                Active::Checkpoint(p) => {
                    let n = &replicas[p.leader];
                    let local = n.local();
                    let core = local.owner.core(p.group).unwrap();
                    let observed = checkpoint_reference(
                        &p,
                        &self.rows[p.row],
                        Current {
                            binding: core.storage_binding(),
                            term: core.state().hard_state.term,
                            leader: core.role() == voteboat::raft::Role::Leader,
                            base: core.state().base_index(),
                            reference: core.state().snapshot,
                            drained: local
                                .snapshots
                                .as_ref()
                                .is_some_and(|s| s.router.is_drained() && s.worker.is_drained()),
                        },
                    )?;
                    if let Some(reference) = observed {
                        let base = reference.index;
                        let row = &mut self.rows[p.row];
                        row.observed = base;
                        row.snapshot = reference.generation.get() as u128;
                        row.completed = now;
                        row.status = "completed";
                        if config.pause.is_some()
                            && self.actual_pause.is_some()
                            && self.actual_resume.is_none()
                            && base > self.pause_prefix[&p.group]
                            && row.requested > self.pause_prefix[&p.group]
                            && row.submitted >= self.actual_pause.unwrap()
                        {
                            self.forced.get_or_insert(Forced {
                                group: p.group,
                                index: base,
                                leader: p.leader,
                                binding: p.binding,
                                term: p.term,
                            });
                        }
                        let (wave, intended) = (row.wave, row.intended);
                        let mut pending = BTreeMap::new();
                        for (i, n) in replicas.iter_mut().enumerate() {
                            let mut row = Row::new(wave, "reclaim", p.group, i, intended, now);
                            match n.reclaim(LogLimits::default().max_wal_bytes) {
                                Ok(ticket) => {
                                    row.sequence = ticket.sequence;
                                    let index = self.push(row)?;
                                    pending.insert(i, (ticket, index));
                                }
                                Err(error) => {
                                    row.status = "failed";
                                    row.completed = now;
                                    row.reason = format!("{error:?}");
                                    self.push(row)?;
                                    return Err(
                                        format!("maintenance admission failed: {error:?}").into()
                                    );
                                }
                            }
                        }
                        self.active = Some(Active::Reclaims(pending));
                    } else {
                        self.active = Some(Active::Checkpoint(p));
                    }
                }
                Active::Reclaims(mut pending) => {
                    for (i, n) in replicas.iter_mut().enumerate() {
                        if let Some(event) = n.poll_reclaim() {
                            let &(ticket, index) =
                                pending.get(&i).ok_or("unexpected reclaim completion")?;
                            complete_reclaim(&mut self.rows[index], ticket, event, now)?;
                            pending.remove(&i);
                        }
                    }
                    if !pending.is_empty() {
                        self.active = Some(Active::Reclaims(pending));
                    }
                }
            }
        }
        Ok(())
    }
    fn skip(
        &mut self,
        wave: usize,
        group: GroupIdentity,
        intended: u128,
        now: u128,
        reason: &str,
    ) -> Result<(), Failure> {
        let mut row = Row::new(wave, "skip", group, 0, intended, now);
        row.completed = now;
        row.status = "skipped";
        row.reason = reason.into();
        self.push(row)?;
        Ok(())
    }
    pub(super) fn validate<L: LogStore + Send + 'static>(
        &self,
        replicas: &[Replica<L>],
        totals: &PollTotals,
    ) -> Result<(), Failure> {
        if self.config.is_none() {
            return Ok(());
        }
        if !self.finished()
            || self
                .rows
                .iter()
                .any(|r| r.status == "pending" || r.status == "failed")
        {
            return Err("unfinished/failed maintenance".into());
        }
        if self
            .rows
            .iter()
            .all(|r| r.kind != "checkpoint" || r.status != "completed")
        {
            return Err("no completed maintenance checkpoint".into());
        }
        if self.config.is_some_and(|c| c.pause.is_some()) {
            let proof = self
                .forced
                .ok_or("no forced post-pause checkpoint prefix")?;
            let source = replicas[proof.leader]
                .local()
                .owner
                .core(proof.group)
                .unwrap();
            let follower_base = replicas[2]
                .local()
                .owner
                .core(proof.group)
                .unwrap()
                .state()
                .base_index();
            if self.actual_resume.is_none()
                || totals.snapshot_installs[2] <= self.installs_at_resume
                || source.storage_binding() != proof.binding
                || source.state().hard_state.term != proof.term
                || source.role() != voteboat::raft::Role::Leader
                || follower_base < proof.index
            {
                return Err(format!(
                    "paused follower lacks verified same-leader snapshot catch-up: resume={:?} installs_at_resume={} installs_now={} forced_group={:?} forced_index={} follower_base={} source_binding_expected={:?} source_binding_now={:?} source_term_expected={} source_term_now={} source_role={:?} follower_before_pause={:?} follower_before_resume={:?} follower_now={:?}",
                    self.actual_resume, self.installs_at_resume, totals.snapshot_installs[2],
                    proof.group, proof.index, follower_base, proof.binding,
                    source.storage_binding(), proof.term, source.state().hard_state.term,
                    source.role(), self.follower_at_pause.get(&proof.group),
                    self.follower_at_resume.get(&proof.group),
                    FollowerProgress::capture(&replicas[2], proof.group),
                ).into());
            }
        }
        Ok(())
    }
    pub(super) fn bases<L: LogStore + Send + 'static>(
        &self,
        replicas: &[Replica<L>],
    ) -> BTreeMap<(usize, GroupIdentity), u64> {
        replicas
            .iter()
            .enumerate()
            .flat_map(|(i, n)| {
                (1..=self.groups).map(move |g| {
                    (
                        (i, group_id(g)),
                        n.local()
                            .owner
                            .core(group_id(g))
                            .unwrap()
                            .state()
                            .base_index(),
                    )
                })
            })
            .collect()
    }
    pub(super) fn verify_recovery<L: LogStore + Send + 'static>(
        &self,
        replicas: &[Replica<L>],
        bases: &BTreeMap<(usize, GroupIdentity), u64>,
    ) -> Result<(), Failure> {
        for (&(i, g), &base) in bases {
            if replicas[i]
                .local()
                .owner
                .core(g)
                .unwrap()
                .state()
                .base_index()
                < base
            {
                return Err("recovered checkpoint base regressed".into());
            }
        }
        Ok(())
    }
    pub(super) fn write(&self, file: &mut File) -> Result<(), Failure> {
        writeln!(file,"wave,kind,group,replica,intended_ns,submitted_ns,completed_ns,status,previous_base,requested_index,observed_base,snapshot_generation,ticket_sequence,before_bytes,after_bytes,reason")?;
        for r in &self.rows {
            writeln!(
                file,
                "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                r.wave,
                r.kind,
                r.group.id.get(),
                r.replica + 1,
                r.intended,
                r.submitted,
                r.completed,
                r.status,
                r.previous,
                r.requested,
                r.observed,
                r.snapshot,
                r.sequence,
                r.before,
                r.after,
                r.reason.replace([',', '\n', '\r'], "|")
            )?;
        }
        file.sync_all()?;
        Ok(())
    }
    pub(super) fn summary(&self, totals: &PollTotals) -> String {
        let Some(c) = self.config else {
            return String::new();
        };
        let count = |kind| {
            self.rows
                .iter()
                .filter(|r| r.kind == kind && r.status == "completed")
                .count()
        };
        let reclaimed: i128 = self
            .rows
            .iter()
            .filter(|r| r.kind == "reclaim" && r.status == "completed")
            .map(|r| r.before as i128 - r.after as i128)
            .sum();
        let (pause_start, pause_end) = c.pause.unwrap_or((0, 0));
        let proof = self.forced;
        let group = proof.map_or(group_id(1), |p| p.group);
        let index = proof.map_or(0, |p| p.index);
        let forced_prior_last = self.pause_prefix.get(&group).copied().unwrap_or(0);
        let forced_leader = proof.map_or(0, |p| p.leader + 1);
        let forced_term = proof.map_or(0, |p| p.term);
        format!(" maintenance_period_ns={} checkpoints={} reclaims={} maintenance_skips={} reclaimed_bytes={reclaimed} snapshot_events={} snapshot_supplies={} follower_snapshot_installs={} pause_start_ns={pause_start} pause_end_ns={pause_end} actual_pause_ns={} actual_resume_ns={} forced_group={} forced_index={index} forced_prior_last={forced_prior_last} forced_leader={forced_leader} forced_term={forced_term} installs_at_resume={}",c.period,count("checkpoint"),count("reclaim"),self.rows.iter().filter(|r|r.kind=="skip").count(),totals.snapshot_events,totals.snapshot_supplies,totals.snapshot_installs[2],self.actual_pause.unwrap_or(0),self.actual_resume.unwrap_or(0),group.id.get(),self.installs_at_resume)
    }
}

struct Current {
    binding: StoreBinding,
    term: u64,
    leader: bool,
    base: u64,
    reference: Option<SnapshotRef>,
    drained: bool,
}
fn checkpoint_reference(
    p: &Checkpoint,
    row: &Row,
    current: Current,
) -> Result<Option<SnapshotRef>, Failure> {
    if current.binding != p.binding || current.term != p.term || !current.leader {
        return Err("checkpoint leader/term/binding changed".into());
    }
    if current.base <= row.previous || current.base < row.requested || !current.drained {
        return Ok(None);
    }
    let reference = current
        .reference
        .ok_or("missing durable checkpoint reference")?;
    if reference.group != p.group
        || reference.store != p.binding.identity
        || reference.index != current.base
    {
        return Err("checkpoint reference scope mismatch".into());
    }
    Ok(Some(reference))
}
fn complete_reclaim(
    row: &mut Row,
    ticket: ReclaimTicket,
    event: ReclaimEvent,
    now: u128,
) -> Result<(), Failure> {
    if event.request != ticket {
        row.status = "failed";
        return Err("reclaim completion identity mismatch".into());
    }
    row.completed = now;
    match event.result {
        Ok(report) => {
            row.before = report.before_bytes;
            row.after = report.after_bytes;
            row.status = "completed";
            Ok(())
        }
        Err(error) => {
            row.status = "failed";
            row.reason = format!("{error:?}");
            Err(format!("reclaim failed: {error:?}").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use voteboat::{
        contracts::StorageError,
        native::log_store::{FileLogIo, NativeLogStore},
        worker::WorkerBinding,
    };
    type TestLog = NativeLogStore<FileLogIo>;
    fn binding() -> StoreBinding {
        StoreBinding {
            identity: store(1),
            session: StoreSession::new(1).unwrap(),
        }
    }
    fn reference() -> SnapshotRef {
        SnapshotRef {
            store: store(1),
            group: group_id(1),
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: ConfigurationId::new(1).unwrap(),
            index: 9,
            term: 1,
            application_schema: 1,
            file_bytes: 1,
            checksum: 0,
        }
    }
    fn current() -> Current {
        Current {
            binding: binding(),
            term: 1,
            leader: true,
            base: 9,
            reference: Some(reference()),
            drained: true,
        }
    }
    #[test]
    fn checkpoint_admission_or_unfinished_snapshot_never_counts_as_completion() {
        let p = Checkpoint {
            row: 0,
            leader: 0,
            group: group_id(1),
            binding: binding(),
            term: 1,
        };
        let mut row = Row::new(1, "checkpoint", group_id(1), 0, 1, 1);
        row.previous = 5;
        row.requested = 9;
        let mut c = current();
        c.base = 5;
        assert!(checkpoint_reference(&p, &row, c).unwrap().is_none());
        let mut c = current();
        c.base = 8;
        assert!(checkpoint_reference(&p, &row, c).unwrap().is_none());
        let mut c = current();
        c.drained = false;
        assert!(checkpoint_reference(&p, &row, c).unwrap().is_none());
        assert_eq!(
            checkpoint_reference(&p, &row, current()).unwrap(),
            Some(reference())
        );
    }
    #[test]
    fn checkpoint_rejects_leader_term_store_session_and_reference_scope_changes() {
        let p = Checkpoint {
            row: 0,
            leader: 0,
            group: group_id(1),
            binding: binding(),
            term: 1,
        };
        let mut row = Row::new(1, "checkpoint", group_id(1), 0, 1, 1);
        row.previous = 5;
        row.requested = 9;
        let mut c = current();
        c.term = 2;
        assert!(checkpoint_reference(&p, &row, c).is_err());
        let mut c = current();
        c.leader = false;
        assert!(checkpoint_reference(&p, &row, c).is_err());
        let mut c = current();
        c.binding.session = StoreSession::new(2).unwrap();
        assert!(checkpoint_reference(&p, &row, c).is_err());
        let mut c = current();
        c.reference.as_mut().unwrap().group = group_id(2);
        assert!(checkpoint_reference(&p, &row, c).is_err());
        let mut c = current();
        c.reference = None;
        assert!(checkpoint_reference(&p, &row, c).is_err());
        let mut c = current();
        c.reference.as_mut().unwrap().index = 10;
        assert!(checkpoint_reference(&p, &row, c).is_err());
    }
    #[test]
    fn reclaim_requires_the_original_full_ticket_and_never_reports_failure_as_freed_bytes() {
        let ticket = ReclaimTicket {
            binding: WorkerBinding {
                store: binding(),
                generation: StorageWorkerGeneration::new(1).unwrap(),
            },
            sequence: 1,
        };
        let mut wrong = ticket;
        wrong.binding.store.session = StoreSession::new(2).unwrap();
        let mut row = Row::new(1, "reclaim", group_id(1), 0, 1, 1);
        let report = LogReclaimed {
            before_bytes: 100,
            after_bytes: 20,
        };
        assert!(complete_reclaim(
            &mut row,
            ticket,
            ReclaimEvent {
                request: wrong,
                result: Ok(report)
            },
            2
        )
        .is_err());
        assert_eq!(row.status, "failed");
        assert_eq!(row.before, 0);
        let mut row = Row::new(1, "reclaim", group_id(1), 0, 1, 1);
        assert!(complete_reclaim(
            &mut row,
            ticket,
            ReclaimEvent {
                request: ticket,
                result: Err(StorageError::Uncertain("test".into()))
            },
            2
        )
        .is_err());
        assert_eq!(row.status, "failed");
        assert_eq!(row.before, 0);
        let mut row = Row::new(1, "reclaim", group_id(1), 0, 1, 1);
        complete_reclaim(
            &mut row,
            ticket,
            ReclaimEvent {
                request: ticket,
                result: Ok(report),
            },
            2,
        )
        .unwrap();
        assert_eq!(row.status, "completed");
        assert_eq!(row.before - row.after, 80);
    }
    #[test]
    fn missed_opportunities_are_bounded_skips_and_must_all_be_reported_before_finish() {
        let c = config(5, 1, 1, None).unwrap();
        let mut m = Maintenance::new(Some(c), 5_000_000_000, 1);
        for i in 1..=4 {
            m.step::<TestLog>(&mut [], 7_000_000_000).unwrap();
            assert_eq!(m.rows.len(), i);
            assert_eq!(m.finished(), i == 4);
        }
        assert!(m
            .rows
            .iter()
            .all(|r| r.kind == "skip" && r.reason == "late"));
        let bound = m.max_rows;
        while m.rows.len() < bound {
            m.push(Row::new(0, "skip", group_id(1), 0, 0, 0)).unwrap();
        }
        assert!(m.push(Row::new(0, "skip", group_id(1), 0, 0, 0)).is_err());
        assert_eq!(m.rows.len(), bound);
    }
    #[test]
    fn late_start_still_retains_the_full_requested_pause_duration() {
        let c = config(120, 8, 2, Some("1:5")).unwrap();
        let mut m = Maintenance::new(Some(c), 15_000_000_000, 8);
        m.actual_pause = Some(1_500_000_000);
        assert_eq!(
            m.paused::<TestLog>(6_000_000_000, &[], &PollTotals::default())
                .unwrap(),
            Some(2)
        );
        assert!(m.actual_resume.is_none());
        assert_eq!(
            m.paused::<TestLog>(6_500_000_000, &[], &PollTotals::default())
                .unwrap(),
            None
        );
        assert_eq!(m.actual_resume, Some(6_500_000_000));
    }
    #[test]
    fn invalid_or_missed_pause_never_silently_becomes_a_shorter_outage() {
        assert!(config(120, 0, 2, None).is_err());
        assert!(config(120, 8, 0, None).is_err());
        assert!(config(120, 8, 2, Some("1:6")).is_err());
        assert!(config(120, 8, 2, Some("14:1")).is_err());
        // A boundary-aligned opportunity is valid, including at the pause start.
        assert!(config(120, 8, 2, Some("2:1")).is_ok());
        let c = config(120, 8, 2, Some("1:5")).unwrap();
        let mut m = Maintenance::new(Some(c), 15_000_000_000, 8);
        assert!(m
            .paused::<TestLog>(7_000_000_000, &[], &PollTotals::default())
            .is_err());
        assert!(m.actual_pause.is_none());
    }
}
