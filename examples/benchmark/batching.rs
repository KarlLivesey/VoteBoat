// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Public-step benchmark correlations; queue selection refusal is unobserved.
use super::*;
use std::collections::BTreeSet;
use voteboat::raft::{Role, MAX_PROPOSAL_BATCH_ENTRIES};

#[derive(Default)]
pub(super) struct Totals {
    rows: BTreeMap<(usize, GroupIdentity), Row>,
}
#[derive(Default)]
struct Row {
    polls: u64,
    heartbeat_due: u64,
    dependency: u64,
    control_persists: u64,
    data_persists: u64,
    histogram: BTreeMap<usize, Width>,
}
#[derive(Default)]
struct Width {
    visits: u64,
    leader: u64,
    heartbeat_due: u64,
    dependency: u64,
}
#[derive(Clone, Copy)]
pub(super) struct Before {
    group: GroupIdentity,
    leader: bool,
    heartbeat_due: bool,
    dependency: bool,
    control_persists: usize,
    data_persists: usize,
}
pub(super) fn before<L: LogStore + Send + 'static>(
    replica: &Replica<L>,
    now: MonoTime,
) -> Vec<Before> {
    let usage = replica.replica_usage();
    replica
        .local()
        .applications
        .keys()
        .map(|&group| {
            let core = replica.local().owner.core(group).unwrap();
            Before {
                group,
                leader: core.role() == Role::Leader,
                heartbeat_due: replica.local().owner.deadline(group).is_some_and(|token| {
                    token.kind == TimerKind::Heartbeat && token.deadline <= now
                }),
                dependency: core.has_pending_dependency(),
                control_persists: usage.control_persists,
                data_persists: usage.data_persists,
            }
        })
        .collect()
}
struct Batch {
    visit: VisitTicket,
    count: usize,
    position: ProposalPosition,
}
fn batches(steps: &[OwnerStep]) -> Result<Vec<Batch>, Failure> {
    let mut batches: Vec<Batch> = Vec::new();
    let mut originals = BTreeSet::new();
    for step in steps {
        let (Some(_), Some(position), None) = (step.operation, step.proposed, &step.error) else {
            continue;
        };
        let admission = step
            .admission
            .ok_or("untracked proposed original in benchmark")?;
        if admission.owner != step.visit.owner
            || admission.group != step.visit.group
            || !originals.insert((admission.group, admission.sequence))
        {
            return Err("duplicate or mismatched original admission in benchmark".into());
        }
        if let Some(last) = batches.last_mut().filter(|last| last.visit == step.visit) {
            if last.position.index.checked_add(1) != Some(position.index)
                || last.position.term != position.term
                || last.count == MAX_PROPOSAL_BATCH_ENTRIES
            {
                return Err("invalid proposed batch positions/width in benchmark".into());
            }
            last.count += 1;
            last.position = position;
        } else {
            if batches.iter().any(|batch| batch.visit == step.visit) {
                return Err("noncontiguous proposed batch visit in benchmark".into());
            }
            batches.push(Batch {
                visit: step.visit,
                count: 1,
                position,
            });
        }
    }
    Ok(batches)
}
impl Totals {
    pub(super) fn observe(
        &mut self,
        replica: usize,
        before: &[Before],
        steps: &[OwnerStep],
    ) -> Result<(), Failure> {
        let batches = batches(steps)?;
        for snapshot in before {
            let row = self.rows.entry((replica, snapshot.group)).or_default();
            row.polls += 1;
            row.heartbeat_due += u64::from(snapshot.heartbeat_due);
            row.dependency += u64::from(snapshot.dependency);
            row.control_persists += snapshot.control_persists as u64;
            row.data_persists += snapshot.data_persists as u64;
        }
        for batch in batches {
            let snapshot = before
                .iter()
                .find(|snapshot| snapshot.group == batch.visit.group)
                .ok_or("proposed group absent from benchmark pre-poll observation")?;
            let width = self
                .rows
                .get_mut(&(replica, batch.visit.group))
                .unwrap()
                .histogram
                .entry(batch.count)
                .or_default();
            width.visits += 1;
            width.leader += u64::from(snapshot.leader);
            width.heartbeat_due += u64::from(snapshot.heartbeat_due);
            width.dependency += u64::from(snapshot.dependency);
        }
        Ok(())
    }
    pub(super) fn originals(&self) -> u64 {
        self.rows
            .values()
            .flat_map(|row| &row.histogram)
            .map(|(&width, counts)| width as u64 * counts.visits)
            .sum()
    }
    pub(super) fn verify(&self, samples: &[Sample]) -> Result<(), Failure> {
        if self.originals() != samples.len() as u64 {
            return Err("batch diagnostic originals disagree with actual applied receipts".into());
        }
        Ok(())
    }
}
pub(super) fn write(root: &Path, phases: &[(&str, &PollTotals)]) -> Result<(), Failure> {
    if phases.iter().all(|(_, polls)| polls.batching.is_none()) {
        return Ok(());
    }
    let mut file = exclusive(&root.join("proposal-batches.csv"))?;
    writeln!(file, "phase,replica,group,incarnation,width,visits,original_proposals,pre_poll_leader_visits,pre_poll_heartbeat_due_visits,pre_poll_dependency_visits,poll_observations,heartbeat_due_observations,dependency_observations,control_persist_lease_observations,data_persist_lease_observations,queue_selection_reason_observed")?;
    for (phase, polls) in phases {
        let Some(totals) = &polls.batching else {
            continue;
        };
        for ((replica, group), row) in &totals.rows {
            write_row(
                &mut file,
                phase,
                *replica,
                *group,
                row,
                0,
                &Width::default(),
            )?;
            for (&width, counts) in &row.histogram {
                write_row(&mut file, phase, *replica, *group, row, width, counts)?;
            }
        }
    }
    file.sync_all()?;
    Ok(())
}
fn write_row(
    file: &mut File,
    phase: &str,
    replica: usize,
    group: GroupIdentity,
    row: &Row,
    width: usize,
    counts: &Width,
) -> Result<(), Failure> {
    writeln!(
        file,
        "{phase},{replica},{},{},{width},{},{},{},{},{},{},{},{},{},{},false",
        group.id.get(),
        group.incarnation.get(),
        counts.visits,
        width as u64 * counts.visits,
        counts.leader,
        counts.heartbeat_due,
        counts.dependency,
        row.polls,
        row.heartbeat_due,
        row.dependency,
        row.control_persists,
        row.data_persists
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn owner() -> RuntimeOwner {
        RuntimeOwner {
            store: StoreBinding {
                identity: store(1),
                session: StoreSession::new(1).unwrap(),
            },
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        }
    }
    fn snapshot(group: GroupIdentity) -> Before {
        Before {
            group,
            leader: true,
            heartbeat_due: true,
            dependency: true,
            control_persists: 1,
            data_persists: 2,
        }
    }
    fn step(group: GroupIdentity, visit: u64, original: u64, index: u64) -> OwnerStep {
        OwnerStep {
            admission: Some(AdmissionTicket {
                owner: owner(),
                group,
                sequence: original,
            }),
            proposed: Some(ProposalPosition { index, term: 1 }),
            visit: VisitTicket {
                owner: owner(),
                group,
                sequence: visit,
            },
            operation: Some(OperationId::new(1).unwrap()),
            read: None,
            error: None,
        }
    }
    #[test]
    fn distinct_original_admissions_preserve_duplicate_operations_and_exact_widths() {
        let mut totals = Totals::default();
        for (replica, width) in [(1, 1), (2, 4), (3, 32)] {
            let group = group_id(replica);
            let steps: Vec<_> = (1..=width).map(|n| step(group, 1, n, n)).collect();
            totals.observe(replica, &[snapshot(group)], &steps).unwrap();
            let counts = &totals.rows[&(replica, group)].histogram[&(width as usize)];
            assert_eq!(counts.visits, 1);
            assert_eq!(counts.leader, 1);
            assert_eq!(counts.heartbeat_due, 1);
            assert_eq!(counts.dependency, 1);
        }
        assert_eq!(totals.originals(), 37);
        assert_eq!(totals.rows.len(), 3);
    }
    #[test]
    fn commit_only_failed_and_unproposed_originals_do_not_count_as_batches() {
        let group = group();
        let mut commit = step(group, 1, 1, 1);
        commit.operation = None;
        commit.proposed = None;
        let mut failed = step(group, 2, 2, 2);
        failed.error = Some(voteboat::raft::RaftError::NotLeader);
        let mut unproposed = step(group, 3, 3, 3);
        unproposed.proposed = None;
        let mut totals = Totals::default();
        totals
            .observe(1, &[snapshot(group)], &[commit, failed, unproposed])
            .unwrap();
        assert_eq!(totals.originals(), 0);
        assert_eq!(totals.rows[&(1, group)].polls, 1);
        assert!(totals.rows[&(1, group)].histogram.is_empty());
    }
    #[test]
    fn invalid_admissions_positions_and_noncontiguous_visit_members_are_refused() {
        let group = group();
        for steps in [
            vec![step(group, 1, 1, 1), step(group, 1, 1, 2)],
            vec![step(group, 1, 1, 1), step(group, 1, 2, 1)],
            vec![
                step(group, 1, 1, 1),
                step(group, 2, 2, 2),
                step(group, 1, 3, 3),
            ],
        ] {
            let mut totals = Totals::default();
            assert!(totals.observe(1, &[snapshot(group)], &steps).is_err());
            assert_eq!(totals.originals(), 0);
        }
        let mut totals = Totals::default();
        assert!(totals
            .verify(&[Sample {
                group,
                operation: 1,
                submitted_ns: 0,
                completed_ns: 1,
                index: 1,
                value: 1
            }])
            .is_err());
        let mut untracked = step(group, 1, 1, 1);
        untracked.admission = None;
        assert!(totals.observe(1, &[snapshot(group)], &[untracked]).is_err());
    }
    fn native_work(root: &Path) -> Result<(), Failure> {
        let clock = Instant::now();
        let (mut replicas, _) = open_recorded(
            root,
            NativeOpenMode::Create,
            NativePeerProtocol::TcpTls,
            16,
            &clock,
            false,
            16,
        )?;
        let result = (|| {
            campaign(&mut replicas, &clock, 1)?;
            let batched = workload_observed(
                &mut replicas,
                &clock,
                Workload {
                    first: 1,
                    count: 8,
                    window: 4,
                    groups: 1,
                    diagnostic: None,
                },
                0,
                Instant::now(),
                true,
            )?;
            assert_eq!(batched.samples.len(), 8);
            assert_eq!(batched.polls.batching.as_ref().unwrap().originals(), 8);
            let singleton = workload_observed(
                &mut replicas,
                &clock,
                Workload {
                    first: 9,
                    count: 3,
                    window: 1,
                    groups: 1,
                    diagnostic: None,
                },
                0,
                Instant::now(),
                true,
            )?;
            assert_eq!(singleton.samples.len(), 3);
            let totals = singleton.polls.batching.as_ref().unwrap();
            assert_eq!(totals.originals(), 3);
            assert_eq!(
                totals
                    .rows
                    .values()
                    .flat_map(|row| &row.histogram)
                    .map(|(&width, count)| {
                        assert_eq!(width, 1);
                        count.visits
                    })
                    .sum::<u64>(),
                3
            );
            write(
                root,
                &[
                    ("batch_window", &batched.polls),
                    ("single_window", &singleton.polls),
                ],
            )?;
            let csv = std::fs::read_to_string(root.join("proposal-batches.csv"))?;
            assert!(csv.contains("pre_poll_heartbeat_due_visits"));
            assert!(csv.lines().skip(1).all(|row| row.ends_with(",false")));
            verify(
                &mut replicas,
                &clock,
                11,
                1,
                &boundaries(
                    batched
                        .samples
                        .iter()
                        .chain(&singleton.samples)
                        .map(|s| (s.group, s.index)),
                ),
            )?;
            Ok(())
        })();
        let cleanup = close(replicas, &clock);
        result.and(cleanup)
    }
    #[test]
    fn real_native_owner_steps_account_for_every_applied_original_and_singleton() {
        let root =
            std::env::temp_dir().join(format!("voteboat-batch-accounting-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let result = native_work(&root);
        std::fs::remove_dir_all(&root).unwrap();
        result.unwrap();
    }
}
