// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Retained benchmark failures are diagnostics, never successful recovery evidence.
use super::*;

pub(super) fn write_samples(file: &mut File, samples: &[Sample]) -> Result<(), Failure> {
    writeln!(
        file,
        "operation,submitted_ns,completed_ns,latency_ns,applied_index,value,group"
    )?;
    for s in samples {
        writeln!(
            file,
            "{},{},{},{},{},{},{}",
            s.operation,
            s.submitted_ns,
            s.completed_ns,
            s.completed_ns - s.submitted_ns,
            s.index,
            s.value,
            s.group.id.get()
        )?;
    }
    file.sync_all()?;
    Ok(())
}
pub(super) fn write_partial(
    root: &Path,
    phase: &str,
    error: &str,
    samples: &[Sample],
    unresolved: &[(u128, u128, u128)],
    polls: &PollTotals,
) -> Result<(), Failure> {
    // Phase is supplied by the benchmark, never by a remote request.
    if !matches!(phase, "warmup" | "measurement") {
        return Err("invalid diagnostic phase".into());
    }
    write_samples(
        &mut exclusive(&root.join(format!("{phase}-partial.csv")))?,
        samples,
    )?;
    let mut pending = exclusive(&root.join(format!("{phase}-unresolved.csv")))?;
    writeln!(pending, "group,operation,submitted_ns")?;
    for (group, operation, submitted) in unresolved {
        writeln!(pending, "{group},{operation},{submitted}")?;
    }
    pending.sync_all()?;
    let mut output = exclusive(&root.join(format!("{phase}-failure.txt")))?;
    writeln!(output, "valid_measurement=false phase={phase} completed={} unresolved={} poll_rounds={} host_poll_ns={} max_host_poll_ns={} persistence_batches={} worker_events={} application_deliveries={}\noriginal_error={error}",
        samples.len(), unresolved.len(), polls.rounds, polls.host_ns, polls.max_host_ns,
        polls.persistence_batches, polls.worker_events, polls.application_deliveries)?;
    output.sync_all()?;
    Ok(())
}
pub(super) fn retain<L: LogStore + Send + 'static>(
    root: &Path,
    phase: &str,
    error: &str,
    samples: &[Sample],
    unresolved: &[(u128, u128, u128)],
    polls: &PollTotals,
    replicas: &[Replica<L>],
) -> Result<(), Failure> {
    write_partial(root, phase, error, samples, unresolved, polls)?;
    let mut state = exclusive(&root.join(format!("{phase}-replicas.txt")))?;
    for (replica, n) in replicas.iter().enumerate() {
        for (group, app) in &n.local().applications {
            let core = n
                .local()
                .owner
                .core(*group)
                .ok_or("missing diagnostic core")?;
            writeln!(
                state,
                "replica={} group={} role={:?} term={} commit={} applied={}",
                replica + 1,
                group.id.get(),
                core.role(),
                core.state().hard_state.term,
                core.state().commit_index,
                app.applied_index()
            )?;
        }
    }
    state.sync_all()?;
    Ok(())
}
pub(super) fn cleanup<L: LogStore + Send + 'static, T>(
    replicas: Vec<Replica<L>>,
    clock: &Instant,
    root: &Path,
    original: Failure,
) -> Result<T, Failure> {
    let cleanup = close(replicas, clock);
    let text = match &cleanup {
        Ok(()) => "workers_joined=true".into(),
        Err(error) => format!("workers_joined=false cleanup_error={error}"),
    };
    if let Err(error) = (|| -> Result<(), Failure> {
        let mut file = exclusive(&root.join("failure-cleanup.txt"))?;
        writeln!(file, "{text}")?;
        file.sync_all()?;
        Ok(())
    })() {
        eprintln!("failed to retain cleanup diagnostic: {error}");
    }
    if let Err(error) = cleanup {
        eprintln!("cleanup failure: {error}; original failure: {original}");
    }
    Err(original)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_capacity_failure_retains_applied_prefix_and_joins_workers() {
        let root =
            std::env::temp_dir().join(format!("voteboat-native-failure-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let clock = Instant::now();
        let mut replicas = open(
            &root,
            NativeOpenMode::Create,
            NativePeerProtocol::TcpTls,
            1,
            &clock,
        )
        .unwrap();
        campaign(&mut replicas, &clock, 1).unwrap();
        let error = workload(
            &mut replicas,
            &clock,
            Workload {
                first: 1,
                count: 2,
                window: 1,
                groups: 1,
                diagnostic: Some((&root, "measurement")),
            },
        )
        .err()
        .expect("second distinct ID must exceed the application envelope");
        let original = error.to_string();
        assert!(
            original.contains("DedupCapacity"),
            "unexpected failure: {original}"
        );
        let cleanup: Result<(), Failure> = cleanup(replicas, &clock, &root, error);
        assert_eq!(cleanup.unwrap_err().to_string(), original);
        let samples = std::fs::read_to_string(root.join("measurement-partial.csv")).unwrap();
        assert_eq!(samples.lines().count(), 2, "one receipt plus header");
        assert!(samples.lines().nth(1).unwrap().starts_with("1,"));
        let state = std::fs::read_to_string(root.join("measurement-replicas.txt")).unwrap();
        assert_eq!(state.lines().count(), 3);
        assert!(state.contains("role=Leader"));
        assert_eq!(
            std::fs::read_to_string(root.join("failure-cleanup.txt")).unwrap(),
            "workers_joined=true\n"
        );
        assert!(!root.join("summary.txt").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn drive_failure_keeps_poll_totals_and_original_error() {
        let clock = Instant::now();
        let mut totals = PollTotals::default();
        let result = drive::<
            voteboat::native::log_store::NativeLogStore<voteboat::native::log_store::FileLogIo>,
        >(&mut [], &clock, &mut totals, |_| {
            Err("injected owner observation failure".into())
        });
        assert_eq!(
            result.unwrap_err().to_string(),
            "injected owner observation failure"
        );
        assert_eq!(totals.rounds, 1);
    }
    #[test]
    fn partial_history_preserves_receipts_and_uncertainty_without_success_summary() {
        let root = std::env::temp_dir().join(format!("voteboat-partial-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let samples = vec![Sample {
            group: group(),
            operation: 65,
            submitted_ns: 10,
            completed_ns: 40,
            index: 70,
            value: 65,
        }];
        let polls = PollTotals {
            rounds: 3,
            worker_events: 2,
            ..Default::default()
        };
        write_partial(
            &root,
            "measurement",
            "operation66: Unknown(LeadershipChanged)",
            &samples,
            &[(1, 67, 50)],
            &polls,
        )
        .unwrap();
        let csv = std::fs::read_to_string(root.join("measurement-partial.csv")).unwrap();
        assert!(csv.contains("65,10,40,30,70,65,1"));
        assert!(!csv.contains("66,"));
        let error = std::fs::read_to_string(root.join("measurement-failure.txt")).unwrap();
        assert!(error.contains("valid_measurement=false"));
        assert!(error.contains("Unknown(LeadershipChanged)"));
        assert!(error.contains("poll_rounds=3"));
        assert_eq!(
            std::fs::read_to_string(root.join("measurement-unresolved.csv")).unwrap(),
            "group,operation,submitted_ns\n1,67,50\n"
        );
        assert!(!root.join("summary.txt").exists());
        assert!(write_partial(&root, "measurement", "replacement", &[], &[], &polls).is_err());
        assert!(write_partial(&root, "../escape", "error", &[], &[], &polls).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
