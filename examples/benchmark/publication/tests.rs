// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}
#[test]
fn explicit_publication_choices_preserve_workload_and_require_offered_mode() {
    for (value, selected, provider) in [
        ("atomic", Publication::Atomic, "FileLogIo"),
        ("witness", Publication::Witness, "WitnessLogIo"),
    ] {
        let mut input = args(&[
            "root",
            "tcp",
            "512",
            "32",
            "1",
            "--offered",
            "128",
            "--publication",
            value,
        ]);
        assert_eq!(Publication::extract(&mut input).unwrap(), selected);
        assert_eq!(
            input,
            args(&["root", "tcp", "512", "32", "1", "--offered", "128"])
        );
        selected.validate(true).unwrap();
        assert!(selected.validate(false).is_err());
        assert!(selected.settings().contains(&format!(
            "publication_requested={value} publication_effective={value}"
        )));
        assert!(selected
            .settings()
            .contains(&format!("publication_provider={provider}")));
        assert!(selected.settings().contains("publication_timings=native"));
    }
    let mut input = args(&["root", "tcp", "512", "32"]);
    assert_eq!(
        Publication::extract(&mut input).unwrap(),
        Publication::Legacy
    );
    assert!(Publication::Legacy.settings().is_empty());
    Publication::Legacy.validate(false).unwrap();
}
#[test]
fn invalid_duplicate_and_missing_publications_leave_arguments_unchanged() {
    for values in [
        vec!["--publication"],
        vec!["--publication", "legacy"],
        vec!["--publication", "WITNESS"],
        vec!["--publication", "atomic", "--publication", "witness"],
        vec!["--publication", "witness", "--publication"],
    ] {
        let mut input = args(&values);
        let before = input.clone();
        assert!(Publication::extract(&mut input).is_err());
        assert_eq!(input, before);
    }
}
#[test]
fn explicit_benchmark_journals_forward_one_combined_publication_and_reopen() {
    use voteboat::native::log_store::NativeLogStore;
    for selected in [Publication::Atomic, Publication::Witness] {
        let root = std::env::temp_dir().join(format!(
            "voteboat-publication-{selected:?}-{}",
            std::process::id()
        ));
        let timings = JournalTimings::default();
        let trace = Trace::journal(timings.clone());
        let journal = match selected {
            Publication::Atomic => BenchJournal::Atomic(
                FileLogIo::create(&root)
                    .unwrap()
                    .with_timings(timings.clone()),
                trace.clone(),
            ),
            Publication::Witness => BenchJournal::Witness(
                WitnessLogIo::create(&root)
                    .unwrap()
                    .with_timings(timings.clone()),
                trace.clone(),
            ),
            Publication::Legacy => unreachable!(),
        };
        assert_eq!(journal.publication(), selected);
        let mut log = NativeLogStore::create(journal, store(1), LogLimits::default()).unwrap();
        let bootstrap = Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node(1)), Limits::default()).unwrap(),
            voter_stores: [(node(1), store(1))].into(),
        };
        let tickets = log
            .append_batch(vec![LogMutation::Create(bootstrap.clone())])
            .unwrap();
        assert!(log.state(group()).is_err());
        assert_eq!(log.barrier(&tickets).unwrap().tickets, tickets);
        assert_eq!(log.state(group()).unwrap().bootstrap, bootstrap);
        let observed = timings.snapshot();
        assert_eq!(
            observed.log_sync.calls, 2,
            "initialization plus one exact barrier; no wrapper-added sync"
        );
        assert_eq!(observed.manifest.calls, 2);
        drop(log);
        let (journal, _) = selected.open(&root, NativeOpenMode::Recover, 1).unwrap();
        let recovered = NativeLogStore::recover(journal, store(1), LogLimits::default()).unwrap();
        assert_eq!(recovered.state(group()).unwrap().bootstrap, bootstrap);
        drop(recovered);
        std::fs::remove_dir_all(root).unwrap();
    }
}
