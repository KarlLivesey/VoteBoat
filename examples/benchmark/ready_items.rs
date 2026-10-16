// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit visit quantum selection; parse before opening benchmark resources.
use super::*;
pub(super) fn extract(args: &mut Vec<String>) -> Result<Option<usize>, Failure> {
    let Some(position) = args.iter().position(|arg| arg == "--ready-items") else {
        return Ok(None);
    };
    if args
        .iter()
        .skip(position + 1)
        .any(|arg| arg == "--ready-items")
    {
        return Err("duplicate --ready-items".into());
    }
    let selected = args
        .get(position + 1)
        .ok_or("missing --ready-items value")?
        .parse::<usize>()?;
    if !matches!(selected, 1 | 16 | 32 | 64 | 128 | 256 | 512 | 1024) {
        return Err("--ready-items must be 1, 16, 32, 64, 128, 256, 512 or 1024".into());
    }
    args.drain(position..position + 2);
    Ok(Some(selected))
}
pub(super) fn settings(items: usize) -> String {
    let limits = ShardLimits::default();
    let core = voteboat::raft::BatchLimits::for_ready_visit(items).unwrap_or_default();
    format!("ready_visit_items_requested={items} ready_visit_items_effective={items} core_proposal_entries={} core_replication_entries={} ready_visit_bytes={} ready_visit_ms={} ready_batch_capable=true",
        core.max_proposal_entries, core.max_replication_entries, limits.visit_bytes, limits.visit_ms)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }
    #[test]
    fn explicit_quantums_preserve_other_arguments_and_report_the_selected_settings() {
        for selected in [1, 16, 32, 64, 128, 256, 512, 1024] {
            let mut input = args(&[
                "root",
                "tcp",
                "256",
                "32",
                "--ready-items",
                &selected.to_string(),
                "--journal-timings",
            ]);
            assert_eq!(extract(&mut input).unwrap(), Some(selected));
            assert_eq!(
                input,
                args(&["root", "tcp", "256", "32", "--journal-timings"])
            );
            assert!(settings(selected).contains(&format!("ready_visit_items_effective={selected}")));
        }
        let mut input = args(&["root", "tcp", "256", "32"]);
        assert_eq!(extract(&mut input).unwrap(), None);
    }
    #[test]
    fn shared_assembly_installs_explicit_quantums_without_changing_other_visit_caps() {
        let clock = Instant::now();
        let owner = RuntimeOwner {
            store: StoreBinding {
                identity: store(1),
                session: StoreSession::new(1).unwrap(),
            },
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        };
        for selected in [1, 16, 32, 64] {
            let (_, timed) =
                shared::timed_shard(Vec::new(), owner, 1, 1, &clock, selected).unwrap();
            let limits = timed.limits();
            assert_eq!(limits.visit_items, selected);
            assert_eq!(limits.visit_bytes, ShardLimits::default().visit_bytes);
            assert_eq!(limits.visit_ms, ShardLimits::default().visit_ms);
        }
    }
    #[test]
    fn invalid_duplicate_and_missing_choices_fail_without_mutating_arguments() {
        for values in [
            vec!["--ready-items"],
            vec!["--ready-items", "0"],
            vec!["--ready-items", "65"],
            vec!["--ready-items", "2048"],
            vec!["--ready-items", "2"],
            vec!["--ready-items", "32", "--ready-items", "64"],
        ] {
            let mut input = args(&values);
            let before = input.clone();
            assert!(extract(&mut input).is_err());
            assert_eq!(input, before);
        }
    }
}
