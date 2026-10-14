// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit visit quantum selection; parse before opening benchmark resources.
use super::*;
#[derive(Clone, Copy)]
pub(super) struct Settings {
    pub items: usize,
    pub replies: usize,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            items: 16,
            replies: 2,
        }
    }
}
pub(super) fn extract_replies(args: &mut Vec<String>) -> Result<Option<usize>, Failure> {
    let Some(position) = args.iter().position(|arg| arg == "--ready-replies") else {
        return Ok(None);
    };
    if args
        .iter()
        .skip(position + 1)
        .any(|arg| arg == "--ready-replies")
    {
        return Err("duplicate --ready-replies".into());
    }
    let selected = args
        .get(position + 1)
        .ok_or("missing --ready-replies value")?
        .parse::<usize>()?;
    if !matches!(selected, 0 | 2) {
        return Err("--ready-replies must be 0 or 2".into());
    }
    args.drain(position..position + 2);
    Ok(Some(selected))
}
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
    if !matches!(selected, 1 | 16 | 32 | 64) {
        return Err("--ready-items must be 1, 16, 32 or 64".into());
    }
    args.drain(position..position + 2);
    Ok(Some(selected))
}
pub(super) fn report(items: usize, replies: usize) -> String {
    format!("{} ready_reply_inputs_requested={replies} ready_reply_inputs_effective={replies} ready_commit_batch_capable=true", settings(items))
}
fn settings(items: usize) -> String {
    let limits = ShardLimits::default();
    format!("ready_visit_items_requested={items} ready_visit_items_effective={items} core_proposal_entries={} ready_visit_bytes={} ready_visit_ms={} ready_batch_capable=true",
        voteboat::raft::BatchLimits::default().max_proposal_entries, limits.visit_bytes, limits.visit_ms)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }
    #[test]
    fn explicit_quantums_preserve_other_arguments_and_report_the_selected_settings() {
        for selected in [1, 16, 32, 64] {
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
            let (_, timed) = shared::timed_shard(
                Vec::new(),
                owner,
                1,
                1,
                &clock,
                Settings {
                    items: selected,
                    ..Settings::default()
                },
            )
            .unwrap();
            let limits = timed.limits();
            assert_eq!(limits.visit_items, selected);
            assert_eq!(limits.visit_bytes, ShardLimits::default().visit_bytes);
            assert_eq!(limits.visit_ms, ShardLimits::default().visit_ms);
        }
    }
    #[test]
    fn shared_assembly_installs_reply_switch_in_recovered_core() {
        let owner = RuntimeOwner {
            store: StoreBinding {
                identity: store(1),
                session: StoreSession::new(1).unwrap(),
            },
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        };
        for replies in [0, 2] {
            let bootstrap = Bootstrap {
                group: group(),
                configuration: ConfigurationId::new(1).unwrap(),
                policy: Policy::new(
                    Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
                    Limits::default(),
                )
                .unwrap(),
                voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
            };
            let mut states = BTreeMap::new();
            apply_batch(
                &mut states,
                &[LogMutation::Create(bootstrap)],
                LogLimits::default(),
            )
            .unwrap();
            let core = voteboat::raft::Raft::recover(
                node(1),
                owner.store,
                states.remove(&group()).unwrap(),
                LogLimits::default(),
            )
            .unwrap();
            let clock = Instant::now();
            let (_, timed) = shared::timed_shard(
                vec![core],
                owner,
                1,
                1,
                &clock,
                Settings { items: 16, replies },
            )
            .unwrap();
            let limits = timed.core(group()).unwrap().batch_limits();
            assert_eq!(limits.max_ready_reply_inputs, replies);
            assert_eq!(limits.max_proposal_entries, 64);
            assert_eq!(limits.max_replication_entries, 64);
            assert_eq!(timed.limits().visit_items, 16);
        }
    }
    #[test]
    fn invalid_duplicate_and_missing_choices_fail_without_mutating_arguments() {
        for values in [
            vec!["--ready-items"],
            vec!["--ready-items", "0"],
            vec!["--ready-items", "65"],
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

#[cfg(test)]
mod reply_tests {
    use super::*;
    #[test]
    fn reply_switch_is_explicit_bounded_and_preserves_other_arguments() {
        for replies in [0, 2] {
            let mut input = vec![
                "root".to_owned(),
                "--ready-replies".to_owned(),
                replies.to_string(),
                "--ready-items".to_owned(),
                "16".to_owned(),
            ];
            assert_eq!(extract_replies(&mut input).unwrap(), Some(replies));
            assert_eq!(input, ["root", "--ready-items", "16"]);
            assert!(
                report(16, replies).contains(&format!("ready_reply_inputs_effective={replies}"))
            );
        }
        for values in [
            vec!["--ready-replies"],
            vec!["--ready-replies", "1"],
            vec!["--ready-replies", "65"],
            vec!["--ready-replies", "2", "--ready-replies", "0"],
        ] {
            let mut input: Vec<_> = values.iter().map(|v| (*v).to_owned()).collect();
            let before = input.clone();
            assert!(extract_replies(&mut input).is_err());
            assert_eq!(input, before);
        }
    }
}
