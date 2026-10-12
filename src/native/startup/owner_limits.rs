// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Derive a bounded owner envelope from the actual selected recovered cores.
use super::{error, EffectOwnerLimits, NativeStartupError};
use crate::log::{LogEntry, LogLimits};
use crate::raft::Raft;

pub(super) fn startup_owner_limits(
    cores: &[Raft],
    event_bytes: usize,
    log: LogLimits,
) -> Result<EffectOwnerLimits, NativeStartupError> {
    let required = cores.iter().try_fold(0, |largest, core| {
        core.effect_reservation(event_bytes)
            .map(|bytes| largest.max(bytes))
            .ok_or_else(|| error("owner limits", "effect reservation overflow"))
    })?;
    let mut limits = EffectOwnerLimits::default();
    if required <= limits.control_bytes {
        return Ok(limits);
    }
    // Exact startup size alone leaves no room even for the first leader Noop.
    // Selected native storage supplies bounded retained-log growth headroom;
    // every actual prospective event still checks its own reservation.
    let growth = log
        .max_entries_per_group
        .checked_mul(std::mem::size_of::<LogEntry>())
        .and_then(|bytes| bytes.checked_add(log.max_wal_bytes))
        .and_then(|bytes| bytes.checked_add(log.max_batch_bytes))
        .ok_or_else(|| error("owner limits", "retained log growth budget overflow"))?;
    let required = required
        .checked_add(growth)
        .ok_or_else(|| error("owner limits", "effect and log growth budget overflow"))?;
    // Ordinary work cannot consume this full control/recovery reservation.
    limits.control_bytes = limits.control_bytes.max(required);
    limits.reserved_bytes = limits.reserved_bytes.max(
        required
            .checked_add(limits.control_bytes)
            .ok_or_else(|| error("owner limits", "ordinary and control budget overflow"))?,
    );
    if limits.reserved_bytes > 4usize.saturating_mul(1024 * 1024 * 1024) {
        return Err(error("owner limits", "effect budget exceeds owner ceiling"));
    }
    Ok(limits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{identity::*, log::*, quorum::*, raft::Event};
    use std::collections::BTreeMap;

    fn core(limits: LogLimits) -> Raft {
        let group = GroupIdentity {
            id: GroupId::new(811).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        };
        let node = NodeId::new(1).unwrap();
        let stores: BTreeMap<_, _> = (1..=3)
            .map(|n| {
                (
                    NodeId::new(n).unwrap(),
                    StoreIdentity {
                        id: StoreId::new(u128::from(n)).unwrap(),
                        incarnation: StoreIncarnation::new(1).unwrap(),
                    },
                )
            })
            .collect();
        let bootstrap = Bootstrap {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(
                Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            )
            .unwrap(),
            voter_stores: stores.clone(),
        };
        let mut groups = BTreeMap::new();
        apply_batch(&mut groups, &[LogMutation::Create(bootstrap)], limits).unwrap();
        Raft::recover(
            node,
            StoreBinding {
                identity: stores[&node],
                session: StoreSession::new(1).unwrap(),
            },
            groups.remove(&group).unwrap(),
            limits,
        )
        .unwrap()
    }

    #[test]
    fn small_actual_core_keeps_exact_default_owner_profile() {
        let defaults = EffectOwnerLimits::default();
        let log = LogLimits::default();
        let selected = startup_owner_limits(&[core(log)], 1024 * 1024, log).unwrap();
        assert_eq!(selected.active_visits, defaults.active_visits);
        assert_eq!(selected.control_visits, defaults.control_visits);
        assert_eq!(selected.control_bytes, defaults.control_bytes);
        assert_eq!(selected.reserved_bytes, defaults.reserved_bytes);
    }

    #[test]
    fn selected_actual_core_preserves_one_ordinary_and_one_control_window() {
        let log = LogLimits {
            max_command_bytes: 1_069_056,
            max_batch_bytes: 33_587_200,
            ..Default::default()
        };
        let core = core(log);
        let event_bytes = log.max_batch_bytes + std::mem::size_of::<Event>();
        let required = core.effect_reservation(event_bytes).unwrap();
        assert!(required > EffectOwnerLimits::default().reserved_bytes);
        let selected = startup_owner_limits(&[core], event_bytes, log).unwrap();
        let growth = log.max_wal_bytes
            + log.max_batch_bytes
            + log.max_entries_per_group * std::mem::size_of::<LogEntry>();
        assert_eq!(selected.control_bytes, required + growth);
        assert_eq!(
            selected.reserved_bytes - selected.control_bytes,
            required + growth
        );
        assert_eq!(
            selected.active_visits,
            EffectOwnerLimits::default().active_visits
        );
        assert_eq!(
            selected.control_visits,
            EffectOwnerLimits::default().control_visits
        );
    }

    #[test]
    fn actual_core_overflow_and_above_ceiling_profiles_are_refused() {
        let core = core(LogLimits::default());
        let log = LogLimits::default();
        assert!(startup_owner_limits(std::slice::from_ref(&core), usize::MAX, log).is_err());
        assert!(startup_owner_limits(&[core], 1024 * 1024 * 1024, log).is_err());
    }
}
