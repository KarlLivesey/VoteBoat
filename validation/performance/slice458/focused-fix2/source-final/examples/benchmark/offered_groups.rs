// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded offered-workload weights; these never change quorum configuration.
use super::*;

pub(super) struct GroupSchedule {
    weights: Vec<usize>,
    slots: Vec<GroupIdentity>,
}

pub(super) fn prepare_workload(
    raw: Option<&str>,
    groups: usize,
    offered: bool,
    count: usize,
) -> Result<Option<GroupSchedule>, Failure> {
    let schedule = GroupSchedule::from_cli(raw, groups, offered)?;
    validate_capacity(
        schedule.as_ref(),
        count,
        groups,
        LogLimits::default().max_entries_per_group,
    )?;
    Ok(schedule)
}

pub(super) fn validate_capacity(
    schedule: Option<&GroupSchedule>,
    count: usize,
    groups: usize,
    max_entries: usize,
) -> Result<(), Failure> {
    if let Some(schedule) = schedule {
        schedule.validate_capacity(count, max_entries)
    } else if (count + WARMUP).div_ceil(groups) + 64 > max_entries {
        Err("workload exceeds default per-group log capacity (including control reserve)".into())
    } else {
        Ok(())
    }
}

pub(super) fn extract(args: &mut Vec<String>) -> Result<Option<String>, Failure> {
    let Some(position) = args.iter().position(|arg| arg == "--group-weights") else {
        return Ok(None);
    };
    if args
        .iter()
        .skip(position + 1)
        .any(|arg| arg == "--group-weights")
    {
        return Err("duplicate --group-weights".into());
    }
    let raw = args
        .get(position + 1)
        .filter(|value| !value.starts_with("--"))
        .ok_or("missing --group-weights value")?
        .clone();
    args.drain(position..position + 2);
    Ok(Some(raw))
}

impl GroupSchedule {
    pub(super) fn parse(raw: &str, groups: usize) -> Result<Self, Failure> {
        if !(1..=32).contains(&groups) {
            return Err("group count out of bounds".into());
        }
        let mut weights = Vec::with_capacity(groups);
        let mut total = 0usize;
        for value in raw.split(',') {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("workload group weights must be positive decimal integers".into());
            }
            let weight = value.parse::<usize>()?;
            if weight == 0 {
                return Err("workload group weights must be positive".into());
            }
            total = total
                .checked_add(weight)
                .ok_or("workload group weight total overflow")?;
            if total > 256 {
                return Err("workload group weight total exceeds 256".into());
            }
            weights.push(weight);
            if weights.len() > groups {
                return Err("workload group weight count must equal groups".into());
            }
        }
        if weights.len() != groups {
            return Err("workload group weight count must equal groups".into());
        }
        let mut slots = Vec::with_capacity(total);
        for (index, &weight) in weights.iter().enumerate() {
            slots.extend(std::iter::repeat_n(group_id(index + 1), weight));
        }
        Ok(Self { weights, slots })
    }

    pub(super) fn from_cli(
        raw: Option<&str>,
        groups: usize,
        offered: bool,
    ) -> Result<Option<Self>, Failure> {
        raw.map(|value| {
            if !offered {
                return Err("--group-weights requires --offered".into());
            }
            Self::parse(value, groups)
        })
        .transpose()
    }

    pub(super) fn group(&self, operation: usize) -> GroupIdentity {
        self.slots[(operation - 1) % self.slots.len()]
    }

    fn offer_counts(&self, count: usize) -> Vec<usize> {
        let cycles = count / self.slots.len();
        let mut counts = self
            .weights
            .iter()
            .map(|weight| weight * cycles)
            .collect::<Vec<_>>();
        for offset in 0..count % self.slots.len() {
            let group = self.group(WARMUP + offset + 1);
            counts[group.id.get() as usize - 1] += 1;
        }
        counts
    }

    pub(super) fn validate_capacity(
        &self,
        count: usize,
        max_entries: usize,
    ) -> Result<(), Failure> {
        let groups = self.weights.len();
        for (index, offers) in self.offer_counts(count).into_iter().enumerate() {
            let warmup = WARMUP / groups + usize::from(index < WARMUP % groups);
            let retained = offers
                .checked_add(warmup)
                .and_then(|value| value.checked_add(64))
                .ok_or("weighted workload log capacity overflow")?;
            if retained > max_entries {
                return Err(
                    "workload exceeds default per-group log capacity (including control reserve)"
                        .into(),
                );
            }
        }
        Ok(())
    }

    pub(super) fn metadata(&self) -> String {
        let weights = self
            .weights
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            " workload_group_weights={weights} workload_group_phase=absolute_operation_minus_one"
        )
    }
}

#[cfg(test)]
#[path = "offered_groups/tests.rs"]
mod tests;
