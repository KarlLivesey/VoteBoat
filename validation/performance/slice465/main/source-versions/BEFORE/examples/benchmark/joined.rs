// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Joined intake observations from returned original steps, without pre-poll work.
//! Originals here have proposed positions; they are not completed writes. The
//! outcome ledger remains the source of completed throughput and latency.
use super::*;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counts {
    transitions: u64,
    replies: u64,
    originals: u64,
}
#[derive(Default)]
pub(super) struct Totals {
    replicas: [Counts; 3],
}
impl Counts {
    fn add(&mut self, other: Self) -> Result<(), Failure> {
        self.transitions = self
            .transitions
            .checked_add(other.transitions)
            .ok_or("joined transition overflow")?;
        self.replies = self
            .replies
            .checked_add(other.replies)
            .ok_or("joined reply overflow")?;
        self.originals = self
            .originals
            .checked_add(other.originals)
            .ok_or("joined original overflow")?;
        Ok(())
    }
}
fn count(steps: &[OwnerStep]) -> Counts {
    let replies = steps
        .iter()
        .take_while(|step| {
            step.operation.is_none()
                && step.proposed.is_none()
                && step.read.is_none()
                && step.error.is_none()
        })
        .count();
    let originals = steps.len() - replies;
    if replies == 0
        || originals == 0
        || steps[replies..].iter().any(|step| {
            step.operation.is_none()
                || step.proposed.is_none()
                || step.read.is_some()
                || step.error.is_some()
        })
    {
        return Counts::default();
    }
    Counts {
        transitions: 1,
        replies: replies as u64,
        originals: originals as u64,
    }
}
impl Totals {
    pub(super) fn observe(
        &mut self,
        replica: usize,
        mut steps: &[OwnerStep],
    ) -> Result<(), Failure> {
        let total = self
            .replicas
            .get_mut(replica)
            .ok_or("invalid joined replica")?;
        while let Some(first) = steps.first() {
            let width = steps
                .iter()
                .take_while(|step| step.visit == first.visit)
                .count();
            total.add(count(&steps[..width]))?;
            steps = &steps[width..];
        }
        Ok(())
    }
    pub(super) fn summary(&self) -> Result<String, Failure> {
        let mut total = Counts::default();
        for counts in self.replicas {
            total.add(counts)?;
        }
        Ok(format!(
            "joined_transitions={} joined_reply_inputs={} joined_originals={}",
            total.transitions, total.replies, total.originals
        ))
    }
}
pub(super) fn write(root: &Path, phases: &[(&str, &PollTotals)]) -> Result<(), Failure> {
    let mut file = exclusive(&root.join("joined.csv"))?;
    writeln!(
        file,
        "phase,replica,joined_transitions,joined_reply_inputs,joined_originals"
    )?;
    for (phase, polls) in phases {
        for (replica, counts) in polls.joined.replicas.iter().enumerate() {
            writeln!(
                file,
                "{phase},{},{},{},{}",
                replica + 1,
                counts.transitions,
                counts.replies,
                counts.originals
            )?;
        }
    }
    file.sync_all()?;
    Ok(())
}
#[cfg(test)]
#[path = "joined_tests.rs"]
mod tests;
