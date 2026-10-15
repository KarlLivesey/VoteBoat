// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded benchmark diagnostics for exact explicit barrier dependencies.
use super::*;

#[derive(Clone)]
pub(super) struct Barriers {
    pending: Vec<(LogTicket, u64)>,
    pub(super) successes: u64,
    pub(super) known_originals: u64,
    pub(super) missing: u64,
    pub(super) dropped: u64,
    synchronized: u64,
    no_io: u64,
    ambiguous_io: u64,
    synchronized_grouped: u64,
    synchronized_grouped_originals: u64,
    units: [u64; 66],
    originals: [u64; 66],
    same_group_units: [u64; 66],
}

impl Default for Barriers {
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            successes: 0,
            known_originals: 0,
            missing: 0,
            dropped: 0,
            synchronized: 0,
            no_io: 0,
            ambiguous_io: 0,
            synchronized_grouped: 0,
            synchronized_grouped_originals: 0,
            units: [0; 66],
            originals: [0; 66],
            same_group_units: [0; 66],
        }
    }
}
impl Barriers {
    pub(super) fn accepted(
        &mut self,
        tickets: &[LogTicket],
        commands: &[(GroupIdentity, usize)],
        limit: usize,
    ) {
        for ticket in tickets {
            let count = commands
                .iter()
                .find(|(group, _)| *group == ticket.group)
                .map(|(_, count)| *count);
            if self.pending.len() < limit && !self.pending.iter().any(|(old, _)| old == ticket) {
                if let Some(count) = count {
                    self.pending.push((*ticket, count as u64));
                    continue;
                }
            }
            self.dropped = self.dropped.saturating_add(1);
        }
    }
    pub(super) fn completed(&mut self, tickets: &[LogTicket], syncs: u64, publications: u64) {
        self.successes = self.successes.saturating_add(1);
        Self::record(&mut self.units, tickets.len() as u64);
        let mut originals = 0u64;
        let mut complete = true;
        let mut same_group = 0;
        for ticket in tickets {
            same_group = same_group.max(
                tickets
                    .iter()
                    .filter(|other| other.group == ticket.group)
                    .count(),
            );
            if let Some(position) = self.pending.iter().position(|(old, _)| old == ticket) {
                originals = originals.saturating_add(self.pending.swap_remove(position).1);
            } else {
                self.missing = self.missing.saturating_add(1);
                complete = false;
            }
        }
        self.known_originals = self.known_originals.saturating_add(originals);
        if complete {
            Self::record(&mut self.originals, originals);
        }
        Self::record(&mut self.same_group_units, same_group as u64);
        match (syncs, publications) {
            (1, 1) => {
                self.synchronized = self.synchronized.saturating_add(1);
                if same_group > 1 && complete {
                    self.synchronized_grouped = self.synchronized_grouped.saturating_add(1);
                    self.synchronized_grouped_originals = self
                        .synchronized_grouped_originals
                        .saturating_add(originals);
                }
            }
            (0, 0) => self.no_io = self.no_io.saturating_add(1),
            _ => self.ambiguous_io = self.ambiguous_io.saturating_add(1),
        }
    }
    fn record(histogram: &mut [u64; 66], count: u64) {
        let bin = usize::try_from(count).unwrap_or(usize::MAX).min(65);
        histogram[bin] = histogram[bin].saturating_add(1);
    }
    pub(super) fn fields(&self) -> String {
        let histogram = |bins: &[u64; 66]| {
            bins.iter()
                .enumerate()
                .filter(|(_, count)| **count > 0)
                .map(|(bin, count)| format!("{bin}:{count}"))
                .collect::<Vec<_>>()
                .join("|")
        };
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.successes,
            self.known_originals,
            self.missing,
            self.dropped,
            self.pending.len(),
            self.pending.capacity() * std::mem::size_of::<(LogTicket, u64)>(),
            histogram(&self.units),
            histogram(&self.originals),
            histogram(&self.same_group_units),
            self.synchronized,
            self.no_io,
            self.ambiguous_io,
            self.synchronized_grouped,
            self.synchronized_grouped_originals,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ticket(batch: u64) -> LogTicket {
        LogTicket {
            binding: StoreBinding {
                identity: store(1),
                session: StoreSession::new(1).unwrap(),
            },
            batch,
            group: group(),
            revision: LogRevision::new(batch).unwrap(),
            generation: LogGeneration::new(1).unwrap(),
            last_index: batch,
            term: 1,
        }
    }
    #[test]
    fn grouped_originals_follow_exact_tickets_not_completion_order_or_group_maximum() {
        let mut counts = Barriers::default();
        let a = ticket(1);
        let b = ticket(2);
        counts.accepted(&[a], &[(a.group, 16)], 2);
        counts.accepted(&[b], &[(b.group, 8)], 2);
        counts.completed(&[b, a], 1, 1);
        assert_eq!(counts.successes, 1);
        assert_eq!(counts.known_originals, 24);
        assert_eq!(counts.units[2], 1);
        assert_eq!(counts.originals[24], 1);
        assert_eq!(counts.same_group_units[2], 1);
        assert_eq!(
            (
                counts.synchronized,
                counts.synchronized_grouped,
                counts.synchronized_grouped_originals
            ),
            (1, 1, 24)
        );
        assert_eq!(
            (counts.pending.len(), counts.missing, counts.dropped),
            (0, 0, 0)
        );
    }
    #[test]
    fn full_group_incarnation_scope_and_missing_or_dropped_tracking_remain_visible() {
        let mut counts = Barriers::default();
        let a = ticket(1);
        let mut b = ticket(2);
        b.group.incarnation = GroupIncarnation::new(2).unwrap();
        counts.accepted(&[a, b], &[(a.group, 64), (b.group, 2)], 2);
        counts.completed(&[a, b], 0, 0);
        assert_eq!(counts.same_group_units[1], 1);
        assert_eq!(counts.originals[65], 1, "65 is the explicit overflow bin");
        assert_eq!((counts.synchronized, counts.no_io), (0, 1));
        counts.accepted(&[a, b], &[(a.group, 1), (b.group, 1)], 1);
        counts.completed(&[b], 1, 0);
        assert_eq!(
            (counts.pending.len(), counts.missing, counts.dropped),
            (1, 1, 1)
        );
        assert_eq!(
            counts.originals[0], 0,
            "missing information must not become a zero-original barrier"
        );
        assert_eq!((counts.ambiguous_io, counts.synchronized_grouped), (1, 0));
    }
}
