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
use super::*;
mod consume;
mod intake;
impl<
        S: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
        W: PersistenceWorker,
        O: OutboundQueue,
        H: SnapshotWorker,
        C: PeerConnector,
        F: PeerTransportFactory<C::Session>,
    > ProgramDriver<'_, S, T, E, W, O, H, C, F>
where
    C::Endpoint: Clone,
{
    /// Progress is bounded by three independent visit limits. Zero completion or
    /// transition visits leaves original Node output ownership unconsumed.
    pub fn advance(
        &mut self,
        now: MonoTime,
        budget: ProgramDriverBudget,
    ) -> Result<ProgramDriverProgress, ProgramDriverError> {
        budget.validate()?;
        if self.now.is_some_and(|old| now < old) {
            return Err(ProgramDriverError::TimeWentBack);
        }
        self.now = Some(now);
        let mut progress = ProgramDriverProgress::default();
        for _ in 0..budget.node_polls.min(self.nodes.len()) {
            let node = self.node_cursor;
            self.node_cursor = (node + 1) % self.nodes.len();
            progress.node_polls += 1;
            if let Err(reason) = self.nodes[node].poll(now, budget.node) {
                self.fail(ProgramDriverError::Node {
                    node,
                    reason: Box::new(reason),
                });
            }
        }
        progress.completions = self.cleanup_retained(budget.completions);
        for _ in progress.completions..budget.completions {
            if !self.consume_one() {
                break;
            }
            progress.completions += 1;
        }
        self.reconcile_stopped();
        if !self.stopped {
            for _ in 0..budget.transitions.min(self.work.len()) {
                let sequence = self
                    .work
                    .range((
                        std::ops::Bound::Excluded(self.work_cursor),
                        std::ops::Bound::Unbounded,
                    ))
                    .next()
                    .or_else(|| self.work.iter().next())
                    .map(|(s, _)| *s);
                let Some(sequence) = sequence else {
                    break;
                };
                self.work_cursor = sequence;
                let mut work = self.work.remove(&sequence).expect("selected owned work");
                if work.attempt.is_none() && work.outcome.is_none() {
                    progress.transitions += 1;
                    if let Err(error) = self.issue(&mut work) {
                        self.restore_work(sequence, work);
                        self.fail(error);
                        break;
                    }
                }
                self.restore_work(sequence, work);
            }
        }
        progress.usage = self.usage();
        if let Some(error) = self.failure.clone() {
            Err(error)
        } else {
            Ok(progress)
        }
    }
}
