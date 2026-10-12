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
impl<C: LogCodec + Clone, S: SnapshotCodec + Clone, W: WireCodec + Clone>
    LogicalApplication<C, S, W>
{
    pub(super) fn execute(
        &mut self,
        c: &Command,
        index: u64,
        term: u64,
    ) -> Result<LogicalOutcome, ApplicationError> {
        if let CommandBody::EnrollmentClaim(import) = &c.body {
            return self.enroll(c, index, term, import);
        }
        if let CommandBody::Claim { expected_epoch } = c.body {
            return self.claim(c.representative, expected_epoch);
        }
        if self.state.representative != Some(c.representative) {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative));
        }
        let Some(parent) = &self.state.parent else {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleImage));
        };
        let outcome = match &c.body {
            CommandBody::Claim { .. } | CommandBody::EnrollmentClaim(_) => unreachable!(),
            CommandBody::Mutate {
                expected_generation,
                update,
                origin,
            } => {
                if *expected_generation != parent.generation
                    || update.expected_revision != parent.revision
                {
                    return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleImage));
                }
                self.persist_origin(*origin, c.representative)?;
                if update
                    .snapshot
                    .is_some_and(|r| !self.state.snapshots.iter().any(|(s, _)| *s == r))
                {
                    return Ok(LogicalOutcome::Refused(LogicalRefusal::MissingSnapshot));
                }
                let mut image = BTreeMap::from([(self.mapping.parent, parent.clone())]);
                if apply_batch(
                    &mut image,
                    &[LogMutation::Update(update.clone())],
                    self.limits.parent_log,
                )
                .is_err()
                {
                    return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleImage));
                }
                self.state.parent = image.remove(&self.mapping.parent);
                LogicalOutcome::Mutated
            }
            CommandBody::Publish {
                expected_revision,
                durable,
                snapshot,
                origin,
            } => {
                self.snapshot_origin(*origin, c.representative)?;
                if *expected_revision != parent.revision || *durable != parent.snapshot {
                    return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleImage));
                }
                self.publish(snapshot)?
            }
            CommandBody::Reconcile {
                expected_revision,
                reference,
                origin,
            } => {
                self.snapshot_origin(*origin, c.representative)?;
                if *expected_revision != parent.revision
                    || parent.snapshot != Some(*reference)
                    || !self.state.snapshots.iter().any(|(r, _)| r == reference)
                {
                    return Ok(LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict));
                }
                self.state.snapshots.retain(|(r, _)| r == reference);
                self.state.published = Some(*reference);
                LogicalOutcome::SnapshotReconciled
            }
            CommandBody::Authorize(capsule) => self.authorize(capsule, c.representative)?,
        };
        Ok(outcome)
    }
    fn claim(
        &mut self,
        r: RepresentativeBinding,
        expected: Option<RepresentativeEpoch>,
    ) -> Result<LogicalOutcome, ApplicationError> {
        if expected != self.state.representative.map(|r| r.epoch)
            || r.epoch.get()
                != expected
                    .map_or(Some(1), |e| e.get().checked_add(1))
                    .unwrap_or(0)
            || r.logical_session.get()
                != self
                    .state
                    .representative
                    .map_or(Some(1), |r| r.logical_session.get().checked_add(1))
                    .unwrap_or(0)
        {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative));
        }
        if self.state.parent.is_none() && self.candidate() {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::MissingSnapshot));
        }
        if self.state.parent.is_none() {
            let mut image = BTreeMap::new();
            apply_batch(
                &mut image,
                &[LogMutation::Create(self.parent_bootstrap.clone())],
                self.limits.parent_log,
            )
            .map_err(|_| ApplicationError::InvalidCommand)?;
            self.state.parent = image.remove(&self.mapping.parent);
        }
        self.state.representative = Some(r);
        Ok(LogicalOutcome::Claimed)
    }
    fn publish(&mut self, snapshot: &Snapshot) -> Result<LogicalOutcome, ApplicationError> {
        let parent = self
            .state
            .parent
            .as_ref()
            .ok_or(ApplicationError::InvalidCommand)?;
        if snapshot.metadata.bootstrap != self.parent_bootstrap
            || snapshot.metadata.validate().is_err()
            || (snapshot.metadata.index <= parent.commit_index
                && (parent.term_at(snapshot.metadata.index) != Some(snapshot.metadata.term)
                    || parent
                        .checkpoint_membership(snapshot.metadata.index)
                        .map_err(|_| ApplicationError::InvalidCommand)?
                        != snapshot.metadata.membership))
        {
            return Ok(LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict));
        }
        let durable = parent.snapshot;
        if let Some(previous) = durable {
            let Some((_, old)) = self.state.snapshots.iter().find(|(r, _)| *r == previous) else {
                return Ok(LogicalOutcome::Refused(LogicalRefusal::MissingSnapshot));
            };
            if old != snapshot && !snapshot.metadata.follows(&old.metadata) {
                return Ok(LogicalOutcome::Refused(LogicalRefusal::SnapshotConflict));
            }
        }
        let existing = self.state.snapshots.iter().find(|(_, old)| old == snapshot);
        let reference = if let Some((reference, _)) = existing {
            *reference
        } else {
            let maximum = self
                .state
                .records
                .values()
                .filter_map(|s| match s.receipt.outcome {
                    LogicalOutcome::SnapshotPublished(r) => Some(r.generation.get()),
                    _ => None,
                })
                .chain(
                    self.state
                        .enrollment
                        .iter()
                        .map(|r| r.target_reference.generation.get()),
                )
                .max()
                .unwrap_or(0);
            let generation = maximum
                .checked_add(1)
                .and_then(SnapshotGeneration::new)
                .ok_or(ApplicationError::DedupCapacity)?;
            self.snapshot_reference(snapshot, generation)?
        };
        // Mutation starts only after every preflight check and selected codec succeeds.
        self.state
            .snapshots
            .retain(|(r, _)| Some(*r) == durable || *r == reference);
        if !self.state.snapshots.iter().any(|(r, _)| *r == reference) {
            self.state.snapshots.push((reference, snapshot.clone()));
        }
        self.state.published = Some(reference);
        Ok(LogicalOutcome::SnapshotPublished(reference))
    }
    fn persist_origin(
        &self,
        o: PersistOrigin,
        r: RepresentativeBinding,
    ) -> Result<(), ApplicationError> {
        if o.request.binding.store != self.binding(r)
            || o.request.sequence == 0
            || !self.visit(o.visit, r)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    fn snapshot_origin(
        &self,
        o: SnapshotOrigin,
        r: RepresentativeBinding,
    ) -> Result<(), ApplicationError> {
        if o.request.binding.store != self.binding(r)
            || o.request.sequence == 0
            || !self.visit(o.visit, r)
        {
            return Err(ApplicationError::InvalidCommand);
        }
        Ok(())
    }
    pub(super) fn binding(&self, r: RepresentativeBinding) -> StoreBinding {
        StoreBinding {
            identity: self.mapping.store,
            session: r.logical_session,
        }
    }
    pub(super) fn visit(&self, v: crate::runtime::VisitTicket, r: RepresentativeBinding) -> bool {
        v.owner.store == self.binding(r) && v.group == self.mapping.parent && v.sequence > 0
    }
}
