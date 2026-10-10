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
//! Private checkpoint decoding; publish the restored application only after full validation.
use super::*;
struct RestoredFences {
    initialized: Option<(OperationId, u64)>,
    fence: Option<OwnershipFence>,
    scope_fences: Vec<ScopedOwnershipFence>,
    scope_operations: BTreeSet<OperationId>,
}
struct RestoredSemantics {
    history: BTreeMap<OperationId, Semantic>,
    indices: BTreeSet<u64>,
    retained: usize,
}
impl<A, P> RoutedApplication<A, P>
where
    A: CheckpointStateMachine + BoundedStateMachine,
    A::Receipt: ApplicationReceipt,
    P: PartitionPolicy + Clone,
{
    pub(super) fn decode_checkpoint(
        &self,
        applied: u64,
        bytes: &[u8],
    ) -> Result<Self, ApplicationError> {
        let mut r = Reader::new(bytes);
        if r.take(8)?
            != if self.metadata_locator_adoption {
                b"VBROUT06"
            } else if self.metadata_adoption {
                b"VBROUT05"
            } else if self.cross_parent_adoption {
                b"VBROUT04"
            } else if self.parent_adoption_limit != 0 {
                b"VBROUT03"
            } else if self.scope_limit == 0 {
                b"VBROUT01"
            } else {
                b"VBROUT02"
            }
            || r.u64()? != applied
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let binding_len = r.u32()? as usize;
        if r.take(binding_len)? != self.binding {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let fences = self.read_checkpoint_fences(&mut r, applied)?;
        let inner_schema = r.u64()?;
        let inner_len = r.u32()? as usize;
        if inner_schema != self.inner.schema_version()
            || inner_len > self.limits.inner_checkpoint_bytes
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut next = self.clone();
        next.inner
            .restore_checkpoint(inner_schema, applied, r.take(inner_len)?)?;
        if next.inner.applied_index() != applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let semantics = self.read_checkpoint_semantics(&mut r, applied, &fences)?;
        let (adoptions, active) =
            self.read_checkpoint_adoptions(&mut r, applied, &fences, &semantics)?;
        if !r.done() {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.initialized = fences.initialized;
        next.fence = fences.fence;
        next.scope_fences = fences.scope_fences;
        next.history = semantics.history;
        next.semantic_bytes = semantics.retained;
        next.parent_adoptions = adoptions;
        next.grant = active;
        Ok(next)
    }
    fn read_checkpoint_full_fence(
        &self,
        r: &mut Reader<'_>,
        applied: u64,
        initialized: Option<(OperationId, u64)>,
    ) -> Result<Option<OwnershipFence>, ApplicationError> {
        let fence = if r.boolean()? {
            let operation = r.operation()?;
            let index = r.u64()?;
            let epoch = OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCheckpoint)?;
            let Some((initial, initial_index)) = initialized else {
                return Err(ApplicationError::InvalidCheckpoint);
            };
            if index <= initial_index
                || index > applied
                || operation == initial
                || epoch != self.grant.input().epoch
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            Some(OwnershipFence {
                group: self.local,
                responsibility: self.grant.input().responsibility,
                epoch,
                operation,
                index,
            })
        } else {
            None
        };
        Ok(fence)
    }
    fn read_checkpoint_fences(
        &self,
        r: &mut Reader<'_>,
        applied: u64,
    ) -> Result<RestoredFences, ApplicationError> {
        let initialized = if r.boolean()? {
            Some((r.operation()?, r.u64()?))
        } else {
            None
        };
        if initialized.is_some_and(|(_, index)| index == 0 || index > applied) {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let fence = self.read_checkpoint_full_fence(r, applied, initialized)?;
        let mut scope_fences = Vec::new();
        let mut scope_operations = BTreeSet::new();
        let mut last_scope_index = 0;
        if self.scope_limit != 0 {
            let count = usize::from(r.u16()?);
            if count > self.scope_limit {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            for _ in 0..count {
                let operation = r.operation()?;
                let index = r.u64()?;
                let epoch =
                    OwnershipEpoch::new(r.u64()?).ok_or(ApplicationError::InvalidCheckpoint)?;
                let scope = r.range()?;
                let Some((initial, initial_index)) = initialized else {
                    return Err(ApplicationError::InvalidCheckpoint);
                };
                if !self.owns_scope(scope)
                    || epoch != self.grant.input().epoch
                    || operation == initial
                    || index <= initial_index
                    || index > applied
                    || index <= last_scope_index
                    || !scope_operations.insert(operation)
                    || fence.is_some_and(|f| f.operation == operation || index >= f.index)
                    || scope_fences.iter().any(|s: &ScopedOwnershipFence| {
                        s.scope.start() < scope.end() && scope.start() < s.scope.end()
                    })
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                scope_fences.push(ScopedOwnershipFence {
                    scope,
                    fence: OwnershipFence {
                        group: self.local,
                        responsibility: self.grant.input().responsibility,
                        epoch,
                        operation,
                        index,
                    },
                });
                last_scope_index = index;
            }
        }
        Ok(RestoredFences {
            initialized,
            fence,
            scope_fences,
            scope_operations,
        })
    }
    fn read_checkpoint_semantics(
        &self,
        r: &mut Reader<'_>,
        applied: u64,
        fences: &RestoredFences,
    ) -> Result<RestoredSemantics, ApplicationError> {
        let initialized = fences.initialized;
        let fence = fences.fence;
        let scope_fences = &fences.scope_fences;
        let scope_operations = &fences.scope_operations;
        let count = r.u32()? as usize;
        if count > self.limits.operations
            || count as u64 > applied
            || (initialized.is_none() && count != 0)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let mut history = BTreeMap::new();
        let mut indices = BTreeSet::new();
        let mut retained = 0;
        let mut previous = 0;
        for _ in 0..count {
            let operation = r.operation()?;
            let index = r.u64()?;
            let key_len = usize::from(r.u16()?);
            let payload_len = r.u32()? as usize;
            let Some((initial, initial_index)) = initialized else {
                return Err(ApplicationError::InvalidCheckpoint);
            };
            if operation.get() <= previous
                || operation == initial
                || scope_operations.contains(&operation)
                || scope_fences.iter().any(|s| s.fence.index == index)
                || fence.is_some_and(|f| f.operation == operation || index >= f.index)
                || index <= initial_index
                || index > applied
                || !indices.insert(index)
                || key_len > MAX_ROUTING_KEY_BYTES
                || payload_len > self.limits.payload_bytes
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let key = r.take(key_len)?;
            let payload = r.take(payload_len)?;
            if !self.initial_key(key)
                || scope_fences.iter().any(|s| {
                    self.policy.bucket(key).is_ok_and(|b| s.scope.contains(b))
                        && index >= s.fence.index
                })
            {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            let semantic = Semantic {
                index,
                key: key.to_vec(),
                payload: payload.to_vec(),
            };
            retained += semantic.key.capacity() + semantic.payload.capacity();
            if retained > self.limits.semantic_bytes {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            previous = operation.get();
            history.insert(operation, semantic);
        }
        Ok(RestoredSemantics {
            history,
            indices,
            retained,
        })
    }
    fn read_checkpoint_adoptions(
        &self,
        r: &mut Reader<'_>,
        applied: u64,
        fences: &RestoredFences,
        semantics: &RestoredSemantics,
    ) -> Result<(Vec<ParentAdoptionRecord>, ResponsibilityManifest), ApplicationError> {
        let initialized = fences.initialized;
        let fence = fences.fence;
        let history = &semantics.history;
        let indices = &semantics.indices;
        let mut adoptions = Vec::new();
        let mut active = if self.parent_adoption_limit == 0 {
            self.grant.clone()
        } else {
            self.bootstrap_grant()?
        };
        if self.parent_adoption_limit != 0 {
            let count = usize::from(r.u16()?);
            if count > self.parent_adoption_limit {
                return Err(ApplicationError::InvalidCheckpoint);
            }
            adoptions
                .try_reserve_exact(count)
                .map_err(|_| ApplicationError::InvalidCheckpoint)?;
            let mut last = 0;
            let mut ops = BTreeSet::new();
            for _ in 0..count {
                let operation = r.operation()?;
                let index = r.u64()?;
                let n = r.u32()? as usize;
                let command = ParentAdoptionCommand::decode_metadata(
                    r.take(n)?,
                    self.cross_parent_adoption,
                    self.metadata_adoption,
                    self.metadata_locator_adoption,
                )?;
                let Some((initial, initial_index)) = initialized else {
                    return Err(ApplicationError::InvalidCheckpoint);
                };
                if operation == initial
                    || history.contains_key(&operation)
                    || !ops.insert(operation)
                    || index <= initial_index
                    || index <= last
                    || index > applied
                    || indices.contains(&index)
                    || fence.is_some_and(|f| f.operation == operation || index >= f.index)
                    || command.before() != &active
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                active = command.after();
                adoptions.push(ParentAdoptionRecord {
                    status: ParentGrantStatus {
                        operation,
                        index,
                        metadata_operation: command.metadata().0,
                        metadata_index: command.metadata().1,
                        generation: active.input().generation,
                    },
                    command,
                });
                last = index;
            }
        }
        Ok((adoptions, active))
    }
}
