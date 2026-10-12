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
impl SnapshotRouter {
    pub(in super::super) fn deliver_export<
        Q: ReadyScheduler,
        T: TimerService,
        E: ElectionEntropy,
    >(
        &mut self,
        owner: &mut EffectOwner<Q, T, E>,
        event: SnapshotWorkEvent,
    ) -> Result<(), SnapshotRouteError> {
        let key = self
            .exports
            .pending
            .iter()
            .find(|(_, p)| p.ticket.request.sequence == event.request.sequence)
            .map(|(k, _)| *k)
            .ok_or(SnapshotRouteError::StaleCompletion)?;
        let p = self.exports.pending.get(&key).unwrap();
        if event.request != p.ticket.request
            || event.visit != p.ticket.visit
            || p.outcome.is_some()
            || p.exported
        {
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            return Err(SnapshotRouteError::StaleCompletion);
        }
        let result = match event.result {
            Err(error) => Ok(SnapshotExportOutcome::Failed(error)),
            Ok(SnapshotOutput::Loaded {
                reference,
                snapshot,
                reconciled: false,
            }) if reference == p.ticket.reference
                && reference.matches(&snapshot)
                && snapshot.metadata == p.expected
                && snapshot.metadata.validate().is_ok()
                && snapshot_image_bytes(&snapshot).is_some_and(|n| n <= p.allowance) =>
            {
                Ok(SnapshotExportOutcome::Loaded(snapshot))
            }
            Ok(_) => Err(SnapshotRouteError::ProviderContract),
        };
        let invalid = result.is_err();
        if p.cancelled {
            self.exports.pending.remove(&key);
        } else {
            self.exports.pending.get_mut(&key).unwrap().outcome = Some(result.unwrap_or(
                SnapshotExportOutcome::Failed(StorageError::Corrupt(
                    "snapshot export provider contract",
                )),
            ));
        }
        if invalid {
            let _ = owner.fail::<()>(EffectOwnerError::ProviderContract);
            return Err(SnapshotRouteError::ProviderContract);
        }
        Ok(())
    }
}
