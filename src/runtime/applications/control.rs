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
//! Selected whole-batch storage reservation, independent of protocol authority.
use super::*;
impl<R: ApplicationReceipt> ApplicationRouter<R> {
    pub(crate) fn logical_batch_reservation(&self) -> Option<usize> {
        self.logical_batch
    }
    pub(super) fn select_logical_batch(
        mut self,
        count: usize,
    ) -> Result<Self, ApplicationRouteError> {
        if count == 0 || count > self.limits.batch_receipts {
            return Err(ApplicationRouteError::InvalidLimits);
        }
        self.logical_batch = Some(count);
        Ok(self)
    }
    pub(crate) fn activation_reservation(
        &self,
    ) -> Option<(&LogicalActivationControlProfile, usize)> {
        self.control_profile
            .as_ref()
            .map(|profile| (profile, self.limits.batch_receipts))
    }
    pub(super) fn select_activation(
        mut self,
        profile: LogicalActivationControlProfile,
        max_committed_entries: usize,
    ) -> Result<Self, ApplicationRouteError> {
        let l = self.limits;
        let invalid = ApplicationRouteError::InvalidLimits;
        let receipts = l
            .control_batches
            .checked_mul(l.batch_receipts)
            .ok_or_else(|| invalid.clone())?;
        let fixed = l
            .batch_receipts
            .checked_mul(size_of::<R>())
            .ok_or_else(|| invalid.clone())?;
        let bytes = l
            .batch_receipts
            .checked_mul(size_of::<ProposalPosition>())
            .and_then(|n| n.checked_add(l.batch_bytes))
            .and_then(|n| n.checked_add(size_of::<Held<R>>()))
            .and_then(|n| n.checked_mul(l.control_batches))
            .ok_or_else(|| invalid.clone())?;
        if l.control_batches < 2
            || max_committed_entries == 0
            || max_committed_entries > l.batch_receipts
            || fixed > l.batch_bytes
            || bytes > l.control_bytes
            || l.receipts
                .checked_sub(receipts)
                .is_none_or(|n| n < l.batch_receipts)
        {
            return Err(invalid);
        }
        self.control_receipts = receipts;
        self.control_profile = Some(profile);
        Ok(self)
    }
    pub(super) fn check_capacity(
        &self,
        count: usize,
        bytes: usize,
        control: bool,
    ) -> Result<(), ApplicationRouteError> {
        let l = self.limits;
        if self.usage.batches >= l.batches
            || count > l.receipts.saturating_sub(self.usage.receipts)
            || bytes > l.bytes.saturating_sub(self.usage.bytes)
            || (!control
                && (self.bulk.batches >= l.batches - l.control_batches
                    || count
                        > (l.receipts - self.control_receipts).saturating_sub(self.bulk.receipts)
                    || bytes > (l.bytes - l.control_bytes).saturating_sub(self.bulk.bytes)))
        {
            return Err(ApplicationRouteError::Overloaded);
        }
        Ok(())
    }
}
