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

/// Correlation only. This visit is not an EffectLease or a Raft action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotExportTicket {
    pub owner: RuntimeOwner,
    pub request: SnapshotWorkTicket,
    pub visit: VisitTicket,
    pub reference: SnapshotRef,
}
#[derive(Debug)]
pub enum SnapshotExportOutcome {
    /// Verified selected-provider data, not readiness or quorum authority.
    Loaded(Snapshot),
    Failed(StorageError),
}
/// One original observation. Polling retains router count/image credits until
/// this exact object is completed; dropping it does not cancel accepted work.
#[derive(Debug)]
pub struct SnapshotExportCompletion {
    pub(super) ticket: SnapshotExportTicket,
    pub(super) outcome: SnapshotExportOutcome,
}
impl SnapshotExportCompletion {
    pub fn ticket(&self) -> SnapshotExportTicket {
        self.ticket
    }
    pub fn outcome(&self) -> &SnapshotExportOutcome {
        &self.outcome
    }
}
#[derive(Debug)]
pub struct SnapshotExportRejected {
    pub reason: SnapshotRouteError,
    pub completion: Box<SnapshotExportCompletion>,
}
pub(in super::super) struct Export {
    pub ticket: SnapshotExportTicket,
    pub expected: SnapshotMetadata,
    pub allowance: usize,
    pub charge: usize,
    pub cancelled: bool,
    pub exported: bool,
    pub outcome: Option<SnapshotExportOutcome>,
}
#[derive(Default)]
pub(in super::super) struct Exports {
    pub pending: BTreeMap<u64, Export>,
    pub failed: BTreeMap<u64, (SnapshotWorkTicket, VisitTicket)>,
    pub sequence: u64,
    pub closed: bool,
}
impl Exports {
    pub fn usage(&self) -> SnapshotRouterUsage {
        SnapshotRouterUsage {
            requests: self.pending.len() + self.failed.len(),
            image_bytes: self.pending.values().map(|p| p.charge).sum::<usize>()
                + self.failed.len()
                    * (size_of::<SnapshotWorkTicket>() + size_of::<VisitTicket>() + 256),
        }
    }
    pub fn has_group(&self, group: GroupIdentity) -> bool {
        self.pending.values().any(|p| p.ticket.visit.group == group)
    }
    pub fn has_request(&self, sequence: u64) -> bool {
        self.pending
            .values()
            .any(|p| p.ticket.request.sequence == sequence)
    }
    pub fn is_drained(&self) -> bool {
        self.pending.is_empty() && self.failed.is_empty()
    }
    pub fn remember_failed(&mut self, request: SnapshotWorkTicket, visit: VisitTicket) {
        self.failed.insert(request.sequence, (request, visit));
    }
}
