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
fn ancestry(
    manifests: &BTreeMap<ResponsibilityIdentity, ResponsibilityManifest>,
    authority: GroupIdentity,
    start: ResponsibilityIdentity,
) -> Result<Vec<ResponsibilityIdentity>, RoutingError> {
    let mut chain = Vec::new();
    let mut current = Some(start);
    while let Some(id) = current {
        if chain.contains(&id) {
            return Err(RoutingError::Cycle);
        }
        if chain.len() == MAX_ROUTE_HOPS {
            return Err(RoutingError::HopLimit);
        }
        chain.push(id);
        let m = manifests.get(&id).ok_or(RoutingError::Missing(id))?;
        current = match m.input().parent {
            None => None,
            Some(p) if p.group == authority => Some(p.responsibility),
            Some(_) => return Err(RoutingError::WrongParent),
        };
    }
    Ok(chain)
}
impl Directory {
    pub(super) fn reparent(&mut self, plan: ReparentPlan) -> DirectoryOutcome {
        let manifests = [plan.old_parent(), plan.new_parent(), plan.child()];
        if manifests.iter().any(|m| {
            m.input().authority != self.plan.authority
                || !self.manifests.contains_key(&m.input().responsibility)
        }) {
            return DirectoryOutcome::UnknownResponsibility;
        }
        if manifests
            .iter()
            .any(|m| self.manifests.get(&m.input().responsibility) != Some(m))
        {
            return DirectoryOutcome::GenerationMismatch;
        }
        let mut affected = BTreeSet::new();
        for m in manifests {
            let chain = match ancestry(
                &self.manifests,
                self.plan.authority,
                m.input().responsibility,
            ) {
                Ok(c) => c,
                Err(e) => return DirectoryOutcome::ReparentRejected(e),
            };
            affected.extend(chain);
        }
        // Inspect the locally owned subtree: a foreign child's ancestry/height
        // needs the later cross-authority protocol, not an assumed empty leaf.
        let mut subtree = BTreeSet::new();
        let mut pending = vec![plan.child().input().responsibility];
        while let Some(id) = pending.pop() {
            if !subtree.insert(id) {
                return DirectoryOutcome::ReparentRejected(RoutingError::Cycle);
            }
            let Some(m) = self.manifests.get(&id) else {
                return DirectoryOutcome::UnknownResponsibility;
            };
            if let ExecutionMode::Delegated(routes) = &m.input().execution {
                for r in routes {
                    if let RouteTarget::Child(c) = r.target {
                        if c.group != self.plan.authority {
                            return DirectoryOutcome::ReparentRejected(RoutingError::WrongChild);
                        }
                        let Some(child) = self.manifests.get(&c.responsibility) else {
                            return DirectoryOutcome::UnknownResponsibility;
                        };
                        if !delegates(m, child) {
                            return DirectoryOutcome::ReparentRejected(RoutingError::WrongChild);
                        }
                        pending.push(c.responsibility);
                    }
                }
            }
        }
        affected.extend(&subtree);
        if affected.iter().any(|id| {
            self.deletion_busy(*id)
                || self.transfers.contains_key(id)
                || self.delegations.contains_key(id)
        }) || self.creations.iter().any(|(g, (_, op))| {
            !self.namespace_publications.contains_key(op)
                && !self.insertion_creations.contains(op)
                && self
                    .group_creation_at(self.applied, *g)
                    .ok()
                    .flatten()
                    .is_some_and(|s| affected.contains(&s.intent.parent))
        }) {
            return DirectoryOutcome::LifecycleBusy;
        }
        let updates = plan.updated_manifests();
        let mut candidate = self.manifests.clone();
        for m in &updates {
            candidate.insert(m.input().responsibility, m.clone());
        }
        for id in subtree {
            if let Err(e) = ancestry(&candidate, self.plan.authority, id) {
                return DirectoryOutcome::ReparentRejected(e);
            }
        }
        for m in updates {
            self.manifests.insert(m.input().responsibility, m);
        }
        DirectoryOutcome::Reparented
    }
    /// Original local applied status; use the original authority quorum read for
    /// a foreign observation. The plan is not a transferable certificate.
    pub fn reparent_status_at(
        &self,
        required: u64,
        operation: OperationId,
    ) -> Result<Option<ReparentStatus>, ApplicationError> {
        if !self.local_reparenting {
            return Err(ApplicationError::UnsupportedSchema);
        }
        if required > self.applied {
            return Err(ApplicationError::NotApplied);
        }
        self.history
            .get(&operation)
            .filter(|h| h.outcome == DirectoryOutcome::Reparented)
            .map(|h| {
                Ok(ReparentStatus {
                    operation,
                    index: h.index,
                    plan: ReparentPlan::decode(&h.bytes)?,
                })
            })
            .transpose()
    }
}
