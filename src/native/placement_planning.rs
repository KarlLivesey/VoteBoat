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
//! Deterministic initial learner placement over explicit finite host samples.
use crate::placement::*;
use std::{cmp::Reverse, collections::BTreeMap};
#[derive(Clone, Copy, Debug, Default)]
pub struct NativePlacementPlanner;
impl PlacementPlanner for NativePlacementPlanner {
    fn select(
        &self,
        request: PlacementRequest<'_>,
    ) -> Result<PlacementRecommendation, PlacementPlanningError> {
        request.validate()?;
        let mut occupancy = BTreeMap::new();
        for c in request.snapshot.candidates {
            if request
                .current
                .stable()
                .voter_stores()
                .contains_key(&c.node)
                || request.current.stable().learners().contains_key(&c.node)
            {
                *occupancy.entry(c.placement.domain).or_insert(0usize) += 1;
            }
        }
        let c = request
            .snapshot
            .candidates
            .iter()
            .filter(|c| request.eligible(c))
            .min_by_key(|c| {
                (
                    occupancy.get(&c.placement.domain).copied().unwrap_or(0),
                    c.load_permille,
                    Reverse(c.free_bytes),
                    c.node,
                )
            })
            .ok_or(PlacementPlanningError::NoCandidate)?;
        Ok(PlacementRecommendation {
            group: request.snapshot.group,
            configuration: request.current.id(),
            sample: request.snapshot.generation,
            node: c.node,
            store: c.placement.store,
        })
    }
}
