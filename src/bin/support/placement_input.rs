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
//! Explicit planning snapshots are hints, never remote durability evidence.
use super::{
    administration::{cid, node, tree},
    setup::{checked, group, Failure},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::Path,
};
use voteboat::{
    identity::*, membership::*, native::placement::NativePlacementAuthorizer, placement::*,
    quorum::*, runtime::MonoTime,
};
pub struct Input {
    pub authorizer: NativePlacementAuthorizer,
    pub current: Membership,
    pub candidates: Vec<PlacementCandidate>,
    generation: PlacementSampleGeneration,
    observed: MonoTime,
    expires: MonoTime,
    now: MonoTime,
    minimum: u64,
}
fn fields<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    name: &str,
) -> Result<Vec<&'a str>, Failure> {
    let mut words = lines
        .next()
        .ok_or("truncated placement input")?
        .split_whitespace();
    if words.next() != Some(name) {
        return Err(format!("expected {name} line").into());
    }
    Ok(words.collect())
}
fn candidates<'a>(
    lines: impl Iterator<Item = &'a str>,
) -> Result<Vec<PlacementCandidate>, Failure> {
    let mut result = Vec::new();
    for line in lines {
        if result.len() == 64 {
            return Err("placement input exceeds64 replicas".into());
        }
        let words = line.split_whitespace().collect::<Vec<_>>();
        let ["replica", id, store, inc, domain, enabled, free, slots, load] = words.as_slice()
        else {
            return Err("expected replica NODE STORE INCARNATION DOMAIN ENABLED FREE_BYTES FREE_SLOTS LOAD_PERMILLE".into());
        };
        result.push(PlacementCandidate {
            node: node(id)?,
            placement: ReplicaPlacement {
                store: StoreIdentity {
                    id: StoreId::new(store.parse()?).ok_or("invalid store")?,
                    incarnation: StoreIncarnation::new(inc.parse()?)
                        .ok_or("invalid store incarnation")?,
                },
                domain: FailureDomainId::new(domain.parse()?).ok_or("invalid failure domain")?,
            },
            enabled: enabled.parse()?,
            free_bytes: free.parse()?,
            free_replica_slots: slots.parse()?,
            load_permille: load.parse()?,
        });
    }
    Ok(result)
}
fn membership(
    words: &[&str],
    history: &[&str],
    candidates: &[PlacementCandidate],
) -> Result<Membership, Failure> {
    let [id, learners, policy @ ..] = words else {
        return Err("expected current CONFIGURATION LEARNERS POLICY".into());
    };
    let stores = candidates
        .iter()
        .map(|c| (c.node, c.placement.store))
        .collect::<BTreeMap<_, _>>();
    let mut tokens = policy.iter().copied();
    let policy = checked(Policy::new(
        tree(&mut tokens, 0, &mut 16384)?,
        Limits::default(),
    ))?;
    if tokens.next().is_some() {
        return Err("trailing current policy fields".into());
    }
    let voters = policy
        .voters()
        .iter()
        .map(|id| {
            stores
                .get(id)
                .copied()
                .map(|s| (*id, s))
                .ok_or("current voter missing from candidates")
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut learner_stores = BTreeMap::new();
    if *learners != "-" {
        for id in learners.split(',') {
            let id = node(id)?;
            let store = *stores
                .get(&id)
                .ok_or("current learner missing from candidates")?;
            if learner_stores.insert(id, store).is_some() {
                return Err("duplicate current learner".into());
            }
        }
    }
    let [history] = history else {
        return Err("expected history OPERATION_IDS or -".into());
    };
    let mut operations = BTreeSet::new();
    if *history != "-" {
        for id in history.split(',') {
            if operations.len() == MAX_CONFIGURATION_OPERATIONS {
                return Err("too many operation IDs".into());
            }
            let id = OperationId::new(id.parse()?).ok_or("invalid used operation ID")?;
            if !operations.insert(id) {
                return Err("duplicate used operation ID".into());
            }
        }
    }
    let configuration = checked(Configuration::new(cid(id)?, policy, voters, learner_stores))?;
    // Synthetic index only validates the caller's planning model. Never a receipt.
    let index = operations.len() as u64;
    checked(Membership::from_checkpoint(
        configuration,
        None,
        index,
        operations,
        index,
    ))
}
impl Input {
    pub fn load(path: &Path) -> Result<Self, Failure> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(65537)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 65536 {
            return Err("placement input exceeds64KiB".into());
        }
        let mut lines = std::str::from_utf8(&bytes)?.lines();
        if lines.next() != Some("voteboat-placement-v1") {
            return Err("expected voteboat-placement-v1".into());
        }
        if fields(&mut lines, "group")? != ["1", "1"] {
            return Err("counter placement requires group1 incarnation1".into());
        }
        let placement = fields(&mut lines, "placement")?;
        let [minimum, survive] = placement.as_slice() else {
            return Err("expected placement MINIMUM_DOMAINS SURVIVE_DOMAIN_LOSS".into());
        };
        let requirements = PlacementRequirements {
            minimum_voting_domains: minimum.parse()?,
            survive_any_single_domain_loss: survive.parse()?,
        };
        let sample = fields(&mut lines, "sample")?;
        let [generation, observed, expires, now, minimum] = sample.as_slice() else {
            return Err(
                "expected sample GENERATION OBSERVED EXPIRES NOW MINIMUM_FREE_BYTES".into(),
            );
        };
        let current = fields(&mut lines, "current")?;
        let history = fields(&mut lines, "history")?;
        let candidates = candidates(lines)?;
        let authorizer = NativePlacementAuthorizer::new(
            group(),
            candidates.iter().map(|c| (c.node, c.placement)).collect(),
            requirements,
        )
        .map_err(|(e, _)| format!("placement authorizer: {e:?}"))?;
        let current = membership(&current, &history, &candidates)?;
        let result = Self {
            authorizer,
            current,
            candidates,
            generation: PlacementSampleGeneration::new(generation.parse()?)
                .ok_or("invalid sample generation")?,
            observed: MonoTime(observed.parse()?),
            expires: MonoTime(expires.parse()?),
            now: MonoTime(now.parse()?),
            minimum: minimum.parse()?,
        };
        checked(result.request(&result.current).validate())?;
        Ok(result)
    }
    pub fn request<'a>(&'a self, current: &'a Membership) -> PlacementRequest<'a> {
        PlacementRequest {
            snapshot: PlacementSnapshot {
                group: self.authorizer.group(),
                configuration: current.id(),
                generation: self.generation,
                observed_at: self.observed,
                expires_at: self.expires,
                candidates: &self.candidates,
            },
            current,
            now: self.now,
            minimum_free_bytes: self.minimum,
        }
    }
}
