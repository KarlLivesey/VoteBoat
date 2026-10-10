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
//! Local policy inspection from caller-supplied hypothetical node IDs.
use super::setup::{group, Service};
use std::{collections::BTreeSet, fmt::Write};
use voteboat::{
    identity::{ConfigurationId, NodeId},
    quorum::{Policy, QuorumExplanation, QuorumRule},
};

pub fn explain(
    service: &Service,
    voters: &str,
    offset: &str,
    count: &str,
) -> Result<String, String> {
    let voters = parse_voters(voters)?;
    let offset = offset.parse().map_err(|_| "invalid quorum offset")?;
    let count = count.parse().map_err(|_| "invalid quorum count")?;
    let core = service.local().owner.core(group()).ok_or("missing group")?;
    let membership = core.membership();
    let stable = membership.stable();
    let next = membership.joint().map(|j| (j.next.id(), j.next.policy()));
    render((stable.id(), stable.policy()), next, &voters, offset, count)
}
fn parse_voters(text: &str) -> Result<BTreeSet<NodeId>, String> {
    let mut voters = BTreeSet::new();
    if text != "-" {
        for word in text.split(',') {
            let id = word
                .parse()
                .ok()
                .and_then(NodeId::new)
                .ok_or("invalid voter ID")?;
            if !voters.insert(id) {
                return Err("duplicate voter ID".into());
            }
        }
    }
    Ok(voters)
}
fn render(
    stable: (ConfigurationId, &Policy),
    next: Option<(ConfigurationId, &Policy)>,
    voters: &BTreeSet<NodeId>,
    offset: usize,
    count: usize,
) -> Result<String, String> {
    if !(1..=16).contains(&count) {
        return Err("quorum count must be 1..16".into());
    }
    let old = stable.1.explain(voters);
    let new = next.map(|(_, p)| p.explain(voters));
    let total = old.nodes.len() + new.as_ref().map_or(0, |n| n.nodes.len());
    if offset > total {
        return Err("quorum offset exceeds row count".into());
    }
    let end = offset.saturating_add(count).min(total);
    let satisfied = old.is_satisfied() && new.as_ref().is_none_or(QuorumExplanation::is_satisfied);
    let unknown = voters
        .iter()
        .filter(|n| {
            !stable.1.voters().contains(n) && next.is_none_or(|(_, p)| !p.voters().contains(n))
        })
        .count();
    let next_id = next.map_or_else(|| "-".into(), |(id, _)| id.get().to_string());
    let mut output = format!("OK evidence=hypothetical_nodes stable={} next={} satisfied={} unknown={} rows={} offset={} next_offset={} details=",
        stable.0.get(), next_id, satisfied, unknown, total, offset, end);
    for index in offset..end {
        let (label, row, base) = if index < old.nodes.len() {
            ("stable", &old.nodes[index], 0)
        } else {
            (
                "next",
                &new.as_ref().unwrap().nodes[index - old.nodes.len()],
                old.nodes.len(),
            )
        };
        let parent = row
            .parent
            .map_or_else(|| "-".into(), |p| (p + base).to_string());
        let rule = match row.rule {
            QuorumRule::Voter(id) => format!("voter{}", id.get()),
            QuorumRule::Majority => "majority".into(),
            QuorumRule::Weighted => "weighted".into(),
        };
        if index != offset {
            output.push(',');
        }
        write!(
            &mut output,
            "{index}:{parent}:{label}:{rule}:{}:{}/{}/{}:{}",
            row.weight, row.observed_weight, row.required_weight, row.total_weight, row.satisfied
        )
        .unwrap();
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    use voteboat::quorum::{Limits, Tree, WeightedChild};
    #[test]
    fn joint_views_require_both_policies_and_keep_parent_indexes_when_paged() {
        let old = Policy::new(
            Tree::Majority(vec![
                Tree::Voter(NodeId::new(1).unwrap()),
                Tree::Voter(NodeId::new(2).unwrap()),
            ]),
            Limits::default(),
        )
        .unwrap();
        let new = Policy::new(Tree::Voter(NodeId::new(3).unwrap()), Limits::default()).unwrap();
        let stable = (ConfigurationId::new(1).unwrap(), &old);
        let next = Some((ConfigurationId::new(3).unwrap(), &new));
        let voters = parse_voters("1,2,99").unwrap();
        let page = render(stable, next, &voters, 2, 2).unwrap();
        assert!(page.contains("satisfied=false unknown=1 rows=4 offset=2 next_offset=4"));
        assert!(page.contains("2:0:stable:voter2:1:1/1/1:true"));
        assert!(page.contains("3:-:next:voter3:1:0/1/1:false"));
        assert!(render(stable, next, &parse_voters("1,2,3").unwrap(), 0, 16)
            .unwrap()
            .contains("satisfied=true"));
        assert!(render(stable, next, &voters, 5, 1).is_err());
        assert!(render(stable, next, &voters, 0, 0).is_err());
        assert!(render(stable, next, &voters, 0, 17).is_err());
        assert!(parse_voters("0").is_err());
        assert!(parse_voters("1,1").is_err());
        assert!(parse_voters("1,").is_err());
    }
    #[test]
    fn maximum_weight_pages_fit_the_bounded_reply() {
        let mut tree = Tree::Voter(NodeId::new(u64::MAX).unwrap());
        for _ in 0..16 {
            tree = Tree::Weighted(vec![WeightedChild {
                weight: u64::MAX,
                node: tree,
            }]);
        }
        let p = Policy::new(tree, Limits::default()).unwrap();
        let page = render(
            (ConfigurationId::new(u64::MAX).unwrap(), &p),
            None,
            &BTreeSet::new(),
            0,
            16,
        )
        .unwrap();
        assert!(page.len() + 1 < 4096);
        assert!(page.contains("rows=17 offset=0 next_offset=16"));
    }
}
