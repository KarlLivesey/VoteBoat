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
//! Canonical existing administration records with exact store bindings.
use super::{placement_input::Input, setup::Failure};
use voteboat::{membership::*, quorum::Tree};
fn tree(value: &Tree, words: &mut Vec<String>) {
    match value {
        Tree::Voter(n) => words.push(format!("v:{}", n.get())),
        Tree::Majority(children) => {
            words.push(format!("m:{}", children.len()));
            for c in children {
                tree(c, words)
            }
        }
        Tree::Weighted(children) => {
            words.push(format!("w:{}", children.len()));
            for c in children {
                words.push(c.weight.to_string());
                tree(&c.node, words)
            }
        }
    }
}
fn configuration(c: &Configuration) -> String {
    let learners = if c.learners().is_empty() {
        "-".to_owned()
    } else {
        c.learners()
            .keys()
            .map(|n| n.get().to_string())
            .collect::<Vec<_>>()
            .join(",")
    };
    let mut words = vec![learners];
    tree(c.policy().tree(), &mut words);
    words.join(" ")
}
pub fn format(input: &Input, records: &[ConfigurationRecord]) -> Result<String, Failure> {
    let r = input.authorizer.requirements();
    let mut output = format!(
        "voteboat-counter-admin-v1\nplacement {} {}\n",
        r.minimum_voting_domains, r.survive_any_single_domain_loss
    );
    for (node, p) in input.authorizer.replicas() {
        output.push_str(&format!(
            "replica {} {} {} {}\n",
            node.get(),
            p.domain.get(),
            p.store.id.get(),
            p.store.incarnation.get()
        ));
    }
    for r in records {
        let row = match &r.change {
            ConfigurationChange::Learners(c) => format!(
                "learners {} {} {} {}\n",
                r.operation.get(),
                r.expected.get(),
                c.id().get(),
                configuration(c)
            ),
            ConfigurationChange::Joint { id, next } => format!(
                "joint {} {} {} {} {}\n",
                r.operation.get(),
                r.expected.get(),
                id.get(),
                next.id().get(),
                configuration(next)
            ),
            ConfigurationChange::Final { id } => format!(
                "final {} {} {}\n",
                r.operation.get(),
                r.expected.get(),
                id.get()
            ),
        };
        output.push_str(&row);
    }
    if output.len() > 65536 {
        return Err("generated administration plan exceeds64KiB".into());
    }
    Ok(output)
}
