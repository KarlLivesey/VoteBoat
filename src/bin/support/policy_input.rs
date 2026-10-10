// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::setup::Failure;
use voteboat::{
    identity::NodeId,
    quorum::{Tree, WeightedChild},
};
pub fn tree<'a>(
    tokens: &mut impl Iterator<Item = &'a str>,
    depth: usize,
    remaining: &mut usize,
    max_node: u64,
) -> Result<Tree, Failure> {
    if depth > 32 || *remaining == 0 {
        return Err("administration policy exceeds bounds".into());
    }
    *remaining -= 1;
    let token = tokens.next().ok_or("truncated administration policy")?;
    let (kind, value) = token
        .split_once(':')
        .ok_or("expected v:N, m:COUNT or w:COUNT")?;
    if kind == "v" {
        let value: u64 = value.parse()?;
        if value == 0 || value > max_node {
            return Err("invalid policy node".into());
        }
        return Ok(Tree::Voter(NodeId::new(value).unwrap()));
    }
    let count: usize = value.parse()?;
    if count == 0 || count > *remaining {
        return Err("invalid administration branch size".into());
    }
    match kind {
        "m" => {
            let mut children = Vec::new();
            for _ in 0..count {
                children.push(tree(tokens, depth + 1, remaining, max_node)?);
            }
            Ok(Tree::Majority(children))
        }
        "w" => {
            let mut children = Vec::new();
            for _ in 0..count {
                let weight = tokens.next().ok_or("missing policy weight")?.parse()?;
                children.push(WeightedChild {
                    weight,
                    node: tree(tokens, depth + 1, remaining, max_node)?,
                });
            }
            Ok(Tree::Weighted(children))
        }
        _ => Err("unknown administration policy branch".into()),
    }
}
