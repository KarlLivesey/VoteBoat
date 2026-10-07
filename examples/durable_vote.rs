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
//! Run: cargo run --example durable_vote -- /tmp/voteboat-demo 2 1
use std::path::PathBuf;
use voteboat::{contracts::*, identity::*, native::vote_store::*, quorum::*, vote::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: durable_vote DIRECTORY CANDIDATE TERM".into());
    }
    let directory = PathBuf::from(&args[1]);
    let candidate = NodeId::new(args[2].parse()?).ok_or("candidate must be nonzero")?;
    let term = args[3].parse()?;
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let identity = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let limits = VoteStoreLimits::default();
    let mut store = if directory.join("MANIFEST").exists() {
        NativeVoteStore::recover(FileVoteIo::open(&directory)?, identity, limits)?
    } else {
        NativeVoteStore::create(FileVoteIo::create(&directory)?, identity, limits)?
    };
    let record = store
        .recovered()
        .get(&group)
        .copied()
        .unwrap_or(VoteRecord {
            group,
            configuration: ConfigurationId::new(1).unwrap(),
            hard_state: HardState::default(),
        });
    let policy = Policy::new(
        Tree::Majority(
            (1..=3)
                .map(|n| Tree::Voter(NodeId::new(n).unwrap()))
                .collect(),
        ),
        Limits::default(),
    )
    .map_err(|e| format!("{e:?}"))?;
    let mut voter = Voter::recover(
        NodeId::new(1).unwrap(),
        record,
        policy,
        store.binding(),
        (0, 0),
    )
    .map_err(|e| format!("{e:?}"))?;
    let response = drive_vote(
        &mut voter,
        &mut store,
        VoteRequest {
            group,
            configuration: record.configuration,
            candidate,
            term,
            last_log_term: 0,
            last_log_index: 0,
        },
    )
    .map_err(|e| format!("{e:?}"))?;
    println!(
        "term={} candidate={} granted={}",
        response.term,
        response.candidate.get(),
        response.granted
    );
    Ok(())
}
