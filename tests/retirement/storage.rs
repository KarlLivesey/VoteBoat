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
use super::{fixture, source_fixture as base};
use crate::support::retirement_storage::history;
use voteboat::{application::*, transfer_target::*};
#[test]
fn original_source_retirement_survives_interrupted_publication_and_native_reclamation() {
    let (source, targets, decision) = fixture::active_split();
    history(
        "source",
        fixture::fresh_source,
        fixture::source_log(),
        fixture::split_proof(&source, &targets, &decision),
    );
}
#[test]
fn activated_source_retirement_survives_interrupted_publication_and_native_reclamation() {
    let (source, mut targets, decision) = fixture::active_split();
    let (merged, merge_decision) = fixture::merge(&mut targets);
    let mut target = fixture::fresh_target(21);
    let import = fixture::target::from_source(source.owner().unwrap(), 21, fixture::cfg());
    let mut prefix = vec![
        base::entry(
            1,
            200,
            target.owner().unwrap().bootstrap_command(65536).unwrap(),
        ),
        base::entry(
            2,
            200,
            target
                .owner()
                .unwrap()
                .import_command(&import, 65536)
                .unwrap(),
        ),
    ];
    target.apply_batch(&prefix).unwrap();
    let activation = base::entry(
        3,
        200,
        target
            .owner()
            .unwrap()
            .activation_command(
                &TargetActivation {
                    metadata_configuration: fixture::cfg(),
                    decision,
                },
                65536,
            )
            .unwrap(),
    );
    target
        .apply_batch(std::slice::from_ref(&activation))
        .unwrap();
    prefix.push(activation);
    let freeze = base::entry(
        4,
        300,
        target
            .owner()
            .unwrap()
            .freeze_command(&fixture::later_intent(), 65536, 65536)
            .unwrap(),
    );
    target.apply_batch(std::slice::from_ref(&freeze)).unwrap();
    prefix.push(freeze);
    assert_eq!(
        target.checkpoint(500000).unwrap(),
        targets[0].checkpoint(500000).unwrap()
    );
    let proof = fixture::proof(
        &target.freeze_status().unwrap().unwrap(),
        [merged.owner().unwrap().status()],
        &merge_decision,
        910,
    );
    history("target", || fixture::fresh_target(21), prefix, proof);
}
