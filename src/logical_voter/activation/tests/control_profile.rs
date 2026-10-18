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
use control_profile::LogicalActivationCommandClass;
#[test]
fn actual_profile_classifies_complete_control_envelopes_without_promoting_business() {
    let a = app(2);
    let profile = a.control_profile();
    let i = intent(1);
    let begin = a.begin(&i).unwrap();
    assert_eq!(
        profile
            .classify_command(i.operations.begin, &begin)
            .unwrap(),
        LogicalActivationCommandClass::Control
    );
    let commands = [
        final_command(&i),
        ControlCommand::Release {
            begin: i.operations.begin,
            outcome: ActivationPosition { index: 9, term: 2 },
        },
    ];
    for (operation, command) in [i.operations.outcome, i.operations.release]
        .into_iter()
        .zip(commands)
    {
        let bytes = codec::command(&command, CONTROL_BYTES).unwrap();
        assert_eq!(
            profile.classify_command(operation, &bytes).unwrap(),
            LogicalActivationCommandClass::Control
        );
        for cut in 0..bytes.len() {
            assert!(profile.classify_command(operation, &bytes[..cut]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(profile.classify_command(operation, &trailing).is_err());
    }
    let wrapped = a.business(&begin).unwrap();
    assert_eq!(
        profile.classify_command(op(100), &wrapped).unwrap(),
        LogicalActivationCommandClass::Business
    );
    assert!(profile.classify_command(op(100), &begin).is_err());
    let mut foreign = i.clone();
    foreign.mapping.parent = group(99);
    let bytes = codec::command(&ControlCommand::Begin(Box::new(foreign)), CONTROL_BYTES).unwrap();
    assert!(profile
        .classify_command(i.operations.begin, &bytes)
        .is_err());
    let mut invalid = final_command(&i);
    let ControlCommand::Outcome { fact, .. } = &mut invalid else {
        panic!("outcome")
    };
    fact.position.index = 0;
    assert!(profile
        .classify_command(
            i.operations.outcome,
            &codec::command(&invalid, CONTROL_BYTES).unwrap()
        )
        .is_err());
}
#[test]
fn profile_encoding_is_bounded_and_pins_all_selected_limits_and_inner_schema() {
    let a = app(2);
    let profile = a.control_profile();
    let original = profile.configuration_bytes(4096).unwrap();
    assert_eq!(
        profile.configuration_bytes(original.len()).unwrap(),
        original
    );
    assert!(profile.configuration_bytes(original.len() - 1).is_err());
    let changed = app(3).control_profile();
    assert_ne!(profile, changed);
    assert_ne!(original, changed.configuration_bytes(4096).unwrap());
    assert_eq!(profile.parent(), a.parent());
    assert_eq!(profile.mappings(), a.mappings());
    assert_eq!(profile.limits(), a.limits());
    assert_eq!(profile.inner_schema(), a.inner().schema_version());
}
