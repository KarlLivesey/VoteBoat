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
//! Original whole-action reservation and selected-profile replay boundaries.
use super::*;
use crate::membership::Configuration;
mod reads;
fn wrapper(command_bytes: usize) -> LogicalActivationApplication<Counter> {
    LogicalActivationApplication::new(
        group(1),
        vec![app().mapping()],
        LogicalActivationLimits {
            command_bytes,
            intents: 2,
            ..LogicalActivationLimits::default()
        },
        Counter::new(8).unwrap(),
    )
    .unwrap()
}
fn selected() -> LogicalApplication {
    app_with_limits(LogicalLimits {
        max_actions: 7,
        control_actions: 6,
        ..LogicalLimits::default()
    })
    .with_activation_control_profile(wrapper(4096).control_profile())
    .unwrap()
}
fn configuration(nodes: [u64; 3], id: u64) -> Configuration {
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Majority(nodes.map(|n| Tree::Voter(NodeId::new(n).unwrap())).to_vec()),
            PolicyLimits::default(),
        )
        .unwrap(),
        nodes
            .into_iter()
            .map(|n| (NodeId::new(n).unwrap(), store(n as u128)))
            .collect(),
        BTreeMap::new(),
    )
    .unwrap()
}
fn begin() -> Vec<u8> {
    let intent = ChildActivationIntent::new(
        app().mapping(),
        ChildActivationOperations::reserved(1).unwrap(),
        &configuration([2, 3, 4], 1),
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &configuration([2, 3, 5], 4),
    )
    .unwrap();
    wrapper(4096).begin(&intent).unwrap()
}
/// The parent operation of the selected-profile `Begin`.
fn begin_operation() -> u128 {
    ChildActivationOperations::reserved(1).unwrap().begin.get()
}
fn parent_entry(index: u64, operation: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes: bytes.into(),
        },
    }
}
fn append(a: &LogicalApplication, id: u128, entries: Vec<LogEntry>) -> Command {
    mutate(
        a,
        id,
        update(
            a,
            0,
            Some(Suffix {
                from: entries[0].index,
                entries,
            }),
        ),
    )
}
fn admitted(a: &LogicalApplication, c: &Command) -> Result<usize, ApplicationError> {
    a.validate_proposal(
        c.operation,
        &a.encode_command(c, usize::MAX).unwrap(),
        std::iter::empty(),
    )
}
#[test]
fn selected_profile_is_immutable_and_checkpoint_binding_is_exact() {
    let ordinary = app();
    assert!(ordinary.activation_control_profile().is_none());
    let original = ordinary.configuration.clone();
    assert_eq!(original, ordinary.configuration_bytes().unwrap());
    let selected = ordinary
        .clone()
        .with_activation_control_profile(wrapper(4096).control_profile())
        .unwrap();
    assert!(selected.configuration.starts_with(&original));
    // The child binds the v2 profile: its embedding tag and the profile's magic.
    let tail = &selected.configuration[original.len()..];
    assert_eq!(&tail[..8], b"VBACTP02");
    assert_eq!(&tail[16..24], b"VBACTP02");
    assert!(!selected.same_configuration(&ordinary));
    assert!(!selected.same_configuration(
        &ordinary
            .clone()
            .with_activation_control_profile(wrapper(2048).control_profile())
            .unwrap()
    ));
    assert!(selected
        .clone()
        .with_activation_control_profile(wrapper(4096).control_profile())
        .is_err());
    assert!(ordinary
        .clone()
        .with_activation_control_profile(wrapper(8192).control_profile())
        .is_err());
    let mut used = ordinary;
    used.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    assert!(used
        .with_activation_control_profile(wrapper(4096).control_profile())
        .is_err());
    let mut restored = app();
    let before = restored.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        restored.restore_checkpoint(
            LOGICAL_APPLICATION_SCHEMA,
            0,
            &selected.checkpoint(usize::MAX).unwrap()
        ),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), before);
    let limits = LogicalLimits {
        control_action_bytes: 512 * 1024 - 1,
        ..LogicalLimits::default()
    };
    assert!(app_with_limits(limits)
        .with_activation_control_profile(wrapper(4096).control_profile())
        .is_err());
}
#[test]
fn original_mixed_action_uses_reserved_capacity_and_replays_without_splitting() {
    let mut a = selected();
    claim(&mut a);
    let business = wrapper(4096).business(&5i64.to_le_bytes()).unwrap();
    let ordinary = append(&a, 2, vec![parent_entry(1, 200, business.clone())]);
    assert!(admitted(&a, &ordinary).is_ok());
    assert_eq!(apply(&mut a, &ordinary).outcome, LogicalOutcome::Mutated);
    let blocked = append(&a, 3, vec![parent_entry(2, 201, business.clone())]);
    assert_eq!(admitted(&a, &blocked), Err(ApplicationError::DedupCapacity));
    let mixed = append(
        &a,
        4,
        vec![
            parent_entry(2, 201, business),
            parent_entry(3, begin_operation(), begin()),
        ],
    );
    let bytes = a.encode_command(&mixed, usize::MAX).unwrap();
    assert!(admitted(&a, &mixed).is_ok());
    assert_eq!(apply(&mut a, &mixed).outcome, LogicalOutcome::Mutated);
    assert_eq!(a.state.records[&mixed.operation].command, bytes);
    let CommandBody::Mutate { update, .. } = &mixed.body else {
        panic!("mutation")
    };
    assert_eq!(
        a.state.parent.as_ref().unwrap().entries[1..],
        update.suffix.as_ref().unwrap().entries
    );
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let mut restored = selected();
    restored
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &checkpoint)
        .unwrap();
    assert_eq!(restored.state, a.state);
    let suffix = mutate(&restored, 5, update_without_suffix(&restored));
    assert_eq!(
        apply(&mut restored, &suffix).outcome,
        LogicalOutcome::Mutated
    );
    assert!(apply(&mut restored, &mixed).duplicate);
    let mut different = app_with_limits(a.limits)
        .with_activation_control_profile(wrapper(2048).control_profile())
        .unwrap();
    let before = different.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        different.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &checkpoint),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(different.checkpoint(usize::MAX).unwrap(), before);
}
fn update_without_suffix(a: &LogicalApplication) -> LogUpdate {
    update(a, 0, None)
}
#[test]
fn malformed_and_business_wrapped_controls_do_not_use_reserved_capacity() {
    let mut a = selected();
    claim(&mut a);
    let b = wrapper(4096).business(&5i64.to_le_bytes()).unwrap();
    let first = append(&a, 2, vec![parent_entry(1, 200, b)]);
    apply(&mut a, &first);
    let mut trailing = begin();
    trailing.push(0);
    for bytes in [trailing, wrapper(4096).business(&begin()).unwrap()] {
        let c = append(&a, 3, vec![parent_entry(2, begin_operation(), bytes)]);
        assert!(!c.control(a.activation_control_profile()));
        assert_eq!(admitted(&a, &c), Err(ApplicationError::DedupCapacity));
    }
}
fn control_capsule(a: &LogicalApplication) -> Command {
    let effect = EffectTicket {
        visit: visit(),
        sequence: 1,
    };
    command(
        10,
        1,
        CommandBody::Authorize(LogicalCapsule {
            expected_revision: a.state.parent.as_ref().unwrap().revision,
            expected_generation: a.state.parent.as_ref().unwrap().generation,
            local_configuration: ConfigurationId::new(1).unwrap(),
            origin: LogicalCapsuleOrigin::Client {
                effect,
                ticket: LogicalClientTicket {
                    owner: owner(),
                    generation: ClientRouterGeneration::new(1).unwrap(),
                    sequence: 1,
                    group: group(1),
                    operation: OperationId::new(begin_operation()).unwrap(),
                },
                position: ProposalPosition { index: 1, term: 1 },
            },
            body: LogicalCapsuleBody::ClientApplied {
                command: begin(),
                schema: LOGICAL_ACTIVATION_RESPONSE_SCHEMA,
                receipt: vec![0],
            },
        }),
    )
}
#[test]
fn control_result_reservation_requires_exact_schema_group_and_operation() {
    let mut a = selected();
    claim(&mut a);
    let b = wrapper(4096).business(&5i64.to_le_bytes()).unwrap();
    let first = append(&a, 2, vec![parent_entry(1, 200, b)]);
    apply(&mut a, &first);
    let original = control_capsule(&a);
    assert!(original.control(a.activation_control_profile()));
    assert!(admitted(&a, &original).is_ok());
    assert!(!original.control(None));
    for field in 0..4 {
        let mut bad = original.clone();
        let CommandBody::Authorize(c) = &mut bad.body else {
            panic!("capsule")
        };
        let LogicalCapsuleOrigin::Client { ticket, .. } = &mut c.origin else {
            panic!("client")
        };
        let LogicalCapsuleBody::ClientApplied {
            schema, command, ..
        } = &mut c.body
        else {
            panic!("result")
        };
        match field {
            0 => *schema = LOGICAL_RESPONSE_SCHEMA,
            1 => ticket.group = group(9),
            2 => ticket.operation = OperationId::new(999).unwrap(),
            _ => *command = wrapper(4096).business(command).unwrap(),
        }
        assert!(!bad.control(a.activation_control_profile()));
        assert_eq!(admitted(&a, &bad), Err(ApplicationError::DedupCapacity));
    }
    // Reserving a result is not proof of authority: the deliberately invalid
    // receipt still follows the original authorization refusal path.
    assert_eq!(
        apply(&mut a, &original).outcome,
        LogicalOutcome::Refused(LogicalRefusal::InvalidResponse)
    );
    assert_eq!(
        a.state.records[&original.operation].command,
        a.encode_command(&original, usize::MAX).unwrap()
    );
}
/// Written by the code at 30a1f6c5: `selected().checkpoint(usize::MAX)`, an
/// unclaimed child whose configuration embeds the v1 activation profile.
const V1_SELECTED_CHECKPOINT: &[u8] = include_bytes!("control_profile/v1-selected-checkpoint.bin");
#[test]
fn child_checkpoint_with_v1_profile_embedding_is_refused_typed() {
    let find = |bytes: &[u8], tag: &[u8; 8]| {
        bytes
            .windows(8)
            .enumerate()
            .filter(|(_, w)| w == tag)
            .map(|(at, _)| at)
            .collect::<Vec<_>>()
    };
    // Frame: magic (8), configuration length (8), configuration, then state.
    let configuration = |bytes: &[u8]| {
        let length = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        bytes[16..16 + length].to_vec()
    };
    // The current frame (checkpoint v4, schema 5); named `v3` since LV1.
    let v3 = selected().checkpoint(usize::MAX).unwrap();
    assert_eq!(&V1_SELECTED_CHECKPOINT[..8], b"VBLOGC01");
    assert_eq!(&v3[..8], b"VBLOGC05");
    let (old, new) = (configuration(V1_SELECTED_CHECKPOINT), configuration(&v3));
    // The embedding tag and the profile's magic, at the same offsets. Before
    // them the configurations are identical; the v1 profile carries one more
    // u64, the removed business limit.
    let at = find(&old, b"VBACTP01");
    assert_eq!(at.len(), 2);
    assert_eq!(at, find(&new, b"VBACTP02"));
    assert!(find(&old, b"VBACTP02").is_empty());
    assert_eq!(old.len(), new.len() + 8);
    assert_eq!(old[..at[0]], new[..at[0]]);
    // Same-schema profile binding: the v1 configuration blob in an otherwise
    // current v3 frame. Only the profile embedding differs, so the refusal
    // comes from the profile binding alone.
    let splice = |blob: &[u8]| {
        let mut bytes = v3[..8].to_vec();
        bytes.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        bytes.extend_from_slice(blob);
        bytes.extend_from_slice(&v3[16 + new.len()..]);
        bytes
    };
    assert_eq!(splice(&new), v3);
    let spliced = splice(&old);
    for mut child in [selected(), app()] {
        let before = child.checkpoint(usize::MAX).unwrap();
        assert_eq!(
            child.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 0, &spliced),
            Err(ApplicationError::InvalidCheckpoint)
        );
        // Old schema: the legacy file is refused by schema number, and under
        // the current schema by its v1 frame.
        assert_eq!(
            child.restore_checkpoint(1, 0, V1_SELECTED_CHECKPOINT),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_eq!(
            child.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 0, V1_SELECTED_CHECKPOINT),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(child.checkpoint(usize::MAX).unwrap(), before);
    }
    let mut child = selected();
    child
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 0, &v3)
        .unwrap();
    assert_eq!(child.checkpoint(usize::MAX).unwrap(), v3);
}
