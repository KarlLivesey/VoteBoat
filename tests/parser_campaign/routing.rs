// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::generator::*;
use voteboat::{
    directory::DirectoryCommand, identity::*, placement::PlacementRequirements, routing::*,
};
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn fixture() -> DirectoryCommand {
    DirectoryCommand {
        expected: Some(RouteGeneration::new(1).unwrap()),
        manifest: ResponsibilityManifest::new(ManifestInput {
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(1).unwrap(),
                incarnation: ResponsibilityIncarnation::new(1).unwrap(),
            },
            parent: None,
            authority: group(1),
            application: ApplicationAdapter {
                id: ApplicationAdapterId::new(1).unwrap(),
                version: 1,
            },
            scheme: PartitionScheme {
                id: RoutingSchemeId::new(1).unwrap(),
                version: 1,
            },
            scope: BucketRange::new(0, 256).unwrap(),
            epoch: OwnershipEpoch::new(1).unwrap(),
            generation: RouteGeneration::new(1).unwrap(),
            placement: PlacementRequirements {
                minimum_voting_domains: 3,
                survive_any_single_domain_loss: true,
            },
            state: ResponsibilityState::Active,
            execution: ExecutionMode::Partitioned(vec![
                RouteEntry {
                    scope: BucketRange::new(0, 128).unwrap(),
                    target: RouteTarget::Group(group(2)),
                },
                RouteEntry {
                    scope: BucketRange::new(128, 256).unwrap(),
                    target: RouteTarget::Group(group(3)),
                },
            ]),
        })
        .unwrap(),
    }
}
fn checked(command: &DirectoryCommand) {
    let input = command.manifest.input();
    assert_eq!(
        ResponsibilityManifest::new(input.clone()).unwrap(),
        command.manifest
    );
    let entries = match &input.execution {
        ExecutionMode::Single(_) => return,
        ExecutionMode::Partitioned(entries) | ExecutionMode::Delegated(entries) => entries,
    };
    assert!(entries.len() <= MAX_MANIFEST_ROUTES && entries.capacity() <= MAX_MANIFEST_ROUTES);
    let mut end = input.scope.start();
    for entry in entries {
        assert_eq!(entry.scope.start(), end);
        assert!(entry.scope.start() < entry.scope.end() && entry.scope.end() <= input.scope.end());
        end = entry.scope.end();
    }
    assert_eq!(end, input.scope.end());
}
#[test]
fn generated_routing_publication_mutations_preserve_checked_bucket_coverage() {
    let original = fixture();
    let bytes = original.encode(4096).unwrap();
    assert_eq!(&bytes[142..144], &2_u16.to_le_bytes());
    campaign(
        "routing-publication",
        &bytes,
        &[
            (16, 4),
            (117, 2),
            (119, 2),
            (142, 2),
            (144, 2),
            (146, 2),
            (173, 2),
            (175, 2),
        ],
        |case| {
            let result = DirectoryCommand::decode(&case.bytes);
            if case.original() {
                assert_eq!(result, Ok(original.clone()));
            }
            match result {
                Ok(command) => {
                    checked(&command);
                    let encoded = command.encode(4096).unwrap();
                    assert_eq!(DirectoryCommand::decode(&encoded), Ok(command));
                    true
                }
                Err(_) => false,
            }
        },
    );
}
