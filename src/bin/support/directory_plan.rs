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
//! Trusted initial assignments; publication still requires a committed command.
use super::setup::{checked, Failure};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Read,
    path::Path,
};
use voteboat::{directory::*, identity::*};
const FILE_LIMIT: u64 = 128 * 1024;
pub struct Plan {
    pub authority: GroupIdentity,
    pub bootstrap: OperationId,
    pub commands: BTreeMap<OperationId, Vec<u8>>,
    pub app: Directory,
}
impl Plan {
    pub fn load(path: &Path) -> Result<Self, Failure> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(FILE_LIMIT + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > FILE_LIMIT {
            return Err("directory plan exceeds128KiB".into());
        }
        let mut lines = std::str::from_utf8(&bytes)?.lines();
        let header = lines
            .next()
            .ok_or("missing directory plan header")?
            .split_whitespace()
            .collect::<Vec<_>>();
        let ["voteboat-directory-plan-v1", group, incarnation, operation] = header.as_slice()
        else {
            return Err(
                "expected voteboat-directory-plan-v1 GROUP INCARNATION BOOTSTRAP_OPERATION".into(),
            );
        };
        let authority = GroupIdentity {
            id: GroupId::new(group.parse()?).ok_or("invalid authority")?,
            incarnation: GroupIncarnation::new(incarnation.parse()?)
                .ok_or("invalid authority incarnation")?,
        };
        let bootstrap = op(operation)?;
        let mut commands = BTreeMap::new();
        let mut manifests = Vec::new();
        let mut identities = BTreeSet::new();
        for line in lines {
            if commands.len() == 64 {
                return Err("directory plan exceeds64 manifests".into());
            }
            let words = line.split_whitespace().collect::<Vec<_>>();
            let [operation, hex] = words.as_slice() else {
                return Err("expected OPERATION CANONICAL_DIRECTORY_COMMAND_HEX".into());
            };
            let operation = op(operation)?;
            let bytes = unhex(hex)?;
            let command = checked(DirectoryCommand::decode(&bytes))?;
            if command.expected.is_some()
                || command.manifest.input().authority != authority
                || operation == bootstrap
                || commands.contains_key(&operation)
                || !identities.insert(command.manifest.input().responsibility)
            {
                return Err("conflicting initial directory plan".into());
            }
            manifests.push(command.manifest);
            commands.insert(operation, bytes);
        }
        let plan = DirectoryPlan::new(authority, manifests)
            .map_err(|(e, _)| format!("directory plan: {e:?}"))?;
        let app = Directory::new(
            plan,
            DirectoryLimits {
                operations: 256,
                history_bytes: 128 * 1024,
            },
        )
        .map_err(|(e, _)| format!("directory application: {e:?}"))?;
        let wire = voteboat::wire::WireLimits::default();
        let initialization = checked(app.bootstrap_command(wire.max_command_bytes))?;
        let plan_bytes = initialization.len() - 46;
        commands.insert(bootstrap, initialization);
        validate_profile(&commands, plan_bytes, &app)?;
        Ok(Self {
            authority,
            bootstrap,
            commands,
            app,
        })
    }
}
pub fn op(text: &str) -> Result<OperationId, Failure> {
    OperationId::new(text.parse()?).ok_or_else(|| "invalid operation ID".into())
}
fn unhex(text: &str) -> Result<Vec<u8>, Failure> {
    if !text.len().is_multiple_of(2) || text.len() > 2 * MAX_DIRECTORY_PUBLICATION_BYTES {
        return Err("invalid directory command size".into());
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |b: u8| {
                (b as char)
                    .to_digit(16)
                    .map(|n| n as u8)
                    .ok_or("invalid command hex")
            };
            Ok((digit(pair[0])? << 4) | digit(pair[1])?)
        })
        .collect()
}

// Schema1 serializes the trusted plan and original unique command records.
// No lifecycle/control records are admitted by this executable.
fn validate_profile(
    commands: &BTreeMap<OperationId, Vec<u8>>,
    plan_bytes: usize,
    app: &Directory,
) -> Result<(), Failure> {
    let wire = voteboat::wire::WireLimits::default();
    let history: usize = commands.values().map(Vec::len).sum();
    let checkpoint = 58 + plan_bytes + 28 * commands.len() + history;
    if commands.values().any(|b| b.len() > wire.max_command_bytes)
        || commands.len() > app.limits().operations
        || history > app.limits().history_bytes
        || checkpoint > wire.max_snapshot_bytes
    {
        return Err(
            "initial directory plan exceeds native command/history/checkpoint limits".into(),
        );
    }
    Ok(())
}

/// A usable single-owner starting plan. Incarnations and ownership are explicit.
pub fn single(args: &[String]) -> Result<String, Failure> {
    use voteboat::{placement::PlacementRequirements, routing::*};
    let [authority, authority_inc, responsibility, responsibility_inc, execution, execution_inc] =
        args
    else {
        return Err(
            "expected AUTHORITY INCARNATION RESPONSIBILITY INCARNATION EXECUTION_GROUP INCARNATION"
                .into(),
        );
    };
    let group = |id: &str, inc: &str| -> Result<GroupIdentity, Failure> {
        Ok(GroupIdentity {
            id: GroupId::new(id.parse()?).ok_or("invalid group")?,
            incarnation: GroupIncarnation::new(inc.parse()?).ok_or("invalid incarnation")?,
        })
    };
    let authority = group(authority, authority_inc)?;
    let input = ManifestInput {
        responsibility: ResponsibilityIdentity {
            id: ResponsibilityId::new(responsibility.parse()?).ok_or("invalid responsibility")?,
            incarnation: ResponsibilityIncarnation::new(responsibility_inc.parse()?)
                .ok_or("invalid responsibility incarnation")?,
        },
        parent: None,
        authority,
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
        execution: ExecutionMode::Single(group(execution, execution_inc)?),
    };
    let manifest = checked(ResponsibilityManifest::new(input))?;
    let bytes = checked(
        DirectoryCommand {
            expected: None,
            manifest,
        }
        .encode(MAX_DIRECTORY_PUBLICATION_BYTES),
    )?;
    let hex = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    Ok(format!(
        "voteboat-directory-plan-v1 {} {} 100\n101 {hex}\n",
        authority.id.get(),
        authority.incarnation.get()
    ))
}
