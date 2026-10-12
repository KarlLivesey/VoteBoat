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
//! Admission uses the normal encoder's count/validation pass, not size formulas.
use super::*;

pub(super) fn check(
    codec: &NativeWireCodec,
    required: &ConfigurationWireRequirements<'_>,
) -> Result<ConfigurationWireCapacity, WireError> {
    let app = required.application;
    if codec.version < 2 {
        return Err(WireError::UnsupportedVersion(codec.version));
    }
    if app.application_schema == 0
        || app.command_bytes == 0
        || app.snapshot_bytes == 0
        || required.index == 0
        || required.index == u64::MAX
        || required.index <= required.current.last_configuration_index()
        || required.committed_index >= required.index
    {
        return Err(WireError::InvalidMessage(
            "configuration admission envelope",
        ));
    }
    if app.command_bytes > codec.limits.max_command_bytes
        || app.snapshot_bytes > codec.limits.max_snapshot_bytes
        || required.record.retained_bytes() > codec.limits.max_command_bytes
    {
        return Err(WireError::TooLarge);
    }
    let next = required
        .current
        .preview_next(required.index, required.record, required.committed_index)
        .map_err(|_| WireError::InvalidMessage("configuration admission journal"))?;
    // IDs/terms have fixed encoded sizes. This fixture is never sent and grants
    // no session, voter or leader authority; all actual bindings are rechecked.
    let (&from, &store) = required
        .bootstrap
        .voter_stores
        .first_key_value()
        .ok_or(WireError::InvalidMessage("empty bootstrap"))?;
    let sender = StoreBinding {
        identity: store,
        session: StoreSession::new(1).unwrap(),
    };
    let to = NodeId::new(if from.get() == 1 { 2 } else { 1 }).unwrap();
    let scope = WireScope { from, sender, to };
    let mut message = Message {
        group: required.bootstrap.group,
        configuration: required.current.id(),
        from,
        sender,
        to,
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: required.index - 1,
            previous_term: u64::from(required.index > 1),
            leader_commit: required.committed_index,
            entries: vec![LogEntry {
                index: required.index,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(required.record.clone())),
            }],
        },
    };
    let append = footprint(codec, scope, &message, 0)?;
    if let Rpc::Append { entries, .. } = &mut message.rpc {
        entries[0].payload = EntryPayload::Command {
            operation: required.record.operation,
            bytes: vec![0],
        };
    }
    let command = footprint(codec, scope, &message, app.command_bytes - 1)?;
    message.configuration = next.id();
    message.rpc = Rpc::Snapshot {
        snapshot: Box::new(Snapshot {
            metadata: SnapshotMetadata {
                bootstrap: required.bootstrap.clone(),
                membership: Some(Box::new(next)),
                index: required.index,
                term: 1,
                application_schema: app.application_schema,
            },
            application: vec![0],
        }),
    };
    let snapshot = footprint(codec, scope, &message, app.snapshot_bytes - 1)?;
    Ok(ConfigurationWireCapacity {
        wire_version: codec.version,
        append,
        command,
        snapshot,
    })
}
fn footprint(
    codec: &NativeWireCodec,
    scope: WireScope,
    message: &Message,
    additional_payload: usize,
) -> Result<WireFootprint, WireError> {
    let mut count = Encoder::new(codec.limits, None, codec.version);
    count.batch(scope, std::slice::from_ref(message))?;
    // Command/application bytes are opaque to wire validation. Account for the
    // full declared length without allocating an application-sized buffer.
    count.budget.charge(additional_payload)?;
    let frame_bytes = count
        .len
        .checked_add(additional_payload)
        .filter(|n| *n <= codec.limits.max_frame_bytes)
        .ok_or(WireError::TooLarge)?;
    Ok(WireFootprint {
        frame_bytes,
        decoded_bytes: count.budget.used,
    })
}
