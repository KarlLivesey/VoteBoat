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
#![cfg(feature = "native")]
mod support;
use support::*;
use voteboat::{
    identity::*, log::*, membership::*, native::wire::NativeWireCodec, raft::*, snapshot::*,
    wire::*,
};
fn record(expected: u64) -> ConfigurationRecord {
    let b = bootstrap(1, 3);
    ConfigurationRecord {
        operation: OperationId::new(expected as u128).unwrap(),
        expected: ConfigurationId::new(expected).unwrap(),
        change: ConfigurationChange::Joint {
            id: ConfigurationId::new(expected + 1).unwrap(),
            next: Configuration::new(
                ConfigurationId::new(expected + 2).unwrap(),
                b.policy,
                b.voter_stores,
                Default::default(),
            )
            .unwrap(),
        },
    }
}
fn app() -> ReadinessRequirements {
    ReadinessRequirements {
        application_schema: 1,
        command_bytes: 64,
        snapshot_bytes: 4096,
    }
}
#[test]
fn full_service_counter_retry_history_fits_selected_native_wire_versions() {
    use voteboat::application::*;
    let mut counter = Counter::new(10000).unwrap();
    let entries = (1..=10000)
        .map(|index| LogEntry {
            index,
            term: 7,
            payload: EntryPayload::Command {
                operation: OperationId::new(index as u128).unwrap(),
                bytes: 1i64.to_le_bytes().to_vec(),
            },
        })
        .collect::<Vec<_>>();
    counter.apply_batch(&entries).unwrap();
    let required = counter.readiness_requirements();
    let image = counter.checkpoint(required.snapshot_bytes).unwrap();
    assert_eq!(image.len(), 330032);
    let bootstrap = bootstrap(1, 3);
    let membership = Membership::replay(&bootstrap, &[], 0).unwrap();
    let record = record(1);
    let input = request(&bootstrap, &membership, &record, 1);
    let scope = WireScope {
        from: node(1),
        sender: HostLogStore::new(1).binding(),
        to: node(2),
    };
    for codec in [
        NativeWireCodec::new(WireLimits::default()).unwrap(),
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
        NativeWireCodec::with_authority(WireLimits::default()).unwrap(),
        NativeWireCodec::with_readiness(WireLimits::default()).unwrap(),
    ] {
        let mut message = messages(&input)[2].clone();
        let Rpc::Snapshot { snapshot } = &mut message.rpc else {
            unreachable!()
        };
        snapshot.metadata.index = 10000;
        snapshot.application = image.clone();
        if codec.format_version() == 1 {
            snapshot.metadata.membership = None;
            message.configuration = bootstrap.configuration;
        }
        let encoded = codec
            .encode_batch(scope, std::slice::from_ref(&message))
            .unwrap();
        assert!(encoded.len() <= codec.limits().max_frame_bytes);
        let decoded = codec.decode_batch(scope, &encoded).unwrap();
        let Rpc::Snapshot { snapshot } = &decoded[0].rpc else {
            unreachable!()
        };
        let mut restored = Counter::new(10000).unwrap();
        restored
            .restore_checkpoint(
                snapshot.metadata.application_schema,
                snapshot.metadata.index,
                &snapshot.application,
            )
            .unwrap();
        assert_eq!(restored.remaining_operations(), 0);
        assert_eq!(restored.read_applied(10000).unwrap(), 10000);
    }
}
fn request<'a>(
    b: &'a Bootstrap,
    m: &'a Membership,
    r: &'a ConfigurationRecord,
    index: u64,
) -> ConfigurationWireRequirements<'a> {
    ConfigurationWireRequirements {
        bootstrap: b,
        current: m,
        record: r,
        index,
        committed_index: index - 1,
        application: app(),
    }
}
fn messages(required: &ConfigurationWireRequirements<'_>) -> [Message; 3] {
    let binding = HostLogStore::new(1).binding;
    let entry = LogEntry {
        index: required.index,
        term: 7,
        payload: EntryPayload::Configuration(Box::new(required.record.clone())),
    };
    let next = Membership::replay_from(
        required.bootstrap,
        Some(required.current),
        required.index - 1,
        std::slice::from_ref(&entry),
        required.index,
    )
    .unwrap();
    let append = Message {
        group: required.bootstrap.group,
        configuration: required.current.id(),
        from: node(1),
        sender: binding,
        to: node(2),
        term: 7,
        context: RequestContext {
            origin: binding,
            sequence: 9,
        },
        rpc: Rpc::Append {
            previous_index: required.index - 1,
            previous_term: if required.index == 1 { 0 } else { 7 },
            entries: vec![entry],
            leader_commit: required.committed_index,
        },
    };
    let mut command = append.clone();
    if let Rpc::Append { entries, .. } = &mut command.rpc {
        entries[0].payload = EntryPayload::Command {
            operation: required.record.operation,
            bytes: vec![0; required.application.command_bytes],
        };
    }
    let mut snapshot = append.clone();
    snapshot.configuration = next.id();
    snapshot.rpc = Rpc::Snapshot {
        snapshot: Box::new(Snapshot {
            metadata: SnapshotMetadata {
                bootstrap: required.bootstrap.clone(),
                membership: Some(Box::new(next)),
                index: required.index,
                term: 7,
                application_schema: required.application.application_schema,
            },
            application: vec![0; required.application.snapshot_bytes],
        }),
    };
    [append, command, snapshot]
}
fn scope(message: &Message) -> WireScope {
    WireScope {
        from: message.from,
        sender: message.sender,
        to: message.to,
    }
}
#[test]
fn native_count_matches_real_frames_and_exact_decode_and_frame_boundaries() {
    let b = bootstrap(1, 3);
    let m = Membership::replay(&b, &[], 0).unwrap();
    let r = record(1);
    let required = request(&b, &m, &r, 1);
    let codec = NativeWireCodec::with_readiness(WireLimits::default()).unwrap();
    let sized = codec.configuration_capacity(&required).unwrap();
    let inputs = messages(&required);
    for (footprint, message) in [sized.append, sized.command, sized.snapshot]
        .into_iter()
        .zip(&inputs)
    {
        let frame = codec
            .encode_batch(scope(message), std::slice::from_ref(message))
            .unwrap();
        assert_eq!(frame.len(), footprint.frame_bytes);
        assert_eq!(
            codec.decode_batch(scope(message), &frame).unwrap(),
            std::slice::from_ref(message)
        );
    }
    let max_frame = [sized.append, sized.command, sized.snapshot]
        .iter()
        .map(|f| f.frame_bytes)
        .max()
        .unwrap();
    let max_decoded = [sized.append, sized.command, sized.snapshot]
        .iter()
        .map(|f| f.decoded_bytes)
        .max()
        .unwrap();
    let limits = WireLimits {
        max_frame_bytes: max_frame,
        max_command_bytes: max_frame,
        max_snapshot_bytes: app().snapshot_bytes,
        max_decoded_bytes: max_decoded,
        ..Default::default()
    };
    let exact = NativeWireCodec::with_readiness(limits).unwrap();
    assert_eq!(exact.configuration_capacity(&required).unwrap(), sized);
    for input in &inputs {
        let frame = exact
            .encode_batch(scope(input), std::slice::from_ref(input))
            .unwrap();
        assert_eq!(
            exact.decode_batch(scope(input), &frame).unwrap(),
            std::slice::from_ref(input)
        );
    }
    for restrictive in [
        WireLimits {
            max_frame_bytes: max_frame - 1,
            max_command_bytes: max_frame - 1,
            ..limits
        },
        WireLimits {
            max_decoded_bytes: max_decoded - 1,
            ..limits
        },
    ] {
        let codec = NativeWireCodec::with_readiness(restrictive).unwrap();
        assert_eq!(
            codec.configuration_capacity(&required),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            codec.encode_batch(scope(&inputs[2]), std::slice::from_ref(&inputs[2])),
            Err(WireError::TooLarge)
        );
    }
}
#[test]
fn checkpoint_operation_history_is_charged_before_another_configuration() {
    let b = bootstrap(1, 3);
    let initial = Membership::replay(&b, &[], 0).unwrap();
    let first = record(1);
    let codec = NativeWireCodec::with_membership(Default::default()).unwrap();
    let baseline = codec
        .configuration_capacity(&request(&b, &initial, &first, 1))
        .unwrap();
    let entries: Vec<_> = (1..=256)
        .map(|n| LogEntry {
            index: n,
            term: 7,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(1000 + n as u128).unwrap(),
                expected: ConfigurationId::new(n).unwrap(),
                change: ConfigurationChange::Learners(
                    Configuration::new(
                        ConfigurationId::new(n + 1).unwrap(),
                        b.policy.clone(),
                        b.voter_stores.clone(),
                        Default::default(),
                    )
                    .unwrap(),
                ),
            })),
        })
        .collect();
    let current = Membership::replay(&b, &entries, 256).unwrap();
    let record = record(257);
    let required = request(&b, &current, &record, 257);
    let grown = codec.configuration_capacity(&required).unwrap();
    assert_eq!(
        grown.snapshot.frame_bytes - baseline.snapshot.frame_bytes,
        256 * 16
    );
    assert_eq!(
        grown.snapshot.decoded_bytes - baseline.snapshot.decoded_bytes,
        256 * 192
    );
    let limits = WireLimits {
        max_decoded_bytes: grown.snapshot.decoded_bytes - 1,
        ..Default::default()
    };
    let restricted = NativeWireCodec::with_membership(limits).unwrap();
    assert_eq!(
        restricted.configuration_capacity(&request(&b, &initial, &first, 1)),
        Ok(baseline)
    );
    assert_eq!(
        restricted.configuration_capacity(&required),
        Err(WireError::TooLarge)
    );
}
#[test]
fn unsupported_formats_policy_limits_and_invalid_envelopes_reject() {
    let b = bootstrap(1, 3);
    let m = Membership::replay(&b, &[], 0).unwrap();
    let r = record(1);
    let mut required = request(&b, &m, &r, 1);
    assert_eq!(
        NativeWireCodec::new(Default::default())
            .unwrap()
            .configuration_capacity(&required),
        Err(WireError::UnsupportedVersion(1))
    );
    let mut limits = WireLimits::default();
    limits.policy.max_voters = 2;
    assert_eq!(
        NativeWireCodec::with_readiness(limits)
            .unwrap()
            .configuration_capacity(&required),
        Err(WireError::TooLarge)
    );
    let codec = NativeWireCodec::with_readiness(Default::default()).unwrap();
    required.application.snapshot_bytes = usize::MAX;
    assert_eq!(
        codec.configuration_capacity(&required),
        Err(WireError::TooLarge)
    );
    required.application = app();
    required.application.application_schema = 0;
    assert!(matches!(
        codec.configuration_capacity(&required),
        Err(WireError::InvalidMessage(_))
    ));
}
#[cfg(feature = "tls")]
#[test]
fn selected_native_transport_factory_preserves_codec_capacity() {
    use voteboat::{
        native::{tls::NativeTlsSession, transport::NativeTransportFactory},
        transport::*,
    };
    let b = bootstrap(1, 3);
    let m = Membership::replay(&b, &[], 0).unwrap();
    let r = record(1);
    let required = request(&b, &m, &r, 1);
    let codec = NativeWireCodec::with_readiness(Default::default()).unwrap();
    let expected = codec.configuration_capacity(&required).unwrap();
    let factory = NativeTransportFactory::new(codec, TransportLimits::default()).unwrap();
    assert_eq!(
        <NativeTransportFactory<NativeWireCodec> as PeerTransportFactory<
            NativeTlsSession<std::net::TcpStream>,
        >>::configuration_capacity(&factory, &required),
        Ok(expected)
    );
}
