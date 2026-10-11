// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{generator::*, support::*};
use voteboat::{identity::*, log::*, native::wire::NativeWireCodec, raft::*, snapshot::*, wire::*};
fn limits() -> WireLimits {
    WireLimits {
        max_frame_bytes: 8192,
        max_messages: 8,
        max_entries_per_message: 8,
        max_command_bytes: 64,
        max_snapshot_bytes: 512,
        max_decoded_bytes: 32768,
        ..WireLimits::default()
    }
}
fn scope() -> WireScope {
    WireScope {
        from: node(1),
        sender: HostLogStore::new(1).binding(),
        to: node(2),
    }
}
fn message(rpc: Rpc) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        sender: scope().sender,
        to: node(2),
        term: 3,
        context: RequestContext {
            origin: scope().sender,
            sequence: 7,
        },
        rpc,
    }
}
fn reseal(bytes: &mut [u8]) {
    let header = crc(&bytes[..20]).to_le_bytes();
    bytes[20..24].copy_from_slice(&header);
    seal_tail(bytes);
}
fn corpus(codec: NativeWireCodec, messages: Vec<Message>, name: &str) {
    let original = codec.encode_batch(scope(), &messages).unwrap();
    let mut wrong = scope();
    wrong.to = node(3);
    assert!(codec.decode_batch(wrong, &original).is_err());
    campaign(
        name,
        &original,
        &[(8, 2), (10, 2), (12, 4), (16, 4), (24, 4)],
        |case| {
            let mut bytes = case.bytes.clone();
            if case.repair() {
                reseal(&mut bytes);
            }
            if let Ok(length) = codec.frame_length(&bytes[..bytes.len().min(codec.header_bytes())])
            {
                assert!(length <= codec.limits().max_frame_bytes);
            }
            let decoded = codec.decode_batch(scope(), &bytes);
            if case.original() {
                assert_eq!(decoded, Ok(messages.clone()));
            }
            if case.integrity_damage() {
                assert!(decoded.is_err());
            }
            match decoded {
                Ok(messages) => {
                    assert!(
                        messages.len() <= limits().max_messages
                            && messages.capacity() <= limits().max_messages
                    );
                    assert!(messages.iter().all(|message| scope().matches(message)));
                    let encoded = codec.encode_batch(scope(), &messages).unwrap();
                    assert!(encoded.len() <= limits().max_frame_bytes);
                    assert_eq!(codec.decode_batch(scope(), &encoded), Ok(messages));
                    true
                }
                Err(_) => false,
            }
        },
    );
}
#[test]
fn generated_rpc_mutations_enforce_framing_scope_and_public_reencoding() {
    let messages = vec![
        message(Rpc::Vote {
            last_index: 1,
            last_term: 1,
        }),
        message(Rpc::ReadProbe),
        message(Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![entry(1, 1, 7), entry(2, 1, 3)],
            leader_commit: 0,
        }),
        message(Rpc::ReadAck),
    ];
    corpus(
        NativeWireCodec::new(limits()).unwrap(),
        messages.clone(),
        "rpc-v1-multiplex",
    );
    corpus(
        NativeWireCodec::with_leadership_transfer(limits()).unwrap(),
        messages,
        "rpc-v8-multiplex",
    );
    corpus(
        NativeWireCodec::with_leadership_transfer(limits()).unwrap(),
        vec![message(Rpc::Snapshot {
            snapshot: Box::new(Snapshot {
                metadata: SnapshotMetadata {
                    bootstrap: bootstrap(1, 3),
                    membership: None,
                    index: 2,
                    term: 2,
                    application_schema: 1,
                },
                application: vec![7; 24],
            }),
        })],
        "rpc-v8-snapshot",
    );
}
