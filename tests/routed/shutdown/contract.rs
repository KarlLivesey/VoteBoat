// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    outbound::{MessageClass, OutboundBinding, SendTicket},
    secure::{LocalIdentity, PeerIdentity, SessionDrainStatus},
    transport::{ConnectTicket, ReceiveInfo, TransportDrainStatus},
};
fn captured() -> PeerDrainStatus {
    // Exact decisive297 cutoff state: node3 -> peer1, original session15 and
    // generation150000, with its real timer still3052ms in the future.
    let local = |n| LocalIdentity {
        node: support::node(n),
        store: StoreBinding {
            identity: support::identity(n.into()),
            session: StoreSession::new(15).unwrap(),
        },
    };
    let binding = SessionBinding {
        local: local(3),
        peer: local(1),
        generation: SecureSessionGeneration::new(150000).unwrap(),
        wire_version: 1,
    };
    PeerDrainStatus {
        peer: PeerIdentity {
            node: binding.peer.node,
            store: binding.peer.store.identity,
        },
        closed: true,
        fenced: false,
        authorized: true,
        retiring: false,
        attempt: None,
        binding: Some(binding),
        accepted: None,
        transport: Some(TransportDrainStatus {
            binding,
            state: TransportState::Draining,
            usage: TransportUsage::default(),
            received: None,
            close_started: Some(true),
            session: Some(SessionDrainStatus {
                state: SessionState::Closing,
                flushed: true,
                next_deadline: Some(MonoTime(60926)),
                close_started: Some(true),
                protocol_drained: Some(false),
            }),
        }),
    }
}
fn pending(close: ProtocolClose) -> Remaining {
    [((2, support::node(1)), close)].into()
}
#[test]
fn captured_future_close_timer_pins_the_original_cutoff_and_never_renews() {
    let original = ProtocolClose::inspect(captured()).unwrap();
    let window = ProtocolWindow::pin(pending(original), MonoTime(57874)).unwrap();
    assert_eq!(window.end, MonoTime(60926));
    assert_eq!(window.end.0 - 57874, 3052);
    assert!(window.retains(&pending(original), MonoTime(60925)));
    assert!(!window.retains(&pending(original), MonoTime(60926)));
    assert!(!window.retains(&pending(original), MonoTime(70000)));
    assert!(ProtocolWindow::pin(pending(original), MonoTime(60926)).is_none());
    assert!(ProtocolWindow::pin(BTreeMap::new(), MonoTime(57874)).is_none());
    let mut changed = original;
    changed.deadline = MonoTime(60927);
    assert!(!window.retains(&pending(changed), MonoTime(60925)));
    changed = original;
    changed.binding.generation = SecureSessionGeneration::new(150001).unwrap();
    assert!(!window.retains(&pending(changed), MonoTime(60925)));
    assert!(!window.retains(&[((1, support::node(1)), original)].into(), MonoTime(60925)));
}
#[test]
fn settled_close_window_keeps_the_maximum_original_timer_after_another_owner_retires() {
    let first = ProtocolClose::inspect(captured()).unwrap();
    let mut second = first;
    second.deadline = MonoTime(61926);
    second.binding.peer.node = support::node(2);
    let remaining = [
        ((2, support::node(1)), first),
        ((2, support::node(2)), second),
    ]
    .into();
    let window = ProtocolWindow::pin(remaining, MonoTime(57874)).unwrap();
    assert_eq!(window.end, MonoTime(61926));
    let retired = [((2, support::node(2)), second)].into();
    assert!(window.retains(&retired, MonoTime(60926)));
    assert_eq!(window.end, MonoTime(61926));
    assert!(!window.retains(&retired, MonoTime(61926)));
    assert!(!window.retains(&BTreeMap::new(), MonoTime(60926)));
}
#[test]
fn accepted_original_work_and_unsupported_protocol_detail_cannot_enter_cleanup() {
    let original = captured();
    let binding = original.binding.unwrap();
    let ticket = SendTicket {
        binding: OutboundBinding {
            node: binding.local.node,
            store: binding.local.store,
            generation: OutboundGeneration::new(1).unwrap(),
        },
        peer: binding.peer.node,
        sequence: 1,
    };
    let mut changed = original;
    changed.accepted = Some(ticket);
    assert!(ProtocolClose::inspect(changed).is_none());
    assert_eq!(changed.accepted, Some(ticket));
    changed = original;
    changed.transport.as_mut().unwrap().usage.completion = true;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed
        .transport
        .as_mut()
        .unwrap()
        .usage
        .receive_frame_bytes = 1;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed
        .transport
        .as_mut()
        .unwrap()
        .session
        .as_mut()
        .unwrap()
        .next_deadline = None;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed
        .transport
        .as_mut()
        .unwrap()
        .session
        .as_mut()
        .unwrap()
        .protocol_drained = None;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed
        .transport
        .as_mut()
        .unwrap()
        .session
        .as_mut()
        .unwrap()
        .flushed = false;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.transport.as_mut().unwrap().state = TransportState::Open;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.binding = None;
    assert!(ProtocolClose::inspect(changed).is_none());
}

#[test]
fn fresh_connection_unread_input_and_nonclosing_identity_refuse_cleanup() {
    let original = captured();
    let binding = original.binding.unwrap();
    let mut changed = original;
    changed.attempt = Some(ConnectTicket {
        local: binding.local,
        peer: original.peer,
        generation: binding.generation,
    });
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.transport.as_mut().unwrap().received = Some(ReceiveInfo {
        connection: binding,
        class: MessageClass::Control,
        messages: 1,
        bytes: 1,
    });
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.peer.node = support::node(2);
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.closed = false;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.fenced = true;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed.authorized = false;
    assert!(ProtocolClose::inspect(changed).is_none());
    changed = original;
    changed
        .transport
        .as_mut()
        .unwrap()
        .session
        .as_mut()
        .unwrap()
        .state = SessionState::Ready;
    assert!(ProtocolClose::inspect(changed).is_none());
}
