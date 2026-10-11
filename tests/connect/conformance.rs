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
use std::fmt::Debug;

fn ownership(connector: &impl PeerConnector) -> (ConnectUsage, Option<MonoTime>) {
    (connector.usage(), connector.next_deadline())
}

fn refused<C: PeerConnector>(
    connector: &mut C,
    request: ConnectRequest<C::Endpoint>,
    now: MonoTime,
    expected: ConnectError,
) -> ConnectRequest<C::Endpoint>
where
    C::Endpoint: Clone + Debug + PartialEq,
{
    let ticket = request.ticket;
    let deadline = request.deadline;
    let direction = request.direction.clone();
    let original = ownership(connector);
    let rejection = connector.submit(request, now).unwrap_err();
    assert_eq!(rejection.reason, expected);
    assert_eq!(rejection.request.ticket, ticket);
    assert_eq!(rejection.request.deadline, deadline);
    match (direction, &rejection.request.direction) {
        (ConnectDirection::Dial(endpoint), ConnectDirection::Dial(returned)) => {
            assert_eq!(&endpoint, returned);
        }
        _ => panic!("original dial direction was replaced"),
    }
    assert_eq!(ownership(connector), original);
    *rejection.request
}

fn pending_close<C: PeerConnector>(connector: &mut C, endpoint: C::Endpoint)
where
    C::Endpoint: Clone + Debug + PartialEq,
{
    let accepted = ticket(1, 2, 1);
    let deadline = MonoTime(90);
    assert_eq!(connector.local(), accepted.local);
    assert_eq!(connector.usage(), ConnectUsage::default());
    connector
        .submit(
            ConnectRequest {
                ticket: accepted,
                direction: ConnectDirection::Accept,
                deadline,
            },
            MonoTime(10),
        )
        .unwrap();
    let original = ownership(connector);
    assert_eq!(original.0.requests, 1);
    assert_eq!(original.1, Some(deadline));
    let zero = ConnectPollBudget {
        visits: 0,
        socket_calls: 0,
        completions: 0,
        ..ConnectPollBudget::default()
    };
    // No client connects to the native listener, and zero budgets do not allow
    // the host's immediate-success fixture to consume the accepted attempt.
    assert!(connector.poll(MonoTime(11), zero).unwrap().is_empty());
    assert!(!connector.cancel(ticket(1, 2, 2)));
    assert_eq!(ownership(connector), original);
    assert!(matches!(
        connector.poll(
            MonoTime(12),
            ConnectPollBudget {
                socket_calls: 1025,
                ..zero
            }
        ),
        Err(ConnectError::InvalidLimits)
    ));
    assert_eq!(ownership(connector), original);
    assert!(connector.poll(MonoTime(11), zero).unwrap().is_empty());
    assert_eq!(ownership(connector), original);
    assert!(matches!(
        connector.poll(MonoTime(10), zero),
        Err(ConnectError::TimeWentBack)
    ));
    assert_eq!(ownership(connector), original);
    let request = refused(
        connector,
        ConnectRequest {
            ticket: ticket(1, 2, 2),
            direction: ConnectDirection::Dial(endpoint),
            deadline,
        },
        MonoTime(10),
        ConnectError::TimeWentBack,
    );
    let request = refused(connector, request, MonoTime(11), ConnectError::Overloaded);
    terminal_close(connector, request, accepted, zero);
}

fn terminal_close<C: PeerConnector>(
    connector: &mut C,
    request: ConnectRequest<C::Endpoint>,
    accepted: ConnectTicket,
    zero: ConnectPollBudget,
) where
    C::Endpoint: Clone + Debug + PartialEq,
{
    let original = connector.usage();
    connector.close();
    assert_eq!(connector.usage(), original);
    assert!(!connector.is_drained());
    let closed = ownership(connector);
    assert!(connector.poll(MonoTime(11), zero).unwrap().is_empty());
    assert_eq!(ownership(connector), closed);
    let _request = refused(connector, request, MonoTime(11), ConnectError::Closed);
    let mut terminal = connector
        .poll(
            MonoTime(11),
            ConnectPollBudget {
                completions: 1,
                ..zero
            },
        )
        .unwrap();
    assert_eq!(terminal.len(), 1);
    let completion = terminal.pop().unwrap();
    assert_eq!(completion.ticket, accepted);
    assert!(matches!(completion.result, Err(ConnectError::Cancelled)));
    assert_eq!(connector.usage(), ConnectUsage::default());
    assert_eq!(connector.next_deadline(), None);
    assert!(connector.is_drained());
    assert!(!connector.cancel(accepted));
    assert!(connector
        .poll(MonoTime(11), ConnectPollBudget::default())
        .unwrap()
        .is_empty());
}

#[test]
fn host_connector_preserves_pending_ownership_under_refusal_and_close() {
    let mut host = HostConnector {
        pending: None,
        generation: 0,
        closed: false,
        now: MonoTime(0),
    };
    pending_close(&mut host, ());
}

#[cfg(feature = "tls")]
#[test]
fn native_connector_preserves_pending_ownership_under_refusal_and_close() {
    let mut connector = native::make(1, &[2], 1);
    // The real listener remains idle: admission, refusal and cancellation are
    // observed without guessing when a socket or handshake becomes ready.
    pending_close(&mut connector, "127.0.0.1:1234".parse().unwrap());
    native::finish(connector, 11);
}
