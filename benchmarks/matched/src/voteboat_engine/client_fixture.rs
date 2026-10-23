// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Isolated producer/observer controls, never consensus evidence. Synthetic
//! entries below are routing fixtures only. The real Counter and normal Actor
//! tests separately require actual quorum/application before publication.
use super::*;
pub(super) fn client(sender: mpsc::Sender<Request>) -> (WriteDomain<OperationId>, Client) {
    let bootstrap = bootstrap().unwrap();
    let node = NodeId::new(1).unwrap();
    let binding = StoreBinding {
        identity: bootstrap.voter_stores[&node],
        session: StoreSession::new(1).unwrap(),
    };
    let (domain, factory) = WriteDomain::new(
        WriteBinding {
            group: bootstrap.group,
            node,
            store: binding,
        },
        WriteDomainLimits {
            ports: 1,
            retained_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let client = Client::new(sender, &factory, writes::port_limits(17).unwrap()).unwrap();
    (domain, client)
}
pub(super) struct Observed<'a> {
    pub operation: OperationId,
    pub bytes: Vec<u8>,
    pub reply: Producer<'a>,
}
pub(super) struct Producer<'a> {
    domain: &'a WriteDomain<OperationId>,
    entry: LogEntry,
    reply: Reply,
}
impl Producer<'_> {
    pub fn send(self, receipt: OperationId) -> Result<(), OperationId> {
        match self.reply {
            Reply::Port(reply) => reply
                .publish_applied(self.domain, &self.entry, receipt, 0)
                .map_err(|rejected| {
                    // The old is_closed + failed-send assertions become one exact
                    // atomic functional check, including ownership of returned R.
                    assert_eq!(rejected.reason, WriteError::ConsumerClosed);
                    rejected.receipt
                }),
            Reply::Owned(reply) => reply.send(receipt),
        }
    }
}
pub(super) fn originals(domain: &WriteDomain<OperationId>, request: Request) -> Vec<Observed<'_>> {
    if !request.is_empty() {
        assert!(
            matches!(request, Request::Port(_)),
            "normal configured Client must use public port"
        );
    }
    request.validate(domain).unwrap();
    request
        .into_originals()
        .enumerate()
        .map(|(ordinal, original)| {
            let Original {
                operation,
                bytes,
                mut reply,
            } = original.unwrap();
            let entry = LogEntry {
                index: ordinal as u64 + 1,
                term: 1,
                payload: EntryPayload::Command {
                    operation,
                    bytes: bytes.as_slice().into(),
                },
            };
            reply.bind(domain, &entry).unwrap();
            Observed {
                operation,
                bytes,
                reply: Producer {
                    domain,
                    entry,
                    reply,
                },
            }
        })
        .collect()
}
