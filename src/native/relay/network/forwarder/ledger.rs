// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact original ticket and RPC scope, independent of local forwarding credits.
use super::*;
use std::mem::size_of;
#[derive(Clone)]
enum ReplyFamily {
    Vote,
    Append,
    Snapshot,
    Read,
    Repair,
    Readiness(Box<LearnerReadinessRequest>),
    Authority(PeerIdentity, ConfigurationId),
}
pub(super) struct ReplyScope {
    group: GroupIdentity,
    configuration: ConfigurationId,
    origin: LocalIdentity,
    target: PeerIdentity,
    term: u64,
    context: RequestContext,
    family: ReplyFamily,
}
pub(super) struct RequestRecord {
    pub original: OutputFrame,
    pub source: RelaySourceCapture,
    pub scopes: Vec<ReplyScope>,
    pub cost: usize,
}
impl ReplyScope {
    fn capture(m: &Message, target: PeerIdentity) -> Result<Self, RelayNetworkError> {
        let family = match &m.rpc {
            Rpc::Vote { .. } => ReplyFamily::Vote,
            Rpc::Append { .. } => ReplyFamily::Append,
            Rpc::Snapshot { .. } => ReplyFamily::Snapshot,
            Rpc::ReadProbe => ReplyFamily::Read,
            Rpc::LearnerRepair { .. } | Rpc::LearnerRepairSnapshot { .. } => ReplyFamily::Repair,
            Rpc::LearnerReadinessRequest(r) => ReplyFamily::Readiness(r.clone()),
            Rpc::AuthorityRequest {
                candidate,
                configuration,
            } => ReplyFamily::Authority(*candidate, *configuration),
            _ => return Err(RelayNetworkError::InvalidFrame),
        };
        if m.context.origin != m.sender || m.context.sequence == 0 || m.term == 0 {
            return Err(RelayNetworkError::InvalidFrame);
        }
        Ok(Self {
            group: m.group,
            configuration: m.configuration,
            origin: LocalIdentity {
                node: m.from,
                store: m.sender,
            },
            target,
            term: m.term,
            context: m.context,
            family,
        })
    }
    pub fn matches(&self, source: RelaySourceCapture, m: &Message) -> bool {
        m.group == self.group
            && m.configuration == self.configuration
            && m.from == self.target.node
            && source.source().node == self.target.node
            && source.source().store.identity == self.target.store
            && m.sender == source.source().store
            && m.to == self.origin.node
            && m.context == self.context
            && m.term >= self.term
            && match (&self.family, &m.rpc) {
                (ReplyFamily::Vote, Rpc::Voted { .. })
                | (ReplyFamily::Append, Rpc::Appended { .. } | Rpc::Compacted { .. })
                | (ReplyFamily::Snapshot, Rpc::SnapshotAck { .. } | Rpc::Compacted { .. })
                | (ReplyFamily::Read, Rpc::ReadAck)
                | (ReplyFamily::Repair, Rpc::LearnerRepaired { .. }) => true,
                (ReplyFamily::Readiness(expected), Rpc::LearnerReadinessReply { request, .. }) => {
                    expected == request
                }
                (
                    ReplyFamily::Authority(c, cfg),
                    Rpc::AuthorityReply {
                        candidate,
                        configuration,
                        ..
                    },
                ) => c == candidate && cfg == configuration,
                _ => false,
            }
    }
}
pub(super) fn decode_output<W: WireCodec>(
    codec: &W,
    output: &OutputFrame,
) -> Result<Vec<Message>, RelayNetworkError> {
    let scope = WireScope {
        from: output.ticket.binding.node,
        sender: output.ticket.binding.store,
        to: output.ticket.peer,
    };
    let messages = codec
        .decode_batch(scope, &output.wire)
        .map_err(RelayNetworkError::Wire)?;
    if messages.len() != output.kinds.len()
        || messages.iter().any(|m| !scope.matches(m))
        || messages
            .iter()
            .zip(&output.kinds)
            .any(|(m, k)| !valid_kind(m, *k))
    {
        return Err(RelayNetworkError::InvalidFrame);
    }
    Ok(messages)
}
fn valid_kind(m: &Message, k: RecordKind) -> bool {
    let reply = matches!(
        m.rpc,
        Rpc::Voted { .. }
            | Rpc::Appended { .. }
            | Rpc::SnapshotAck { .. }
            | Rpc::Compacted { .. }
            | Rpc::ReadAck
            | Rpc::LearnerReadinessReply { .. }
            | Rpc::AuthorityReply { .. }
            | Rpc::LearnerRepaired { .. }
    );
    match k {
        RecordKind::Reply => reply,
        RecordKind::Request => {
            !reply
                && !matches!(
                    m.rpc,
                    Rpc::TimeoutNow { .. }
                        | Rpc::CommitNotice { .. }
                        | Rpc::CommittedLearnerRepairSnapshot { .. }
                )
        }
        RecordKind::OneWay => matches!(
            m.rpc,
            Rpc::TimeoutNow { .. }
                | Rpc::CommitNotice { .. }
                | Rpc::Append { .. }
                | Rpc::CommittedLearnerRepairSnapshot { .. }
        ),
    }
}
impl RequestRecord {
    pub fn capture(
        original: &OutputFrame,
        source: RelaySourceCapture,
        target: PeerIdentity,
        messages: &[Message],
    ) -> Result<Option<Self>, RelayNetworkError> {
        let scopes: Vec<_> = messages
            .iter()
            .zip(&original.kinds)
            .filter(|(_, k)| **k == RecordKind::Request)
            .map(|(m, _)| ReplyScope::capture(m, target))
            .collect::<Result<_, _>>()?;
        if scopes.is_empty() {
            return Ok(None);
        }
        let original = original.clone();
        let cost = size_of::<Self>()
            + original.wire.capacity()
            + original.kinds.capacity() * size_of::<RecordKind>()
            + scopes.capacity() * (size_of::<ReplyScope>() + size_of::<LearnerReadinessRequest>());
        Ok(Some(Self {
            original,
            source,
            scopes,
            cost,
        }))
    }
    pub fn same_original(&self, original: &OutputFrame) -> bool {
        self.original.ticket == original.ticket
            && self.original.kinds == original.kinds
            && self.original.wire == original.wire
    }
}

#[cfg(test)]
mod commit_notice_tests {
    use super::*;
    #[test]
    fn notice_cannot_be_relabelled_as_request_reply_or_capture_a_reply_family() {
        let node = |n| NodeId::new(n).unwrap();
        let binding = StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        };
        let m = Message {
            group: GroupIdentity {
                id: GroupId::new(1).unwrap(),
                incarnation: GroupIncarnation::new(1).unwrap(),
            },
            configuration: ConfigurationId::new(1).unwrap(),
            from: node(1),
            sender: binding,
            to: node(2),
            term: 1,
            context: RequestContext {
                origin: binding,
                sequence: 1,
            },
            rpc: Rpc::CommitNotice {
                index: 1,
                log_term: 1,
            },
        };
        assert!(valid_kind(&m, RecordKind::OneWay));
        assert!(!valid_kind(&m, RecordKind::Request));
        assert!(!valid_kind(&m, RecordKind::Reply));
        assert!(ReplyScope::capture(
            &m,
            PeerIdentity {
                node: m.to,
                store: binding.identity
            }
        )
        .is_err());
    }
}
