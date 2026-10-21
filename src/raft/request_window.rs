// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original volatile request collectors, observed only through owned relay output.
use super::*;
use crate::secure::LocalIdentity;
mod replication;
mod special;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Boundary {
    Vote {
        index: u64,
        term: u64,
    },
    Append {
        start: u64,
        end: u64,
    },
    Snapshot(SnapshotRef),
    Read,
    /// Boxed: the request carries the full application identity, and every
    /// window (one per outbound request) holds a `Boundary` inline.
    Readiness(Box<LearnerReadinessRequest>),
    Authority {
        candidate: PeerIdentity,
        configuration: ConfigurationId,
    },
    Repair {
        start: u64,
        end: u64,
    },
    RepairSnapshot(SnapshotRef),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestWindow {
    group: GroupIdentity,
    configuration: ConfigurationId,
    requester: NodeId,
    sender: StoreBinding,
    recipient: PeerIdentity,
    term: u64,
    context: RequestContext,
    boundary: Boundary,
}
#[derive(Eq, PartialEq)]
pub(crate) enum OriginalScope {
    Request(Box<RequestWindow>),
    OneWay,
    Reply,
    Retired,
}
impl Raft {
    pub(crate) fn request_window(&self, m: &Message) -> OriginalScope {
        if self.fenced
            || m.group != self.durable.bootstrap.group
            || m.from != self.node
            || m.sender != self.binding
            || m.to == self.node
            || m.context.sequence == 0
        {
            return OriginalScope::Retired;
        }
        if matches!(
            m.rpc,
            Rpc::Voted { .. }
                | Rpc::Appended { .. }
                | Rpc::SnapshotAck { .. }
                | Rpc::Compacted { .. }
                | Rpc::ReadAck
                | Rpc::LearnerReadinessReply { .. }
                | Rpc::AuthorityReply { .. }
                | Rpc::LearnerRepaired { .. }
        ) {
            return OriginalScope::Reply;
        }
        if m.context.origin != self.binding || m.term != self.durable.hard_state.term.max(1) {
            return OriginalScope::Retired;
        }
        let found = self.ordinary_window(m).or_else(|| self.special_window(m));
        if let Some((recipient, boundary)) = found {
            return OriginalScope::Request(Box::new(RequestWindow {
                group: m.group,
                configuration: m.configuration,
                requester: m.from,
                sender: m.sender,
                recipient,
                term: m.term,
                context: m.context,
                boundary,
            }));
        }
        if self.original_one_way(m) {
            OriginalScope::OneWay
        } else {
            OriginalScope::Retired
        }
    }
    fn ordinary_window(&self, m: &Message) -> Option<(PeerIdentity, Boundary)> {
        if m.configuration != self.membership().id() || !self.local_voter() {
            return None;
        }
        let recipient = PeerIdentity {
            node: m.to,
            store: self.membership().replica_store(m.to)?,
        };
        let boundary = match &m.rpc {
            Rpc::Vote {
                last_index,
                last_term,
            } if self.role == Role::Candidate
                && self.vote_context == Some(m.context)
                && !self.votes.contains(&m.to)
                && self.membership().is_voter(m.to)
                && self.durable.hard_state.voted_for == Some(self.node)
                && *last_index == self.durable.last_index()
                && *last_term == self.durable.last_term() =>
            {
                Boundary::Vote {
                    index: *last_index,
                    term: *last_term,
                }
            }
            Rpc::ReadProbe
                if self.role == Role::Leader
                    && self.membership().is_voter(m.to)
                    && self.read.as_ref().is_some_and(|r| {
                        r.barrier.context == m.context
                            && r.barrier.term == m.term
                            && r.barrier.configuration == m.configuration
                            && !r.acknowledgements.contains(&m.to)
                    }) =>
            {
                Boundary::Read
            }
            Rpc::Append { .. } | Rpc::Snapshot { .. } if self.role == Role::Leader => {
                self.replication_window(m)?
            }
            _ => return None,
        };
        Some((recipient, boundary))
    }
}
impl RequestWindow {
    pub(crate) fn group(&self) -> GroupIdentity {
        self.group
    }
    /// Bytes this window owns outside its own allocation: the boxed readiness
    /// request, else none.
    pub(crate) fn owned_bytes(&self) -> usize {
        match self.boundary {
            Boundary::Readiness(_) => std::mem::size_of::<LearnerReadinessRequest>(),
            _ => 0,
        }
    }
    pub(crate) fn current(&self, core: &Raft) -> bool {
        if core.fenced
            || core.binding != self.sender
            || core.node != self.requester
            || core.durable.bootstrap.group != self.group
            || core.durable.hard_state.term.max(1) != self.term
        {
            return false;
        }
        self.current_boundary(core)
    }
    pub(crate) fn accepts_reply(&self, core: &Raft, source: LocalIdentity, m: &Message) -> bool {
        self.current(core)
            && source.node == self.recipient.node
            && source.store.identity == self.recipient.store
            && m.sender == source.store
            && m.group == self.group
            && m.configuration == self.configuration
            && m.from == self.recipient.node
            && m.to == self.requester
            && m.context == self.context
            && m.term >= self.term
            && self.reply_family(&m.rpc)
    }
    fn reply_family(&self, rpc: &Rpc) -> bool {
        match (&self.boundary, rpc) {
            (Boundary::Vote { .. }, Rpc::Voted { .. })
            | (Boundary::Append { .. }, Rpc::Appended { .. } | Rpc::Compacted { .. })
            | (Boundary::Snapshot(_), Rpc::SnapshotAck { .. } | Rpc::Compacted { .. })
            | (Boundary::Read, Rpc::ReadAck)
            | (
                Boundary::Repair { .. } | Boundary::RepairSnapshot(_),
                Rpc::LearnerRepaired { .. },
            ) => true,
            (Boundary::Readiness(expected), Rpc::LearnerReadinessReply { request, .. }) => {
                **request == **expected
            }
            (
                Boundary::Authority {
                    candidate: expected,
                    configuration: wanted,
                },
                Rpc::AuthorityReply {
                    candidate,
                    configuration,
                    ..
                },
            ) => candidate == expected && configuration == wanted,
            _ => false,
        }
    }
}
