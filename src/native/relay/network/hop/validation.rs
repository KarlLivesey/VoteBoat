// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fixed origin/full-path guard over the actual authenticated receiving channel.
use super::*;
impl<S: SecureSession> RelayHop<S> {
    pub(super) fn validate_frame(
        &self,
        frame: &RelayFrame,
        incoming: bool,
    ) -> Result<(), RelayNetworkError> {
        let valid = match (&self.plan, incoming, &frame.body) {
            (HopPlan::Voter(p), true, FrameBody::Opened(s) | FrameBody::Closed(s)) => {
                p.tree.path_matches(**s, self.binding.peer)
            }
            (HopPlan::Voter(p), true, FrameBody::Forwarded(f)) => {
                p.tree.path_matches(f.source, self.binding.peer)
                    && local_ticket(f.output.ticket, f.source.source())
                    && f.output.ticket.peer == self.binding.local.node
                    && f.returns.len() == f.output.kinds.len()
            }
            (HopPlan::Voter(p), false, FrameBody::Output(o)) => {
                local_ticket(o.ticket, self.binding.local) && p.source(o.ticket.peer).is_some()
            }
            (HopPlan::Voter(p), false, FrameBody::Retire(t)) => {
                local_ticket(*t, self.binding.local) && p.source(t.peer).is_some()
            }
            (HopPlan::Helper(p), true, FrameBody::Output(o)) => {
                p.source(self.binding.peer.node).is_some()
                    && local_ticket(o.ticket, self.binding.peer)
                    && p.source(o.ticket.peer).is_some()
            }
            (HopPlan::Helper(p), true, FrameBody::Retire(t)) => {
                p.source(self.binding.peer.node).is_some()
                    && local_ticket(*t, self.binding.peer)
                    && p.source(t.peer).is_some()
            }
            (HopPlan::Helper(p), true, FrameBody::Opened(s) | FrameBody::Closed(s)) => {
                incoming_capture(p, **s, self.binding)
            }
            (HopPlan::Helper(p), true, FrameBody::Forwarded(f)) => {
                incoming_capture(p, f.source, self.binding)
                    && local_ticket(f.output.ticket, f.source.source())
                    && p.source(f.output.ticket.peer).is_some()
                    && p.next_hop(f.output.ticket.peer) != Some(self.binding.peer.node)
                    && f.returns.len() == f.output.kinds.len()
            }
            (HopPlan::Helper(p), true, FrameBody::Retired(t)) => {
                incoming_capture(p, t.source, self.binding)
                    && local_ticket(t.original, t.source.source())
                    && p.source(t.original.peer).is_some()
                    && p.next_hop(t.original.peer) != Some(self.binding.peer.node)
            }
            (HopPlan::Helper(p), false, FrameBody::Opened(s) | FrameBody::Closed(s)) => {
                p.tree.path_matches(**s, self.binding.local)
                    && s.source().node != self.binding.peer.node
                    && !s
                        .path
                        .hops()
                        .any(|h| h.local.node == self.binding.peer.node)
            }
            (HopPlan::Helper(p), false, FrameBody::Forwarded(f)) => {
                p.tree.path_matches(f.source, self.binding.local)
                    && local_ticket(f.output.ticket, f.source.source())
                    && p.next_hop(f.output.ticket.peer) == Some(self.binding.peer.node)
                    && f.returns.len() == f.output.kinds.len()
            }
            (HopPlan::Helper(p), false, FrameBody::Retired(t)) => {
                p.tree.path_matches(t.source, self.binding.local)
                    && local_ticket(t.original, t.source.source())
                    && p.tree.helper(self.binding.peer.node).is_some()
                    && p.next_hop(t.original.peer) == Some(self.binding.peer.node)
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(RelayNetworkError::WrongSource)
        }
    }
    pub(super) fn observed_source(&self, frame: &RelayFrame) -> Option<AuthenticatedSourceView> {
        let captured = frame.source_capture()?;
        let valid = match &self.plan {
            HopPlan::Voter(p) => p.tree.path_matches(captured, self.binding.peer),
            HopPlan::Helper(p) => incoming_capture(p, captured, self.binding),
        };
        valid.then(|| {
            AuthenticatedSourceView::from_delegated_capture(&VerifiedDelegatedSource {
                receiving: self.binding,
                captured,
            })
        })
    }
}
fn incoming_capture(
    plan: &RelayHelperPlan,
    source: RelaySourceCapture,
    actual: SessionBinding,
) -> bool {
    plan.tree.helper(actual.peer.node).is_some()
        && plan.tree.path_matches(source, actual.peer)
        && source.path.len() < plan.tree.max_hops()
        && !source
            .path
            .hops()
            .any(|h| h.local.node == actual.local.node)
}
fn local_ticket(t: SendTicket, actual: LocalIdentity) -> bool {
    t.sequence != 0
        && t.binding.node == actual.node
        && t.binding.store == actual.store
        && t.peer != actual.node
}
