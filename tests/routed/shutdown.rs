// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Keep routed background work within the shared original-close contract.
use super::*;
#[cfg(feature = "quic")]
use crate::protocol_close::{ProtocolClose, ProtocolWindow};
#[cfg(feature = "quic")]
use voteboat::{
    secure::SessionState,
    transport::{PeerDrainStatus, TransportState, TransportUsage},
};
#[cfg(feature = "quic")]
#[path = "shutdown/long_rtt.rs"]
mod long_rtt;

#[track_caller]
pub(super) fn drive<A>(nodes: &mut [Node<A>], clock: &Instant, background: &mut impl FnMut())
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    crate::protocol_close::drive(
        nodes,
        clock,
        |nodes, clock| {
            poll_nodes(nodes, clock);
            background();
        },
        diagnose_shutdown,
    );
}
