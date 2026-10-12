// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Failure-edge immutable original association only; never watchdog progress.
use super::{host::Host, setup};
use voteboat::{logical_voter::*, runtime::*};
pub fn retained(host: &Host<'_, '_>, node: usize, ticket: ReadInvocationTicket) -> String {
    let read = host.nodes[node].inspect_read(ticket);
    let top = host
        .nodes
        .iter()
        .enumerate()
        .map(|(slot, n)| {
            let held = n.local().owner.inspect_visit(setup::top().group);
            format!(
                "top={slot},owner={:?},held={held:?},work={}",
                n.local().owner.identity(),
                work(&host.upper[slot], held.as_ref().ok().copied().flatten())
            )
        })
        .collect::<Vec<_>>();
    let middle = host.upper.iter().enumerate().flat_map(|(cohort, bridge)| {
        (0..3).map(move |slot| {
            bridge.inspect_backing(slot, |local| {
                let held = local.owner.inspect_visit(setup::middle(cohort).group);
                let child = bridge.inspect_work(|w| {
                    w.and_then(|w| w.attempt()).filter(|a| a.node() == slot).map(|attempt| match attempt.ticket() {
                        LogicalBridgeChildTicket::Read { ticket, query } => format!("original_child_read={:?},query={query:?},view={:?}", ticket,local.reads.inspect(ticket)),
                        LogicalBridgeChildTicket::Client(ticket) => format!("original_child_client={ticket:?},sameOriginalOwner={},sameOriginalRouter={}",ticket.binding.owner==local.owner.identity(), ticket.binding==local.clients.binding()),
                    }).unwrap_or_else(|| "AssociationUnavailable: no current attempt for this slot".into())
                });
                format!("middle={cohort}/{slot},owner={:?},held={held:?},upperChild={child},lowerWork={}", local.owner.identity(),work(&host.lower.bridges[cohort*3+slot],held.as_ref().ok().copied().flatten()))
            }).unwrap_or_else(|error| format!("AssociationUnavailable: middle={cohort}/{slot},error={error:?}"))
        })
    }).collect::<Vec<_>>();
    format!("original-read-inspection node={node},ticket={ticket:?},view={read:?}; top={top:?}; middle={middle:?}")
}
fn work<N: LogicalBackingChild>(
    bridge: &LogicalBackingBridge<'_, N>,
    held: Option<EffectVisitInspection>,
) -> String {
    bridge.inspect_work(|work| {
        let Some(work) = work else { return "AssociationUnavailable: no retained work".into(); };
        let association = match (held.and_then(EffectVisitInspection::leased), work.capsule()) {
            (Some(lease), Some(capsule)) => {
                let effect = match capsule.origin {
                    LogicalCapsuleOrigin::Protocol(effect)
                    | LogicalCapsuleOrigin::Client { effect, .. }
                    | LogicalCapsuleOrigin::Read { effect, .. } => effect,
                };
                if lease == effect { "exactOriginalHeldEffect" } else { "AssociationUnavailable: capsule/held effect mismatch" }
            }
            _ => "AssociationUnavailable: no held lease or no Authorize capsule",
        };
        format!("phase={:?},operation={:?},commandBytes={:?},capsule={:?},attempt={:?},retainedOutput={},association={association}",work.phase(),work.operation(),work.command_bytes(),work.capsule(),work.attempt(),work.retained_output())
    })
}
