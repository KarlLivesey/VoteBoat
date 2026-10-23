// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Unchanged original R3 maintain/publication/peer order; visibility only differs.
use super::*;
pub(super) fn r3_service<const TRACE: bool>(
    actor: &mut Actor<TRACE>,
    publish: bool,
) -> Result<(), Failure> {
    actor.maintain()?;
    actor.parts.stats[actor.parts.slot]
        .retained
        .store(actor.parts.core.state().entries.len(), Ordering::Relaxed);
    if publish {
        actor.parts.frontiers[actor.parts.slot]
            .store(actor.parts.application.applied, Ordering::Release);
    }
    Ok(())
}
pub(super) fn r3_pump<const TRACE: bool>(
    actors: &mut [Actor<TRACE>],
    publish: bool,
) -> Result<(), Failure> {
    loop {
        let mut any = false;
        for actor in actors.iter_mut() {
            while let Ok(message) = actor.parts.peer_receiver.try_recv() {
                actor.step(Event::Receive(message))?;
                r3_service(actor, publish)?;
                any = true;
            }
        }
        if !any {
            return Ok(());
        }
    }
}
