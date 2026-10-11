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
//! Assertions have no weakening argument and do not call protocol transitions.
use super::protocol::*;

pub fn check(state: &State) -> Result<(), &'static str> {
    for t in 0..2 {
        let c = state.coordinator[t];
        if c.phase.decision() != c.first {
            return Err("durable coordinator decision changed");
        }
        if c.phase.decision() == Some(Decision::Commit) && c.prepared != 3 {
            return Err("commit lacks both exact prepared receipts");
        }
        if c.phase.decision() == Some(Decision::Abort) && c.refused == 0 {
            return Err("abort lacks original durable refusal proof");
        }
        observations(state, t)?;
        for p in 0..2 {
            participant(state, t, p)?;
        }
        if c.phase.all() && c.applied != 3 {
            return Err("AppliedAll lacks both exact application receipts");
        }
        if matches!(c.phase, CoordinatorPhase::Finished(_)) && c.released != 3 {
            return Err("Finished lacks both exact release receipts");
        }
    }
    for p in 0..2 {
        if state.balances[p] < 0 {
            return Err("negative account balance");
        }
        if let Some(t) = state.locks[p] {
            if !matches!(
                state.participant[t][p].phase,
                ParticipantPhase::Prepared | ParticipantPhase::DecisionApplied(_)
            ) {
                return Err("lock owner has no retained durable work");
            }
            if state.read(p).is_some() {
                return Err("locked account became readable");
            }
        }
    }
    if let (Some(a), Some(b)) = (state.read(0), state.read(1)) {
        if a + b != state.initial {
            return Err("observable accounts expose a partial transfer");
        }
    }
    Ok(())
}
fn observations(state: &State, t: usize) -> Result<(), &'static str> {
    let phase = state.coordinator[t].phase;
    let observed = state.observed[t];
    if (observed.prepared | observed.refused) != 0 && phase != CoordinatorPhase::Started {
        return Err("prepare observations outlive their collection phase");
    }
    if observed.applied != 0 && !matches!(phase, CoordinatorPhase::Decided(_)) {
        return Err("application observations outlive their collection phase");
    }
    if observed.released != 0 && !matches!(phase, CoordinatorPhase::AppliedAll(_)) {
        return Err("release observations outlive their collection phase");
    }
    Ok(())
}
fn participant(state: &State, t: usize, p: usize) -> Result<(), &'static str> {
    let c = state.coordinator[t];
    let part = state.participant[t][p];
    let observed = state.observed[t];
    if (c.prepared | observed.prepared) & (1 << p) != 0 && !part.prepared {
        return Err("prepared receipt lacks original durable participant fact");
    }
    if (c.refused | observed.refused) & (1 << p) != 0 && !part.refused {
        return Err("refusal receipt lacks original durable participant fact");
    }
    match part.phase {
        ParticipantPhase::Prepared if state.locks[p] != Some(t) => {
            return Err("prepared account lost its exact lock")
        }
        ParticipantPhase::DecisionApplied(d) | ParticipantPhase::Released(d) => {
            if c.phase.decision() != Some(d) {
                return Err("participant outcome has no matching coordinator decision");
            }
            if d == Decision::Commit && !part.prepared {
                return Err("commit participant was never prepared");
            }
            if part.phase == ParticipantPhase::DecisionApplied(d)
                && part.prepared
                && state.locks[p] != Some(t)
            {
                return Err("decision-applied account released its lock early");
            }
            if part.phase == ParticipantPhase::Released(d) && !c.phase.all() {
                return Err("participant released before exact AppliedAll");
            }
        }
        _ => {}
    }
    if (c.applied | observed.applied) & (1 << p) != 0
        && !matches!(
            part.phase,
            ParticipantPhase::DecisionApplied(_) | ParticipantPhase::Released(_)
        )
    {
        return Err("application receipt lacks durable decision application");
    }
    if (c.released | observed.released) & (1 << p) != 0
        && !matches!(part.phase, ParticipantPhase::Released(_))
    {
        return Err("release receipt lacks durable release");
    }
    Ok(())
}
