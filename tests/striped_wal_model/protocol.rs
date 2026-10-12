// SPDX-License-Identifier: RPL-1.5
use super::types::*;

pub fn successors(s: State, scenario: Scenario, rule: Rule) -> Vec<(Action, State)> {
    let mut out = Vec::new();
    if !s.crashed {
        for (i, phase) in s.payload.iter().enumerate() {
            match phase {
                Phase::Absent => push(&mut out, s, Action::WritePayload(i)),
                Phase::Written => push(&mut out, s, Action::SyncPayload(i)),
                Phase::Durable => {}
            }
        }
        if scenario == Scenario::Snapshot {
            match s.snapshot {
                Phase::Absent => push(&mut out, s, Action::WriteSnapshot),
                Phase::Written => push(&mut out, s, Action::SyncSnapshot),
                Phase::Durable => {}
            }
        }
        control_steps(&mut out, s, scenario, rule);
        releases(&mut out, s, scenario, rule);
        push(&mut out, s, Action::Crash);
    } else {
        if s.lost.is_none() && !s.recovered {
            for lane in 0..2 {
                push(&mut out, s, Action::LoseDevice(lane));
            }
        }
        if !s.recovered {
            let mut next = s;
            next.recovered = true;
            next.quarantined = !s.has_control() || s.prefix() < 3;
            next.recovered_prefix = if next.quarantined { 0 } else { s.prefix() };
            for generation in next
                .recovered_generations
                .iter_mut()
                .take(next.recovered_prefix as usize)
            {
                *generation = Some(Generation::Selected);
            }
            // Only the deliberately weakened suffix rule confuses durable old
            // generation records with absent records of the selected new suffix.
            if scenario == Scenario::Suffix && rule == Rule::ReuseOldGeneration && s.has_control() {
                next.quarantined = false;
                next.recovered_prefix = 0;
                for i in 0..3 {
                    let generation = if s.payload[i] == Phase::Durable {
                        Generation::Selected
                    } else if s.previous_payload[i] == Phase::Durable {
                        Generation::Previous
                    } else {
                        break;
                    };
                    next.recovered_generations[i] = Some(generation);
                    next.recovered_prefix += 1;
                }
            }
            out.push((Action::Recover, next));
        }
    }
    out
}
fn control_steps(out: &mut Vec<(Action, State)>, s: State, scenario: Scenario, rule: Rule) {
    if scenario == Scenario::Snapshot && s.snapshot != Phase::Durable && rule != Rule::EarlySnapshot
    {
        return;
    }
    for (i, phase) in s.control.iter().enumerate() {
        match phase {
            Phase::Absent => push(out, s, Action::WriteControl(i)),
            Phase::Written => push(out, s, Action::SyncControl(i)),
            Phase::Durable => {}
        }
    }
}
fn releases(out: &mut Vec<(Action, State)>, s: State, scenario: Scenario, rule: Rule) {
    if !s.mirrored() && !(rule == Rule::SingleControlCopy && s.has_control()) {
        return;
    }
    if !s.promised {
        push(out, s, Action::ReleaseVote);
    }
    let prefix = if rule == Rule::MaximumPrefix {
        s.payload
            .iter()
            .rposition(|p| *p == Phase::Durable)
            .map_or(0, |i| (i + 1) as u8)
    } else {
        s.prefix()
    };
    if prefix > s.released {
        let mut next = s;
        next.released = prefix;
        out.push((Action::ReleasePrefix, next));
    }
    // Snapshot controls select original bytes before any reclaim/publication.
    // The oracle checks even control-only publication against that dependency.
    if scenario == Scenario::Snapshot && rule == Rule::EarlySnapshot && !s.recovered {
        let mut next = s;
        next.recovered = true;
        out.push((Action::Recover, next));
    }
}
fn push(out: &mut Vec<(Action, State)>, s: State, action: Action) {
    out.push((action, apply(s, action)));
}
pub fn apply(mut s: State, action: Action) -> State {
    match action {
        Action::WritePayload(i) => s.payload[i] = Phase::Written,
        Action::SyncPayload(i) => s.payload[i] = Phase::Durable,
        Action::WriteControl(i) => s.control[i] = Phase::Written,
        Action::SyncControl(i) => s.control[i] = Phase::Durable,
        Action::WriteSnapshot => s.snapshot = Phase::Written,
        Action::SyncSnapshot => s.snapshot = Phase::Durable,
        Action::ReleaseVote => s.promised = true,
        Action::Crash => {
            s.crashed = true;
            for phase in s
                .payload
                .iter_mut()
                .chain(s.previous_payload.iter_mut())
                .chain(s.control.iter_mut())
                .chain(std::iter::once(&mut s.snapshot))
            {
                if *phase == Phase::Written {
                    *phase = Phase::Absent;
                }
            }
        }
        Action::LoseDevice(lane) => {
            s.lost = Some(lane);
            s.control[lane] = Phase::Absent;
            for (index, phase) in s.payload.iter_mut().enumerate() {
                if index % 2 == lane {
                    *phase = Phase::Absent;
                }
            }
            // The independent snapshot is explicitly on another durable provider.
            for (index, phase) in s.previous_payload.iter_mut().enumerate() {
                if index % 2 == lane {
                    *phase = Phase::Absent;
                }
            }
        }
        Action::ReleasePrefix | Action::Recover => unreachable!("derived transition"),
    }
    s
}
