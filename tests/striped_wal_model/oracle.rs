// SPDX-License-Identifier: RPL-1.5
use super::types::*;
pub fn violation(s: State, action: Action, scenario: Scenario) -> Option<Violation> {
    if s.promised && s.crashed && !s.has_control() {
        return Some(Violation::LostPromise);
    }
    if action == Action::ReleasePrefix && s.released > s.prefix() {
        return Some(Violation::PrefixHole);
    }
    if action == Action::Recover
        && (s.recovered_prefix > s.prefix()
            || s.recovered_generations
                .contains(&Some(Generation::Previous))
            || (s.prefix() < 3 && !s.quarantined))
    {
        // Missing current-generation payload quarantines recovery; it cannot
        // become old-generation bytes under the selected generation's identity.
        return Some(Violation::WrongGeneration);
    }
    if scenario == Scenario::Snapshot && s.has_control() && s.snapshot != Phase::Durable {
        return Some(Violation::MissingSnapshot);
    }
    None
}
