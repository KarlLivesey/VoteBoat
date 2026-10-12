// SPDX-License-Identifier: RPL-1.5
use super::types::*;
pub fn check(s: &State) -> Result<(), Violation> {
    for (t, tx) in s.tx.iter().enumerate() {
        if tx.decision == Some(Decision::Commit)
            && tx.participants.iter().any(|p| p.prepare != Some(true))
        {
            return Err(Violation::UnpreparedCommit);
        }
        for (p, record) in tx.participants.iter().enumerate() {
            let locks = s.keys[p].iter().filter(|k| k.lock == Some(t as u8)).count();
            if record.prepare != Some(true) && locks != 0 {
                return Err(Violation::PartialFootprint);
            }
            if record.prepare == Some(true) && !record.released && locks != 2 {
                return Err(Violation::EarlyVisibility);
            }
            if record.applied.is_some_and(|d| Some(d) != tx.decision) {
                return Err(Violation::SplitDecision);
            }
            let expected = u8::from(record.applied == Some(Decision::Commit));
            if s.keys[p][t].version != expected || s.keys[p][t].value != 1 - expected {
                return Err(Violation::DuplicateWrite);
            }
            if record.released && !tx.all {
                return Err(Violation::EarlyVisibility);
            }
        }
    }
    // Both immutable programs read both initial versions, then write different
    // keys. If both commit, each read precedes the other's write: a real cycle.
    if s.tx.iter().all(|t| t.decision == Some(Decision::Commit)) {
        return Err(Violation::ReadDependencyCycle);
    }
    Ok(())
}
