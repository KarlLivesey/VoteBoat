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
//! Independent bounded distributed activation/election specification. Enumerates
//! durable accepted-prefix placements and possible election certificates, then
//! checks receipt persistence/restart separately. No production quorum or Raft
//! helper is used. Log forks, arbitrary term traces and network liveness are not
//! proved by this finite, factored model; actual-core histories complement it.

#[derive(Clone, Copy, Debug)]
enum Shape {
    Majority,
    Weighted,
    Nested,
}
impl Shape {
    fn satisfied(self, nodes: [usize; 3], mask: u8) -> bool {
        let present = nodes.map(|n| mask & (1 << n) != 0);
        match self {
            Self::Majority => present.into_iter().filter(|b| *b).count() >= 2,
            Self::Weighted => {
                present
                    .into_iter()
                    .zip([1, 1, 3])
                    .filter_map(|(p, w)| p.then_some(w))
                    .sum::<u8>()
                    > 2
            }
            // Majority(Voter(a), Majority(Voter(b), Voter(c))). Both
            // top-level children are needed; the inner two-leaf majority
            // likewise needs both. This deliberately differs from flat counts.
            Self::Nested => present[0] && present[1] && present[2],
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Topology {
    nodes: usize,
    old: [usize; 3],
    new: [usize; 3],
}
const OVERLAP: Topology = Topology {
    nodes: 4,
    old: [0, 1, 2],
    new: [1, 2, 3],
};
const DISJOINT: Topology = Topology {
    nodes: 6,
    old: [0, 1, 2],
    new: [3, 4, 5],
};
#[derive(Clone, Copy, Debug, PartialEq)]
enum Variant {
    Correct,
    JointUsesNewOnly,
    FinalBeforeDurableJoint,
    EqualRequestScope,
}
#[derive(Clone, Debug)]
struct Case {
    topology: Topology,
    old: Shape,
    new: Shape,
    // 0 = committed learner-assignment view; 1 = accepted joint;
    // 2 = accepted final. Every head here is already durable. Final implies
    // the joint was committed; activation uses accepted, not applied, heads.
    heads: Vec<u8>,
}
impl Case {
    fn old_voter(&self, n: usize) -> bool {
        self.topology.old.contains(&n)
    }
    fn new_voter(&self, n: usize) -> bool {
        self.topology.new.contains(&n)
    }
    fn voter(&self, head: u8, n: usize) -> bool {
        match head {
            0 => self.old_voter(n),
            1 => self.old_voter(n) || self.new_voter(n),
            2 => self.new_voter(n),
            _ => unreachable!(),
        }
    }
    fn quorum(&self, head: u8, mask: u8, variant: Variant) -> bool {
        let old = self.old.satisfied(self.topology.old, mask);
        let new = self.new.satisfied(self.topology.new, mask);
        match head {
            0 => old,
            1 if variant == Variant::JointUsesNewOnly => new,
            1 => old && new,
            2 => new,
            _ => unreachable!(),
        }
    }
    fn admitted(&self, variant: Variant) -> bool {
        if !self.heads.contains(&2) || variant == Variant::FinalBeforeDurableJoint {
            return true;
        }
        let durable_joint = self
            .heads
            .iter()
            .enumerate()
            .fold(0, |m, (n, &h)| m | if h >= 1 { 1 << n } else { 0 });
        self.quorum(1, durable_joint, Variant::Correct)
    }
    fn grants(&self, candidate: usize, alive: u8, variant: Variant) -> u8 {
        let head = self.heads[candidate];
        if !self.voter(head, candidate) || alive & (1 << candidate) == 0 {
            return 0;
        }
        self.heads
            .iter()
            .enumerate()
            .fold(0, |mask, (voter, &local)| {
                let eligible = alive & (1 << voter) != 0
                && self.voter(local, voter) && self.voter(local, candidate)
                // Entries are from one fixed term/prefix in this bounded model.
                && head >= local
                && (variant != Variant::EqualRequestScope || head == local);
                mask | if eligible { 1 << voter } else { 0 }
            })
    }
    fn certificates(&self, candidate: usize, variant: Variant) -> Vec<u8> {
        let available = (1 << self.topology.nodes) - 1;
        let grants = self.grants(candidate, available, variant);
        if grants & (1 << candidate) == 0 {
            return Vec::new();
        }
        (1..=available)
            .filter(|mask| {
                mask & !grants == 0
                    && mask & (1 << candidate) != 0
                    && self.quorum(self.heads[candidate], *mask, variant)
            })
            .collect()
    }
}
#[derive(Debug, Default)]
struct Coverage {
    placements: usize,
    certificates: usize,
    pairs: usize,
}
fn check(variant: Variant) -> Result<Coverage, String> {
    let mut coverage = Coverage::default();
    for topology in [OVERLAP, DISJOINT] {
        for old in [Shape::Majority, Shape::Weighted, Shape::Nested] {
            for new in [Shape::Majority, Shape::Weighted, Shape::Nested] {
                for encoded in 0..3usize.pow(topology.nodes as u32) {
                    let mut value = encoded;
                    let heads = (0..topology.nodes)
                        .map(|_| {
                            let h = (value % 3) as u8;
                            value /= 3;
                            h
                        })
                        .collect();
                    let case = Case {
                        topology,
                        old,
                        new,
                        heads,
                    };
                    if !case.admitted(variant) {
                        continue;
                    }
                    coverage.placements += 1;
                    let candidates = (0..topology.nodes)
                        .map(|n| case.certificates(n, variant))
                        .collect::<Vec<_>>();
                    coverage.certificates += candidates.iter().map(Vec::len).sum::<usize>();
                    for a in 0..topology.nodes {
                        for b in a + 1..topology.nodes {
                            for &qa in &candidates[a] {
                                for &qb in &candidates[b] {
                                    coverage.pairs += 1;
                                    if qa & qb == 0 {
                                        return Err(format!("disjoint same-term elections: {case:?}, candidates {a}/{b}, certificates {qa:06b}/{qb:06b}"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(coverage)
}
#[test]
fn distributed_accepted_activation_preserves_election_certificate_intersection() {
    let coverage = check(Variant::Correct).unwrap();
    assert!(
        coverage.placements > 2000 && coverage.certificates > 10000 && coverage.pairs > 10000,
        "{coverage:?}"
    );
    eprintln!("activation coverage: {coverage:?}");
    for mutant in [Variant::JointUsesNewOnly, Variant::FinalBeforeDurableJoint] {
        assert!(
            check(mutant).is_err(),
            "missing activation counterexample for {mutant:?}"
        );
    }
}
#[test]
fn final_partial_delivery_requires_request_scope_independent_ballots() {
    let case = Case {
        topology: OVERLAP,
        old: Shape::Majority,
        new: Shape::Majority,
        heads: vec![1, 2, 1, 1],
    };
    assert!(case.admitted(Variant::Correct));
    let alive = 0b1110; // retiring leader 0 has failed
    assert!(case.quorum(1, alive, Variant::Correct));
    let can_elect = |variant| {
        (0..case.topology.nodes).any(|candidate| {
            let grants = case.grants(candidate, alive, variant);
            grants & (1 << candidate) != 0 && case.quorum(case.heads[candidate], grants, variant)
        })
    };
    assert!(
        !can_elect(Variant::EqualRequestScope),
        "mutant must reproduce the production stall"
    );
    assert!(can_elect(Variant::Correct));
}

#[derive(Clone, Copy, Debug, Default)]
struct Ballot {
    durable: Option<u8>,
    pending: Option<(u8, bool, bool, u8)>, // candidate, written, synchronized, session
    session: u8,
}
impl Ballot {
    fn begin(&mut self, candidate: u8) -> bool {
        if self.pending.is_some() || self.durable.is_some_and(|c| c != candidate) {
            return false;
        }
        self.pending = Some((candidate, false, false, self.session));
        true
    }
    fn write(&mut self, reply_at_write: bool) -> Option<(u8, u8)> {
        let (candidate, written, _, session) = self.pending.as_mut().unwrap();
        *written = true;
        reply_at_write.then_some((*candidate, *session))
    }
    fn synchronize(&mut self) {
        let (candidate, written, synchronized, _) = self.pending.as_mut().unwrap();
        assert!(*written);
        self.durable = Some(*candidate);
        *synchronized = true;
    }
    fn complete(&mut self, session: u8) -> Option<(u8, u8)> {
        let (candidate, _, synchronized, original) = self.pending?;
        if !synchronized || original != session || original != self.session {
            return None;
        }
        self.pending = None;
        Some((candidate, original))
    }
    fn restart(&mut self) {
        self.pending = None;
        self.session += 1;
    }
}
#[test]
fn election_receipts_need_exact_durable_promises_across_restart() {
    for reply_at_write in [false, true] {
        let mut voters = [Ballot::default(); 3];
        let mut first = 0u8;
        // The two quorum certificates share voter 1. Lose unsynchronized writes
        // after an early reply, then try a different candidate in the same term.
        for voter in [0, 1] {
            assert!(voters[voter].begin(0));
            if voters[voter].write(reply_at_write).is_some() {
                first |= 1 << voter;
            }
        }
        for voter in &mut voters {
            voter.restart();
        }
        let mut second = 0u8;
        for voter in [1, 2] {
            if voters[voter].begin(1) {
                voters[voter].write(false);
                voters[voter].synchronize();
                assert!(voters[voter].complete(0).is_none()); // prior session cannot release it
                assert!(voters[voter].complete(1).is_some());
                second |= 1 << voter;
            }
        }
        let double = first.count_ones() >= 2 && second.count_ones() >= 2;
        assert_eq!(
            double, reply_at_write,
            "Written mutation must have a counterexample"
        );
    }
    let mut promise = Ballot::default();
    assert!(promise.begin(0));
    promise.write(false);
    promise.synchronize();
    promise.restart();
    assert!(
        !promise.begin(1),
        "lost completion cannot erase a synchronized ballot"
    );
    assert!(promise.begin(0));
    promise.write(false);
    promise.synchronize();
    assert!(promise.complete(0).is_none());
    assert!(promise.complete(1).is_some());
}
