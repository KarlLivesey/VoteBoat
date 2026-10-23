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
//! Retirement (LV2, architect plan §2.4 and §5): the session watermark, the
//! `Expired` refusal, pruning, the tri-state lookup and the admission credit.
//! Commands follow the stream a bridge produces: a claim, then sequences
//! `1..` of the claimed session, each acknowledging the previous one.
use super::*;
use crate::raft::{Message, RequestContext, Rpc};
use std::collections::BTreeSet;

const EXPIRED: LogicalOutcome = LogicalOutcome::Refused(LogicalRefusal::Expired);

/// The command stream of one bridge in one logical session.
struct Stream {
    epoch: u64,
    next: u64,
}
impl Stream {
    fn new(epoch: u64) -> Self {
        Self { epoch, next: 1 }
    }
    /// The next sequence, acknowledging the previous one as a bridge does.
    fn next(&mut self, body: CommandBody) -> Command {
        let sequence = self.next;
        self.next += 1;
        acknowledging(u128::from(sequence), self.epoch, sequence - 1, body)
    }
}
fn session(epoch: u64, sequence: u64) -> OperationId {
    OperationId::new(command::session_operation(epoch, sequence)).unwrap()
}
fn claim_at(id: u128, epoch: u64) -> Command {
    command(
        id,
        epoch,
        CommandBody::Claim {
            expected_epoch: RepresentativeEpoch::new(epoch - 1),
        },
    )
}
/// The runtime visit of the representative's logical session.
fn visit_for(a: &LogicalApplication) -> VisitTicket {
    let r = a.state.representative.unwrap();
    VisitTicket {
        owner: RuntimeOwner {
            store: a.binding(r),
            ..owner()
        },
        ..visit()
    }
}
/// One parent heartbeat send (`Append` from the leader) or response
/// (`Appended` from a follower), captured against the current image.
fn heartbeat(a: &LogicalApplication, n: u64, leader: bool) -> CommandBody {
    let p = a.state.parent.as_ref().unwrap();
    let sender = a.binding(a.state.representative.unwrap());
    let rpc = if leader {
        Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: Vec::new().into(),
            leader_commit: 0,
        }
    } else {
        Rpc::Appended {
            success: true,
            matching_index: 0,
        }
    };
    CommandBody::Authorize(LogicalCapsule {
        expected_revision: p.revision,
        expected_generation: p.generation,
        local_configuration: p.membership().unwrap().id(),
        origin: LogicalCapsuleOrigin::Protocol(EffectTicket {
            visit: visit_for(a),
            sequence: n,
        }),
        body: LogicalCapsuleBody::Protocol(Box::new(Message {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            from: NodeId::new(1).unwrap(),
            sender,
            to: NodeId::new(3).unwrap(),
            term: p.hard_state.term,
            context: RequestContext {
                origin: sender,
                sequence: n,
            },
            rpc,
        })),
    })
}
/// A parent mutation at term 1 with no suffix: the image keeps its size.
fn persist(a: &LogicalApplication) -> CommandBody {
    let r = a.state.representative.unwrap();
    let mut o = origin();
    o.request.binding.store = a.binding(r);
    o.visit = visit_for(a);
    CommandBody::Mutate {
        expected_generation: a.state.parent.as_ref().unwrap().generation,
        update: update(a, 0, None),
        origin: o,
    }
}
fn batch(a: &mut LogicalApplication, commands: &[Command]) -> Vec<LogicalReceipt> {
    let first = a.applied_index() + 1;
    let entries: Vec<_> = commands
        .iter()
        .zip(first..)
        .map(|(c, index)| entry(a, c, index))
        .collect();
    a.apply_batch(&entries).unwrap()
}
/// A claimed child whose parent is at term 1 (one acknowledged mutation),
/// so heartbeats carry a real term. Sequence 1 is used.
fn claimed() -> (LogicalApplication, Stream) {
    let mut a = app();
    claim(&mut a);
    let mut s = Stream::new(1);
    let c = s.next(persist(&a));
    assert_eq!(apply(&mut a, &c).outcome, LogicalOutcome::Mutated);
    (a, s)
}

/// Architect §5.1: 30 minutes of an idle leader child at the default 50 ms
/// heartbeat (two followers: 40 authorised sends per second, 72,000
/// actions) with a parent mutation every 1,000 actions, then a follower
/// child's 36,000 heartbeat responses. Each child retains one original after
/// every apply, at constant bytes and checkpoint size, and every 5,000
/// actions it restores from its checkpoint and continues from the copy. With
/// pruning removed (the behaviour before LV2) the leader stream fails with
/// `DedupCapacity` at the default `max_actions` of 4,096.
#[test]
fn idle_heartbeat_streams_retain_one_original() {
    for (leader, actions) in [(true, 72_000u64), (false, 36_000)] {
        let (mut a, mut s) = claimed();
        a.journal = None;
        let mut sizes = BTreeSet::new();
        let mut done = 0;
        while done < actions {
            // A leader mutation every 1,000 actions; heartbeats in batches of
            // 64 as a child applies them, singly for the first 64 so retention
            // is observed after every apply there too.
            let commands: Vec<Command> = if leader && done % 1_000 == 999 {
                vec![s.next(persist(&a))]
            } else {
                let mut n = if done < 64 { 1 } else { 64 }.min(actions - done);
                if leader {
                    n = n.min(999 - done % 1_000);
                }
                (0..n)
                    .map(|k| s.next(heartbeat(&a, done + k + 1, leader)))
                    .collect()
            };
            for r in batch(&mut a, &commands) {
                assert!(!r.duplicate);
                assert!(
                    matches!(
                        r.outcome,
                        LogicalOutcome::CapsuleAuthorized | LogicalOutcome::Mutated
                    ),
                    "{:?}",
                    r.outcome
                );
            }
            done += commands.len() as u64;
            let last = commands.last().unwrap();
            assert_eq!(a.state.records.len(), 1, "after {done} actions");
            assert!(a.state.records.contains_key(&last.operation));
            assert_eq!(
                a.state.bytes,
                a.state.records[&last.operation].command.len()
            );
            assert_eq!(a.state.retired_below, last.operation.get());
            if done > 64 {
                sizes.insert((a.state.bytes, a.checkpoint(usize::MAX).unwrap().len()));
            }
            if done % 5_000 < commands.len() as u64 {
                let bytes = a.checkpoint(usize::MAX).unwrap();
                let mut b = app();
                b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, a.applied_index(), &bytes)
                    .unwrap();
                assert_eq!(b.state, a.state);
                assert_eq!(b.checkpoint(usize::MAX).unwrap(), bytes);
                a = b;
            }
        }
        // One size per retained command shape (heartbeat or mutation).
        assert!(sizes.len() <= 2, "retained sizes {sizes:?}");
        assert_eq!(a.metadata().retained_actions, 1);
        assert_eq!(a.metadata().retired_below, a.state.retired_below);
        eprintln!(
            "LV2 soak leader={leader} actions={done} applied={} retained=1 sizes={sizes:?}",
            a.applied_index()
        );
    }
}

/// E1, E2, E3: a lost receipt resolves to the original receipt before the
/// next sequence; a late copy after it is `Expired`, changes nothing, and is
/// never executed, although the same capsule would authorise again under a
/// fresh ID (the idle image does not change).
#[test]
fn retry_before_acknowledgement_is_original_and_after_is_expired() {
    let (mut a, mut s) = claimed();
    let n = s.next(heartbeat(&a, 1, true));
    let original = apply(&mut a, &n);
    assert_eq!(original.outcome, LogicalOutcome::CapsuleAuthorized);
    // E1: lookup and retry before n + 1.
    let LogicalRead::Outcome(LogicalOutcomeLookup::Retained(status)) = a
        .read_at(a.applied_index(), LogicalQuery::Outcome(n.operation))
        .unwrap()
    else {
        panic!("unacknowledged original is retained");
    };
    assert_eq!(status.receipt, original);
    let retry = apply(&mut a, &n);
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, original.outcome);
    assert_eq!(retry.first_index, original.first_index);
    // E2: n + 1 acknowledges n, which is retired.
    let next = s.next(heartbeat(&a, 2, true));
    let second = apply(&mut a, &next);
    assert_eq!(second.outcome, LogicalOutcome::CapsuleAuthorized);
    assert_eq!(a.state.retired_below, next.operation.get());
    assert!(!a.state.records.contains_key(&n.operation));
    let before = a.state.clone();
    let late = apply(&mut a, &n);
    assert_eq!(late.outcome, EXPIRED);
    assert!(late.duplicate);
    assert_eq!(late.index, a.applied_index());
    assert_eq!(late.epoch, before.representative.map(|r| r.epoch));
    assert_eq!(
        a.state,
        State {
            applied: before.applied + 1,
            ..before.clone()
        }
    );
    let copy = apply(&mut a, &next);
    assert!(copy.duplicate);
    assert_eq!(copy.outcome, second.outcome);
    assert_eq!(copy.first_index, second.first_index);
    // E3: the retired capsule is still authorisable content.
    let CommandBody::Authorize(capsule) = &n.body else {
        unreachable!()
    };
    let fresh = s.next(CommandBody::Authorize(capsule.clone()));
    assert_eq!(
        apply(&mut a, &fresh).outcome,
        LogicalOutcome::CapsuleAuthorized
    );
}

/// E4: retained → `Retained`; below the watermark → `Expired`; at or above
/// and unseen → `Unknown`; a pruned claim → `Unknown`, and re-applying it
/// refuses and never claims.
#[test]
fn outcome_lookup_is_tri_state() {
    let (mut a, mut s) = claimed();
    let first = session(1, 1);
    let c = s.next(heartbeat(&a, 1, true));
    apply(&mut a, &c);
    let lookup = |a: &LogicalApplication, op: OperationId| {
        let LogicalRead::Outcome(lookup) = a
            .read_at(a.applied_index(), LogicalQuery::Outcome(op))
            .unwrap()
        else {
            unreachable!()
        };
        lookup
    };
    assert!(matches!(
        lookup(&a, c.operation),
        LogicalOutcomeLookup::Retained(s) if s.operation == c.operation
    ));
    assert_eq!(lookup(&a, first), LogicalOutcomeLookup::Expired);
    assert_eq!(lookup(&a, session(1, 3)), LogicalOutcomeLookup::Unknown);
    assert_eq!(lookup(&a, session(2, 1)), LogicalOutcomeLookup::Unknown);
    // The claim (op 1) was pruned by the first session command.
    let claim_id = OperationId::new(1).unwrap();
    assert_eq!(lookup(&a, claim_id), LogicalOutcomeLookup::Unknown);
    let reapplied = apply(
        &mut a,
        &command(
            1,
            1,
            CommandBody::Claim {
                expected_epoch: None,
            },
        ),
    );
    assert!(!reapplied.duplicate);
    assert_eq!(
        reapplied.outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    assert_eq!(a.state.representative, Some(representative(1)));
    // The response codec gives the three answers distinct encodings (exact
    // round trips are in the response tests).
    let bound = a
        .read_encoding_bound(&LogicalQuery::Outcome(first))
        .unwrap();
    let encodings: BTreeSet<Vec<u8>> = [
        lookup(&a, c.operation),
        LogicalOutcomeLookup::Expired,
        LogicalOutcomeLookup::Unknown,
    ]
    .into_iter()
    .map(|value| {
        let mut out = vec![0; bound];
        let used = a
            .encode_read(&LogicalRead::Outcome(value), &mut out)
            .unwrap();
        assert_eq!(&out[..8], b"VBLOGR03");
        out.truncate(used);
        out
    })
    .collect();
    assert_eq!(encodings.len(), 3);
}

/// E5: commands outside the partition are `InvalidCommand` at decode,
/// admission and apply, and a refused batch installs nothing.
#[test]
fn partition_violations_are_invalid_everywhere() {
    let (a, _) = claimed();
    let mutate_at = |op: u128, consumed_through: u64| Command {
        operation: OperationId::new(op).unwrap(),
        consumed_through,
        ..acknowledging(1, 1, 0, persist(&a))
    };
    let claim_with = |op: u128, consumed_through: u64| Command {
        operation: OperationId::new(op).unwrap(),
        consumed_through,
        ..claim_at(1, 2)
    };
    let cases = [
        (
            "session bits differ",
            mutate_at(command::session_operation(2, 5), 0),
        ),
        ("claim-space session body", mutate_at(5, 0)),
        ("sequence 0", mutate_at(command::session_operation(1, 0), 0)),
        (
            "acknowledges itself",
            mutate_at(command::session_operation(1, 5), 5),
        ),
        (
            "acknowledges the future",
            mutate_at(command::session_operation(1, 5), 9),
        ),
        (
            "claim in session space",
            claim_with(command::SESSION_SPACE, 0),
        ),
        ("claim acknowledging", claim_with(7, 1)),
    ];
    for (name, c) in cases {
        let bytes = a.command_bytes(&c, usize::MAX).unwrap();
        assert_eq!(
            a.decode_command(&bytes),
            Err(ApplicationError::InvalidCommand),
            "{name}"
        );
        assert_eq!(
            a.encode_command(&c, usize::MAX),
            Err(ApplicationError::InvalidCommand),
            "{name}"
        );
        assert_eq!(
            a.validate_proposal(c.operation, &bytes, std::iter::empty()),
            Err(ApplicationError::InvalidCommand),
            "{name}"
        );
        let mut b = a.clone();
        let e = LogEntry {
            index: b.applied_index() + 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: c.operation,
                bytes: bytes.into(),
            },
        };
        assert_eq!(
            b.apply_batch(&[e]),
            Err(ApplicationError::InvalidCommand),
            "{name}"
        );
        assert_eq!(b.state, a.state, "{name}");
    }
}

/// E6: the watermark is a maximum moved only by the current, eligible
/// representative: a lower acknowledgement, another binding in the session,
/// a future session and an ineligible representative never move it.
#[test]
fn only_the_current_eligible_representative_moves_the_watermark() {
    let (mut a, mut s) = claimed();
    for n in 1..=4 {
        {
            let next = s.next(heartbeat(&a, n, true));
            apply(&mut a, &next)
        };
    }
    // Sequence 5 acknowledged 4.
    let mark = session(1, 5).get();
    assert_eq!(a.state.retired_below, mark);
    // A lower acknowledgement (sequence 6 acknowledging only 2).
    let low = acknowledging(6, 1, 2, heartbeat(&a, 6, true));
    assert_eq!(
        apply(&mut a, &low).outcome,
        LogicalOutcome::CapsuleAuthorized
    );
    assert_eq!(a.state.retired_below, mark);
    // Another binding in the current session.
    let mut other = acknowledging(9, 1, 8, heartbeat(&a, 9, true));
    other.representative.physical_session = StoreSession::new(9).unwrap();
    assert_eq!(
        apply(&mut a, &other).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    assert_eq!(a.state.retired_below, mark);
    // A future session.
    let future = acknowledging(9, 2, 8, persist(&a));
    assert_eq!(
        apply(&mut a, &future).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    assert_eq!(a.state.retired_below, mark);
    // An ineligible current representative: refused and recorded, nothing
    // retired (the earlier records and the watermark are unchanged).
    super::eligibility::remove_representative(&mut a);
    let records = a.state.records.len();
    let ineligible = acknowledging(10, 1, 9, heartbeat(&a, 10, true));
    assert_eq!(
        apply(&mut a, &ineligible).outcome,
        LogicalOutcome::Refused(LogicalRefusal::Ineligible)
    );
    assert_eq!(a.state.retired_below, mark);
    assert_eq!(a.state.records.len(), records + 1);
}

/// E7 and the admission credit: a retired ID is admitted without any
/// reservation even when the table is full, and commits `Expired`; a live
/// command is credited only with its own planned prune, never with pending
/// entries' plans.
#[test]
fn admission_admits_retired_ids_and_credits_only_its_own_prune() {
    let limits = LogicalLimits {
        max_actions: 4,
        control_actions: 1,
        ..LogicalLimits::default()
    };
    let mut a = app_with_limits(limits);
    claim(&mut a);
    let mut s = Stream::new(1);
    let first = s.next(persist(&a));
    apply(&mut a, &first);
    // Fill the table: three claims refused at the current epoch stay live,
    // plus sequence 1.
    for id in 2..5 {
        let c = command(
            id,
            3,
            CommandBody::Claim {
                expected_epoch: Some(representative(1).epoch),
            },
        );
        assert_eq!(
            apply(&mut a, &c).outcome,
            LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
        );
    }
    assert_eq!(a.state.records.len(), 4);
    // Without acknowledgement there is no room.
    let unacknowledged = acknowledging(2, 1, 0, heartbeat(&a, 2, true));
    let bytes = a.encode_command(&unacknowledged, usize::MAX).unwrap();
    assert_eq!(
        a.validate_proposal(unacknowledged.operation, &bytes, std::iter::empty()),
        Err(ApplicationError::DedupCapacity)
    );
    // Acknowledging sequence 1 frees its slot, at admission and at apply.
    let second = s.next(heartbeat(&a, 2, true));
    let bytes = a.encode_command(&second, usize::MAX).unwrap();
    assert!(a
        .validate_proposal(second.operation, &bytes, std::iter::empty())
        .is_ok());
    // A pending copy of sequence 2 earns no credit for sequence 3.
    let third = s.next(heartbeat(&a, 3, true));
    let third_bytes = a.encode_command(&third, usize::MAX).unwrap();
    assert_eq!(
        a.validate_proposal(
            third.operation,
            &third_bytes,
            std::iter::once((second.operation, bytes.as_slice()))
        ),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(
        apply(&mut a, &second).outcome,
        LogicalOutcome::CapsuleAuthorized
    );
    assert_eq!(a.state.records.len(), 4);
    // E7: the retired sequence 1 is admitted with the table full, also when
    // copies of it are pending, and commits `Expired` without a record.
    let retired = a.encode_command(&first, usize::MAX).unwrap();
    assert!(a
        .validate_proposal(
            first.operation,
            &retired,
            std::iter::repeat_n((first.operation, retired.as_slice()), 8)
        )
        .is_ok());
    // Sequence 3 is admitted only through its own prune credit, so live plus
    // pending now exceeds the table; the retired ID still needs no room.
    assert!(a
        .validate_proposal(third.operation, &third_bytes, std::iter::empty())
        .is_ok());
    assert!(a
        .validate_proposal(
            first.operation,
            &retired,
            std::iter::once((third.operation, third_bytes.as_slice()))
        )
        .is_ok());
    let r = apply(&mut a, &first);
    assert_eq!(r.outcome, EXPIRED);
    assert!(r.duplicate);
    assert_eq!(a.state.records.len(), 4);
}

/// P1, P2: a claim closes every older session. The old session's applied
/// but unacknowledged action becomes `Expired` (lookup and re-proposal), and
/// any later old-session command is `Expired` immediately.
#[test]
fn replacement_retires_the_older_session() {
    let (mut a, mut s) = claimed();
    let unconsumed = s.next(heartbeat(&a, 1, true));
    assert_eq!(
        apply(&mut a, &unconsumed).outcome,
        LogicalOutcome::CapsuleAuthorized
    );
    // The superseded bridge's next command, built under its own binding.
    let later = s.next(heartbeat(&a, 2, true));
    let replace = claim_at(7, 2);
    assert_eq!(apply(&mut a, &replace).outcome, LogicalOutcome::Claimed);
    assert_eq!(a.state.retired_below, session(2, 1).get());
    assert!(a
        .state
        .records
        .keys()
        .all(|op| op.get() < command::SESSION_SPACE));
    assert_eq!(
        a.lookup(unconsumed.operation),
        LogicalOutcomeLookup::Expired
    );
    let r = apply(&mut a, &unconsumed);
    assert_eq!(r.outcome, EXPIRED);
    assert!(r.duplicate);
    assert_eq!(apply(&mut a, &later).outcome, EXPIRED);
    assert!(!a.state.records.contains_key(&later.operation));
}

/// P3: the new claim stays resolvable until its session's first command,
/// which prunes it; a re-applied copy then refuses and never claims again.
/// P4: a claim refused `Ineligible` at the current epoch stays retained
/// across session commands, until the epoch advances.
#[test]
fn claim_records_live_until_consumed_or_dead() {
    let (mut a, mut s) = claimed();
    // P4: ineligible claim at the current epoch (its store is no voter).
    let mut ineligible = claim_at(20, 2);
    ineligible.representative.physical.node = NodeId::new(9).unwrap();
    let refused = apply(&mut a, &ineligible);
    assert_eq!(
        refused.outcome,
        LogicalOutcome::Refused(LogicalRefusal::Ineligible)
    );
    for n in 1..=3 {
        {
            let next = s.next(heartbeat(&a, n, true));
            apply(&mut a, &next)
        };
    }
    let retry = apply(&mut a, &ineligible);
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, refused.outcome);
    // P3: a successful replacement claim is retained, with its receipt.
    let replace = claim_at(21, 2);
    let claimed = apply(&mut a, &replace);
    assert_eq!(claimed.outcome, LogicalOutcome::Claimed);
    assert!(matches!(
        a.lookup(replace.operation),
        LogicalOutcomeLookup::Retained(s) if s.receipt == claimed
    ));
    assert!(a.state.records.contains_key(&ineligible.operation));
    let resolved = apply(&mut a, &replace);
    assert!(resolved.duplicate);
    assert_eq!(resolved.outcome, LogicalOutcome::Claimed);
    // The new session's first command proves the claim consumed: both claims
    // expected epoch 1 and are now dead, so both are pruned.
    let mut s2 = Stream::new(2);
    assert_eq!(
        {
            let next = s2.next(heartbeat(&a, 1, true));
            apply(&mut a, &next)
        }
        .outcome,
        LogicalOutcome::CapsuleAuthorized
    );
    assert_eq!(a.lookup(replace.operation), LogicalOutcomeLookup::Unknown);
    assert_eq!(
        a.lookup(ineligible.operation),
        LogicalOutcomeLookup::Unknown
    );
    assert_eq!(a.state.records.len(), 1);
    let again = apply(&mut a, &replace);
    assert!(!again.duplicate);
    assert_eq!(
        again.outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    assert_eq!(a.state.representative, Some(representative(2)));
}

/// A representative history with every retirement transition: claim,
/// acknowledged session commands, a lost-receipt retry, a late retired copy,
/// a refused claim at the current epoch, a replacement claim, a superseded
/// command and the new session's commands. Returns the entries in order.
fn history() -> (LogicalApplication, Vec<LogEntry>) {
    let mut a = app();
    let mut entries = Vec::new();
    let mut push = |a: &mut LogicalApplication, c: &Command| {
        let e = entry(a, c, a.applied_index() + 1);
        a.apply_batch(std::slice::from_ref(&e)).unwrap();
        entries.push(e);
    };
    push(&mut a, &claim_at(1, 1));
    let mut s = Stream::new(1);
    let first = s.next(persist(&a));
    push(&mut a, &first);
    let mut sent = Vec::new();
    for n in 1..=4 {
        let c = s.next(heartbeat(&a, n, true));
        push(&mut a, &c);
        sent.push(c);
    }
    push(&mut a, &sent[3]);
    push(&mut a, &sent[0]);
    push(&mut a, &first);
    let stale = claim_at(2, 3);
    push(&mut a, &stale);
    {
        let next = s.next(persist(&a));
        push(&mut a, &next)
    };
    push(&mut a, &claim_at(3, 2));
    push(&mut a, &sent[3]);
    let mut s2 = Stream::new(2);
    for n in 1..=3 {
        {
            let next = s2.next(heartbeat(&a, n, true));
            push(&mut a, &next)
        };
    }
    {
        let next = s.next(persist(&a));
        push(&mut a, &next)
    };
    (a, entries)
}

/// R1, R2, R3: a checkpoint taken between any two applies restores to the
/// exact state, and replaying the remaining entries reaches the exact
/// uninterrupted state, receipts included (so replicas checkpointing at
/// different indexes converge). Late retired copies stay `Expired` across the
/// restore: the watermark survives restart.
#[test]
fn checkpoint_between_every_apply_restores_and_replays_exactly() {
    let (expected, entries) = history();
    assert_eq!(expected.state.retired_below, session(2, 3).get());
    let mut live = app();
    let mut checkpoints = vec![live.checkpoint(usize::MAX).unwrap()];
    let mut receipts = Vec::new();
    for e in &entries {
        receipts.extend(live.apply_batch(std::slice::from_ref(e)).unwrap());
        checkpoints.push(live.checkpoint(usize::MAX).unwrap());
    }
    assert_eq!(live.state, expected.state);
    assert!(receipts.iter().any(|r| r.outcome == EXPIRED));
    for (k, bytes) in checkpoints.iter().enumerate() {
        let mut b = app();
        b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, k as u64, bytes)
            .unwrap();
        let mut replayed = Vec::new();
        for e in &entries[k..] {
            replayed.extend(b.apply_batch(std::slice::from_ref(e)).unwrap());
        }
        assert_eq!(b.state, expected.state, "checkpoint at {k}");
        assert_eq!(replayed, receipts[k..], "checkpoint at {k}");
        assert_eq!(
            b.checkpoint(usize::MAX).unwrap(),
            checkpoints[entries.len()],
            "checkpoint at {k}"
        );
    }
    // R2: after a restore, a copy of a retired ID is still `Expired`.
    let mut b = app();
    b.restore_checkpoint(
        LOGICAL_APPLICATION_SCHEMA,
        entries.len() as u64,
        &checkpoints[entries.len()],
    )
    .unwrap();
    let late = LogEntry {
        index: b.applied_index() + 1,
        ..entries[3].clone()
    };
    let r = b.apply_batch(&[late]).unwrap();
    assert_eq!(r[0].outcome, EXPIRED);
}
