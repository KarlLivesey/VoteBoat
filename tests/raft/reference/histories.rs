// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn settled(actions: &mut Vec<Action>, leader: u64) {
    actions.extend([
        Action::Pump,
        Action::Heartbeat(leader),
        Action::Pump,
        Action::Check,
    ]);
}
fn generated(seed: u64) -> Trace {
    let mut actions = vec![Action::Campaign(1)];
    settled(&mut actions, 1);
    let mut leader = 1;
    let mut random = seed;
    for round in 0..6u64 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        let operation = u128::from(seed) * 1000 + u128::from(round) * 10 + 1;
        let delta = i64::try_from(random % 19).unwrap() - 9;
        actions.push(Action::Write(leader, operation, delta));
        settled(&mut actions, leader);
        actions.push(Action::Write(leader, operation, delta));
        settled(&mut actions, leader);
        actions.push(Action::Read(leader, round * 10 + 1));
        actions.push(Action::Isolate(leader));
        // A locally accepted old-leader command must not escape its partition.
        actions.push(Action::Write(leader, operation + 1, 97));
        actions.push(Action::Pump);
        actions.push(Action::Read(leader, round * 10 + 2));
        leader = leader % 3 + 1;
        actions.push(Action::Campaign(leader));
        actions.push(Action::Pump);
        actions.push(Action::Write(leader, operation + 2, -3));
        actions.push(Action::Pump);
        actions.push(Action::Read(leader, round * 10 + 3));
        actions.push(Action::Heal);
        settled(&mut actions, leader);
    }
    Trace { seed, actions }
}
#[test]
fn known_majority_reference_matches_original_commands_retries_partition_repair_and_reads() {
    for seed in 1..=32 {
        let trace = generated(seed);
        let decoded = Trace::decode(&trace.encode()).unwrap();
        assert_eq!(decoded, trace);
        let result = replay(&trace).unwrap_or_else(|error| {
            panic!(
                "seed {seed}: {error:?}\n{}",
                String::from_utf8(trace.encode()).unwrap()
            )
        });
        assert_eq!(
            replay(&decoded),
            Ok(result.clone()),
            "seed {seed} exact replay"
        );
        assert_eq!(result.evidence.campaigns, 7);
        assert_eq!(result.evidence.writes, 24);
        assert_eq!(result.evidence.reads, 12);
        assert_eq!(result.evidence.unavailable_reads, 6);
        assert_eq!(result.evidence.partitions, 6);
        assert_eq!(result.evidence.checks, 19);
        for commands in result.commands.values() {
            assert_eq!(commands.len(), 18);
            assert!(commands.iter().all(|(_, delta)| *delta != 97));
        }
    }
}
pub(super) fn reduce(mut trace: Trace, target: &Mismatch) -> Trace {
    let mut cursor = 0;
    while cursor < trace.actions.len() {
        let mut candidate = trace.clone();
        candidate.actions.remove(cursor);
        if replay(&candidate).as_ref().err() == Some(target) {
            trace = candidate;
            cursor = 0;
        } else {
            cursor += 1;
        }
    }
    trace
}
#[test]
fn reference_host_application_corruption_is_detected_and_reduced_after_real_commit() {
    let mut trace = generated(41);
    trace
        .actions
        .extend([Action::CorruptReference(1), Action::Check]);
    let target = Mismatch::Application(1);
    assert_eq!(replay(&trace), Err(target.clone()));
    let reduced = reduce(trace.clone(), &target);
    assert!(reduced.actions.len() < trace.actions.len());
    assert!(reduced
        .actions
        .iter()
        .any(|action| matches!(action, Action::Write(..))));
    assert_eq!(
        replay(&Trace::decode(&reduced.encode()).unwrap()),
        Err(target)
    );
    let mut corrected = reduced.clone();
    corrected
        .actions
        .retain(|action| !matches!(action, Action::CorruptReference(_)));
    assert!(replay(&corrected).is_ok());
    println!(
        "reduced reference-host mismatch:\n{}",
        String::from_utf8(reduced.encode()).unwrap()
    );
}
#[test]
fn differential_trace_rejects_unknown_versions_ids_truncation_and_unbounded_actions() {
    for bytes in [
        b"VBDIF3 1\n".as_slice(),
        b"VBDIF1 0\n",
        b"VBDIF1 1\nwrite 1 0 2\n",
        b"VBDIF1 1\ncampaign 4\n",
        b"VBDIF1 1\nread 1 0\n",
        b"VBDIF1 1\nwrite 1 9",
        b"VBDIF1 1\ncheck extra\n",
    ] {
        assert!(Trace::decode(bytes).is_err());
    }
    assert!(Trace::decode(&vec![b'x'; 65537]).is_err());
    assert!(
        Trace::decode(format!("VBDIF1 1\n{}", "check\n".repeat(MAX_ACTIONS + 1)).as_bytes())
            .is_err()
    );
}
