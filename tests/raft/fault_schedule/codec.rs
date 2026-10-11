// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{fmt::Write, str::FromStr};

#[derive(Debug, Eq, PartialEq)]
pub(super) enum DecodeError {
    Bounds,
    Header,
    Action(usize),
}
fn number<T: FromStr>(word: Option<&str>) -> Result<T, ()> {
    word.ok_or(())?.parse().map_err(|_| ())
}
fn actor(word: Option<&str>) -> Result<u64, ()> {
    let id = number(word)?;
    if (1..=3).contains(&id) {
        Ok(id)
    } else {
        Err(())
    }
}
fn operation(word: Option<&str>) -> Result<u128, ()> {
    let operation = number(word)?;
    if operation == 0 {
        Err(())
    } else {
        Ok(operation)
    }
}
fn action(line: &str) -> Result<Action, ()> {
    let mut words = line.split_whitespace();
    let action = match words.next() {
        Some("campaign") => Action::Campaign(actor(words.next())?),
        Some("write") => {
            let id = actor(words.next())?;
            let operation = operation(words.next())?;
            Action::TryWrite(id, operation, number(words.next())?)
        }
        Some("counter") => Action::TryCounter(
            actor(words.next())?,
            operation(words.next())?,
            number(words.next())?,
        ),
        Some("isolate") => Action::Isolate(actor(words.next())?),
        Some("heal") => Action::Heal,
        Some("newest") => Action::DeliverNewest,
        Some("duplicate") => Action::DuplicateOldest,
        Some("power_loss") => Action::PowerLoss(actor(words.next())?),
        Some("heartbeat") => Action::Heartbeat(actor(words.next())?),
        Some("pump") => Action::Pump,
        Some("bad_sync") => Action::BadSync(actor(words.next())?),
        Some("delay") => Action::Delay(actor(words.next())?),
        Some("sync") => Action::Sync(actor(words.next())?),
        Some("complete") => Action::Complete(actor(words.next())?),
        Some("stale") => Action::Stale(actor(words.next())?),
        Some("process_crash") => Action::ProcessCrash(actor(words.next())?),
        Some("fail_sync") => Action::FailSync(actor(words.next())?),
        _ => return Err(()),
    };
    if words.next().is_some() {
        Err(())
    } else {
        Ok(action)
    }
}
impl Trace {
    pub(super) fn encode(&self) -> String {
        assert_ne!(self.seed, 0);
        assert!(self.actions.len() <= MAX_ACTIONS);
        let mut text = format!("VBSIM 1 {}\n", self.seed);
        for action in &self.actions {
            match *action {
                Action::Campaign(id) => writeln!(text, "campaign {id}"),
                Action::TryWrite(id, operation, byte) => {
                    writeln!(text, "write {id} {operation} {byte}")
                }
                Action::TryCounter(id, operation, delta) => {
                    writeln!(text, "counter {id} {operation} {delta}")
                }
                Action::Isolate(id) => writeln!(text, "isolate {id}"),
                Action::Heal => writeln!(text, "heal"),
                Action::DeliverNewest => writeln!(text, "newest"),
                Action::DuplicateOldest => writeln!(text, "duplicate"),
                Action::PowerLoss(id) => writeln!(text, "power_loss {id}"),
                Action::Heartbeat(id) => writeln!(text, "heartbeat {id}"),
                Action::Pump => writeln!(text, "pump"),
                Action::BadSync(id) => writeln!(text, "bad_sync {id}"),
                Action::Delay(id) => writeln!(text, "delay {id}"),
                Action::Sync(id) => writeln!(text, "sync {id}"),
                Action::Complete(id) => writeln!(text, "complete {id}"),
                Action::Stale(id) => writeln!(text, "stale {id}"),
                Action::ProcessCrash(id) => writeln!(text, "process_crash {id}"),
                Action::FailSync(id) => writeln!(text, "fail_sync {id}"),
            }
            .unwrap();
        }
        text
    }
    pub(super) fn decode(text: &str) -> Result<Self, DecodeError> {
        if text.len() > 65536 || !text.is_ascii() {
            return Err(DecodeError::Bounds);
        }
        let mut lines = text.lines();
        let mut header = lines.next().ok_or(DecodeError::Header)?.split_whitespace();
        if header.next() != Some("VBSIM") || header.next() != Some("1") {
            return Err(DecodeError::Header);
        }
        let seed: u64 = number(header.next()).map_err(|()| DecodeError::Header)?;
        if seed == 0 || header.next().is_some() {
            return Err(DecodeError::Header);
        }
        let mut actions = Vec::new();
        for (i, line) in lines.enumerate() {
            if actions.len() == MAX_ACTIONS {
                return Err(DecodeError::Bounds);
            }
            actions.push(action(line).map_err(|()| DecodeError::Action(i + 2))?);
        }
        Ok(Self { seed, actions })
    }
}

#[test]
fn all_actions_round_trip_with_original_ids_and_bytes() {
    let trace = Trace {
        seed: 42,
        actions: vec![
            Action::Campaign(1),
            Action::TryWrite(2, u128::MAX, 255),
            Action::Isolate(3),
            Action::Heal,
            Action::DeliverNewest,
            Action::DuplicateOldest,
            Action::PowerLoss(1),
            Action::Heartbeat(2),
            Action::Pump,
            Action::BadSync(3),
            Action::Delay(1),
            Action::Sync(2),
            Action::Complete(3),
            Action::Stale(1),
            Action::ProcessCrash(2),
            Action::FailSync(3),
            Action::TryCounter(1, u128::MAX, i64::MIN),
        ],
    };
    assert_eq!(Trace::decode(&trace.encode()).unwrap(), trace);
    let empty = Trace {
        seed: 1,
        actions: vec![],
    };
    assert_eq!(Trace::decode(&empty.encode()).unwrap(), empty);
}

#[test]
fn malformed_and_oversized_traces_are_rejected_before_execution() {
    for input in [
        "",
        "VBSIM 2 1",
        "VBSIM 1 0",
        "VBSIM 1 1 extra",
        "VBSIM 1 1\nunknown",
        "VBSIM 1 1\ncampaign 0",
        "VBSIM 1 1\nisolate 4",
        "VBSIM 1 1\npump extra",
        "VBSIM 1 1\nwrite 1 0 7",
        "VBSIM 1 1\nwrite 1 4 256",
        "VBSIM 1 1\nwrite 1 4",
        "VBSIM 1 1\nheal\n\n",
        "VBSIM 1 1\npower_loss -1",
        "VBSIM 1 1\ndelay 0",
        "VBSIM 1 1\nsync 4",
        "VBSIM 1 1\ncomplete 1 extra",
        "VBSIM 1 1\nstale -1",
        "VBSIM 1 1\nprocess_crash 4",
        "VBSIM 1 1\nfail_sync 0",
        "VBSIM 1 1\ncounter 1 0 9",
        "VBSIM 1 1\ncounter 1 1 9223372036854775808",
        "VBSIM 1 1\nλ",
    ] {
        assert!(Trace::decode(input).is_err(), "{input:?}");
    }
    assert_eq!(Trace::decode(&"x".repeat(65537)), Err(DecodeError::Bounds));
    let too_many = format!("VBSIM 1 1\n{}", "pump\n".repeat(MAX_ACTIONS + 1));
    assert_eq!(Trace::decode(&too_many), Err(DecodeError::Bounds));
}
