// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Standalone single-voter, three-group Rust embedding; not a distributed service.
//! Supply original immutable arguments when retrying submit, or use resume by ID.
#[path = "embedded_transfer/host.rs"]
mod host;
use host::*;
use std::{
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{application::StateMachine, native::startup::*, runtime::*, transaction::*};

fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [mode, root, tls, action, id, rest @ ..] = args.as_slice() else {
        return Err("usage: embedded_transfer create|recover DIRECTORY TLS_DIRECTORY submit|resume ID [AMOUNT forward|reverse]".into());
    };
    let mode = match mode.as_str() {
        "create" => NativeOpenMode::Create,
        "recover" => NativeOpenMode::Recover,
        _ => return Err("expected create or recover".into()),
    };
    let id = TransactionId::new(id.parse()?).ok_or("nonzero transaction ID required")?;
    let command = match (action.as_str(), rest) {
        ("resume", []) => None,
        ("submit", [amount, direction]) => Some(spec(id, amount.parse()?, direction)?),
        _ => return Err("submit requires AMOUNT forward|reverse; resume takes only ID".into()),
    };
    let mut node = open(root, Path::new(tls), mode)?;
    let clock = Instant::now();
    let result = run(&mut node, clock, id, command);
    let cleanup = close(node, clock);
    match result {
        Err(first) => {
            if let Err(error) = cleanup {
                eprintln!("cleanup failed: {error}");
            }
            Err(first)
        }
        Ok(()) => cleanup,
    }
}
fn run(
    node: &mut HostNode,
    clock: Instant,
    id: TransactionId,
    command: Option<TransferSpec>,
) -> Result<(), Failure> {
    for group in groups() {
        check(node.control(group, NodeControl::Campaign))?;
    }
    drive(node, clock, |n| {
        Ok(groups().into_iter().all(|g| {
            let c = n.local().owner.core(g).unwrap();
            c.role() == voteboat::raft::Role::Leader
                && !c.has_pending_dependency()
                && n.local().applications[&g].applied_index() >= c.state().commit_index
        }))
    })?;
    transfer(node, clock, id, command)?;
    for (slot, group) in profile().participants().into_iter().enumerate() {
        println!(
            "account{}={:?}",
            slot + 1,
            account_read(node, clock, group, account(slot))?
        );
    }
    Ok(())
}
fn transfer(
    node: &mut HostNode,
    clock: Instant,
    id: TransactionId,
    command: Option<TransferSpec>,
) -> Result<(), Failure> {
    // This example attaches once to this recovered RuntimeOwner. A host that
    // reattaches under one owner must allocate a fresh generation each time.
    let mut driver = check(TransferDriver::new(
        profile(),
        TransferDriverGeneration::new(1).unwrap(),
        check(TransferDriverLimits::new(1))?,
        std::slice::from_mut(node),
    ))?;
    let ticket = match command {
        Some(spec) => check(driver.submit(spec).map_err(|r| r.reason))?,
        None => check(driver.resume(id).map_err(|r| r.reason))?,
    };
    let end = Instant::now() + Duration::from_secs(15);
    let mut result = None;
    while result.is_none() {
        check(driver.advance(now(clock), DriverBudget::default()))?;
        if let Some(output) = driver.poll_completion() {
            if output.ticket() != ticket {
                return Err("unexpected driver invocation".into());
            }
            let outcome = check(driver.complete(output).map_err(|r| r.reason))?;
            println!("transaction={id:?} outcome={outcome:?}");
            result = Some(match outcome {
                DriverOutcome::Finished { .. } => Ok(()),
                DriverOutcome::Unknown { .. } => {
                    Err("unresolved; authoritative files retained, recover and resume ID".into())
                }
            });
        } else if Instant::now() > end {
            check(driver.stop_waiting())?;
            result = Some(Err(
                "observation timed out; files retained, recover and resume ID".into(),
            ));
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    driver.close();
    let end = Instant::now() + Duration::from_secs(15);
    while !driver.is_drained() {
        check(driver.advance(
            now(clock),
            DriverBudget {
                transitions: 0,
                ..DriverBudget::default()
            },
        ))?;
        while let Some(output) = driver.poll_completion() {
            check(driver.complete(output).map_err(|r| r.reason))?;
        }
        if Instant::now() > end {
            return Err("observation cleanup timed out; files retained".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    result.unwrap()
}
fn account_read(
    node: &mut HostNode,
    clock: Instant,
    group: voteboat::identity::GroupIdentity,
    account: AccountId,
) -> Result<AccountRead, Failure> {
    let ticket = check(
        node.read(group, TransferQuery::Account(account))
            .map_err(|r| r.reason),
    )?;
    let mut value = None;
    drive(node, clock, |n| {
        let Some(output) = n.poll_read() else {
            return Ok(false);
        };
        if output.ticket() != ticket {
            return Err("unexpected read invocation".into());
        }
        match check(n.complete_read(output).map_err(|r| r.reason))? {
            ReadOutcome::Read {
                barrier,
                result: Ok(TransferRead::Account(result)),
            } if barrier.group() == group && barrier.request() == ticket.request => {
                value = Some(result)
            }
            other => return Err(format!("account read unresolved: {other:?}").into()),
        }
        Ok(true)
    })?;
    value.ok_or_else(|| "missing account result".into())
}

#[cfg(test)]
#[path = "embedded_transfer/tests.rs"]
mod tests;
