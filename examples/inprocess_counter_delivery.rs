// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Run with `cargo run --example inprocess_counter_delivery --no-default-features`.
//! Ports report observations, not commit certificates. This host explicitly
//! drives three volatile replicas per independent group and the public Counter.
//! A canceled accepted wait has an unknown outcome; recover a failed host with
//! a fresh domain, then retry the immutable operation through Counter dedup.
#[path = "inprocess_counter_delivery/host.rs"]
pub mod host;
use host::*;
use voteboat::{
    embedding::writes::*,
    identity::{GroupId, GroupIdentity, GroupIncarnation, OperationId},
};
fn main() -> Result<(), Failure> {
    let mut fleet = Fleet::new();
    let (engineering, factory) = fleet.add_group(GroupIdentity {
        id: GroupId::new(11).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    })?;
    let (bridge, other_factory) = fleet.add_group(GroupIdentity {
        id: GroupId::new(29).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    })?;
    let mut port = factory
        .open_port(port_limits(7)?)
        .map_err(|e| format!("{e:?}"))?;
    let mut other = other_factory
        .open_port(port_limits(2)?)
        .map_err(|e| format!("{e:?}"))?;
    for (group, port, id, delta) in [
        (engineering, &mut port, u128::MAX, 7i64),
        (bridge, &mut other, 1u128 << 103, -3i64),
    ] {
        let bytes = delta.to_le_bytes();
        let (submission, mut invocation) = port
            .begin(&[WriteInput {
                operation: OperationId::new(id).unwrap(),
                bytes: &bytes,
            }])
            .map_err(|e| format!("{e:?}"))?;
        fleet.submit(group, submission)?;
        fleet.pump()?;
        let result = invocation
            .try_take(0)
            .map_err(|e| format!("{e:?}"))?
            .ok_or("host did not deliver original")?;
        println!(
            "group={group} operation={:?} result={:?}",
            result.operation(),
            result.outcome()
        );
        drop(result);
        invocation.finish().map_err(|e| format!("{e:?}"))?;
        println!("authorized read={}", fleet.read(group)?);
    }
    fleet.close_group(engineering);
    println!("bridge remains usable: {}", fleet.read(bridge)?);
    Ok(())
}
