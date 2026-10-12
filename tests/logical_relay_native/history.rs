// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The selected relay cell drives original logical persistence and read permits.
use super::*;
use std::time::{Duration, Instant};
use voteboat::{
    application::*,
    identity::*,
    native::{connect::NativePeerProtocol, startup::NativeOpenMode},
    raft::Role,
    runtime::*,
};
#[path = "history/controls.rs"]
mod controls;
#[test]
fn tcp_actual_relay_physical_cell_logical_facade_checkpoint_wal_recovery() {
    run(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_actual_relay_physical_cell_logical_facade_checkpoint_wal_recovery() {
    run(NativePeerProtocol::Quic);
}
fn run(protocol: NativePeerProtocol) {
    let clock = Instant::now();
    let root = std::env::temp_dir().join(format!(
        "voteboat-logical-relay371-{}-{:?}",
        std::process::id(),
        protocol
    ));
    let mut cohort = cluster::Cluster::open(root, protocol, NativeOpenMode::Create, clock);
    cohort.elect();
    controls::source_and_original_controls(&mut cohort);
    let mut bridge = Bridge::new(&mut cohort.boats, setup::mapping(), support::node(1)).unwrap();
    let image = claim(&mut cohort.helper, &mut bridge, clock, 371001);
    let old = image.binding();
    let mut facade = build(&image, clock);
    elect(&mut cohort.helper, &mut bridge, &mut facade, clock);
    write(
        &mut cohort.helper,
        &mut bridge,
        &mut facade,
        clock,
        371010,
        7,
        7,
    );
    facade.checkpoint().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while facade
        .local()
        .owner
        .core(setup::mapping().parent)
        .unwrap()
        .state()
        .snapshot
        .is_none()
    {
        step(&mut cohort.helper, &mut bridge, &mut facade, clock);
        assert!(
            Instant::now() < end,
            "actual logical checkpoint through relay cell"
        );
    }
    close(&mut cohort.helper, &mut bridge, facade, clock);
    let root = cohort.finish();
    let mut cohort = cluster::Cluster::open(root, protocol, NativeOpenMode::Recover, clock);
    cohort.elect();
    let mut bridge = Bridge::new(&mut cohort.boats, setup::mapping(), support::node(1)).unwrap();
    let fresh = claim(&mut cohort.helper, &mut bridge, clock, 371100);
    assert_eq!(fresh.binding().identity, old.identity);
    assert!(fresh.binding().session > old.session);
    assert!(fresh.image().parent.snapshot.is_some());
    let mut facade = build(&fresh, clock);
    assert_eq!(
        facade.local().applications[&setup::mapping().parent]
            .read_at(
                facade.local().applications[&setup::mapping().parent].applied_index(),
                ()
            )
            .unwrap(),
        7
    );
    elect(&mut cohort.helper, &mut bridge, &mut facade, clock);
    write(
        &mut cohort.helper,
        &mut bridge,
        &mut facade,
        clock,
        371010,
        7,
        7,
    );
    write(
        &mut cohort.helper,
        &mut bridge,
        &mut facade,
        clock,
        371011,
        5,
        12,
    );
    close(&mut cohort.helper, &mut bridge, facade, clock);
    std::fs::remove_dir_all(cohort.finish()).unwrap();
}
type Helper = voteboat::native::relay::RelayForwarder<
    channels::Gated,
    voteboat::native::wire::NativeWireCodec,
>;
fn now(clock: Instant) -> MonoTime {
    MonoTime(clock.elapsed().as_millis() as u64)
}
fn helper(h: &mut Helper, clock: Instant) {
    let p = h
        .poll(
            now(clock),
            3,
            voteboat::transport::TransportPollBudget::default(),
        )
        .unwrap();
    assert!(p.source_failures.is_empty());
}
fn budget() -> LogicalBridgeBudget {
    LogicalBridgeBudget {
        node_polls: 3,
        completions: 32,
        transitions: 1,
        node: NodePollBudget::default(),
    }
}
fn claim(h: &mut Helper, b: &mut Bridge<'_>, clock: Instant, id: u128) -> VerifiedLogicalImage {
    b.begin_claim(OperationId::new(id).unwrap()).unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        helper(h, clock);
        b.advance(now(clock), budget()).unwrap();
        if let Some(image) = b.take_image() {
            return image;
        }
        if Instant::now() >= end {
            b.inspect_work(|work| {
                eprintln!("original Claim work: {work:?}");
                if let Some(attempt) = work.and_then(|w| w.attempt()) {
                    if let LogicalBridgeChildTicket::Read { ticket, .. } = attempt.ticket() {
                        b.inspect_backing(attempt.node(), |p| {
                            eprintln!("original read: {:?}", p.reads.inspect(ticket));
                        })
                        .unwrap();
                    }
                }
            });
            for node in 0..3 {
                b.inspect_backing(node, |p| {
                    let core = p.owner.core(setup::child().group).unwrap();
                    eprintln!("original child {node}: role={:?} term={} commit={} last={} applied={} pending={} read_floor={} owner={:?} reads={:?} clients={:?}", core.role(), core.state().hard_state.term, core.state().commit_index, core.state().last_index(), p.applications[&setup::child().group].applied_index(), core.has_pending_dependency(), core.read_request_floor(), p.owner.usage(), p.reads.usage(), p.clients.usage());
                }).unwrap();
            }
            panic!("actual relay-backed Claim and fresh quorum image");
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn build(v: &VerifiedLogicalImage, clock: Instant) -> Facade {
    Facade::from_parts(
        v,
        parts::build(v, application::App::new(), setup::limits(), now(clock)),
        parts::node_limits::<CounterReceipt, i64>(setup::limits()),
        now(clock),
    )
    .unwrap_or_else(|e| panic!("actual relay-backed logical owner {:?}", e.reason))
}
fn step(h: &mut Helper, b: &mut Bridge<'_>, f: &mut Facade, clock: Instant) {
    helper(h, clock);
    b.advance(now(clock), budget()).unwrap();
    f.pump_backing(|p| {
        b.pump_persistence(p.persistence).unwrap();
        b.pump_release(p.releases).unwrap();
        if let Some(s) = p.snapshots {
            b.pump_snapshots(s, p.persistence).unwrap();
        }
    });
    f.poll(now(clock), LogicalNodePollBudget::default())
        .unwrap();
    std::thread::park_timeout(Duration::from_millis(1));
}
fn elect(h: &mut Helper, b: &mut Bridge<'_>, f: &mut Facade, clock: Instant) {
    f.campaign().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(h, b, f, clock);
        let core = f.local().owner.core(setup::mapping().parent).unwrap();
        if core.role() == Role::Leader && !core.has_pending_dependency() && f.work_is_drained() {
            break;
        }
        assert!(
            Instant::now() < end,
            "actual logical election through relay backing"
        );
    }
}
fn write(
    h: &mut Helper,
    b: &mut Bridge<'_>,
    f: &mut Facade,
    clock: Instant,
    id: u128,
    delta: i64,
    value: i64,
) {
    let ticket = f
        .propose(ClientRequest {
            group: setup::mapping().parent,
            operation: OperationId::new(id).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        })
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        step(h, b, f, clock);
        if let Some(output) = f.poll_client() {
            assert_eq!(output.ticket(), ticket);
            let ClientOutcome::Applied { receipt, .. } = f.complete_client(output).unwrap() else {
                panic!("original logical Applied through selected relay cell")
            };
            assert_eq!(receipt.operation, ticket.operation);
            assert_eq!(receipt.outcome, CounterOutcome::Value(value));
            break;
        }
        assert!(Instant::now() < end, "actual logical release permit");
    }
    let ticket = f.read(setup::mapping().parent, ()).unwrap();
    loop {
        step(h, b, f, clock);
        if let Some(output) = f.poll_read() {
            assert_eq!(output.ticket(), ticket);
            assert!(
                matches!(f.complete_read(output).unwrap(),ReadOutcome::Read{result:Ok(v),..}if v==value)
            );
            break;
        }
        assert!(
            Instant::now() < end,
            "actual fresh logical read permit through relay"
        );
    }
}
fn close(h: &mut Helper, b: &mut Bridge<'_>, mut f: Facade, clock: Instant) {
    f.close_intake().unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    while !f.is_drained() || !b.observations_are_drained() {
        step(h, b, &mut f, clock);
        assert!(
            Instant::now() < end,
            "same original relay-backed logical work drains"
        );
    }
    f.into_parts()
        .unwrap_or_else(|_| panic!("original logical providers reclaimed"));
    b.close();
    assert!(b.is_drained());
}
