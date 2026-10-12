// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub struct Held {
    pub read: ReadInvocationInspection,
    pub visit: EffectVisitInspection,
    pub capsule: LogicalCapsule,
    pub bytes: Vec<u8>,
    pub operation: OperationId,
    pub attempt: LogicalBridgeAttemptInspection,
}
impl Held {
    pub fn assert_same(&self, host: &Host<'_>) {
        assert_eq!(
            host.parents[0]
                .routers
                .reads
                .inspect(self.read.ticket())
                .unwrap(),
            self.read
        );
        assert_eq!(
            host.parents[0]
                .owner
                .inspect_visit(self.read.ticket().group)
                .unwrap(),
            Some(self.visit)
        );
        host.bridges[0].inspect_work(|work| {
            let work = work.expect("same original retained work");
            assert_eq!(work.phase(), LogicalBridgeWorkPhase::Applying);
            assert_eq!(work.operation(), self.operation);
            assert_eq!(work.capsule(), Some(&self.capsule));
            assert_eq!(work.command_bytes(), Some(self.bytes.as_slice()));
            assert_eq!(work.attempt(), Some(self.attempt));
            assert!(!work.retained_output());
        });
    }
    pub fn child_credit(&self, bridge: &Bridge<'_>) -> ClientUsage {
        bridge
            .inspect_backing(self.attempt.node(), |local| local.clients.usage())
            .unwrap()
    }
}
pub fn refusals(host: &Host<'_>, ticket: ReadInvocationTicket) {
    let reads = &host.parents[0].routers.reads;
    let before = reads.inspect(ticket).unwrap();
    let credit = reads.usage();
    assert_eq!(
        host.parents[1].routers.reads.inspect(ticket),
        Err(ReadInvocationError::StaleTicket)
    );
    let mut changed = ticket;
    changed.binding = ReadInvocationBinding::new(
        ticket.binding.authorization(),
        ReadInvocationGeneration::new(2).unwrap(),
    );
    assert_eq!(
        reads.inspect(changed),
        Err(ReadInvocationError::StaleTicket)
    );
    changed = ticket;
    changed.request = ReadRequestId::new(ticket.request.get() + 1).unwrap();
    assert_eq!(
        reads.inspect(changed),
        Err(ReadInvocationError::StaleTicket)
    );
    assert_eq!(reads.inspect(ticket).unwrap(), before);
    assert_eq!(reads.usage(), credit);
    assert_eq!(
        host.parents[0].owner.inspect_visit(setup::mapping(0).child),
        Err(EffectOwnerError::Runtime(RuntimeError::UnknownGroup))
    );
}
pub fn wait(host: &mut Host<'_>, ticket: ReadInvocationTicket) -> Held {
    let end = Instant::now() + Duration::from_secs(15);
    let mut original = None;
    loop {
        for step in host.poll_with_completions(0) {
            let read = host.parents[0].routers.reads.inspect(ticket).unwrap();
            if step.admission == Some(read.admission()) {
                assert_eq!(step.read, Some(ticket.request));
                assert_eq!(step.error, None);
                assert_eq!(original.replace(step.visit), None);
            }
        }
        let read = host.parents[0].routers.reads.inspect(ticket).unwrap();
        if let Some(visit) = host.parents[0].owner.inspect_visit(ticket.group).unwrap() {
            if let Some(held) = capture(&host.bridges[0], read, visit, original) {
                return held;
            }
        }
        assert!(
            Instant::now() < end,
            "original read-to-child association not observed"
        );
    }
}
fn capture(
    bridge: &Bridge<'_>,
    read: ReadInvocationInspection,
    visit: EffectVisitInspection,
    original: Option<VisitTicket>,
) -> Option<Held> {
    bridge.inspect_work(|work| {
        let work = work?;
        let capsule = work.capsule()?;
        let LogicalCapsuleOrigin::Protocol(effect) = capsule.origin else {
            panic!("AssociationUnavailable: actual work is not original Send");
        };
        assert_eq!(visit.leased(), Some(effect), "AssociationUnavailable");
        assert_eq!(Some(visit.visit()), original);
        assert_eq!(effect.visit, visit.visit());
        let LogicalCapsuleBody::Protocol(message) = &capsule.body else {
            panic!("original protocol capsule");
        };
        assert_eq!(message.rpc, Rpc::ReadProbe);
        assert_eq!(message.group, read.ticket().group);
        assert_eq!(message.from, setup::mapping(0).voter);
        assert_eq!(
            visit.kind(),
            Some(EffectLeaseInspectionKind::Send(message.to))
        );
        assert_eq!(read.admission().owner, visit.visit().owner);
        assert!(read.stepped() && !read.terminal() && !read.has_output() && !read.exported());
        let attempt = work.attempt()?;
        let LogicalBridgeChildTicket::Client(ticket) = attempt.ticket() else {
            panic!("original Authorize proposal");
        };
        assert_eq!(ticket.operation, work.operation());
        assert_eq!(ticket.binding.owner, attempt.owner());
        assert_eq!(ticket.group, setup::mapping(0).child);
        bridge
            .inspect_backing(attempt.node(), |local| {
                assert_eq!(local.clients.binding(), ticket.binding)
            })
            .unwrap();
        let bytes = work.command_bytes().unwrap();
        assert!(bytes.len() <= setup::app(0).limits().max_command_bytes);
        Some(Held {
            read,
            visit,
            capsule: capsule.clone(),
            bytes: bytes.to_vec(),
            operation: work.operation(),
            attempt,
        })
    })
}
