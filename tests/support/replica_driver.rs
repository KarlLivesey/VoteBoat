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

use super::*;
use support::outbound::HostOutbound;
use voteboat::outbound::*;
type Parts<'a> = ReplicaParts<'a, Ready, Timers, Entropy, Counter, HostWorker, HostOutbound>;
struct Fixture {
    owner: Owner,
    worker: HostWorker,
    apps: BTreeMap<GroupIdentity, Counter>,
    results: ApplicationRouter<CounterReceipt>,
    clients: ClientRouter<CounterReceipt>,
    reads: ReadRequests<(), i64>,
    outbound: HostOutbound,
    driver: Option<ReplicaDriver<CounterReceipt>>,
}
impl Fixture {
    fn new(count: u128) -> Self {
        Self::voters(count, 1)
    }
    fn voters(count: u128, voters: u64) -> Self {
        let mut store = HostLogStore::new(1);
        let runtime = timed(1, &mut store, count, voters);
        let worker = HostWorker::new(store);
        let owner =
            EffectOwner::new(runtime, worker.binding(), EffectOwnerLimits::default()).unwrap();
        let id = owner.identity();
        let mut f = Self {
            results: ApplicationRouter::new(
                ApplicationRouterBinding {
                    owner: id,
                    generation: ApplicationRouterGeneration::new(1).unwrap(),
                },
                ApplicationRouterLimits::default(),
            )
            .unwrap(),
            clients: ClientRouter::new(
                ClientRouterBinding {
                    owner: id,
                    generation: ClientRouterGeneration::new(1).unwrap(),
                },
                ClientRouterLimits::default(),
            )
            .unwrap(),
            reads: ReadRequests::new(
                ReadInvocationBinding {
                    owner: id,
                    generation: ReadInvocationGeneration::new(1).unwrap(),
                },
                ReadInvocationLimits::default(),
                ReadRouter::new(
                    ReadRouterBinding {
                        owner: id,
                        generation: ReadRouterGeneration::new(1).unwrap(),
                    },
                    ReadRouterLimits::default(),
                )
                .unwrap(),
            )
            .unwrap(),
            outbound: HostOutbound::new(
                OutboundBinding {
                    node: node(1),
                    store: id.store,
                    generation: OutboundGeneration::new(1).unwrap(),
                },
                OutboundLimits::default(),
            )
            .unwrap(),
            apps: (1..=count)
                .map(|g| (group(g), Counter::new(100).unwrap()))
                .collect(),
            owner,
            worker,
            driver: None,
        };
        f.parts(|driver, p| {
            *driver =
                Some(ReplicaDriver::new(p, ReplicaDriverLimits::default(), MonoTime(0)).unwrap())
        });
        f
    }
    fn parts<R>(
        &mut self,
        call: impl FnOnce(&mut Option<ReplicaDriver<CounterReceipt>>, &mut Parts<'_>) -> R,
    ) -> R {
        let mut p = ReplicaParts {
            owner: &mut self.owner,
            persistence: &mut self.worker,
            applications: &mut self.apps,
            results: &mut self.results,
            clients: &mut self.clients,
            reads: &mut self.reads,
            outbound: &mut self.outbound,
            snapshots: None,
        };
        call(&mut self.driver, &mut p)
    }
    fn poll(&mut self) -> Result<ReplicaProgress, ReplicaError> {
        self.parts(|d, p| {
            d.as_mut()
                .unwrap()
                .poll(p, MonoTime(400), ReplicaPollBudget::default())
        })
    }
    fn settle(&mut self) {
        for _ in 0..200 {
            let progress = self.poll().unwrap();
            assert!(progress.steps.iter().all(|s| s.error.is_none()));
            if self.owner.is_drained()
                && self.worker.is_drained()
                && self.driver.as_ref().unwrap().is_drained()
            {
                return;
            }
        }
        panic!("local replica did not drain");
    }
    fn propose(&mut self, g: u128) -> ClientTicket {
        self.clients
            .submit(
                &mut self.owner,
                &self.apps[&group(g)],
                ClientRequest {
                    group: group(g),
                    operation: OperationId::new(g).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap()
    }
}

struct BadAcceptedTicket(HostOutbound);
impl OutboundQueue for BadAcceptedTicket {
    fn binding(&self) -> OutboundBinding {
        self.0.binding()
    }
    fn limits(&self) -> OutboundLimits {
        self.0.limits()
    }
    fn usage(&self) -> OutboundUsage {
        self.0.usage()
    }
    fn peer_usage(&self, peer: NodeId) -> OutboundUsage {
        self.0.peer_usage(peer)
    }
    fn submit(&mut self, messages: Vec<Message>) -> Result<SendTicket, SendRejected> {
        self.0.submit(messages).map(|mut ticket| {
            ticket.sequence = 0;
            ticket
        })
    }
    fn poll(&mut self, limit: usize) -> Vec<OutboundBatch> {
        self.0.poll(limit)
    }
    fn complete(
        &mut self,
        batch: OutboundBatch,
        result: LocalSendResult,
    ) -> Result<LocalSendCompletion, CompletionRejected> {
        self.0.complete(batch, result)
    }
    fn close(&mut self) {
        self.0.close();
    }
}
#[test]
fn invalid_accepted_send_ticket_fences_and_external_payload_stays_provider_owned() {
    let mut f = Fixture::voters(1, 3);
    let mut outbound = BadAcceptedTicket(f.outbound);
    let mut p = ReplicaParts {
        owner: &mut f.owner,
        persistence: &mut f.worker,
        applications: &mut f.apps,
        results: &mut f.results,
        clients: &mut f.clients,
        reads: &mut f.reads,
        outbound: &mut outbound,
        snapshots: None,
    };
    let mut driver = ReplicaDriver::new(&p, ReplicaDriverLimits::default(), MonoTime(0)).unwrap();
    let mut failed = false;
    for _ in 0..20 {
        if let Err(reason) = driver.poll(&mut p, MonoTime(400), ReplicaPollBudget::default()) {
            assert_eq!(reason, ReplicaError::ProviderContract);
            failed = true;
            break;
        }
    }
    assert!(failed);
    assert!(p.owner.is_failed());
    assert!(p.outbound.usage().batches > 0);
    driver.discard_failed(&mut p).unwrap();
    assert_eq!(p.owner.usage().leased, 0);
    assert!(driver.is_drained());
    assert!(p.outbound.usage().batches > 0);
    for batch in p.outbound.poll(10) {
        p.outbound.complete(batch, LocalSendResult::Sent).unwrap();
    }
    assert!(p.outbound.is_drained());
}
#[test]
fn public_host_components_drive_exact_applied_and_original_read_with_consumer_credits() {
    let mut f = Fixture::new(2);
    let mut independent = Fixture::new(1);
    f.settle();
    independent.settle();
    let ticket = f.propose(1);
    f.settle();
    let output = f.clients.poll().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert_eq!(f.clients.usage().requests, 1);
    assert!(matches!(
        f.clients.complete(output).unwrap(),
        ClientOutcome::Applied { .. }
    ));
    let ticket = f
        .reads
        .submit(&mut f.owner, &f.apps[&group(1)], group(1), ())
        .unwrap();
    f.settle();
    let output = f.reads.poll().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert_eq!(f.reads.usage().requests, 1);
    assert!(matches!(
        f.reads.complete(output).unwrap(),
        ReadOutcome::Read { result: Ok(7), .. }
    ));
    assert_eq!(independent.apps[&group(1)].read_applied(1).unwrap(), 0);
    f.parts(|d, p| d.as_mut().unwrap().close_intake(p).unwrap());
    assert_eq!(
        f.clients
            .submit(
                &mut f.owner,
                &f.apps[&group(1)],
                ClientRequest {
                    group: group(1),
                    operation: OperationId::new(99).unwrap(),
                    bytes: vec![],
                }
            )
            .unwrap_err()
            .reason,
        ClientError::Closed
    );
    assert!(!f.worker.closed);
    assert!(f.driver.as_ref().unwrap().is_drained());
}
#[test]
fn overloaded_wal_retains_exact_leases_and_written_cannot_publish_applied() {
    let mut f = Fixture::new(2);
    f.settle();
    f.worker.reject = true;
    f.propose(1);
    f.propose(2);
    f.poll().unwrap();
    let held = f.driver.as_ref().unwrap().usage();
    assert_eq!(held.data_persists, 2);
    let reserved = f.owner.usage().reserved_bytes;
    assert!(reserved > 0);
    f.poll().unwrap();
    assert_eq!(f.driver.as_ref().unwrap().usage(), held);
    assert_eq!(f.owner.usage().reserved_bytes, reserved);
    assert!(f.clients.poll().is_none());
    f.worker.reject = false;
    assert_eq!(f.poll().unwrap().persistence_batches, 1);
    assert_eq!(f.worker.accepted.as_ref().unwrap().1.len(), 2);
    assert_eq!(f.poll().unwrap().worker_events, 1); // Written only.
    assert!(f.clients.poll().is_none());
    assert_eq!(f.apps[&group(1)].read_applied(1).unwrap(), 0);
    f.settle();
    for _ in 0..2 {
        let result = f.clients.poll().unwrap();
        assert!(matches!(
            f.clients.complete(result).unwrap(),
            ClientOutcome::Applied { .. }
        ));
    }
}
#[test]
fn wrong_binding_clock_and_budget_reject_before_provider_poll_and_can_resume() {
    let mut f = Fixture::new(1);
    f.poll().unwrap();
    assert!(f.worker.accepted.is_some());
    f.worker.binding.generation = StorageWorkerGeneration::new(2).unwrap();
    assert_eq!(f.poll().unwrap_err(), ReplicaError::WrongBinding);
    assert!(f.worker.accepted.is_some());
    f.worker.binding.generation = StorageWorkerGeneration::new(1).unwrap();
    let err = f.parts(|d, p| {
        d.as_mut()
            .unwrap()
            .poll(p, MonoTime(399), ReplicaPollBudget::default())
    });
    assert_eq!(err.unwrap_err(), ReplicaError::ClockRegressed);
    let err = f.parts(|d, p| {
        d.as_mut().unwrap().poll(
            p,
            MonoTime(400),
            ReplicaPollBudget {
                effects: 0,
                ..Default::default()
            },
        )
    });
    assert_eq!(err.unwrap_err(), ReplicaError::InvalidLimits);
    assert!(f.worker.accepted.is_some());
    assert!(!f.owner.is_failed());
    f.settle();
}
#[test]
fn assembly_rejects_missing_or_lagging_application_and_held_work() {
    let mut f = Fixture::new(1);
    let app = f.apps.remove(&group(1)).unwrap();
    f.parts(|_, p| {
        assert!(matches!(
            ReplicaDriver::<CounterReceipt>::new(p, ReplicaDriverLimits::default(), MonoTime(0)),
            Err(ReplicaError::MissingApplication)
        ))
    });
    f.apps.insert(group(1), app);
    f.settle();
    f.apps.insert(group(1), Counter::new(100).unwrap());
    f.parts(|_, p| {
        assert!(matches!(
            ReplicaDriver::<CounterReceipt>::new(p, ReplicaDriverLimits::default(), MonoTime(400)),
            Err(ReplicaError::MissingApplication)
        ))
    });
    let mut f = Fixture::new(1);
    f.poll().unwrap();
    f.parts(|_, p| {
        assert!(matches!(
            ReplicaDriver::<CounterReceipt>::new(p, ReplicaDriverLimits::default(), MonoTime(400)),
            Err(ReplicaError::NotQuiescent)
        ))
    });
}
#[test]
fn failed_worker_fences_without_applied_and_explicit_cleanup_releases_rejected_work() {
    let mut f = Fixture::new(2);
    f.settle();
    f.parts(|d, p| {
        *d = Some(
            ReplicaDriver::new(
                p,
                ReplicaDriverLimits {
                    persistence_batch_units: 1,
                    ..Default::default()
                },
                MonoTime(400),
            )
            .unwrap(),
        );
    });
    f.propose(1);
    f.propose(2);
    f.poll().unwrap(); // Accepted.
    assert_eq!(f.driver.as_ref().unwrap().usage().data_persists, 1);
    f.poll().unwrap(); // Written.
    f.worker.fail = true;
    assert!(f.poll().is_err());
    assert!(f.owner.is_failed());
    assert!(f.driver.as_ref().unwrap().is_failed());
    assert_eq!(f.poll().unwrap_err(), ReplicaError::Fenced);
    f.parts(|d, p| d.as_mut().unwrap().discard_failed(p).unwrap());
    for _ in 0..2 {
        let output = f.clients.poll().unwrap();
        assert!(matches!(
            f.clients.complete(output).unwrap(),
            ClientOutcome::Unknown(_)
        ));
    }
    assert!(f.driver.as_ref().unwrap().is_drained());
    assert!(f.results.is_drained());
    assert_eq!(f.apps[&group(1)].read_applied(1).unwrap(), 0);
    assert_eq!(f.owner.usage().leased, 0);
}
