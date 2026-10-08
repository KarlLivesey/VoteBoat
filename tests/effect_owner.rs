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
mod application_results {
    use super::*;
    fn router<R: ApplicationReceipt>(
        owner: &Owner,
        generation: u64,
        limits: ApplicationRouterLimits,
    ) -> ApplicationRouter<R> {
        ApplicationRouter::new(
            ApplicationRouterBinding {
                owner: owner.identity(),
                generation: ApplicationRouterGeneration::new(generation).unwrap(),
            },
            limits,
        )
        .unwrap()
    }
    fn committed(owner: &mut Owner, worker: &mut HostWorker, g: u128, event: Event) -> EffectLease {
        owner.admit(group(g), event).unwrap();
        for _ in 0..100 {
            for event in worker.poll(1) {
                owner.deliver_worker(event, MonoTime(0)).unwrap();
            }
            for step in owner.advance(MonoTime(0), 3).unwrap() {
                assert!(step.error.is_none(), "{:?}", step.error);
            }
            while let Some(lease) = owner.take_effect().unwrap() {
                match lease.effect {
                    Effect::Persist(_) => {
                        owner
                            .submit_persists(worker, vec![lease], MonoTime(0))
                            .unwrap();
                    }
                    Effect::Committed(_) => return lease,
                    _ => panic!("unexpected single-voter output"),
                }
            }
        }
        panic!("no committed output")
    }
    fn propose(id: u128, value: i64) -> Event {
        Event::Propose {
            operation: OperationId::new(id).unwrap(),
            bytes: value.to_le_bytes().to_vec(),
        }
    }
    fn start(owner: &mut Owner, worker: &mut HostWorker, g: u128, app: &mut Counter) {
        let lease = committed(owner, worker, g, Event::Campaign);
        let mut r = router(owner, 99, ApplicationRouterLimits::default());
        r.submit(owner, lease, app, MonoTime(0)).unwrap();
        let output = r.poll().unwrap();
        assert!(r.complete(output).unwrap().is_empty());
    }
    #[test]
    fn written_and_wrong_effect_never_apply_or_publish_and_durable_application_is_ordered() {
        let (mut owner, mut worker) = single(1);
        let mut app = Counter::new(10).unwrap();
        let mut r = router(&owner, 1, ApplicationRouterLimits::default());
        owner.admit(group(1), Event::Campaign).unwrap();
        owner.advance(MonoTime(0), 1).unwrap();
        let lease = owner.take_effect().unwrap().unwrap();
        let rejected = r
            .submit(&mut owner, lease, &mut app, MonoTime(0))
            .unwrap_err();
        assert_eq!(rejected.reason, ApplicationRouteError::WrongEffect);
        assert_eq!(app.applied_index(), 0);
        assert!(r.poll().is_none());
        owner
            .submit_persists(&mut worker, vec![*rejected.lease], MonoTime(0))
            .unwrap();
        let written = worker.poll(1).pop().unwrap();
        assert!(matches!(written, WorkerEvent::Written { .. }));
        owner.deliver_worker(written, MonoTime(0)).unwrap();
        assert!(owner.take_effect().unwrap().is_none());
        assert!(r.poll().is_none());
        // Complete the election and its durable no-op through the ordinary pump.
        let mut apps = BTreeMap::new();
        pump(&mut owner, &mut worker, &mut apps, MonoTime(0));
        app = apps.remove(&group(1)).unwrap();
        let lease = committed(&mut owner, &mut worker, 1, propose(5, 7));
        let effect = lease.ticket;
        let t = r.submit(&mut owner, lease, &mut app, MonoTime(0)).unwrap();
        let charge = r.usage();
        let output = r.poll().unwrap();
        assert_eq!(output.ticket(), t);
        assert_eq!(output.effect(), effect);
        assert_eq!(output.through(), 2);
        assert_eq!(output.receipts()[0].outcome, CounterOutcome::Value(7));
        assert_eq!(
            r.usage(),
            charge,
            "polling cannot release consumer-owned storage"
        );
        assert!(!r.is_drained());
        let receipts = r.complete(output).unwrap();
        assert_eq!(receipts[0].operation, OperationId::new(5).unwrap());
        assert_eq!(r.usage(), ApplicationResultUsage::default());
        assert!(r.is_drained());
    }
    #[test]
    fn held_results_backpressure_before_apply_control_reserve_and_retry_after_release() {
        let (mut owner, mut worker) = single(2);
        let mut app = Counter::new(10).unwrap();
        start(&mut owner, &mut worker, 1, &mut app);
        let mut r = router(
            &owner,
            1,
            ApplicationRouterLimits {
                batches: 2,
                control_batches: 1,
                ..ApplicationRouterLimits::default()
            },
        );
        let lease = committed(&mut owner, &mut worker, 1, propose(1, 4));
        r.submit(&mut owner, lease, &mut app, MonoTime(0)).unwrap();
        let held = r.poll().unwrap();
        let lease = committed(&mut owner, &mut worker, 1, propose(2, 3));
        let ticket = lease.ticket;
        let rejected = r
            .submit(&mut owner, lease, &mut app, MonoTime(0))
            .unwrap_err();
        assert_eq!(rejected.reason, ApplicationRouteError::Overloaded);
        assert_eq!(rejected.lease.ticket, ticket);
        assert_eq!(app.applied_index(), 2);
        assert_eq!(app.read_applied(2), Ok(4));
        // Group 1's blocked command lease does not prevent another group's
        // election no-op from using reserved application-result capacity.
        let control = committed(&mut owner, &mut worker, 2, Event::Campaign);
        let mut second = Counter::new(10).unwrap();
        r.submit(&mut owner, control, &mut second, MonoTime(0))
            .unwrap();
        assert_eq!(r.usage().batches, 2);
        let control = r.poll().unwrap();
        assert!(control.receipts().is_empty());
        r.complete(control).unwrap();
        let mut other = router::<CounterReceipt>(&owner, 2, ApplicationRouterLimits::default());
        let wrong = other.complete(held).unwrap_err();
        assert_eq!(wrong.reason, ApplicationRouteError::StaleResult);
        assert_eq!(r.usage().batches, 1);
        r.complete(*wrong.results).unwrap();
        r.submit(&mut owner, *rejected.lease, &mut app, MonoTime(0))
            .unwrap();
        assert_eq!(app.read_applied(3), Ok(7));
        let output = r.poll().unwrap();
        assert_eq!(
            r.complete(output).unwrap()[0].outcome,
            CounterOutcome::Value(7)
        );
        assert!(r.is_drained());
    }
    #[test]
    fn retries_and_conflicting_content_emit_original_application_outcomes() {
        let (mut owner, mut worker) = single(1);
        let mut app = Counter::new(10).unwrap();
        start(&mut owner, &mut worker, 1, &mut app);
        let mut r = router(&owner, 1, ApplicationRouterLimits::default());
        for (delta, expected, duplicate) in [
            (7, CounterOutcome::Value(7), false),
            (7, CounterOutcome::Value(7), true),
            (9, CounterOutcome::OperationConflict, true),
        ] {
            let lease = committed(&mut owner, &mut worker, 1, propose(1, delta));
            r.submit(&mut owner, lease, &mut app, MonoTime(0)).unwrap();
            let output = r.poll().unwrap();
            assert_eq!(output.receipts()[0].outcome, expected);
            assert_eq!(output.receipts()[0].duplicate, duplicate);
            r.complete(output).unwrap();
        }
        assert_eq!(app.read_applied(4), Ok(7));
    }
    #[test]
    fn wrong_runtime_and_modified_committed_content_reject_before_application() {
        let (mut owner, mut worker) = single(1);
        let mut app = Counter::new(10).unwrap();
        start(&mut owner, &mut worker, 1, &mut app);
        let lease = committed(&mut owner, &mut worker, 1, propose(1, 4));
        let mut binding = ApplicationRouterBinding {
            owner: owner.identity(),
            generation: ApplicationRouterGeneration::new(1).unwrap(),
        };
        binding.owner.generation = RuntimeGeneration::new(2).unwrap();
        let mut wrong =
            ApplicationRouter::new(binding, ApplicationRouterLimits::default()).unwrap();
        let rejected = wrong
            .submit(&mut owner, lease, &mut app, MonoTime(0))
            .unwrap_err();
        assert_eq!(rejected.reason, ApplicationRouteError::WrongBinding);
        assert_eq!(app.applied_index(), 1);
        let mut lease = *rejected.lease;
        if let Effect::Committed(entries) = &mut lease.effect {
            entries[0].payload = EntryPayload::Noop;
        }
        let mut r = router(&owner, 1, ApplicationRouterLimits::default());
        let rejected = r
            .submit(&mut owner, lease, &mut app, MonoTime(0))
            .unwrap_err();
        assert_eq!(rejected.reason, ApplicationRouteError::WrongEffect);
        assert_eq!(app.applied_index(), 1);
        assert!(!owner.is_failed());
    }
    #[derive(Debug)]
    struct Receipt {
        index: u64,
        operation: OperationId,
        data: Vec<u8>,
    }
    impl ApplicationReceipt for Receipt {
        fn index(&self) -> u64 {
            self.index
        }
        fn operation(&self) -> OperationId {
            self.operation
        }
        fn nested_bytes(&self, limit: usize) -> Result<usize, ApplicationError> {
            if self.data.capacity() > limit {
                Err(ApplicationError::InvalidCommand)
            } else {
                Ok(self.data.capacity())
            }
        }
    }
    struct HostApp {
        counter: Counter,
        mode: u8,
        calls: usize,
    }
    impl StateMachine for HostApp {
        type Receipt = Receipt;
        fn applied_index(&self) -> u64 {
            self.counter.applied_index() - u64::from(self.mode == 4 && self.calls > 0)
        }
        fn apply_batch(&mut self, entries: &[LogEntry]) -> Result<Vec<Receipt>, ApplicationError> {
            self.calls += 1;
            let applied = self.counter.apply_batch(entries)?;
            if self.mode == 3 {
                return Err(ApplicationError::InvalidCommand);
            }
            if self.mode == 5 {
                return Ok(vec![]);
            }
            let mut receipts =
                Vec::with_capacity(applied.len() + if self.mode == 6 { 64 } else { 0 });
            for receipt in applied {
                receipts.push(Receipt {
                    index: receipt.index + u64::from(self.mode == 1),
                    operation: receipt.operation,
                    data: Vec::with_capacity(if self.mode == 2 { 2048 } else { 8 }),
                });
            }
            Ok(receipts)
        }
    }
    impl BoundedStateMachine for HostApp {
        fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
            Ok(entries
                .iter()
                .filter(|e| matches!(e.payload, EntryPayload::Command { .. }))
                .count()
                * (std::mem::size_of::<Receipt>() + 8))
        }
    }
    #[test]
    fn host_receipts_nested_capacity_wrong_identity_and_partial_application_fail_closed() {
        for mode in 0..=6 {
            let (mut owner, mut worker) = single(1);
            let mut counter = Counter::new(10).unwrap();
            start(&mut owner, &mut worker, 1, &mut counter);
            let mut app = HostApp {
                counter,
                mode,
                calls: 0,
            };
            let mut r = router(&owner, 1, ApplicationRouterLimits::default());
            let lease = committed(&mut owner, &mut worker, 1, propose(1, 4));
            let result = r.submit(&mut owner, lease, &mut app, MonoTime(0));
            assert_eq!(app.calls, 1);
            if mode == 0 {
                result.unwrap();
                let charged = r.usage();
                let output = r.poll().unwrap();
                assert_eq!(output.receipts()[0].data.capacity(), 8);
                assert_eq!(r.usage(), charged);
                r.complete(output).unwrap();
                assert!(!owner.is_failed());
            } else {
                let rejected = result.unwrap_err();
                assert!(matches!(
                    rejected.reason,
                    ApplicationRouteError::ProviderViolation
                        | ApplicationRouteError::Application(ApplicationError::InvalidCommand)
                ));
                assert!(r.poll().is_none());
                assert!(r.is_fenced());
                assert!(owner.is_failed());
                assert_eq!(
                    app.counter.applied_index(),
                    2,
                    "unknown partial apply must be recovered"
                );
                owner.discard_failed(*rejected.lease).unwrap();
            }
        }
    }
    #[test]
    fn byte_ceiling_prevents_application_and_close_drains_without_closing_shared_app() {
        let (mut owner, mut worker) = single(1);
        let mut counter = Counter::new(10).unwrap();
        start(&mut owner, &mut worker, 1, &mut counter);
        let mut app = HostApp {
            counter,
            mode: 0,
            calls: 0,
        };
        let lease = committed(&mut owner, &mut worker, 1, propose(1, 4));
        let mut tiny = router(
            &owner,
            1,
            ApplicationRouterLimits {
                batch_bytes: 1,
                ..ApplicationRouterLimits::default()
            },
        );
        let rejected = tiny
            .submit(&mut owner, lease, &mut app, MonoTime(0))
            .unwrap_err();
        assert_eq!(rejected.reason, ApplicationRouteError::ResultTooLarge);
        assert_eq!(app.calls, 0);
        let mut r = router(&owner, 2, ApplicationRouterLimits::default());
        r.submit(&mut owner, *rejected.lease, &mut app, MonoTime(0))
            .unwrap();
        r.close();
        assert!(!r.is_drained());
        let output = r.poll().unwrap();
        r.complete(output).unwrap();
        assert!(r.is_drained());
        let lease = committed(&mut owner, &mut worker, 1, propose(2, 3));
        let rejected = r
            .submit(&mut owner, lease, &mut app, MonoTime(0))
            .unwrap_err();
        assert_eq!(rejected.reason, ApplicationRouteError::Closed);
        assert_eq!(app.calls, 1);
        let mut replacement = router(&owner, 3, ApplicationRouterLimits::default());
        replacement
            .submit(&mut owner, *rejected.lease, &mut app, MonoTime(0))
            .unwrap();
        assert_eq!(app.calls, 2);
        assert_eq!(app.counter.read_applied(3), Ok(7));
        let output = replacement.poll().unwrap();
        replacement.complete(output).unwrap();
    }
}
mod client_admission {
    use super::*;
    #[test]
    fn stopping_runtime_returns_exact_unprocessed_tracked_inputs() {
        let mut store = HostLogStore::new(1);
        let mut runtime = timed(1, &mut store, 1, 1);
        let event = || Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        };
        let first = runtime.admit_tracked(group(1), event()).unwrap();
        runtime.admit(group(1), Event::Heartbeat).unwrap();
        let second = runtime.admit_tracked(group(1), event()).unwrap();
        assert_ne!(first, second);
        let stopped = runtime.stop_group(group(1)).unwrap();
        assert_eq!(stopped.admissions, vec![first, second]);
        assert_eq!(stopped.queued.len(), 3);
        assert!(!stopped.had_active_visit);
        assert_eq!(runtime.usage().items, 0);
    }
    type Clients = ClientRouter<CounterReceipt>;
    fn setup(
        count: u128,
        capacity: usize,
    ) -> (
        Owner,
        HostWorker,
        Clients,
        ApplicationRouter<CounterReceipt>,
        BTreeMap<GroupIdentity, Counter>,
    ) {
        let (mut owner, mut worker) = single(count);
        let mut apps = BTreeMap::new();
        pump(&mut owner, &mut worker, &mut apps, MonoTime(400));
        for g in 1..=count {
            let mut app = Counter::new(capacity).unwrap();
            app.apply_batch(owner.core(group(g)).unwrap().replay_committed())
                .unwrap();
            apps.insert(group(g), app);
        }
        let clients = Clients::new(
            ClientRouterBinding {
                owner: owner.identity(),
                generation: ClientRouterGeneration::new(1).unwrap(),
            },
            ClientRouterLimits::default(),
        )
        .unwrap();
        let results = ApplicationRouter::new(
            ApplicationRouterBinding {
                owner: owner.identity(),
                generation: ApplicationRouterGeneration::new(1).unwrap(),
            },
            ApplicationRouterLimits::default(),
        )
        .unwrap();
        (owner, worker, clients, results, apps)
    }
    fn request(g: u128, id: u128, delta: i64) -> ClientRequest {
        ClientRequest {
            group: group(g),
            operation: OperationId::new(id).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        }
    }
    fn drive(
        owner: &mut Owner,
        worker: &mut HostWorker,
        clients: &mut Clients,
        results: &mut ApplicationRouter<CounterReceipt>,
        apps: &mut BTreeMap<GroupIdentity, Counter>,
    ) {
        for _ in 0..1000 {
            for event in worker.poll(1) {
                owner.deliver_worker(event, MonoTime(400)).unwrap();
            }
            clients
                .advance(owner, MonoTime(400), 3, |g| apps.get(&g))
                .unwrap();
            let mut persists = Vec::new();
            while let Some(lease) = owner.take_effect().unwrap() {
                match &lease.effect {
                    Effect::Persist(_) => persists.push(lease),
                    Effect::Committed(_) => {
                        let group = lease.ticket.visit.group;
                        results
                            .submit(owner, lease, apps.get_mut(&group).unwrap(), MonoTime(400))
                            .unwrap();
                        let output = results.poll().unwrap();
                        clients.deliver(owner, results, output).unwrap();
                    }
                    _ => panic!("unexpected single-voter effect"),
                }
            }
            if !persists.is_empty() {
                owner
                    .submit_persists(worker, persists, MonoTime(400))
                    .unwrap();
            }
            clients.reconcile(owner, 64).unwrap();
            if owner.is_drained() && worker.is_drained() {
                return;
            }
        }
        panic!("client fixture did not drain");
    }
    #[test]
    fn queued_retries_conflicts_and_capacity_use_exact_invocation_positions() {
        let (mut owner, mut worker, mut clients, mut results, mut apps) = setup(1, 1);
        let mut tickets = Vec::new();
        for delta in [7, 9, 7] {
            tickets.push(
                clients
                    .submit(&mut owner, &apps[&group(1)], request(1, 1, delta))
                    .unwrap(),
            );
        }
        assert_eq!(clients.usage().requests, 3);
        assert!(clients.poll().is_none());
        let rejected = clients
            .submit(&mut owner, &apps[&group(1)], request(1, 2, 5))
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            ClientError::Application(ApplicationError::DedupCapacity)
        );
        assert_eq!(apps[&group(1)].applied_index(), 1);
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        for (i, expected) in [
            CounterOutcome::Value(7),
            CounterOutcome::OperationConflict,
            CounterOutcome::Value(7),
        ]
        .into_iter()
        .enumerate()
        {
            let charge = clients.usage();
            let completion = clients.poll().unwrap();
            assert_eq!(completion.ticket(), tickets[i]);
            assert_eq!(clients.usage(), charge);
            match clients.complete(completion).unwrap() {
                ClientOutcome::Applied { position, receipt } => {
                    assert_eq!(position.index, i as u64 + 2);
                    assert_eq!(position.term, 1);
                    assert_eq!(receipt.index, position.index);
                    assert_eq!(receipt.outcome, expected);
                    assert_eq!(receipt.duplicate, i > 0);
                }
                _ => panic!("expected applied outcome"),
            }
        }
        assert!(clients.is_drained());
        assert_eq!(apps[&group(1)].read_applied(4), Ok(7));
    }
    #[test]
    fn cancelled_queued_wait_retains_capacity_until_execution_then_retry_returns_original() {
        let (mut owner, mut worker, mut clients, mut results, mut apps) = setup(1, 1);
        let ticket = clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 7))
            .unwrap();
        let charge = clients.usage();
        clients.cancel_wait(ticket).unwrap();
        let completion = clients.poll().unwrap();
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::Unknown(ClientUnknown::CancelledWait)
        ));
        assert_eq!(
            clients.usage(),
            charge,
            "queued command still owns its dedup reservation"
        );
        assert_eq!(
            clients
                .submit(&mut owner, &apps[&group(1)], request(1, 2, 8))
                .unwrap_err()
                .reason,
            ClientError::Application(ApplicationError::DedupCapacity)
        );
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        assert!(
            clients.poll().is_none(),
            "cancel cannot produce a second completion"
        );
        assert!(clients.is_drained());
        assert_eq!(apps[&group(1)].read_applied(2), Ok(7));
        clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 7))
            .unwrap();
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        let completion = clients.poll().unwrap();
        let ClientOutcome::Applied { receipt, .. } = clients.complete(completion).unwrap() else {
            panic!()
        };
        assert_eq!(receipt.outcome, CounterOutcome::Value(7));
        assert!(receipt.duplicate);
    }
    #[test]
    fn tracked_step_and_written_do_not_publish_applied_before_commit_and_application() {
        let (mut owner, mut worker, mut clients, mut results, mut apps) = setup(1, 2);
        clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 7))
            .unwrap();
        let steps = clients
            .advance(&mut owner, MonoTime(400), 1, |g| apps.get(&g))
            .unwrap();
        assert!(steps[0].admission.is_some());
        assert_eq!(
            steps[0].proposed,
            Some(ProposalPosition { index: 2, term: 1 })
        );
        let lease = owner.take_effect().unwrap().unwrap();
        owner
            .submit_persists(&mut worker, vec![lease], MonoTime(400))
            .unwrap();
        let written = worker.poll(1).pop().unwrap();
        assert!(matches!(written, WorkerEvent::Written { .. }));
        owner.deliver_worker(written, MonoTime(400)).unwrap();
        assert!(clients.poll().is_none());
        assert_eq!(apps[&group(1)].applied_index(), 1);
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        let completion = clients.poll().unwrap();
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::Applied { .. }
        ));
    }
    #[test]
    fn recovered_unapplied_log_reserves_capacity_without_old_router_memory() {
        let (mut owner, mut worker, mut clients, mut results, mut apps) = setup(1, 1);
        owner
            .admit(
                group(1),
                Event::Propose {
                    operation: OperationId::new(1).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap();
        let held = loop {
            for event in worker.poll(1) {
                owner.deliver_worker(event, MonoTime(400)).unwrap();
            }
            clients
                .advance(&mut owner, MonoTime(400), 1, |g| apps.get(&g))
                .unwrap();
            if let Some(lease) = owner.take_effect().unwrap() {
                match &lease.effect {
                    Effect::Persist(_) => {
                        owner
                            .submit_persists(&mut worker, vec![lease], MonoTime(400))
                            .unwrap();
                    }
                    Effect::Committed(_) => break lease,
                    _ => panic!(),
                }
            }
        };
        assert_eq!(owner.core(group(1)).unwrap().state().commit_index, 2);
        assert_eq!(apps[&group(1)].applied_index(), 1);
        assert!(clients.is_drained());
        assert_eq!(
            clients
                .submit(&mut owner, &apps[&group(1)], request(1, 2, 5))
                .unwrap_err()
                .reason,
            ClientError::Application(ApplicationError::DedupCapacity)
        );
        clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 7))
            .unwrap();
        results
            .submit(
                &mut owner,
                held,
                apps.get_mut(&group(1)).unwrap(),
                MonoTime(400),
            )
            .unwrap();
        let output = results.poll().unwrap();
        assert_eq!(
            clients.deliver(&mut owner, &mut results, output).unwrap(),
            0
        );
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        let output = clients.poll().unwrap();
        let ClientOutcome::Applied { position, receipt } = clients.complete(output).unwrap() else {
            panic!()
        };
        assert_eq!(position.index, 3);
        assert!(receipt.duplicate);
        assert_eq!(receipt.outcome, CounterOutcome::Value(7));
    }
    #[test]
    fn execution_revalidates_current_application_before_creating_persist() {
        let (mut owner, mut worker, mut clients, _, apps) = setup(1, 2);
        let ticket = clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 3))
            .unwrap();
        // A stricter selected host gate refuses this command at execution.
        // Its state/boundary are identical; the runtime cannot trust the old check.
        let host = HostApplication(apps[&group(1)].clone(), 0);
        let steps = clients
            .advance(&mut owner, MonoTime(400), 1, |_| Some(&host))
            .unwrap();
        assert_eq!(steps[0].proposed, None);
        assert_eq!(
            steps[0].error,
            Some(RaftError::Admission(ApplicationError::InvalidCommand))
        );
        assert!(owner.take_effect().unwrap().is_none());
        assert!(worker.poll(1).is_empty());
        assert_eq!(owner.core(group(1)).unwrap().state().last_index(), 1);
        let completion = clients.poll().unwrap();
        assert_eq!(completion.ticket(), ticket);
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::NotProposed(RaftError::Admission(ApplicationError::InvalidCommand))
        ));
        assert!(clients.is_drained());
        assert!(!owner.is_failed());
        clients
            .submit(&mut owner, &apps[&group(1)], request(1, 2, 7))
            .unwrap();
        let grown = HostApplication(apps[&group(1)].clone(), 1);
        let steps = clients
            .advance(&mut owner, MonoTime(400), 1, |_| Some(&grown))
            .unwrap();
        assert_eq!(
            steps[0].error,
            Some(RaftError::Admission(ApplicationError::ReceiptBudget))
        );
        assert!(owner.take_effect().unwrap().is_none());
        let completion = clients.poll().unwrap();
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::NotProposed(RaftError::Admission(ApplicationError::ReceiptBudget))
        ));
        assert_eq!(owner.core(group(1)).unwrap().state().last_index(), 1);
    }
    #[test]
    fn delayed_applied_reply_retains_verified_position_through_actual_log_compaction() {
        use voteboat::snapshot::{checkpoint_application, compact_replica};
        let (mut owner, mut worker, mut clients, mut results, mut apps) = setup(1, 2);
        let ticket = clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 7))
            .unwrap();
        let output = loop {
            for event in worker.poll(1) {
                owner.deliver_worker(event, MonoTime(400)).unwrap();
            }
            clients
                .advance(&mut owner, MonoTime(400), 1, |g| apps.get(&g))
                .unwrap();
            if let Some(lease) = owner.take_effect().unwrap() {
                match &lease.effect {
                    Effect::Persist(_) => {
                        owner
                            .submit_persists(&mut worker, vec![lease], MonoTime(400))
                            .unwrap();
                    }
                    Effect::Committed(_) => {
                        results
                            .submit(
                                &mut owner,
                                lease,
                                apps.get_mut(&group(1)).unwrap(),
                                MonoTime(400),
                            )
                            .unwrap();
                        break results.poll().unwrap();
                    }
                    _ => panic!(),
                }
            }
        };
        assert_eq!(
            output.positions(),
            &[ProposalPosition { index: 2, term: 1 }]
        );
        let charge = results.usage();
        let mut snapshots = support::snapshot::HostSnapshots::new();
        let receipt = checkpoint_application(
            owner.core(group(1)).unwrap(),
            &apps[&group(1)],
            &mut snapshots,
        )
        .unwrap();
        // Use the public synchronous compaction contract only while this host
        // store has no accepted async work. The callback performs memory I/O.
        assert!(worker.is_drained());
        owner
            .admit(
                group(1),
                Event::Read {
                    request: ReadRequestId::new(1).unwrap(),
                },
            )
            .unwrap();
        clients
            .advance(&mut owner, MonoTime(400), 1, |g| apps.get(&g))
            .unwrap();
        let lease = owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::ReadReady(_)));
        owner
            .complete_effect(
                lease,
                apps[&group(1)].applied_index(),
                MonoTime(400),
                |core| {
                    let effects = compact_replica(
                        core,
                        &mut worker.store,
                        &mut snapshots,
                        &apps[&group(1)],
                        receipt.reference(),
                    )
                    .unwrap();
                    Ok((effects, ()))
                },
            )
            .unwrap();
        assert_eq!(owner.core(group(1)).unwrap().state().base_index(), 2);
        assert!(owner.core(group(1)).unwrap().state().entry_at(2).is_none());
        assert_eq!(results.usage(), charge);
        assert_eq!(
            clients.deliver(&mut owner, &mut results, output).unwrap(),
            1
        );
        let completion = clients.poll().unwrap();
        assert_eq!(completion.ticket(), ticket);
        let ClientOutcome::Applied { position, receipt } = clients.complete(completion).unwrap()
        else {
            panic!()
        };
        assert_eq!(position, ProposalPosition { index: 2, term: 1 });
        assert_eq!(receipt.outcome, CounterOutcome::Value(7));
        assert!(clients.is_drained());
        assert!(results.is_drained());
    }
    struct HostApplication(Counter, usize);
    impl StateMachine for HostApplication {
        type Receipt = CounterReceipt;
        fn applied_index(&self) -> u64 {
            self.0.applied_index()
        }
        fn apply_batch(
            &mut self,
            entries: &[LogEntry],
        ) -> Result<Vec<CounterReceipt>, ApplicationError> {
            self.0.apply_batch(entries)
        }
    }
    impl BoundedStateMachine for HostApplication {
        fn receipt_bytes_bound(&self, entries: &[LogEntry]) -> Result<usize, ApplicationError> {
            self.0.receipt_bytes_bound(entries)
        }
    }
    impl ProposalAdmission for HostApplication {
        fn validate_proposal<'a>(
            &self,
            operation: OperationId,
            bytes: &[u8],
            pending: impl Iterator<Item = (OperationId, &'a [u8])>,
        ) -> Result<usize, ApplicationError> {
            // A host's deterministic command syntax can be stricter than Counter.
            if bytes != 7i64.to_le_bytes() {
                return Err(ApplicationError::InvalidCommand);
            }
            self.0
                .validate_proposal(operation, bytes, pending)
                .map(|n| n + self.1)
        }
    }
    #[test]
    fn host_admission_policy_and_wrong_runtime_reject_without_touching_shared_application() {
        let (mut owner, _, mut clients, _, mut apps) = setup(1, 2);
        let app = HostApplication(apps.remove(&group(1)).unwrap(), 0);
        assert_eq!(
            clients
                .submit(&mut owner, &app, request(1, 1, 3))
                .unwrap_err()
                .reason,
            ClientError::Application(ApplicationError::InvalidCommand)
        );
        assert_eq!(app.applied_index(), 1);
        assert!(clients.is_drained());
        let mut binding = clients.binding();
        binding.owner.generation = RuntimeGeneration::new(9).unwrap();
        let mut wrong = Clients::new(binding, ClientRouterLimits::default()).unwrap();
        assert_eq!(
            wrong
                .submit(&mut owner, &app, request(1, 1, 7))
                .unwrap_err()
                .reason,
            ClientError::WrongBinding
        );
        clients.submit(&mut owner, &app, request(1, 1, 7)).unwrap();
        assert_eq!(clients.usage().requests, 1);
        clients.abort(&mut owner).unwrap();
        let completion = clients.poll().unwrap();
        assert!(matches!(
            clients.complete(completion).unwrap(),
            ClientOutcome::Unknown(ClientUnknown::Aborted)
        ));
        assert!(clients.is_drained());
        assert_eq!(app.applied_index(), 1);
    }
    #[test]
    fn runtime_rejection_preserves_original_request_without_spending_client_ticket() {
        let (mut owner, mut worker, mut clients, mut results, mut apps) = setup(1, 2);
        for _ in 0..ShardLimits::default().group_items {
            owner.admit(group(1), Event::Heartbeat).unwrap();
        }
        let mut input = request(1, 1, 7);
        input.bytes.reserve(200);
        let original = input.bytes.as_ptr();
        let rejected = clients
            .submit(&mut owner, &apps[&group(1)], input)
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            ClientError::Runtime(RuntimeError::Overloaded)
        );
        assert_eq!(rejected.request.bytes.as_ptr(), original);
        assert_eq!(clients.usage(), ClientUsage::default());
        clients
            .advance(&mut owner, MonoTime(400), 1, |g| apps.get(&g))
            .unwrap();
        let ticket = clients
            .submit(&mut owner, &apps[&group(1)], rejected.request)
            .unwrap();
        assert_eq!(ticket.sequence, 1);
        drive(
            &mut owner,
            &mut worker,
            &mut clients,
            &mut results,
            &mut apps,
        );
        let completion = clients.poll().unwrap();
        clients.complete(completion).unwrap();
        assert!(clients.is_drained());
    }
    #[test]
    fn per_group_budget_close_and_foreign_completion_preserve_shared_owner() {
        let (mut owner, mut worker, _, results, apps) = setup(2, 2);
        let mut clients = Clients::new(
            ClientRouterBinding {
                owner: owner.identity(),
                generation: ClientRouterGeneration::new(2).unwrap(),
            },
            ClientRouterLimits {
                requests: 2,
                group_requests: 1,
                ..ClientRouterLimits::default()
            },
        )
        .unwrap();
        clients
            .submit(&mut owner, &apps[&group(1)], request(1, 1, 7))
            .unwrap();
        assert_eq!(
            clients
                .submit(&mut owner, &apps[&group(1)], request(1, 2, 8))
                .unwrap_err()
                .reason,
            ClientError::Overloaded
        );
        let ticket = clients
            .submit(&mut owner, &apps[&group(2)], request(2, 1, 3))
            .unwrap();
        clients.cancel_wait(ticket).unwrap();
        let completion = clients.poll().unwrap();
        let mut other = Clients::new(
            ClientRouterBinding {
                owner: owner.identity(),
                generation: ClientRouterGeneration::new(3).unwrap(),
            },
            ClientRouterLimits::default(),
        )
        .unwrap();
        let rejected = other.complete(completion).unwrap_err();
        assert_eq!(rejected.reason, ClientError::StaleTicket);
        clients.complete(*rejected.completion).unwrap();
        // Explicit failed-owner abort resolves queued reservations without
        // pretending cancellation reversed accepted work.
        clients.abort(&mut owner).unwrap();
        assert!(owner.is_failed());
        while let Some(completion) = clients.poll() {
            clients.complete(completion).unwrap();
        }
        assert!(clients.is_drained());
        assert!(results.is_drained());
        worker.close();
        // A separate healthy owner remains independent.
        let (mut independent, mut w, mut c, mut r, mut a) = setup(1, 2);
        c.submit(&mut independent, &a[&group(1)], request(1, 1, 3))
            .unwrap();
        c.close();
        assert_eq!(
            c.submit(&mut independent, &a[&group(1)], request(1, 2, 8))
                .unwrap_err()
                .reason,
            ClientError::Closed
        );
        drive(&mut independent, &mut w, &mut c, &mut r, &mut a);
        let completion = c.poll().unwrap();
        c.complete(completion).unwrap();
        assert!(!independent.is_failed());
        assert!(c.is_drained());
    }
}
mod support;
use std::collections::{BTreeMap, VecDeque};
use support::*;
use voteboat::{application::*, identity::*, log::*, raft::*, runtime::*, worker::*};
struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        100
    }
    fn enqueue(&mut self, g: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&g) {
            self.0.push_back(g);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, g: GroupIdentity) {
        self.0.retain(|x| *x != g);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
struct Timers {
    owner: RuntimeOwner,
    sequence: u64,
    entries: BTreeMap<u64, TimerToken>,
}
impl TimerService for Timers {
    fn owner(&self) -> RuntimeOwner {
        self.owner
    }
    fn capacity(&self) -> usize {
        100
    }
    fn register(
        &mut self,
        group: GroupIdentity,
        kind: TimerKind,
        deadline: MonoTime,
    ) -> Result<TimerToken, RuntimeError> {
        self.sequence += 1;
        let token = TimerToken {
            owner: self.owner,
            group,
            kind,
            deadline,
            sequence: self.sequence,
        };
        self.entries.insert(token.sequence, token);
        Ok(token)
    }
    fn cancel(&mut self, token: TimerToken) -> Result<(), RuntimeError> {
        self.entries.remove(&token.sequence);
        Ok(())
    }
    fn poll(&mut self, now: MonoTime, limit: usize) -> Result<Vec<Expiration>, RuntimeError> {
        let mut due = self
            .entries
            .values()
            .filter(|t| t.deadline <= now)
            .copied()
            .collect::<Vec<_>>();
        due.sort_by_key(|t| (t.deadline, t.sequence));
        due.truncate(limit);
        Ok(due
            .into_iter()
            .map(|token| {
                self.entries.remove(&token.sequence);
                Expiration {
                    token,
                    late_ms: now.0 - token.deadline.0,
                }
            })
            .collect())
    }
    fn len(&self) -> usize {
        self.entries.len()
    }
}
struct Entropy(u64);
impl ElectionEntropy for Entropy {
    fn sample(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}
type Owner = EffectOwner<Ready, Timers, Entropy>;
fn timed<L: LogStore>(
    id: u64,
    store: &mut L,
    count: u128,
    voters: u64,
) -> TimedShard<Ready, Timers, Entropy> {
    append(
        store,
        (1..=count)
            .map(|g| LogMutation::Create(bootstrap(g, voters)))
            .collect(),
    );
    let owner = RuntimeOwner {
        store: store.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        owner,
        ShardLimits {
            max_groups: 100,
            visit_items: 1,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    for g in 1..=count {
        shard
            .register(
                Raft::recover(
                    node(id),
                    store.binding(),
                    store.state(group(g)).unwrap(),
                    store.limits(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    TimedShard::new(
        shard,
        Timers {
            owner,
            sequence: 0,
            entries: BTreeMap::new(),
        },
        Entropy(id * 17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap()
}
struct HostWorker {
    store: HostLogStore,
    binding: WorkerBinding,
    sequence: u64,
    accepted: Option<(WorkerTicket, Vec<PersistUnit>)>,
    written: Option<(WorkerTicket, Vec<VisitTicket>, Vec<LogTicket>)>,
    reject: bool,
    fail: bool,
    closed: bool,
}
impl HostWorker {
    fn new(store: HostLogStore) -> Self {
        let binding = WorkerBinding {
            store: store.binding(),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        };
        Self {
            store,
            binding,
            sequence: 0,
            accepted: None,
            written: None,
            reject: false,
            fail: false,
            closed: false,
        }
    }
}
impl PersistenceWorker for HostWorker {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        WorkerLimits::default()
    }
    fn usage(&self) -> WorkerUsage {
        WorkerUsage {
            requests: usize::from(self.accepted.is_some() || self.written.is_some()),
            units: self.accepted.as_ref().map_or_else(
                || self.written.as_ref().map_or(0, |(_, v, _)| v.len()),
                |(_, u)| u.len(),
            ),
            bytes: 0,
        }
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        let reason = if self.closed {
            Some(WorkerError::Closed)
        } else if self.reject || !self.is_drained() {
            Some(WorkerError::Overloaded)
        } else {
            batch_cost(&units, units.capacity(), self.limits()).err()
        };
        if let Some(reason) = reason {
            return Err(WorkerRejected { reason, units });
        }
        self.sequence += 1;
        let ticket = WorkerTicket {
            binding: self.binding,
            sequence: self.sequence,
        };
        self.accepted = Some((ticket, units));
        Ok(ticket)
    }
    fn poll(&mut self, limit: usize) -> Vec<WorkerEvent> {
        if limit == 0 {
            return vec![];
        }
        if let Some((request, visits, tickets)) = self.written.take() {
            return vec![if self.fail {
                WorkerEvent::Failed {
                    request,
                    visits,
                    error: voteboat::contracts::StorageError::Rejected("injected failure"),
                }
            } else {
                WorkerEvent::Durable {
                    request,
                    visits,
                    completion: self.store.barrier(&tickets).unwrap(),
                }
            }];
        }
        if let Some((request, units)) = self.accepted.take() {
            let visits = units.iter().map(|u| u.visit).collect::<Vec<_>>();
            let tickets = self
                .store
                .append_batch(
                    units
                        .into_iter()
                        .map(|u| LogMutation::Update(u.update))
                        .collect(),
                )
                .unwrap();
            let admissions = visits
                .iter()
                .copied()
                .zip(tickets.iter().copied())
                .collect();
            self.written = Some((request, visits, tickets));
            return vec![WorkerEvent::Written {
                request,
                admissions,
            }];
        }
        vec![]
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
fn single(count: u128) -> (Owner, HostWorker) {
    let mut store = HostLogStore::new(1);
    let runtime = timed(1, &mut store, count, 1);
    let worker = HostWorker::new(store);
    (
        EffectOwner::new(runtime, worker.binding(), EffectOwnerLimits::default()).unwrap(),
        worker,
    )
}
fn pump(
    owner: &mut Owner,
    worker: &mut impl PersistenceWorker,
    app: &mut BTreeMap<GroupIdentity, Counter>,
    now: MonoTime,
) {
    for _ in 0..10000 {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        for step in owner.advance(now, 100).unwrap() {
            assert!(step.error.is_none(), "step: {:?}", step.error);
        }
        let mut persists = Vec::new();
        while let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => persists.push(lease),
                Effect::Committed(entries) => {
                    let a = app
                        .entry(lease.ticket.visit.group)
                        .or_insert_with(|| Counter::new(100).unwrap());
                    a.apply_batch(entries).unwrap();
                    owner.release(lease, a.applied_index(), now).unwrap();
                }
                _ => panic!("unexpected single-voter effect"),
            }
        }
        if !persists.is_empty() {
            owner.submit_persists(worker, persists, now).unwrap();
        }
        if owner.is_drained() && worker.is_drained() {
            return;
        }
        std::thread::yield_now();
    }
    panic!("owner did not drain");
}
#[test]
fn automatic_election_rejection_written_and_exact_durable_ownership() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    let ticket = lease.ticket;
    let reserved = owner.usage().reserved_bytes;
    let rejected = owner.release(lease, 0, now).unwrap_err();
    assert_eq!(rejected.reason, EffectOwnerError::PersistenceEffect);
    worker.reject = true;
    let rejected = owner
        .submit_persists(&mut worker, vec![*rejected.lease], now)
        .unwrap_err();
    assert_eq!(rejected.leases[0].ticket, ticket);
    assert_eq!(owner.usage().reserved_bytes, reserved);
    worker.reject = false;
    owner
        .submit_persists(&mut worker, rejected.leases, now)
        .unwrap();
    let written = worker.poll(1).pop().unwrap();
    owner.deliver_worker(written.clone(), now).unwrap();
    assert!(owner.take_effect().unwrap().is_none());
    assert_eq!(owner.core(group(1)).unwrap().state().hard_state.term, 0);
    assert_eq!(
        owner.deliver_worker(written, now),
        Err(EffectOwnerError::StaleCompletion)
    );
    let durable = worker.poll(1).pop().unwrap();
    let mut stale = durable.clone();
    if let WorkerEvent::Durable { request, .. } = &mut stale {
        request.binding.generation = StorageWorkerGeneration::new(2).unwrap();
    }
    assert_eq!(
        owner.deliver_worker(stale, now),
        Err(EffectOwnerError::StaleCompletion)
    );
    owner.deliver_worker(durable.clone(), now).unwrap();
    assert_eq!(
        owner.deliver_worker(durable, now),
        Err(EffectOwnerError::StaleCompletion)
    );
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert_eq!(owner.core(group(1)).unwrap().role(), Role::Leader);
    assert_eq!(owner.usage().reserved_bytes, 0);
}
#[test]
fn held_effect_stops_its_group_while_other_group_commits() {
    let (mut owner, mut worker) = single(2);
    let now = MonoTime(400);
    owner.advance(now, 2).unwrap();
    let held = owner.take_effect().unwrap().unwrap();
    let held_group = held.ticket.visit.group;
    let mut apps = BTreeMap::new();
    // Keep the first owned persistence effect outside the owner. The second
    // group must finish without freeing the first group's reservation.
    for _ in 0..1000 {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        owner.advance(now, 2).unwrap();
        let mut persists = Vec::new();
        while let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => persists.push(lease),
                Effect::Committed(entries) => {
                    let a = apps
                        .entry(lease.ticket.visit.group)
                        .or_insert_with(|| Counter::new(100).unwrap());
                    a.apply_batch(entries).unwrap();
                    owner.release(lease, a.applied_index(), now).unwrap();
                }
                _ => panic!(),
            }
        }
        if !persists.is_empty() {
            owner.submit_persists(&mut worker, persists, now).unwrap();
        }
        if owner.usage().active_visits == 1 && worker.is_drained() {
            break;
        }
    }
    assert_eq!(owner.usage().leased, 1);
    assert!(owner.usage().reserved_bytes > 0);
    let other = if held_group == group(1) {
        group(2)
    } else {
        group(1)
    };
    assert_eq!(owner.core(other).unwrap().state().commit_index, 1);
    owner.submit_persists(&mut worker, vec![held], now).unwrap();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert!(owner.is_drained());
}
#[test]
fn application_boundary_and_one_use_read_are_checked_before_release() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    owner
        .admit(
            group(1),
            Event::Propose {
                operation: OperationId::new(1).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        )
        .unwrap();
    loop {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        owner.advance(now, 1).unwrap();
        if let Some(lease) = owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => {
                    owner
                        .submit_persists(&mut worker, vec![lease], now)
                        .unwrap();
                }
                Effect::Committed(entries) => {
                    let rejected = owner
                        .release(
                            EffectLease {
                                ticket: lease.ticket,
                                effect: lease.effect.clone(),
                            },
                            0,
                            now,
                        )
                        .unwrap_err();
                    assert_eq!(rejected.reason, EffectOwnerError::NotApplied);
                    apps.get_mut(&group(1))
                        .unwrap()
                        .apply_batch(entries)
                        .unwrap();
                    owner
                        .release(lease, apps[&group(1)].applied_index(), now)
                        .unwrap();
                    break;
                }
                _ => panic!(),
            }
        }
    }
    owner
        .admit(
            group(1),
            Event::Read {
                request: ReadRequestId::new(1).unwrap(),
            },
        )
        .unwrap();
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    let ticket = lease.ticket;
    let original = lease.effect.clone();
    let rejected = owner
        .complete_effect::<()>(lease, 0, now, |_| {
            panic!("read callback ran before application catch-up")
        })
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        EffectOwnerError::Consensus(RaftError::NotApplied)
    );
    let lease = *rejected.lease;
    let value = owner
        .complete_effect(lease, apps[&group(1)].applied_index(), now, |_| {
            Ok((vec![], apps[&group(1)].read_at(2, ()).unwrap()))
        })
        .unwrap();
    assert_eq!(value, 7);
    assert_eq!(
        owner
            .release(
                EffectLease {
                    ticket,
                    effect: original
                },
                2,
                now
            )
            .unwrap_err()
            .reason,
        EffectOwnerError::StaleEffect
    );
}
#[test]
fn failure_keeps_external_lease_charged_until_explicit_discard() {
    let (mut owner, mut worker) = single(2);
    let now = MonoTime(400);
    owner.advance(now, 2).unwrap();
    let held = owner.take_effect().unwrap().unwrap();
    let failing = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![failing], now)
        .unwrap();
    owner
        .deliver_worker(worker.poll(1).pop().unwrap(), now)
        .unwrap();
    worker.fail = true;
    assert!(owner
        .deliver_worker(worker.poll(1).pop().unwrap(), now)
        .is_err());
    assert!(owner.core(held.ticket.visit.group).unwrap().is_fenced());
    assert_eq!(owner.usage().leased, 1);
    assert!(owner.usage().reserved_bytes > 0);
    owner.discard_failed(held).unwrap();
    assert_eq!(owner.usage().reserved_bytes, 0);
    assert!(!owner.is_drained());
}
#[test]
fn invalid_reservation_is_rejected_before_core_progress() {
    let mut store = HostLogStore::new(1);
    let runtime = timed(1, &mut store, 1, 1);
    let worker = HostWorker::new(store);
    assert!(matches!(
        EffectOwner::new(
            runtime,
            worker.binding(),
            EffectOwnerLimits {
                active_visits: 2,
                reserved_bytes: 2,
                control_visits: 1,
                control_bytes: 1,
            }
        ),
        Err(EffectOwnerError::ReservationTooLarge)
    ));
}

#[test]
fn bulk_leases_leave_a_reserved_visit_for_control() {
    let mut store = HostLogStore::new(1);
    let runtime = timed(1, &mut store, 4, 1);
    let mut worker = HostWorker::new(store);
    let mut owner = EffectOwner::new(
        runtime,
        worker.binding(),
        EffectOwnerLimits {
            active_visits: 4,
            control_visits: 1,
            ..EffectOwnerLimits::default()
        },
    )
    .unwrap();
    let now = MonoTime(400);
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    for g in 1..=3 {
        owner
            .admit(
                group(g),
                Event::Propose {
                    operation: OperationId::new(1).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            )
            .unwrap();
    }
    owner.advance(now, 4).unwrap();
    let mut held = Vec::new();
    while let Some(lease) = owner.take_effect().unwrap() {
        held.push(lease);
    }
    assert_eq!(held.len(), 3);
    owner.admit(group(4), Event::Campaign).unwrap();
    assert_eq!(owner.advance(now, 4).unwrap().len(), 1);
    held.push(owner.take_effect().unwrap().unwrap());
    assert_eq!(owner.usage().active_visits, 4);
    owner.submit_persists(&mut worker, held, now).unwrap();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert!(owner.is_drained());
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };
    use voteboat::{
        native::{log_store::*, outbound::NativeOutbound, worker::*},
        outbound::*,
    };
    use voteboat::{
        native::{
            runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
            snapshot_store::*,
            snapshot_worker::NativeSnapshotWorker,
        },
        snapshot::*,
        snapshot_worker::*,
    };
    type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
    type Snapshots = NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>;
    type Worker = NativeLogWorker<NativeLogStore<FileLogIo>>;
    struct Node {
        owner: Owner,
        worker: Worker,
        apps: BTreeMap<GroupIdentity, Counter>,
        outbound: NativeOutbound,
        pending: Vec<EffectLease>,
        reads: Vec<i64>,
        transports:
            Option<voteboat::transport::PeerRoster<Box<dyn voteboat::transport::PeerTransport>>>,
        ingress: IngressRouter,
        results: ApplicationRouter<CounterReceipt>,
        read_results: ReadRouter<i64>,
        clients: ClientRouter<CounterReceipt>,
        client_applied: usize,
        client_unknown: usize,
        #[cfg(feature = "tls")]
        connector: Option<voteboat::native::connect::NativePeerConnector>,
        staged: VecDeque<OutboundBatch>,
        sent: usize,
        received: usize,
        snapshots: Option<Snapshots>,
        router: Option<SnapshotRouter>,
        snapshot_pending: VecDeque<EffectLease>,
        send_pending: VecDeque<EffectLease>,
        installed: usize,
        supplied: usize,
    }
    fn make(id: u64, path: &std::path::Path, recover: bool) -> Node {
        make_snapshots(id, path, recover, false)
    }
    fn make_snapshots(
        id: u64,
        path: &std::path::Path,
        recover: bool,
        with_snapshots: bool,
    ) -> Node {
        let mut store = if recover {
            NativeLogStore::recover(
                FileLogIo::open(path).unwrap(),
                identity(id.into()),
                LogLimits::default(),
            )
            .unwrap()
        } else {
            NativeLogStore::create(
                FileLogIo::create(path).unwrap(),
                identity(id.into()),
                LogLimits::default(),
            )
            .unwrap()
        };
        // Bootstrap is explicit in this test; incoming peer traffic cannot create groups.
        if !recover {
            append(
                &mut store,
                (1..=100)
                    .map(|g| LogMutation::Create(bootstrap(g, 3)))
                    .collect(),
            );
        }
        let runtime_owner = RuntimeOwner {
            store: store.binding(),
            lane: ExecutionLaneId::new(1).unwrap(),
            generation: RuntimeGeneration::new(1).unwrap(),
        };
        let mut shard = Shard::new(
            runtime_owner,
            ShardLimits {
                max_groups: 100,
                visit_items: 1,
                ..ShardLimits::default()
            },
            FairScheduler::new(100).unwrap(),
        )
        .unwrap();
        if with_snapshots && !recover && id <= 2 {
            let mutations = (1..=100)
                .map(|g| {
                    let state = store.state(group(g)).unwrap();
                    update(
                        &state,
                        1,
                        1,
                        Some(Suffix {
                            from: 1,
                            entries: vec![LogEntry {
                                index: 1,
                                term: 1,
                                payload: EntryPayload::Command {
                                    operation: OperationId::new(1).unwrap(),
                                    bytes: 7i64.to_le_bytes().to_vec(),
                                },
                            }],
                        }),
                    )
                })
                .collect();
            append(&mut store, mutations);
        }
        let mut apps = BTreeMap::new();
        let mut snapshot_stores = BTreeMap::new();
        if with_snapshots && !recover {
            std::fs::create_dir(path.join("snapshots")).unwrap();
        }
        for g in 1..=100 {
            let mut app = Counter::new(100).unwrap();
            let mut snap = with_snapshots.then(|| {
                let directory = path.join("snapshots").join(g.to_string());
                let identity = SnapshotIdentity {
                    store: store.binding().identity,
                    group: group(g),
                };
                let limits = SnapshotLimits {
                    max_application_bytes: 65536,
                    max_metadata_bytes: 4096,
                    max_chunk_bytes: 1024,
                };
                if recover {
                    NativeSnapshotStore::recover(
                        FileSnapshotIo::open(directory).unwrap(),
                        identity,
                        limits,
                    )
                    .unwrap()
                } else {
                    NativeSnapshotStore::create(
                        FileSnapshotIo::create(directory).unwrap(),
                        identity,
                        limits,
                    )
                    .unwrap()
                }
            });
            let mut core = if recover && with_snapshots {
                recover_replica(node(id), group(g), &store, snap.as_mut().unwrap(), &mut app)
                    .unwrap()
                    .0
            } else {
                let core = Raft::recover(
                    node(id),
                    store.binding(),
                    store.state(group(g)).unwrap(),
                    store.limits(),
                )
                .unwrap();
                app.apply_batch(core.replay_committed()).unwrap();
                core
            };
            if with_snapshots && !recover && id <= 2 {
                let receipt = checkpoint_application(&core, &app, snap.as_mut().unwrap()).unwrap();
                compact_replica(
                    &mut core,
                    &mut store,
                    snap.as_mut().unwrap(),
                    &app,
                    receipt.reference(),
                )
                .unwrap();
            }
            if let Some(snap) = snap {
                snapshot_stores.insert(group(g), snap);
            }
            apps.insert(group(g), app);
            shard.register(core).unwrap();
        }
        let runtime = TimedShard::new(
            shard,
            DeadlineQueue::new(runtime_owner, 100).unwrap(),
            JitterEntropy::new(id * 17),
            TimerConfig::default(),
            MonoTime(0),
        )
        .unwrap();
        let worker = NativeLogWorker::spawn(
            store,
            StorageWorkerGeneration::new(1).unwrap(),
            WorkerLimits::default(),
            Arc::new(ThreadWake::current()),
        )
        .unwrap();
        let owner =
            EffectOwner::new(runtime, worker.binding(), EffectOwnerLimits::default()).unwrap();
        let outbound = NativeOutbound::new(
            OutboundBinding {
                node: node(id),
                store: runtime_owner.store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            OutboundLimits::default(),
        )
        .unwrap();
        let snapshots = with_snapshots.then(|| {
            NativeSnapshotWorker::spawn(
                snapshot_stores,
                SnapshotWorkerBinding {
                    store: runtime_owner.store,
                    generation: SnapshotWorkerGeneration::new(1).unwrap(),
                },
                SnapshotWorkLimits {
                    max_groups: 100,
                    ..SnapshotWorkLimits::default()
                },
                Arc::new(ThreadWake::current()),
            )
            .unwrap()
        });
        let router = snapshots.as_ref().map(|worker| {
            SnapshotRouter::new(
                owner.identity(),
                worker.binding(),
                SnapshotRouterLimits::default(),
            )
            .unwrap()
        });
        Node {
            owner,
            worker,
            apps,
            outbound,
            pending: Vec::new(),
            reads: Vec::new(),
            transports: None,
            clients: ClientRouter::new(
                ClientRouterBinding {
                    owner: runtime_owner,
                    generation: ClientRouterGeneration::new(1).unwrap(),
                },
                ClientRouterLimits::default(),
            )
            .unwrap(),
            client_applied: 0,
            client_unknown: 0,
            read_results: ReadRouter::new(
                ReadRouterBinding {
                    owner: runtime_owner,
                    generation: ReadRouterGeneration::new(1).unwrap(),
                },
                ReadRouterLimits::default(),
            )
            .unwrap(),
            results: ApplicationRouter::new(
                ApplicationRouterBinding {
                    owner: runtime_owner,
                    generation: ApplicationRouterGeneration::new(1).unwrap(),
                },
                ApplicationRouterLimits::default(),
            )
            .unwrap(),
            ingress: IngressRouter::new(
                IngressBinding {
                    owner: runtime_owner,
                    local: voteboat::secure::LocalIdentity {
                        node: node(id),
                        store: runtime_owner.store,
                    },
                    generation: IngressGeneration::new(1).unwrap(),
                },
                IngressLimits::default(),
            )
            .unwrap(),
            #[cfg(feature = "tls")]
            connector: None,
            staged: VecDeque::new(),
            sent: 0,
            received: 0,
            snapshots,
            router,
            snapshot_pending: VecDeque::new(),
            send_pending: VecDeque::new(),
            installed: 0,
            supplied: 0,
        }
    }
    #[cfg(feature = "tls")]
    fn establish(
        nodes: &mut [Node],
        tickets: Vec<(usize, voteboat::transport::ConnectTicket)>,
        now: MonoTime,
    ) {
        use voteboat::{
            connect::*,
            native::{transport::NativePeerTransport, wire::NativeWireCodec},
            transport::TransportLimits,
            wire::WireLimits,
        };
        let endpoints = nodes
            .iter()
            .map(|n| {
                (
                    n.outbound.binding().node,
                    n.connector
                        .as_ref()
                        .unwrap()
                        .listener_addr()
                        .unwrap()
                        .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for (i, ticket) in tickets {
            let deadline = nodes[i]
                .transports
                .as_ref()
                .unwrap()
                .attempt_deadline(ticket)
                .unwrap();
            let direction = if ticket.local.node < ticket.peer.node {
                ConnectDirection::Dial(endpoints[&ticket.peer.node])
            } else {
                ConnectDirection::Accept
            };
            nodes[i]
                .connector
                .as_mut()
                .unwrap()
                .submit(
                    ConnectRequest {
                        ticket,
                        direction,
                        deadline,
                    },
                    now,
                )
                .unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while nodes
            .iter()
            .any(|n| n.connector.as_ref().unwrap().usage().requests != 0)
        {
            for n in nodes.iter_mut() {
                let budget = ConnectPollBudget {
                    visits: 1,
                    socket_calls: 2,
                    completions: 1,
                    ..ConnectPollBudget::default()
                };
                for event in n.connector.as_mut().unwrap().poll(now, budget).unwrap() {
                    let session = event.result.unwrap();
                    let transport = NativePeerTransport::new(
                        session,
                        NativeWireCodec::new(WireLimits::default()).unwrap(),
                        &n.outbound,
                        TransportLimits::default(),
                    )
                    .unwrap();
                    n.transports
                        .as_mut()
                        .unwrap()
                        .attach(event.ticket, Box::new(transport), now)
                        .unwrap_or_else(|r| panic!("connector attach: {:?}", r.reason));
                    assert!(n
                        .transports
                        .as_ref()
                        .unwrap()
                        .attempt_deadline(event.ticket)
                        .is_none());
                }
                assert!(n.connector.as_ref().unwrap().usage().anonymous <= 16);
            }
            assert!(
                Instant::now() < deadline,
                "native connections did not establish"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    #[cfg(feature = "tls")]
    fn mesh(nodes: &mut [Node]) {
        use voteboat::{
            connect::ConnectLimits,
            dial::DialLimits,
            native::{connect::*, dial::NativeTcpDialer},
            secure::{LocalIdentity, SessionLimits},
            transport::{PeerRoster, PeerRosterConfig, PeerRosterLimits, TransportLimits},
        };
        let locals = nodes
            .iter()
            .map(|n| LocalIdentity {
                node: n.outbound.binding().node,
                store: n.outbound.binding().store,
            })
            .collect::<Vec<_>>();
        let mut tickets = Vec::new();
        for (i, n) in nodes.iter_mut().enumerate() {
            let peers = locals
                .iter()
                .filter(|l| l.node != locals[i].node)
                .map(|l| (l.node, l.store.identity))
                .collect::<BTreeMap<_, _>>();
            let dialer = NativeTcpDialer::spawn(
                locals[i],
                peers.clone(),
                DialLimits::default(),
                std::sync::Arc::new(ThreadWake::current()),
            )
            .unwrap();
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            n.connector = Some(
                NativePeerConnector::new(
                    NativeConnectConfig {
                        local: locals[i],
                        limits: ConnectLimits::default(),
                        session: SessionLimits {
                            write_buffer_bytes: 256,
                            ..SessionLimits::default()
                        },
                    },
                    support::tls::configuration(locals[i].node.get()),
                    locals
                        .iter()
                        .filter(|l| l.node != locals[i].node)
                        .map(|l| (l.node, support::tls::peer(*l)))
                        .collect(),
                    dialer,
                    Some(listener),
                    MonoTime(0),
                )
                .unwrap_or_else(|r| panic!("connector construct: {:?}", r.reason)),
            );
            let mut roster = PeerRoster::new(
                PeerRosterConfig {
                    local: locals[i],
                    outbound: n.outbound.binding(),
                    first_generation: SecureSessionGeneration::new(1).unwrap(),
                    last_generation: SecureSessionGeneration::new(u64::MAX).unwrap(),
                    wire_version: 1,
                    limits: PeerRosterLimits::default(),
                    transport_limits: TransportLimits::default(),
                },
                peers,
                MonoTime(0),
            )
            .unwrap();
            tickets.extend(
                roster
                    .due_connections(MonoTime(0), 3)
                    .unwrap()
                    .into_iter()
                    .map(|t| (i, t)),
            );
            n.transports = Some(roster);
        }
        establish(nodes, tickets, MonoTime(0));
    }
    fn drain(nodes: &mut [Node], isolated: Option<NodeId>, now: MonoTime) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut network = VecDeque::new();
        loop {
            assert!(
                Instant::now() < deadline,
                "effect-owner cluster did not drain"
            );
            for n in nodes.iter_mut() {
                if let (Some(router), Some(worker)) = (&mut n.router, &mut n.snapshots) {
                    for event in worker.poll(1) {
                        let installed = matches!(
                            &event.result,
                            Ok(SnapshotOutput::Loaded {
                                reconciled: true,
                                ..
                            })
                        );
                        let supplied = matches!(
                            &event.result,
                            Ok(SnapshotOutput::Loaded {
                                reconciled: false,
                                ..
                            })
                        );
                        let app = n.apps.get_mut(&event.visit.group).unwrap();
                        router.deliver(&mut n.owner, app, event, now).unwrap();
                        n.installed += usize::from(installed);
                        n.supplied += usize::from(supplied);
                    }
                    for _ in 0..n.snapshot_pending.len() {
                        let lease = n.snapshot_pending.pop_front().unwrap();
                        let app = &n.apps[&lease.ticket.visit.group];
                        if let Err(rejected) = router.submit(&mut n.owner, worker, lease, app) {
                            assert!(matches!(
                                rejected.reason,
                                SnapshotRouteError::Overloaded
                                    | SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
                                    | SnapshotRouteError::Owner(EffectOwnerError::Overloaded)
                            ));
                            n.snapshot_pending.push_back(*rejected.lease);
                        }
                    }
                }
                for event in n.worker.poll(1) {
                    n.owner.deliver_worker(event, now).unwrap();
                }
                for step in n
                    .clients
                    .advance(&mut n.owner, now, 100, |g| n.apps.get(&g))
                    .unwrap()
                {
                    assert!(step.error.is_none(), "{:?}", step.error);
                }
                while let Some(lease) = n.owner.take_effect().unwrap() {
                    match &lease.effect {
                        Effect::Persist(_) => n.pending.push(lease),
                        Effect::Committed(_) => {
                            let app = n.apps.get_mut(&lease.ticket.visit.group).unwrap();
                            let ticket = n.results.submit(&mut n.owner, lease, app, now).unwrap();
                            let output = n.results.poll().unwrap();
                            assert_eq!(output.ticket(), ticket);
                            assert_eq!(output.through(), app.applied_index());
                            n.clients
                                .deliver(&mut n.owner, &mut n.results, output)
                                .unwrap();
                            assert!(n.results.is_drained());
                        }
                        Effect::ReadReady(barrier) => {
                            let app = &n.apps[&lease.ticket.visit.group];
                            let request = barrier.request();
                            let ticket = n
                                .read_results
                                .submit(&mut n.owner, lease, app, (), now)
                                .unwrap();
                            let output = n.read_results.poll().unwrap();
                            assert_eq!(output.ticket(), ticket);
                            assert_eq!(output.barrier().request(), request);
                            assert_eq!(n.read_results.usage().results, 1);
                            let value = n.read_results.complete(output).unwrap().unwrap();
                            assert!(n.read_results.is_drained());
                            n.reads.push(value);
                        }
                        Effect::Send(_) => n.send_pending.push_back(lease),
                        Effect::StageSnapshot(_)
                        | Effect::SnapshotRequired { .. }
                        | Effect::SnapshotInstalled(_)
                        | Effect::CheckpointRequired { .. }
                        | Effect::CheckpointCompacted(_) => {
                            assert!(n.router.is_some());
                            n.snapshot_pending.push_back(lease);
                        }
                    }
                }
                n.clients.reconcile(&n.owner, 128).unwrap();
                while let Some(completion) = n.clients.poll() {
                    match n.clients.complete(completion).unwrap() {
                        ClientOutcome::Applied { .. } => n.client_applied += 1,
                        ClientOutcome::Unknown(_) => n.client_unknown += 1,
                        ClientOutcome::NotProposed(e) => {
                            panic!("unexpected client rejection: {e:?}")
                        }
                    }
                }
                n.clients.reconcile(&n.owner, 128).unwrap();
                assert!(n.clients.usage().bytes <= ClientRouterLimits::default().bytes);
                for _ in 0..n.send_pending.len() {
                    let EffectLease {
                        ticket,
                        effect: Effect::Send(message),
                    } = n.send_pending.pop_front().unwrap()
                    else {
                        unreachable!()
                    };
                    match n.outbound.submit(vec![message]) {
                        Ok(_) => n.owner.release_transferred_send(ticket).unwrap(),
                        Err(mut rejected) => {
                            assert_eq!(rejected.reason, OutboundError::Overloaded);
                            assert_eq!(rejected.messages.len(), 1);
                            n.send_pending.push_back(EffectLease {
                                ticket,
                                effect: Effect::Send(rejected.messages.remove(0)),
                            });
                        }
                    }
                }
                if !n.pending.is_empty() {
                    let leases = std::mem::take(&mut n.pending);
                    if let Err(rejected) = n.owner.submit_persists(&mut n.worker, leases, now) {
                        assert_eq!(
                            rejected.reason,
                            EffectOwnerError::Worker(WorkerError::Overloaded)
                        );
                        n.pending = rejected.leases;
                    }
                }
                match &mut n.transports {
                    None => {
                        for mut batch in n.outbound.poll(32) {
                            assert!(network.len() + batch.messages.len() <= 8192);
                            network.extend(batch.messages.drain(..));
                            n.outbound.complete(batch, LocalSendResult::Sent).unwrap();
                        }
                    }
                    Some(roster) => {
                        if n.staged.is_empty() {
                            n.staged.extend(n.outbound.poll(32));
                        }
                        for _ in 0..n.staged.len().min(32) {
                            let batch = n.staged.pop_front().unwrap();
                            match roster.submit(batch) {
                                Ok(()) => n.sent += 1,
                                Err(rejected) => {
                                    assert_eq!(
                                        rejected.reason,
                                        voteboat::transport::PeerRosterError::Overloaded
                                    );
                                    n.staged.push_back(*rejected.batch);
                                }
                            }
                        }
                        for event in roster
                            .poll(now, 3, voteboat::transport::TransportPollBudget::default())
                            .unwrap()
                        {
                            assert!(matches!(
                                event,
                                voteboat::transport::PeerPoll::Progress { .. }
                            ));
                        }
                        let local_node = n.outbound.binding().node;
                        for peer in (1..=3).map(node).filter(|id| *id != local_node) {
                            if let Some(done) = roster.take_send(peer).unwrap() {
                                n.outbound.complete(done.batch, done.result).unwrap();
                            }
                            if isolated.is_some_and(|id| id == peer || id == local_node) {
                                // Fixture-owned partition injection after authentication.
                                // Production ingress contains no test fault policy.
                                if roster.take_received(peer).unwrap().is_some() {
                                    n.received += 1;
                                }
                            } else {
                                match n.ingress.receive(roster, peer) {
                                    Ok(Some(_)) => n.received += 1,
                                    Ok(None) => (),
                                    Err(rejected) => {
                                        assert_eq!(rejected.reason, IngressError::Overloaded);
                                        assert!(
                                            rejected.batch.is_none(),
                                            "overload must leave the transport owning its frame"
                                        );
                                    }
                                }
                            }
                        }
                        let progress = n.ingress.dispatch(roster, &mut n.owner, 32).unwrap();
                        assert!(progress.rejected.is_empty());
                        assert_eq!(progress.discarded, 0);
                        assert!(n.ingress.usage().bytes <= IngressLimits::default().bytes);
                    }
                }
                assert!(
                    n.owner.usage().reserved_bytes <= EffectOwnerLimits::default().reserved_bytes
                );
                assert_eq!(
                    n.owner.usage().leased,
                    n.pending.len()
                        + n.send_pending.len()
                        + n.snapshot_pending.len()
                        + n.router.as_ref().map_or(0, |r| r.usage().requests)
                );
            }
            for _ in 0..32 {
                let Some(message) = network.pop_front() else {
                    break;
                };
                if isolated.is_some_and(|id| id == message.from || id == message.to) {
                    continue;
                }
                let target = message.to.get() as usize - 1;
                if let Err(rejected) = nodes[target]
                    .owner
                    .admit(message.group, Event::Receive(message))
                {
                    assert_eq!(rejected.reason, RuntimeError::Overloaded);
                    let Event::Receive(message) = *rejected.event else {
                        panic!()
                    };
                    network.push_back(message);
                }
            }
            if network.is_empty()
                && nodes.iter().all(|n| {
                    n.owner.is_drained()
                        && n.worker.is_drained()
                        && n.outbound.is_drained()
                        && n.pending.is_empty()
                        && n.staged.is_empty()
                        && n.ingress.is_drained()
                        && n.send_pending.is_empty()
                        && n.snapshot_pending.is_empty()
                        && n.router.as_ref().is_none_or(|r| r.is_drained())
                        && n.snapshots.as_ref().is_none_or(|w| w.is_drained())
                })
                && nodes.iter().map(|n| n.sent).sum::<usize>()
                    == nodes.iter().map(|n| n.received).sum::<usize>()
            {
                return;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    #[cfg(feature = "tls")]
    fn reconnect(nodes: &mut [Node], a: usize, b: usize, disconnected: MonoTime, ready: MonoTime) {
        let old_left = nodes[a]
            .transports
            .as_ref()
            .unwrap()
            .binding(node(b as u64 + 1))
            .unwrap();
        let old_right = nodes[b]
            .transports
            .as_ref()
            .unwrap()
            .binding(node(a as u64 + 1))
            .unwrap();
        nodes[a]
            .transports
            .as_mut()
            .unwrap()
            .disconnect(node(b as u64 + 1), disconnected)
            .unwrap();
        nodes[b]
            .transports
            .as_mut()
            .unwrap()
            .disconnect(node(a as u64 + 1), disconnected)
            .unwrap();
        let left_ticket = nodes[a]
            .transports
            .as_mut()
            .unwrap()
            .due_connections(ready, 3)
            .unwrap()
            .pop()
            .unwrap();
        let right_ticket = nodes[b]
            .transports
            .as_mut()
            .unwrap()
            .due_connections(ready, 3)
            .unwrap()
            .pop()
            .unwrap();
        assert!(left_ticket.generation > old_left.generation);
        assert!(right_ticket.generation > old_right.generation);
        establish(nodes, vec![(a, left_ticket), (b, right_ticket)], ready);
    }
    fn close(nodes: &mut [Node], now: MonoTime) {
        #[cfg(feature = "tls")]
        for n in nodes.iter_mut() {
            use voteboat::{connect::*, dial::PeerDialer};
            if let Some(mut connector) = n.connector.take() {
                connector.close();
                let deadline = Instant::now() + Duration::from_secs(5);
                while !connector.is_drained() {
                    for event in connector.poll(now, ConnectPollBudget::default()).unwrap() {
                        assert!(event.result.is_err());
                    }
                    assert!(Instant::now() < deadline, "connector did not drain");
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                let mut dialer = connector
                    .into_dialer()
                    .unwrap_or_else(|_| panic!("connector not reclaimable"));
                assert!(dialer.is_drained());
                while !dialer.try_finish().unwrap() {
                    assert!(
                        Instant::now() < deadline,
                        "connector dial worker did not finish"
                    );
                    std::thread::yield_now();
                }
            }
        }
        for n in nodes.iter_mut() {
            if let Some(roster) = &mut n.transports {
                roster.close();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while nodes
            .iter()
            .any(|n| n.transports.as_ref().is_some_and(|r| !r.is_drained()))
        {
            for n in nodes.iter_mut() {
                if let Some(roster) = &mut n.transports {
                    roster
                        .poll(now, 3, voteboat::transport::TransportPollBudget::default())
                        .unwrap();
                    for peer in (1..=3).map(node) {
                        assert!(roster.take_send(peer).unwrap().is_none());
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "native peer roster did not close"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
        for n in nodes {
            n.ingress.close();
            assert!(n.ingress.is_drained());
            n.results.close();
            assert!(n.results.is_drained());
            n.read_results.close();
            assert!(n.read_results.is_drained());
            n.clients.close();
            assert!(
                n.clients.is_drained(),
                "all client waits must resolve before healthy shutdown"
            );
            assert!(n
                .transports
                .as_ref()
                .is_none_or(|r| r.usage().reserved_bytes == 0));
            n.owner.close_admission().unwrap();
            assert!(n.owner.is_drained());
            assert!(n.snapshot_pending.is_empty());
            assert!(n.send_pending.is_empty());
            assert!(n.router.as_ref().is_none_or(|r| r.is_drained()));
            if let Some(worker) = &mut n.snapshots {
                worker.close();
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    if let Some(stores) = worker.try_reclaim().unwrap() {
                        drop(stores);
                        break;
                    }
                    assert!(Instant::now() < deadline);
                    std::thread::park_timeout(Duration::from_millis(1));
                }
            }
            n.worker.close();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(store) = n.worker.try_reclaim().unwrap() {
                    drop(store);
                    break;
                }
                assert!(Instant::now() < deadline);
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
    fn proposals(
        nodes: &mut [Node],
        leader: usize,
        operation: u128,
        delta: i64,
        isolated: Option<NodeId>,
        now: MonoTime,
    ) {
        let before = nodes[leader].client_applied;
        for g in 1..=100 {
            let n = &mut nodes[leader];
            n.clients
                .submit(
                    &mut n.owner,
                    &n.apps[&group(g)],
                    ClientRequest {
                        group: group(g),
                        operation: OperationId::new(operation).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                )
                .unwrap();
        }
        drain(nodes, isolated, now);
        let expected = if isolated == Some(node(leader as u64 + 1)) {
            0
        } else {
            100
        };
        assert_eq!(nodes[leader].client_applied - before, expected);
    }
    #[test]
    fn hundred_compacted_groups_catch_up_over_workers_and_authenticated_transport() {
        let root =
            std::env::temp_dir().join(format!("voteboat-snapshot-router-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut nodes = (1..=3)
            .map(|id| make_snapshots(id, &root.join(id.to_string()), false, true))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for g in 1..=100 {
            nodes[0].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        assert_eq!(nodes[2].installed, 100);
        assert!(nodes[0].supplied >= 100);
        for n in &nodes {
            for g in 1..=100 {
                assert!(n.owner.core(group(g)).unwrap().state().snapshot.is_some());
                let app = &n.apps[&group(g)];
                assert_eq!(app.read_applied(app.applied_index()), Ok(7));
            }
        }
        // No explicit heartbeat: the native timer provider drives these sends.
        #[cfg(feature = "tls")]
        reconnect(&mut nodes, 0, 1, MonoTime(0), MonoTime(100));
        #[cfg(feature = "tls")]
        let before_heartbeat = nodes[0].sent;
        let before_deadline = nodes[0].owner.deadline(group(1)).unwrap().deadline;
        drain(&mut nodes, None, MonoTime(100));
        assert!(nodes[0].owner.deadline(group(1)).unwrap().deadline > before_deadline);
        #[cfg(feature = "tls")]
        assert!(nodes[0].sent > before_heartbeat);
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(101));
        proposals(&mut nodes, 0, 2, 3, None, MonoTime(101));
        checkpoints(&mut nodes, MonoTime(101));
        // Continue writing with both workers still owning all storage handles.
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(101));
        checkpoints(&mut nodes, MonoTime(101));
        for g in 1..=100 {
            nodes[0]
                .owner
                .admit(
                    group(g),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, None, MonoTime(101));
        assert_eq!(nodes[0].reads, vec![10; 100]);
        let bindings = nodes
            .iter()
            .map(|n| n.owner.identity().store)
            .collect::<Vec<_>>();
        close(&mut nodes, MonoTime(101));
        drop(nodes);
        let mut nodes = (1..=3)
            .map(|id| make_snapshots(id, &root.join(id.to_string()), true, true))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for (i, n) in nodes.iter().enumerate() {
            assert_ne!(n.owner.identity().store, bindings[i]);
            for app in n.apps.values() {
                assert_eq!(app.read_applied(app.applied_index()), Ok(10));
            }
        }
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        proposals(&mut nodes, 1, 1, 7, None, MonoTime(0));
        proposals(&mut nodes, 1, 3, 5, None, MonoTime(0));
        checkpoints(&mut nodes, MonoTime(0));
        for n in &nodes {
            for app in n.apps.values() {
                assert_eq!(app.read_applied(app.applied_index()), Ok(15));
            }
        }
        close(&mut nodes, MonoTime(0));
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    }
    fn checkpoints(nodes: &mut [Node], now: MonoTime) {
        let before = nodes
            .iter()
            .map(|n| {
                (1..=100)
                    .map(|g| n.owner.core(group(g)).unwrap().state().snapshot.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        // Background admission has a smaller ceiling than foreground traffic.
        for start in (1..=100).step_by(8) {
            for n in nodes.iter_mut() {
                for g in start..=(start + 7).min(100) {
                    n.owner.admit(group(g), Event::Checkpoint).unwrap();
                }
            }
            drain(nodes, None, now);
        }
        for (i, n) in nodes.iter().enumerate() {
            for g in 1..=100 {
                let core = n.owner.core(group(g)).unwrap();
                let reference = core.state().snapshot.unwrap();
                assert!(reference.index > before[i][g as usize - 1].index);
                assert!(reference.generation > before[i][g as usize - 1].generation);
                assert_eq!(reference.index, n.apps[&group(g)].applied_index());
                assert_eq!(core.state().base_index(), reference.index);
                assert!(!core.has_pending_dependency());
            }
        }
    }
    fn leaders(nodes: &[Node], isolated: Option<NodeId>) -> Option<Vec<usize>> {
        (1..=100)
            .map(|g| {
                let leaders = nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, n)| {
                        Some(n.outbound.binding().node) != isolated
                            && n.owner.core(group(g)).unwrap().role() == Role::Leader
                    })
                    .map(|(i, _)| i)
                    .collect::<Vec<_>>();
                (leaders.len() == 1).then(|| leaders[0])
            })
            .collect()
    }
    fn elect_automatically(
        nodes: &mut [Node],
        isolated: Option<NodeId>,
        start: u64,
        value: Option<i64>,
    ) -> (Vec<usize>, MonoTime) {
        for tick in 0..=60 {
            let now = MonoTime(start + tick * 25);
            drain(nodes, isolated, now);
            if let Some(leaders) = leaders(nodes, isolated) {
                let caught_up = nodes
                    .iter()
                    .filter(|n| Some(n.outbound.binding().node) != isolated)
                    .all(|n| {
                        n.apps.values().all(|a| {
                            value.is_none_or(|v| a.read_applied(a.applied_index()) == Ok(v))
                        })
                    });
                if caught_up {
                    return (leaders, now);
                }
            }
        }
        panic!("automatic elections/catch-up did not converge");
    }
    fn routed_proposals(
        nodes: &mut [Node],
        leaders: &[usize],
        operation: u128,
        delta: i64,
        isolated: Option<NodeId>,
        now: MonoTime,
    ) {
        for (g, leader) in leaders.iter().enumerate() {
            let n = &mut nodes[*leader];
            let group = group(g as u128 + 1);
            n.clients
                .submit(
                    &mut n.owner,
                    &n.apps[&group],
                    ClientRequest {
                        group,
                        operation: OperationId::new(operation).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                )
                .unwrap();
        }
        drain(nodes, isolated, now);
    }
    #[test]
    fn automatic_elections_and_partition_replacement_preserve_hundred_group_history() {
        let root =
            std::env::temp_dir().join(format!("voteboat-automatic-network-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut nodes = (1..=3)
            .map(|id| make(id, &root.join(id.to_string()), false))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        let (initial, now) = elect_automatically(&mut nodes, None, 150, Some(0));
        routed_proposals(&mut nodes, &initial, 1, 7, None, now);
        let isolated_index = (0..3)
            .max_by_key(|i| initial.iter().filter(|l| **l == *i).count())
            .unwrap();
        let isolated = Some(node(isolated_index as u64 + 1));
        for (g, leader) in initial
            .iter()
            .enumerate()
            .filter(|(_, l)| **l == isolated_index)
        {
            let n = &mut nodes[*leader];
            let group = group(g as u128 + 1);
            n.clients
                .submit(
                    &mut n.owner,
                    &n.apps[&group],
                    ClientRequest {
                        group,
                        operation: OperationId::new(99).unwrap(),
                        bytes: 100i64.to_le_bytes().to_vec(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, isolated, MonoTime(now.0 + 1));
        let (replacement, now) = elect_automatically(&mut nodes, isolated, now.0 + 25, Some(7));
        assert!(replacement.iter().all(|i| *i != isolated_index));
        routed_proposals(&mut nodes, &replacement, 2, 3, isolated, now);
        let (healed, now) = elect_automatically(&mut nodes, None, now.0 + 25, Some(10));
        routed_proposals(&mut nodes, &healed, 1, 7, None, now);
        for (g, leader) in healed.iter().enumerate() {
            nodes[*leader]
                .owner
                .admit(
                    group(g as u128 + 1),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, None, now);
        assert_eq!(nodes.iter().map(|n| n.reads.len()).sum::<usize>(), 100);
        assert!(nodes.iter().all(|n| n.reads.iter().all(|v| *v == 10)));
        for n in &nodes {
            for app in n.apps.values() {
                assert_eq!(app.read_applied(app.applied_index()), Ok(10));
            }
        }
        close(&mut nodes, now);
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn hundred_groups_run_through_reserved_owner_native_wal_and_secure_transport() {
        let root =
            std::env::temp_dir().join(format!("voteboat-effect-owner-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let mut nodes = (1..=3)
            .map(|id| make(id, &root.join(id.to_string()), false))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for g in 1..=100 {
            nodes[0].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(0));
        proposals(&mut nodes, 0, 1, 7, None, MonoTime(0));
        for g in 1..=100 {
            nodes[0]
                .owner
                .admit(
                    group(g),
                    Event::Read {
                        request: ReadRequestId::new(1).unwrap(),
                    },
                )
                .unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        assert_eq!(nodes[0].reads, vec![7; 100]);
        proposals(&mut nodes, 0, 99, 100, Some(node(1)), MonoTime(1));
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, Some(node(1)), MonoTime(1));
        proposals(&mut nodes, 1, 2, 3, Some(node(1)), MonoTime(1));
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Heartbeat).unwrap();
        }
        drain(&mut nodes, None, MonoTime(2));
        assert_eq!(nodes[0].client_unknown, 100);
        for n in &nodes {
            for a in n.apps.values() {
                assert_eq!(a.read_applied(a.applied_index()).unwrap(), 10);
            }
        }
        let old = nodes
            .iter()
            .map(|n| n.worker.binding().store)
            .collect::<Vec<_>>();
        close(&mut nodes, MonoTime(2));
        drop(nodes);
        let mut nodes = (1..=3)
            .map(|id| make(id, &root.join(id.to_string()), true))
            .collect::<Vec<_>>();
        #[cfg(feature = "tls")]
        mesh(&mut nodes);
        for (id, n) in nodes.iter().enumerate() {
            assert_ne!(n.worker.binding().store, old[id]);
            for a in n.apps.values() {
                assert_eq!(a.read_applied(a.applied_index()).unwrap(), 10);
            }
        }
        for g in 1..=100 {
            nodes[1].owner.admit(group(g), Event::Campaign).unwrap();
        }
        drain(&mut nodes, None, MonoTime(0));
        proposals(&mut nodes, 1, 1, 7, None, MonoTime(0));
        proposals(&mut nodes, 1, 3, 5, None, MonoTime(0));
        for n in &nodes {
            for a in n.apps.values() {
                assert_eq!(a.read_applied(a.applied_index()).unwrap(), 15);
            }
        }
        close(&mut nodes, MonoTime(0));
        drop(nodes);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn durable_before_written_fences_without_releasing_a_vote() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![lease], now)
        .unwrap();
    let _written = worker.poll(1).pop().unwrap();
    let durable = worker.poll(1).pop().unwrap();
    assert_eq!(
        owner.deliver_worker(durable, now),
        Err(EffectOwnerError::ProviderContract)
    );
    assert!(owner.core(group(1)).unwrap().is_fenced());
    assert_eq!(owner.core(group(1)).unwrap().state().hard_state.term, 0);
    assert!(owner.take_effect().is_err());
}
#[test]
fn callback_capacity_and_reservation_extension_are_bounded() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    let lease = loop {
        for event in worker.poll(1) {
            owner.deliver_worker(event, now).unwrap();
        }
        owner.advance(now, 1).unwrap();
        if let Some(lease) = owner.take_effect().unwrap() {
            if matches!(lease.effect, Effect::Committed(_)) {
                break lease;
            }
            owner
                .submit_persists(&mut worker, vec![lease], now)
                .unwrap();
        }
    };
    let before = owner.usage().reserved_bytes;
    assert_eq!(
        owner.extend_reservation(lease.ticket, EffectOwnerLimits::default().reserved_bytes),
        Err(EffectOwnerError::Overloaded)
    );
    assert_eq!(owner.usage().reserved_bytes, before);
    owner.extend_reservation(lease.ticket, 4096).unwrap();
    let capacity = owner.usage().reserved_bytes / (4 * std::mem::size_of::<Effect>()) + 1;
    let rejected = owner
        .complete_effect(lease, 1, now, |_| Ok((Vec::with_capacity(capacity), ())))
        .unwrap_err();
    assert_eq!(rejected.reason, EffectOwnerError::ReservationTooLarge);
    assert!(owner.core(group(1)).unwrap().is_fenced());
    assert_eq!(owner.usage().leased, 1);
    owner.discard_failed(*rejected.lease).unwrap();
    assert_eq!(owner.usage().reserved_bytes, 0);
}
#[test]
fn closing_drains_accepted_work_and_disarms_automatic_timers() {
    let (mut owner, mut worker) = single(1);
    let now = MonoTime(400);
    owner.advance(now, 1).unwrap();
    let lease = owner.take_effect().unwrap().unwrap();
    owner
        .submit_persists(&mut worker, vec![lease], now)
        .unwrap();
    owner.close_admission().unwrap();
    assert_eq!(
        owner.admit(group(1), Event::Heartbeat).unwrap_err().reason,
        RuntimeError::Closed
    );
    let mut apps = BTreeMap::new();
    pump(&mut owner, &mut worker, &mut apps, now);
    assert!(owner.is_drained());
    assert!(owner.deadline(group(1)).is_none());
    assert_eq!(apps[&group(1)].applied_index(), 1);
}

mod snapshot_routes {
    use super::*;
    use voteboat::{contracts::StorageError, snapshot::*, snapshot_worker::*};
    struct Worker {
        binding: SnapshotWorkerBinding,
        sequence: u64,
        pending: VecDeque<(SnapshotWorkTicket, SnapshotWork)>,
        reject: bool,
        bad_ticket: bool,
        closed: bool,
        allowance: usize,
        images: BTreeMap<GroupIdentity, Snapshot>,
    }
    impl SnapshotWorker for Worker {
        fn checkpoint_bytes(&self, _: GroupIdentity) -> Option<usize> {
            Some(self.allowance / 2)
        }
        fn binding(&self) -> SnapshotWorkerBinding {
            self.binding
        }
        fn limits(&self) -> SnapshotWorkLimits {
            SnapshotWorkLimits::default()
        }
        fn usage(&self) -> SnapshotWorkUsage {
            SnapshotWorkUsage {
                requests: self.pending.len(),
                bytes: self.pending.len() * self.allowance,
            }
        }
        fn load_reservation(&self, _: GroupIdentity) -> Option<usize> {
            Some(self.allowance)
        }
        fn submit(
            &mut self,
            work: SnapshotWork,
        ) -> Result<SnapshotWorkTicket, SnapshotWorkRejected> {
            if self.reject || self.closed {
                return Err(SnapshotWorkRejected {
                    reason: if self.closed {
                        SnapshotWorkError::Closed
                    } else {
                        SnapshotWorkError::Overloaded
                    },
                    work: Box::new(work),
                });
            }
            self.sequence += 1;
            let ticket = SnapshotWorkTicket {
                binding: self.binding,
                sequence: if self.bad_ticket { 0 } else { self.sequence },
            };
            self.pending.push_back((ticket, work));
            Ok(ticket)
        }
        fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent> {
            (0..limit)
                .filter_map(|_| self.pending.pop_front())
                .map(|(request, work)| {
                    let result = match work.job {
                        SnapshotJob::Reconcile { reference } => {
                            SnapshotOutput::Reconciled(reference)
                        }
                        SnapshotJob::Publish { snapshot, .. } => {
                            let reference = reference(&snapshot);
                            self.images.insert(work.visit.group, snapshot);
                            SnapshotOutput::Published(reference)
                        }
                        SnapshotJob::Load { reference, install } => SnapshotOutput::Loaded {
                            reference,
                            snapshot: self.images[&work.visit.group].clone(),
                            reconciled: install,
                        },
                    };
                    SnapshotWorkEvent {
                        request,
                        visit: work.visit,
                        result: Ok(result),
                    }
                })
                .collect()
        }
        fn close(&mut self) {
            self.closed = true;
        }
    }
    fn reference(snapshot: &Snapshot) -> SnapshotRef {
        SnapshotRef {
            store: identity(2),
            group: snapshot.metadata.bootstrap.group,
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: snapshot.metadata.bootstrap.configuration,
            index: snapshot.metadata.index,
            term: snapshot.metadata.term,
            application_schema: snapshot.metadata.application_schema,
            file_bytes: snapshot.application.len() as u64,
            checksum: 7,
        }
    }
    fn setup(count: u128, requests: usize) -> (Owner, HostWorker, Worker, SnapshotRouter) {
        let mut log = HostLogStore::new(2);
        let runtime = timed(2, &mut log, count, 3);
        let wal = HostWorker::new(log);
        let owner = EffectOwner::new(runtime, wal.binding(), EffectOwnerLimits::default()).unwrap();
        let binding = SnapshotWorkerBinding {
            store: owner.identity().store,
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        };
        let worker = Worker {
            binding,
            sequence: 0,
            pending: VecDeque::new(),
            reject: false,
            bad_ticket: false,
            closed: false,
            allowance: 65536,
            images: BTreeMap::new(),
        };
        let router = SnapshotRouter::new(
            owner.identity(),
            binding,
            SnapshotRouterLimits {
                requests,
                ..SnapshotRouterLimits::default()
            },
        )
        .unwrap();
        (owner, wal, worker, router)
    }
    fn stage(owner: &mut Owner, g: u128) -> EffectLease {
        let mut app = Counter::new(20).unwrap();
        app.apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(1).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        }])
        .unwrap();
        let snapshot = Snapshot {
            metadata: SnapshotMetadata {
                bootstrap: bootstrap(g, 3),
                index: 1,
                term: 1,
                application_schema: app.schema_version(),
            },
            application: app.checkpoint(65536).unwrap(),
        };
        let sender = StoreBinding {
            identity: identity(1),
            session: StoreSession::new(1).unwrap(),
        };
        let message = Message {
            group: group(g),
            configuration: ConfigurationId::new(1).unwrap(),
            from: node(1),
            sender,
            to: node(2),
            term: 1,
            context: RequestContext {
                origin: sender,
                sequence: 1,
            },
            rpc: Rpc::Snapshot {
                snapshot: Box::new(snapshot),
            },
        };
        owner.admit(group(g), Event::Receive(message)).unwrap();
        owner.advance(MonoTime(0), 1).unwrap();
        let lease = owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::StageSnapshot(_)));
        lease
    }
    #[test]
    fn rejection_retries_keep_exact_lease_and_stale_completions_do_not_release_it() {
        let (mut owner, mut wal, mut worker, mut router) = setup(1, 1);
        let lease = stage(&mut owner, 1);
        let ticket = lease.ticket;
        let mut app = Counter::new(20).unwrap();
        worker.reject = true;
        let rejected = router
            .submit(&mut owner, &mut worker, lease, &app)
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            SnapshotRouteError::Worker(SnapshotWorkError::Overloaded)
        );
        assert_eq!(rejected.lease.ticket, ticket);
        let charged = owner.usage().reserved_bytes;
        let rejected = router
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(owner.usage().reserved_bytes, charged);
        assert!(router.is_drained());
        worker.reject = false;
        let request = router
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap();
        assert_eq!(router.usage().requests, 1);
        assert_eq!(owner.usage().leased, 1);
        assert!(owner.take_effect().unwrap().is_none());
        let stale = SnapshotWorkEvent {
            request: SnapshotWorkTicket {
                binding: SnapshotWorkerBinding {
                    generation: SnapshotWorkerGeneration::new(2).unwrap(),
                    ..request.binding
                },
                ..request
            },
            visit: ticket.visit,
            result: Err(StorageError::Fenced),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, stale, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        let mut wrong = ticket.visit;
        wrong.owner.generation = RuntimeGeneration::new(2).unwrap();
        let stale = SnapshotWorkEvent {
            request,
            visit: wrong,
            result: Err(StorageError::Fenced),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, stale, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        assert!(!owner.is_failed());
        assert_eq!(router.usage().requests, 1);
        let event = worker.poll(1).pop().unwrap();
        router
            .deliver(&mut owner, &mut app, event, MonoTime(0))
            .unwrap();
        assert!(router.is_drained());
        let duplicate = SnapshotWorkEvent {
            request,
            visit: ticket.visit,
            result: Err(StorageError::Fenced),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, duplicate, MonoTime(0)),
            Err(SnapshotRouteError::StaleCompletion)
        );
        assert!(!owner.is_failed());
        let persist = owner.take_effect().unwrap().unwrap();
        assert!(matches!(persist.effect, Effect::Persist(_)));
        owner
            .submit_persists(&mut wal, vec![persist], MonoTime(0))
            .unwrap();
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        assert!(owner.take_effect().unwrap().is_none());
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        let installed = owner.take_effect().unwrap().unwrap();
        assert!(matches!(installed.effect, Effect::SnapshotInstalled(_)));
        assert_eq!(app.applied_index(), 0);
    }
    #[test]
    fn accepted_snapshot_failure_keeps_other_leases_until_explicit_failed_discard() {
        let (mut owner, _, mut worker, mut router) = setup(2, 2);
        let mut app = Counter::new(20).unwrap();
        let first = stage(&mut owner, 1);
        let first_visit = first.ticket.visit;
        let request = router.submit(&mut owner, &mut worker, first, &app).unwrap();
        let second = stage(&mut owner, 2);
        router
            .submit(&mut owner, &mut worker, second, &app)
            .unwrap();
        assert!(router.discard_failed(&mut owner).is_err());
        assert_eq!(router.usage().requests, 2);
        let event = SnapshotWorkEvent {
            request,
            visit: first_visit,
            result: Err(StorageError::Uncertain("lost publication".into())),
        };
        assert!(matches!(
            router.deliver(&mut owner, &mut app, event, MonoTime(0)),
            Err(SnapshotRouteError::Checkpoint(_))
        ));
        assert!(owner.is_failed());
        assert_eq!(owner.usage().leased, 1);
        assert!(owner.usage().reserved_bytes > 0);
        assert_eq!(router.usage().requests, 1);
        router.discard_failed(&mut owner).unwrap();
        assert_eq!(owner.usage().reserved_bytes, 0);
        assert!(router.is_drained());
        // Dropping leases does not undo accepted provider work.
        assert_eq!(worker.usage().requests, 2);
        worker.close();
        worker.poll(2);
        assert!(worker.is_drained());
    }
    #[test]
    fn bounded_routing_and_wrong_binding_reject_before_transfer() {
        let (mut owner, _, mut worker, mut router) = setup(2, 1);
        let app = Counter::new(20).unwrap();
        let first = stage(&mut owner, 1);
        router.submit(&mut owner, &mut worker, first, &app).unwrap();
        let second = stage(&mut owner, 2);
        let rejected = router
            .submit(&mut owner, &mut worker, second, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::Overloaded);
        assert_eq!(worker.usage().requests, 1);
        let mut other = SnapshotRouter::new(
            owner.identity(),
            SnapshotWorkerBinding {
                generation: SnapshotWorkerGeneration::new(2).unwrap(),
                ..worker.binding
            },
            SnapshotRouterLimits::default(),
        )
        .unwrap();
        let rejected = other
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::WrongWorker);
        let mut obsolete = SnapshotRouter::new(
            RuntimeOwner {
                generation: RuntimeGeneration::new(2).unwrap(),
                ..owner.identity()
            },
            worker.binding(),
            SnapshotRouterLimits::default(),
        )
        .unwrap();
        let rejected = obsolete
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::WrongOwner);
        worker.allowance = 240 * 1024 * 1024;
        let mut excessive = SnapshotRouter::new(
            owner.identity(),
            worker.binding(),
            SnapshotRouterLimits {
                requests: 1,
                max_image_bytes: 300 * 1024 * 1024,
            },
        )
        .unwrap();
        let rejected = excessive
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(
            rejected.reason,
            SnapshotRouteError::Owner(EffectOwnerError::ReservationTooLarge)
        );
        worker.allowance = usize::MAX;
        let mut roomy = SnapshotRouter::new(
            owner.identity(),
            worker.binding(),
            SnapshotRouterLimits::default(),
        )
        .unwrap();
        let rejected = roomy
            .submit(&mut owner, &mut worker, *rejected.lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::TooLarge);
        assert!(!owner.is_failed());
    }
    #[test]
    fn invalid_accepted_ticket_fences_but_returns_original_lease_for_discard() {
        let (mut owner, _, mut worker, mut router) = setup(1, 1);
        let app = Counter::new(20).unwrap();
        let lease = stage(&mut owner, 1);
        worker.bad_ticket = true;
        let rejected = router
            .submit(&mut owner, &mut worker, lease, &app)
            .unwrap_err();
        assert_eq!(rejected.reason, SnapshotRouteError::ProviderContract);
        assert!(owner.is_failed());
        assert_eq!(owner.usage().leased, 1);
        assert_eq!(worker.usage().requests, 1);
        owner.discard_failed(*rejected.lease).unwrap();
        assert_eq!(owner.usage().reserved_bytes, 0);
        worker.close();
        worker.poll(1);
    }
    #[test]
    fn closing_owner_drains_snapshot_wal_and_application_dependencies() {
        let (mut owner, mut wal, mut worker, mut router) = setup(1, 1);
        let mut app = Counter::new(20).unwrap();
        let lease = stage(&mut owner, 1);
        router.submit(&mut owner, &mut worker, lease, &app).unwrap();
        owner.close_admission().unwrap();
        assert!(!owner.is_drained());
        router
            .deliver(
                &mut owner,
                &mut app,
                worker.poll(1).pop().unwrap(),
                MonoTime(0),
            )
            .unwrap();
        let lease = owner.take_effect().unwrap().unwrap();
        owner
            .submit_persists(&mut wal, vec![lease], MonoTime(0))
            .unwrap();
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        owner
            .deliver_worker(wal.poll(1).pop().unwrap(), MonoTime(0))
            .unwrap();
        let installed = owner.take_effect().unwrap().unwrap();
        assert!(matches!(installed.effect, Effect::SnapshotInstalled(_)));
        // Closing ingress must still permit this dependency of accepted work.
        router
            .submit(&mut owner, &mut worker, installed, &app)
            .unwrap();
        router
            .deliver(
                &mut owner,
                &mut app,
                worker.poll(1).pop().unwrap(),
                MonoTime(0),
            )
            .unwrap();
        let ack = owner.take_effect().unwrap().unwrap();
        assert!(matches!(
            &ack.effect,
            Effect::Send(Message {
                rpc: Rpc::SnapshotAck { index: 1 },
                ..
            })
        ));
        assert_eq!(app.read_applied(1), Ok(7));
        owner
            .release(ack, app.applied_index(), MonoTime(0))
            .unwrap();
        assert!(owner.is_drained());
        assert!(router.is_drained());
        assert!(worker.is_drained());
        worker.close();
    }
    #[test]
    fn oversized_provider_result_fences_and_releases_only_router_owned_payload() {
        let (mut owner, _, mut worker, mut router) = setup(1, 1);
        let mut app = Counter::new(20).unwrap();
        let lease = stage(&mut owner, 1);
        let visit = lease.ticket.visit;
        let request = router.submit(&mut owner, &mut worker, lease, &app).unwrap();
        let SnapshotJob::Publish { snapshot, .. } = &worker.pending.front().unwrap().1.job else {
            panic!()
        };
        let mut snapshot = snapshot.clone();
        snapshot.application.reserve(worker.allowance * 2);
        let event = SnapshotWorkEvent {
            request,
            visit,
            result: Ok(SnapshotOutput::Loaded {
                reference: reference(&snapshot),
                snapshot,
                reconciled: true,
            }),
        };
        assert_eq!(
            router.deliver(&mut owner, &mut app, event, MonoTime(0)),
            Err(SnapshotRouteError::ProviderContract)
        );
        assert!(owner.is_failed());
        assert_eq!(owner.usage().reserved_bytes, 0);
        assert!(router.is_drained());
        assert_eq!(app.applied_index(), 0);
        assert_eq!(worker.usage().requests, 1);
        worker.close();
        worker.poll(1);
    }
}

mod ingress {
    use super::*;
    use std::{cell::RefCell, rc::Rc};
    use voteboat::{
        outbound::*,
        secure::*,
        snapshot::{Snapshot, SnapshotMetadata},
        transport::*,
    };
    #[derive(Default)]
    struct Input {
        batch: Option<ReceivedBatch>,
        takes: usize,
        wrong_class: bool,
    }
    /// Host channel security is attested only for ownership/fault scheduling.
    struct Peer {
        binding: SessionBinding,
        input: Rc<RefCell<Input>>,
    }
    impl PeerTransport for Peer {
        fn security(&self) -> SessionSecurity {
            SessionSecurity::Authenticated
        }
        fn binding(&self) -> SessionBinding {
            self.binding
        }
        fn state(&self) -> TransportState {
            TransportState::Open
        }
        fn limits(&self) -> TransportLimits {
            TransportLimits::default()
        }
        fn usage(&self) -> TransportUsage {
            TransportUsage {
                decoded_bytes: self.received_info().map_or(0, |i| i.bytes),
                ..TransportUsage::default()
            }
        }
        fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
            Err(TransportRejected {
                reason: TransportError::Overloaded,
                batch: Box::new(batch),
            })
        }
        fn poll(
            &mut self,
            _: MonoTime,
            b: TransportPollBudget,
        ) -> Result<TransportProgress, TransportError> {
            b.validate()?;
            Ok(TransportProgress::default())
        }
        fn take_send(&mut self) -> Option<TransportSend> {
            None
        }
        fn received_info(&self) -> Option<ReceiveInfo> {
            let input = self.input.borrow();
            let mut info = input
                .batch
                .as_ref()?
                .info(self.limits().decoded_bytes)
                .ok()?;
            if input.wrong_class {
                info.class = MessageClass::Data;
            }
            Some(info)
        }
        fn take_received(&mut self) -> Option<ReceivedBatch> {
            let mut input = self.input.borrow_mut();
            input.takes += 1;
            input.batch.take()
        }
        fn close(&mut self) {}
        fn abort(&mut self) {
            self.input.borrow_mut().batch = None;
        }
    }
    fn setup() -> (
        Owner,
        PeerRoster<Peer>,
        BTreeMap<NodeId, Rc<RefCell<Input>>>,
    ) {
        let mut store = HostLogStore::new(1);
        let runtime = timed(1, &mut store, 2, 3);
        let owner = EffectOwner::new(
            runtime,
            WorkerBinding {
                store: store.binding(),
                generation: StorageWorkerGeneration::new(1).unwrap(),
            },
            EffectOwnerLimits::default(),
        )
        .unwrap();
        let local = LocalIdentity {
            node: node(1),
            store: store.binding(),
        };
        let mut roster = PeerRoster::new(
            PeerRosterConfig {
                local,
                outbound: OutboundBinding {
                    node: local.node,
                    store: local.store,
                    generation: OutboundGeneration::new(1).unwrap(),
                },
                first_generation: SecureSessionGeneration::new(1).unwrap(),
                last_generation: SecureSessionGeneration::new(u64::MAX).unwrap(),
                wire_version: 1,
                limits: PeerRosterLimits::default(),
                transport_limits: TransportLimits::default(),
            },
            [(node(2), identity(2)), (node(3), identity(3))].into(),
            MonoTime(0),
        )
        .unwrap();
        let mut inputs = BTreeMap::new();
        for t in roster.due_connections(MonoTime(0), 2).unwrap() {
            let input = Rc::new(RefCell::new(Input::default()));
            let binding = SessionBinding {
                local,
                peer: LocalIdentity {
                    node: t.peer.node,
                    store: StoreBinding {
                        identity: t.peer.store,
                        session: StoreSession::new(1).unwrap(),
                    },
                },
                generation: t.generation,
                wire_version: 1,
            };
            roster
                .attach(
                    t,
                    Peer {
                        binding,
                        input: input.clone(),
                    },
                    MonoTime(0),
                )
                .unwrap_or_else(|r| panic!("attach: {:?}", r.reason));
            inputs.insert(t.peer.node, input);
        }
        (owner, roster, inputs)
    }
    fn router(owner: &Owner, roster: &PeerRoster<Peer>, limits: IngressLimits) -> IngressRouter {
        IngressRouter::new(
            IngressBinding {
                owner: owner.identity(),
                local: roster.local(),
                generation: IngressGeneration::new(1).unwrap(),
            },
            limits,
        )
        .unwrap()
    }
    fn message(binding: SessionBinding, g: u128, class: MessageClass) -> Message {
        let rpc = match class {
            MessageClass::Control => Rpc::ReadProbe,
            MessageClass::Data => Rpc::Append {
                previous_index: 0,
                previous_term: 0,
                leader_commit: 0,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(g).unwrap(),
                        bytes: 1i64.to_le_bytes().to_vec(),
                    },
                }],
            },
            MessageClass::Background => Rpc::Snapshot {
                snapshot: Box::new(Snapshot {
                    metadata: SnapshotMetadata {
                        bootstrap: bootstrap(g, 3),
                        index: 1,
                        term: 1,
                        application_schema: 1,
                    },
                    application: vec![],
                }),
            },
        };
        Message {
            group: group(g),
            configuration: ConfigurationId::new(1).unwrap(),
            from: binding.peer.node,
            sender: binding.peer.store,
            to: binding.local.node,
            term: 1,
            context: RequestContext {
                origin: binding.peer.store,
                sequence: g as u64,
            },
            rpc,
        }
    }
    fn inject(
        roster: &PeerRoster<Peer>,
        input: &Rc<RefCell<Input>>,
        peer: u64,
        groups: &[u128],
        class: MessageClass,
    ) {
        assert!(input.borrow().batch.is_none());
        let connection = roster.binding(node(peer)).unwrap();
        input.borrow_mut().batch = Some(ReceivedBatch {
            connection,
            messages: groups
                .iter()
                .map(|g| message(connection, *g, class))
                .collect(),
        });
    }
    fn small() -> IngressLimits {
        IngressLimits {
            batches: 4,
            messages: 512,
            bytes: 1024 * 1024,
            control_batches: 1,
            control_messages: 128,
            control_bytes: 128 * 1024,
            background_batches: 1,
            background_messages: 128,
            background_bytes: 256 * 1024,
            batch_messages: 128,
            batch_bytes: 64 * 1024,
        }
    }
    #[test]
    fn blocked_group_retains_full_frame_charge_while_other_group_progresses_and_retry_completes() {
        let (mut owner, mut roster, inputs) = setup();
        let mut router = router(&owner, &roster, IngressLimits::default());
        for _ in 0..ShardLimits::default().group_items {
            owner.admit(group(1), Event::Heartbeat).unwrap();
        }
        inject(
            &roster,
            &inputs[&node(2)],
            2,
            &[1, 2],
            MessageClass::Control,
        );
        let ticket = router.receive(&mut roster, node(2)).unwrap().unwrap();
        let charged = router.usage();
        assert!(router
            .dispatch(&roster, &mut owner, 0)
            .unwrap()
            .completed
            .is_empty());
        assert_eq!(router.usage(), charged);
        let blocked = router.dispatch(&roster, &mut owner, 1).unwrap();
        assert_eq!(blocked.blocked, 1);
        assert_eq!(router.usage(), charged);
        let other = router.dispatch(&roster, &mut owner, 1).unwrap();
        assert_eq!(other.admitted, 1);
        assert!(other.completed.is_empty());
        assert_eq!(router.usage(), charged);
        // Free one event in group 1 without performing storage or application I/O.
        let step = owner.advance(MonoTime(0), 1).unwrap();
        assert_eq!(step.len(), 1);
        assert_eq!(step[0].visit.group, group(1));
        assert!(owner.take_effect().unwrap().is_none());
        let completed = router.dispatch(&roster, &mut owner, 1).unwrap();
        assert_eq!(completed.admitted, 1);
        assert_eq!(completed.completed.len(), 1);
        assert_eq!(completed.completed[0].ticket, ticket);
        assert_eq!(completed.completed[0].admitted, 2);
        assert_eq!(completed.completed[0].end, IngressEnd::Drained);
        assert_eq!(router.usage(), IngressUsage::default());
        assert!(router.is_drained());
        assert_eq!(
            owner.core(group(1)).unwrap().state().hard_state.term,
            0,
            "ingress admission proves no election or durable progress"
        );
    }
    #[test]
    fn bulk_saturation_leaves_transport_ownership_and_control_reserve_for_another_peer() {
        let (owner, mut roster, inputs) = setup();
        let mut router = router(&owner, &roster, small());
        for _ in 0..3 {
            inject(&roster, &inputs[&node(2)], 2, &[1], MessageClass::Data);
            router.receive(&mut roster, node(2)).unwrap().unwrap();
        }
        inject(&roster, &inputs[&node(2)], 2, &[1], MessageClass::Data);
        let takes = inputs[&node(2)].borrow().takes;
        let rejected = router.receive(&mut roster, node(2)).unwrap_err();
        assert_eq!(rejected.reason, IngressError::Overloaded);
        assert!(rejected.batch.is_none());
        assert_eq!(inputs[&node(2)].borrow().takes, takes);
        assert!(inputs[&node(2)].borrow().batch.is_some());
        inject(&roster, &inputs[&node(3)], 3, &[2], MessageClass::Control);
        router.receive(&mut roster, node(3)).unwrap().unwrap();
        assert_eq!(router.usage().batches, 4);
        let completed = router.abort();
        assert_eq!(completed.len(), 4);
        assert!(completed
            .iter()
            .all(|c| c.end == IngressEnd::Cancelled && c.discarded == 1 && c.admitted == 0));
        assert!(router.is_drained());
        assert_eq!(router.usage(), IngressUsage::default());
        assert!(inputs[&node(2)].borrow().batch.is_some());
        assert!(owner.is_drained());
    }
    #[test]
    fn snapshots_have_separate_ceiling_and_cannot_consume_all_data_or_control_slots() {
        let (owner, mut roster, inputs) = setup();
        let mut router = router(&owner, &roster, small());
        inject(
            &roster,
            &inputs[&node(2)],
            2,
            &[1],
            MessageClass::Background,
        );
        router.receive(&mut roster, node(2)).unwrap().unwrap();
        inject(
            &roster,
            &inputs[&node(3)],
            3,
            &[1],
            MessageClass::Background,
        );
        assert_eq!(
            router.receive(&mut roster, node(3)).unwrap_err().reason,
            IngressError::Overloaded
        );
        inject(&roster, &inputs[&node(2)], 2, &[1], MessageClass::Data);
        router.receive(&mut roster, node(2)).unwrap().unwrap();
        inject(&roster, &inputs[&node(2)], 2, &[2], MessageClass::Control);
        router.receive(&mut roster, node(2)).unwrap().unwrap();
        assert_eq!(router.usage().batches, 3);
        assert_eq!(router.abort().len(), 3);
        assert!(inputs[&node(3)].borrow().batch.is_some());
    }
    #[test]
    fn retired_connection_discards_only_still_held_input_and_new_generation_can_progress() {
        let (mut owner, mut roster, mut inputs) = setup();
        let mut router = router(&owner, &roster, IngressLimits::default());
        inject(
            &roster,
            &inputs[&node(2)],
            2,
            &[1, 2],
            MessageClass::Control,
        );
        let old = router.receive(&mut roster, node(2)).unwrap().unwrap();
        assert_eq!(router.dispatch(&roster, &mut owner, 1).unwrap().admitted, 1);
        let old_binding = roster.binding(node(2)).unwrap();
        roster.disconnect(node(2), MonoTime(0)).unwrap();
        let t = roster
            .due_connections(MonoTime(100), 2)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(t.peer.node, node(2));
        let binding = SessionBinding {
            generation: t.generation,
            ..old_binding
        };
        let input = Rc::new(RefCell::new(Input::default()));
        roster
            .attach(
                t,
                Peer {
                    binding,
                    input: input.clone(),
                },
                MonoTime(100),
            )
            .unwrap_or_else(|r| panic!("reattach: {:?}", r.reason));
        inputs.insert(node(2), input);
        let progress = router.dispatch(&roster, &mut owner, 1).unwrap();
        assert_eq!(progress.discarded, 1);
        assert_eq!(progress.admitted, 0);
        let c = progress.completed[0];
        assert_eq!(c.ticket, old);
        assert_eq!(c.admitted, 1);
        assert_eq!(c.discarded, 1);
        assert_eq!(c.end, IngressEnd::StaleConnection);
        assert!(
            !owner.is_drained(),
            "already admitted old input remains owned by the runtime"
        );
        inject(&roster, &inputs[&node(2)], 2, &[2], MessageClass::Control);
        let fresh = router.receive(&mut roster, node(2)).unwrap().unwrap();
        assert!(fresh.sequence > old.sequence);
        assert_eq!(
            router.dispatch(&roster, &mut owner, 1).unwrap().completed[0].admitted,
            1
        );
        assert!(router.is_drained());
    }
    #[test]
    fn false_receive_metadata_returns_original_batch_and_fences_without_credit_release() {
        let (mut owner, mut roster, inputs) = setup();
        let mut router = router(&owner, &roster, IngressLimits::default());
        inject(&roster, &inputs[&node(2)], 2, &[1], MessageClass::Control);
        router.receive(&mut roster, node(2)).unwrap().unwrap();
        let charged = router.usage();
        inject(&roster, &inputs[&node(2)], 2, &[2], MessageClass::Control);
        inputs[&node(2)].borrow_mut().wrong_class = true;
        let rejected = router.receive(&mut roster, node(2)).unwrap_err();
        assert_eq!(
            rejected.reason,
            IngressError::Peer(PeerRosterError::ProviderViolation)
        );
        let batch = rejected.batch.unwrap();
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].group, group(2));
        assert!(roster.is_fenced());
        assert_eq!(router.usage(), charged);
        assert!(matches!(
            router.dispatch(&roster, &mut owner, 1),
            Err(IngressError::Peer(PeerRosterError::Fenced))
        ));
        assert_eq!(router.abort()[0].discarded, 1);
        assert!(owner.is_drained());
    }
    #[test]
    fn capacity_bytes_are_inspected_before_take_and_wrong_owner_cannot_dispatch() {
        let (mut owner, mut roster, inputs) = setup();
        let mut router = router(
            &owner,
            &roster,
            IngressLimits {
                batch_bytes: 1024,
                ..IngressLimits::default()
            },
        );
        let connection = roster.binding(node(2)).unwrap();
        let mut messages = Vec::with_capacity(256);
        messages.push(message(connection, 1, MessageClass::Control));
        inputs[&node(2)].borrow_mut().batch = Some(ReceivedBatch {
            connection,
            messages,
        });
        let takes = inputs[&node(2)].borrow().takes;
        let rejected = router.receive(&mut roster, node(2)).unwrap_err();
        assert_eq!(rejected.reason, IngressError::BatchTooLarge);
        assert!(rejected.batch.is_none());
        assert_eq!(inputs[&node(2)].borrow().takes, takes);
        assert!(router.is_drained());
        // This host explicitly abandons the oversized input, then offers a valid one.
        inputs[&node(2)].borrow_mut().batch = None;
        inject(&roster, &inputs[&node(2)], 2, &[1], MessageClass::Control);
        let mut binding = router.binding();
        binding.owner.generation = RuntimeGeneration::new(2).unwrap();
        let mut replacement = IngressRouter::new(binding, IngressLimits::default()).unwrap();
        replacement.receive(&mut roster, node(2)).unwrap().unwrap();
        let charged = replacement.usage();
        assert_eq!(
            replacement.dispatch(&roster, &mut owner, 1).unwrap_err(),
            IngressError::WrongBinding
        );
        assert_eq!(replacement.usage(), charged);
        assert_eq!(replacement.abort()[0].discarded, 1);
    }
    #[test]
    fn unknown_group_reports_terminal_rejection_and_closing_one_router_preserves_shared_owner() {
        let (mut owner, mut roster, inputs) = setup();
        let mut first = router(&owner, &roster, IngressLimits::default());
        inject(&roster, &inputs[&node(2)], 2, &[999], MessageClass::Control);
        let ticket = first.receive(&mut roster, node(2)).unwrap().unwrap();
        first.close();
        let progress = first.dispatch(&roster, &mut owner, 1).unwrap();
        assert_eq!(progress.rejected.len(), 1);
        assert_eq!(progress.rejected[0].reason, RuntimeError::UnknownGroup);
        assert_eq!(progress.completed[0].ticket, ticket);
        assert_eq!(progress.completed[0].rejected, 1);
        assert_eq!(progress.completed[0].admitted, 0);
        assert!(first.is_drained());
        let mut binding = first.binding();
        binding.generation = IngressGeneration::new(2).unwrap();
        let mut second = IngressRouter::new(binding, IngressLimits::default()).unwrap();
        inject(&roster, &inputs[&node(2)], 2, &[1], MessageClass::Control);
        assert_eq!(
            first.receive(&mut roster, node(2)).unwrap_err().reason,
            IngressError::Closed
        );
        let fresh = second.receive(&mut roster, node(2)).unwrap().unwrap();
        assert_ne!(fresh, ticket);
        assert_eq!(second.dispatch(&roster, &mut owner, 1).unwrap().admitted, 1);
        assert!(!owner.is_drained());
        assert_eq!(second.usage(), IngressUsage::default());
    }
}

mod read_results {
    use super::*;
    use std::cell::Cell;
    struct App {
        counter: Counter,
        calls: Cell<usize>,
        bound: usize,
        invalid: bool,
        fail_query: bool,
    }
    impl StateMachine for App {
        type Receipt = CounterReceipt;
        fn applied_index(&self) -> u64 {
            self.counter.applied_index()
        }
        fn apply_batch(
            &mut self,
            entries: &[LogEntry],
        ) -> Result<Vec<CounterReceipt>, ApplicationError> {
            self.counter.apply_batch(entries)
        }
    }
    impl ReadableStateMachine for App {
        type Query = Vec<u8>;
        type ReadResult = Vec<u8>;
        fn read_at(&self, index: u64, query: Vec<u8>) -> Result<Vec<u8>, ApplicationError> {
            self.calls.set(self.calls.get() + 1);
            self.counter.read_applied(index)?;
            if self.fail_query {
                return Err(ApplicationError::InvalidCommand);
            }
            Ok(query)
        }
    }
    impl BoundedReadableStateMachine for App {
        fn query_bytes(&self, q: &Vec<u8>, _: usize) -> Result<usize, ApplicationError> {
            Ok(q.capacity())
        }
        fn read_result_bound(&self, _: &Vec<u8>) -> Result<usize, ApplicationError> {
            Ok(self.bound)
        }
        fn read_result_bytes(&self, r: &Vec<u8>, _: usize) -> Result<usize, ApplicationError> {
            Ok(if self.invalid {
                usize::MAX
            } else {
                r.capacity()
            })
        }
    }
    fn setup(count: u128) -> (Owner, App) {
        let (mut owner, mut worker) = single(count);
        let mut apps = BTreeMap::new();
        pump(&mut owner, &mut worker, &mut apps, MonoTime(400));
        (
            owner,
            App {
                counter: apps.remove(&group(1)).unwrap(),
                calls: Cell::new(0),
                bound: 128,
                invalid: false,
                fail_query: false,
            },
        )
    }
    fn router<R>(owner: &Owner, generation: u64, limits: ReadRouterLimits) -> ReadRouter<R> {
        ReadRouter::new(
            ReadRouterBinding {
                owner: owner.identity(),
                generation: ReadRouterGeneration::new(generation).unwrap(),
            },
            limits,
        )
        .unwrap()
    }
    fn read(owner: &mut Owner, g: u128, request: u64) -> EffectLease {
        owner
            .admit(
                group(g),
                Event::Read {
                    request: ReadRequestId::new(request).unwrap(),
                },
            )
            .unwrap();
        let steps = owner.advance(MonoTime(400), 1).unwrap();
        assert_eq!(steps.len(), 1);
        assert!(steps[0].error.is_none(), "{:?}", steps[0].error);
        let lease = owner.take_effect().unwrap().unwrap();
        assert!(matches!(lease.effect, Effect::ReadReady(_)));
        lease
    }
    fn rejected<Q>(error: ReadSubmitError<Q>) -> (ReadRouteError, EffectLease, Q) {
        match error {
            ReadSubmitError::Rejected {
                reason,
                lease,
                query,
            } => (reason, *lease, query),
            ReadSubmitError::Failed { reason } => panic!("unexpected consumed failure: {reason:?}"),
        }
    }
    #[test]
    fn global_byte_backpressure_and_wrong_effect_do_not_run_application() {
        let (mut owner, app) = setup(1);
        let mut probe = router::<Vec<u8>>(&owner, 1, ReadRouterLimits::default());
        let lease = read(&mut owner, 1, 1);
        probe
            .submit(&mut owner, lease, &app, vec![], MonoTime(400))
            .unwrap();
        let bytes = probe.usage().bytes;
        let output = probe.poll().unwrap();
        probe.complete(output).unwrap().unwrap();
        let mut r = router::<Vec<u8>>(
            &owner,
            2,
            ReadRouterLimits {
                bytes,
                result_bytes: 128,
                ..Default::default()
            },
        );
        let lease = read(&mut owner, 1, 2);
        r.submit(&mut owner, lease, &app, vec![], MonoTime(400))
            .unwrap();
        let first = r.poll().unwrap();
        let lease = read(&mut owner, 1, 3);
        let (reason, lease, query) = rejected(
            r.submit(&mut owner, lease, &app, vec![], MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::Overloaded);
        assert_eq!(app.calls.get(), 2);
        assert_eq!(r.usage().bytes, bytes);
        r.complete(first).unwrap().unwrap();
        r.submit(&mut owner, lease, &app, query, MonoTime(400))
            .unwrap();
        let output = r.poll().unwrap();
        r.complete(output).unwrap().unwrap();
        assert_eq!(app.calls.get(), 3);

        let (mut other, _worker) = single(1);
        other.admit(group(1), Event::Campaign).unwrap();
        other.advance(MonoTime(0), 1).unwrap();
        let lease = other.take_effect().unwrap().unwrap();
        let mut other_results = router::<Vec<u8>>(&other, 1, ReadRouterLimits::default());
        let (reason, lease, _) = rejected(
            other_results
                .submit(&mut other, lease, &app, vec![], MonoTime(0))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::WrongEffect);
        assert!(matches!(lease.effect, Effect::Persist(_)));
        assert_eq!(app.calls.get(), 3);
        assert!(other_results.is_drained());
        assert!(!other.is_failed());
    }
    #[test]
    fn original_read_is_one_use_and_poll_keeps_allocation_credits() {
        let (mut owner, app) = setup(1);
        let mut r = router::<Vec<u8>>(&owner, 1, ReadRouterLimits::default());
        let lease = read(&mut owner, 1, 1);
        let duplicate = EffectLease {
            ticket: lease.ticket,
            effect: lease.effect.clone(),
        };
        let ticket = r
            .submit(&mut owner, lease, &app, vec![7; 48], MonoTime(400))
            .unwrap();
        assert_eq!(app.calls.get(), 1);
        let used = r.usage();
        assert!(used.bytes >= 48 + 128);
        let output = r.poll().unwrap();
        assert_eq!(output.ticket(), ticket);
        assert_eq!(output.barrier().request().get(), 1);
        assert_eq!(output.barrier().group(), group(1));
        assert_eq!(output.result().as_ref().unwrap(), &vec![7; 48]);
        assert_eq!(r.usage(), used);
        let (reason, _, query) = rejected(
            r.submit(&mut owner, duplicate, &app, vec![8], MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::Owner(EffectOwnerError::StaleEffect));
        assert_eq!(query, vec![8]);
        assert_eq!(app.calls.get(), 1);
        let mut foreign = router::<Vec<u8>>(&owner, 2, ReadRouterLimits::default());
        let error = foreign.complete(output).unwrap_err();
        assert_eq!(error.reason, ReadRouteError::StaleResult);
        assert_eq!(r.usage(), used);
        assert_eq!(r.complete(*error.results).unwrap().unwrap(), vec![7; 48]);
        assert_eq!(r.usage(), ReadResultUsage::default());
        assert!(r.is_drained());
    }
    #[test]
    fn per_group_overload_retains_original_query_and_other_group_progresses() {
        let (mut owner, app) = setup(2);
        let mut r = router::<Vec<u8>>(
            &owner,
            1,
            ReadRouterLimits {
                results: 2,
                group_results: 1,
                ..Default::default()
            },
        );
        let lease = read(&mut owner, 1, 1);
        r.submit(&mut owner, lease, &app, vec![1], MonoTime(400))
            .unwrap();
        let first = r.poll().unwrap();
        let lease = read(&mut owner, 1, 2);
        let query = Vec::with_capacity(64);
        let pointer = query.as_ptr();
        let (reason, lease, query) = rejected(
            r.submit(&mut owner, lease, &app, query, MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::Overloaded);
        assert_eq!(pointer, query.as_ptr());
        assert_eq!(app.calls.get(), 1);
        let other = read(&mut owner, 2, 1);
        r.submit(&mut owner, other, &app, vec![2], MonoTime(400))
            .unwrap();
        assert_eq!(app.calls.get(), 2);
        r.complete(first).unwrap().unwrap();
        r.submit(&mut owner, lease, &app, query, MonoTime(400))
            .unwrap();
        assert_eq!(app.calls.get(), 3);
        r.close();
        while let Some(output) = r.poll() {
            r.complete(output).unwrap().unwrap();
        }
        assert!(r.is_drained());
        let lease = read(&mut owner, 1, 3);
        let (reason, lease, _) = rejected(
            r.submit(&mut owner, lease, &app, vec![], MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::Closed);
        owner
            .release(lease, app.applied_index(), MonoTime(400))
            .unwrap();
    }
    #[test]
    fn application_catchup_retries_without_consuming_query_or_barrier() {
        let (mut owner, app) = setup(1);
        let mut r = router::<Vec<u8>>(&owner, 1, ReadRouterLimits::default());
        let lease = read(&mut owner, 1, 1);
        let lagging = App {
            counter: Counter::new(100).unwrap(),
            ..app
        };
        let (reason, lease, query) = rejected(
            r.submit(&mut owner, lease, &lagging, vec![4], MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(
            reason,
            ReadRouteError::Owner(EffectOwnerError::Consensus(RaftError::NotApplied))
        );
        assert_eq!(lagging.calls.get(), 0);
        assert_eq!(r.usage(), ReadResultUsage::default());
        let mut caught_up = lagging;
        caught_up
            .counter
            .apply_batch(owner.core(group(1)).unwrap().replay_committed())
            .unwrap();
        r.submit(&mut owner, lease, &caught_up, query, MonoTime(400))
            .unwrap();
        let output = r.poll().unwrap();
        assert_eq!(r.complete(output).unwrap().unwrap(), vec![4]);
        assert_eq!(caught_up.calls.get(), 1);
    }
    #[test]
    fn nested_capacity_and_result_bound_reject_before_callback_and_wrong_runtime_is_intact() {
        let (mut owner, mut app) = setup(1);
        let mut r = router::<Vec<u8>>(
            &owner,
            1,
            ReadRouterLimits {
                query_bytes: 32,
                result_bytes: 128,
                ..Default::default()
            },
        );
        let lease = read(&mut owner, 1, 1);
        let query = Vec::with_capacity(64);
        let pointer = query.as_ptr();
        let (reason, lease, query) = rejected(
            r.submit(&mut owner, lease, &app, query, MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::ResultTooLarge);
        assert_eq!(pointer, query.as_ptr());
        app.bound = 129;
        let (reason, lease, _) = rejected(
            r.submit(&mut owner, lease, &app, vec![], MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::ResultTooLarge);
        assert_eq!(app.calls.get(), 0);
        let mut foreign = router::<Vec<u8>>(&owner, 2, ReadRouterLimits::default());
        // A different runtime generation is not the same owner even at this store.
        foreign = ReadRouter::new(
            ReadRouterBinding {
                owner: RuntimeOwner {
                    generation: RuntimeGeneration::new(999).unwrap(),
                    ..foreign.binding().owner
                },
                ..foreign.binding()
            },
            ReadRouterLimits::default(),
        )
        .unwrap();
        let (reason, lease, _) = rejected(
            foreign
                .submit(&mut owner, lease, &app, vec![], MonoTime(400))
                .unwrap_err(),
        );
        assert_eq!(reason, ReadRouteError::WrongBinding);
        owner
            .release(lease, app.applied_index(), MonoTime(400))
            .unwrap();
        assert!(!owner.is_failed());
    }
    #[test]
    fn query_failure_consumes_authority_but_invalid_provider_fences_without_success() {
        let (mut owner, mut app) = setup(1);
        let mut r = router::<Vec<u8>>(&owner, 1, ReadRouterLimits::default());
        app.fail_query = true;
        let lease = read(&mut owner, 1, 1);
        r.submit(&mut owner, lease, &app, vec![], MonoTime(400))
            .unwrap();
        let output = r.poll().unwrap();
        assert_eq!(
            r.complete(output).unwrap(),
            Err(ApplicationError::InvalidCommand)
        );
        assert!(!owner.is_failed());
        app.fail_query = false;
        let lease = read(&mut owner, 1, 2);
        r.submit(&mut owner, lease, &app, vec![9], MonoTime(400))
            .unwrap();
        let valid_before_failure = r.poll().unwrap();
        app.invalid = true;
        let lease = read(&mut owner, 1, 3);
        assert!(matches!(
            r.submit(&mut owner, lease, &app, vec![1], MonoTime(400)),
            Err(ReadSubmitError::Failed {
                reason: ReadRouteError::ProviderViolation
            })
        ));
        assert!(owner.is_failed());
        assert!(r.poll().is_none());
        assert_eq!(r.usage().results, 1);
        assert_eq!(r.complete(valid_before_failure).unwrap().unwrap(), vec![9]);
        assert!(r.is_drained());
        assert_eq!(app.calls.get(), 3);
    }
}
