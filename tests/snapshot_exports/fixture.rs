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
pub(super) const ALLOWANCE: usize = 65536;
pub(super) struct Fixture {
    pub owner: Owner,
    pub worker: Worker,
    pub router: SnapshotRouter,
    pub app: Counter,
}
pub(super) fn committed(id: u128) -> (HostLogStore, Raft, Counter) {
    let mut log = HostLogStore::new(id);
    let mut b = bootstrap(1, 1);
    b.voter_stores.insert(node(1), identity(id));
    append(&mut log, vec![LogMutation::Create(b)]);
    let entry = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(9).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        },
    };
    let mutation = update(
        &log.state(group(1)).unwrap(),
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![entry.clone()],
        }),
    );
    append(&mut log, vec![mutation]);
    let mut app = Counter::new(16).unwrap();
    app.apply_batch(&[entry]).unwrap();
    let core = Raft::recover(
        node(1),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    (log, core, app)
}
impl Fixture {
    pub fn new(id: u128) -> Self {
        Self::with_groups(id, false)
    }
    pub fn with_groups(id: u128, second: bool) -> Self {
        Self::make(id, second, false)
    }
    pub fn with_suffix(id: u128) -> Self {
        Self::make(id, false, true)
    }
    fn make(id: u128, second: bool, suffix: bool) -> Self {
        let (mut log, mut core, mut app) = committed(id);
        let mut cores = Vec::new();
        if second {
            let mut b = bootstrap(2, 1);
            b.voter_stores.insert(node(1), identity(id));
            append(&mut log, vec![LogMutation::Create(b)]);
            let e = LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Command {
                    operation: OperationId::new(19).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            };
            let mutation = update(
                &log.state(group(2)).unwrap(),
                1,
                1,
                Some(Suffix {
                    from: 1,
                    entries: vec![e],
                }),
            );
            append(&mut log, vec![mutation]);
            cores.push(
                Raft::recover(
                    node(1),
                    log.binding(),
                    log.state(group(2)).unwrap(),
                    log.limits(),
                )
                .unwrap(),
            );
        }
        let mut snapshots = support::snapshot::HostSnapshots::new();
        snapshots.identity.store = identity(id);
        snapshots.binding = log.binding();
        let reference = checkpoint_application(&core, &app, &mut snapshots)
            .unwrap()
            .reference();
        compact_replica(&mut core, &mut log, &mut snapshots, &app, reference).unwrap();
        let image = snapshots.load_pinned(reference).unwrap();
        if suffix {
            append_suffix(&mut log, &mut snapshots, &mut core, &mut app);
        }
        cores.push(core);
        let owner = providers::owner_many(&log, cores);
        let binding = SnapshotWorkerBinding {
            store: log.binding(),
            generation: SnapshotWorkerGeneration::new(1).unwrap(),
        };
        let worker = Worker {
            binding,
            sequence: 0,
            bad_ticket: false,
            pending: None,
            image,
            closed: false,
            shared: Default::default(),
        };
        let router = SnapshotRouter::new_with_recovery_limits(
            owner.identity(),
            binding,
            SnapshotRouterLimits {
                requests: 2,
                max_image_bytes: ALLOWANCE,
            },
            SnapshotRecoveryLimits {
                requests: 2,
                image_bytes: 4 * ALLOWANCE,
            },
        )
        .unwrap();
        Self {
            owner,
            worker,
            router,
            app,
        }
    }
    pub fn admit(&mut self) -> SnapshotExportTicket {
        self.router
            .export_checkpoint(&mut self.owner, &mut self.worker, &self.app, group(1))
            .unwrap()
    }
    pub fn event(&mut self) -> SnapshotWorkEvent {
        self.worker.poll(1).pop().unwrap()
    }
    pub fn deliver(&mut self, event: SnapshotWorkEvent) -> Result<(), SnapshotRouteError> {
        self.router
            .deliver(&mut self.owner, &mut self.app, event, MonoTime(0))
    }
    pub fn complete(&mut self) -> SnapshotExportOutcome {
        let original = self.router.poll_export().unwrap();
        self.router.complete_export(original).unwrap()
    }
}
fn append_suffix(
    log: &mut HostLogStore,
    snapshots: &mut support::snapshot::HostSnapshots,
    core: &mut Raft,
    app: &mut Counter,
) {
    let entry = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(10).unwrap(),
            bytes: 1_i64.to_le_bytes().to_vec(),
        },
    };
    let mutation = update(
        &log.state(group(1)).unwrap(),
        1,
        2,
        Some(Suffix {
            from: 2,
            entries: vec![entry.clone()],
        }),
    );
    append(log, vec![mutation]);
    let mut restored = Counter::new(16).unwrap();
    *core = recover_replica(node(1), group(1), log, snapshots, &mut restored)
        .unwrap()
        .0;
    *app = restored;
}
pub(super) struct Worker {
    pub binding: SnapshotWorkerBinding,
    pub sequence: u64,
    pub bad_ticket: bool,
    pub pending: Option<(SnapshotWorkTicket, SnapshotWork)>,
    pub image: Snapshot,
    pub closed: bool,
    pub shared: std::rc::Rc<std::cell::Cell<usize>>,
}
impl SnapshotWorker for Worker {
    fn binding(&self) -> SnapshotWorkerBinding {
        self.binding
    }
    fn limits(&self) -> SnapshotWorkLimits {
        SnapshotWorkLimits::default()
    }
    fn usage(&self) -> SnapshotWorkUsage {
        SnapshotWorkUsage {
            requests: usize::from(self.pending.is_some()),
            bytes: usize::from(self.pending.is_some()) * ALLOWANCE,
        }
    }
    fn load_reservation(&self, g: GroupIdentity) -> Option<usize> {
        (g == group(1) || g == group(2)).then_some(ALLOWANCE)
    }
    fn checkpoint_bytes(&self, _: GroupIdentity) -> Option<usize> {
        Some(ALLOWANCE / 2)
    }
    fn submit(&mut self, work: SnapshotWork) -> Result<SnapshotWorkTicket, SnapshotWorkRejected> {
        if self.closed || self.pending.is_some() {
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
        let request = SnapshotWorkTicket {
            binding: self.binding,
            sequence: if self.bad_ticket { 0 } else { self.sequence },
        };
        self.shared.set(self.shared.get() + 1);
        self.pending = Some((request, work));
        Ok(request)
    }
    fn poll(&mut self, limit: usize) -> Vec<SnapshotWorkEvent> {
        if limit == 0 {
            return vec![];
        }
        let Some((request, work)) = self.pending.take() else {
            return vec![];
        };
        self.shared.set(self.shared.get() - 1);
        let result = match work.job {
            SnapshotJob::Load { reference, install } => Ok(SnapshotOutput::Loaded {
                reference,
                snapshot: self.image.clone(),
                reconciled: install,
            }),
            SnapshotJob::Publish { .. } => Err(StorageError::Uncertain(
                "controlled original publication".into(),
            )),
            _ => panic!("unexpected work"),
        };
        vec![SnapshotWorkEvent {
            request,
            visit: work.visit,
            result,
        }]
    }
    fn close(&mut self) {
        self.closed = true;
    }
}
