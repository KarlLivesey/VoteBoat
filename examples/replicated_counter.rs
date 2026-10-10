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
//! Three real WALs, deterministic core and host-driven in-process delivery.
//! This composition demo includes quorum-backed reads, but is not a network daemon.
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{log_store::*, snapshot_store::*},
    quorum::*,
    raft::*,
    snapshot::*,
};

struct Replica {
    core: Raft,
    store: NativeLogStore<FileLogIo>,
    application: Counter,
    receipts: Vec<CounterReceipt>,
    read_value: Option<i64>,
    snapshots: NativeSnapshotStore<FileSnapshotIo>,
    restored_checkpoint: u64,
}
struct Demo {
    replicas: BTreeMap<NodeId, Replica>,
    messages: VecDeque<Message>,
}
type Failure = Box<dyn std::error::Error>;
impl Demo {
    fn effects(&mut self, id: NodeId, effects: Vec<Effect>) -> Result<(), Failure> {
        let mut effects = VecDeque::from(effects);
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::VerifyLearnerReadiness(_)
                | Effect::CheckpointRequired { .. }
                | Effect::CheckpointCompacted(_) => {
                    return Err("synchronous demo does not request asynchronous checkpoints".into())
                }
                Effect::SnapshotRequired {
                    to,
                    context,
                    reference,
                } => {
                    let r = self.replicas.get_mut(&id).unwrap();
                    effects.extend(
                        supply_snapshot(&r.core, &mut r.snapshots, to, context, reference)
                            .map_err(|e| format!("{e:?}"))?,
                    );
                }
                Effect::StageSnapshot(message) => {
                    let r = self.replicas.get_mut(&id).unwrap();
                    effects.extend(
                        stage_snapshot_effect(
                            &mut r.core,
                            &mut r.store,
                            &mut r.snapshots,
                            &r.application,
                            message,
                        )
                        .map_err(|e| format!("{e:?}"))?,
                    );
                }
                Effect::SnapshotInstalled(reference) => {
                    let r = self.replicas.get_mut(&id).unwrap();
                    effects.extend(
                        finish_snapshot_install(
                            &mut r.core,
                            &r.store,
                            &mut r.snapshots,
                            &mut r.application,
                            reference,
                        )
                        .map_err(|e| format!("{e:?}"))?,
                    );
                }
                Effect::ReadReady(barrier) => {
                    let r = self.replicas.get_mut(&id).unwrap();
                    r.read_value = Some(
                        read_at_barrier(&mut r.core, &barrier, &r.application, ())
                            .map_err(|e| format!("{e:?}"))?,
                    );
                }
                Effect::Send(m) => {
                    if self.messages.len() >= 1024 {
                        return Err("demo message budget exhausted".into());
                    }
                    self.messages.push_back(m);
                }
                Effect::Persist(u) => {
                    let r = self.replicas.get_mut(&id).unwrap();
                    effects.extend(
                        persist_effect(&mut r.core, &mut r.store, u)
                            .map_err(|e| format!("{e:?}"))?,
                    );
                }
                Effect::Committed(entries) => {
                    let r = self.replicas.get_mut(&id).unwrap();
                    let receipts = r
                        .application
                        .apply_batch(&entries)
                        .map_err(|e| format!("{e:?}"))?;
                    r.receipts.extend(receipts);
                }
            }
        }
        Ok(())
    }
    fn act(&mut self, id: NodeId, event: Event) -> Result<(), Failure> {
        let effects = self
            .replicas
            .get_mut(&id)
            .unwrap()
            .core
            .step(event)
            .map_err(|e| format!("{e:?}"))?;
        self.effects(id, effects)
    }
    fn pump(&mut self) -> Result<(), Failure> {
        let mut delivered = 0;
        while let Some(m) = self.messages.pop_front() {
            delivered += 1;
            if delivered > 10000 {
                return Err("demo delivery budget exhausted".into());
            }
            self.act(m.to, Event::Receive(m))?;
        }
        Ok(())
    }
}
fn main() -> Result<(), Failure> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: replicated_counter DIRECTORY OPERATION_ID DELTA".into());
    }
    let root = PathBuf::from(&args[1]);
    let operation = OperationId::new(args[2].parse()?).ok_or("operation must be nonzero")?;
    let delta: i64 = args[3].parse()?;
    match std::fs::create_dir(&root) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.into()),
    }
    let mut demo = open_demo(&root)?;
    let leader = NodeId::new(1).unwrap();
    demo.act(leader, Event::Campaign)?;
    demo.pump()?;
    for _ in 0..2 {
        demo.act(
            leader,
            Event::Propose {
                operation,
                bytes: delta.to_le_bytes().to_vec(),
            },
        )?;
        demo.pump()?;
    }
    let receipts = &demo.replicas[&leader].receipts;
    let receipt = receipts.last().ok_or("no committed and applied result")?;
    println!(
        "operation={} outcome={:?} retry_duplicate={}",
        operation.get(),
        receipt.outcome,
        receipt.duplicate
    );
    demo.act(
        leader,
        Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        },
    )?;
    demo.pump()?;
    println!(
        "linearizable_value={}",
        demo.replicas[&leader]
            .read_value
            .ok_or("read did not reach quorum")?
    );
    let mut compaction_effects = Vec::new();
    for (node, r) in &mut demo.replicas {
        let checkpoint = checkpoint_application(&r.core, &r.application, &mut r.snapshots)
            .map_err(|e| format!("{e:?}"))?;
        compaction_effects.push((
            *node,
            compact_replica(
                &mut r.core,
                &mut r.store,
                &mut r.snapshots,
                &r.application,
                checkpoint.reference(),
            )
            .map_err(|e| format!("{e:?}"))?,
        ));
        println!(
            "replica={} committed={} applied={} value={} restored_checkpoint={} checkpoint={} compacted_through={}",
            node.get(),
            r.core.state().commit_index,
            r.application.applied_index(),
            r.application
                .read_applied(r.core.state().commit_index)
                .map_err(|e| format!("{e:?}"))?,
            r.restored_checkpoint,
            checkpoint.metadata.index,
            r.core.state().base_index(),
        );
    }
    for (node, effects) in compaction_effects {
        demo.effects(node, effects)?;
    }
    demo.pump()?;
    Ok(())
}

fn open_replica(
    root: &std::path::Path,
    node: NodeId,
    identity: StoreIdentity,
    bootstrap: &Bootstrap,
) -> Result<Replica, Failure> {
    let group = bootstrap.group;
    let directory = root.join(node.get().to_string());
    let limits = LogLimits::default();
    let store = if directory.join("MANIFEST").exists() {
        NativeLogStore::recover(FileLogIo::open(&directory)?, identity, limits)?
    } else {
        let mut s = NativeLogStore::create(FileLogIo::create(&directory)?, identity, limits)?;
        let tickets = s.append_batch(vec![LogMutation::Create(bootstrap.clone())])?;
        s.barrier(&tickets)?;
        s
    };
    let state = store.state(group)?;
    if state.bootstrap != *bootstrap {
        return Err("demo configuration differs from recovered configuration".into());
    }
    let mut application = Counter::new(10000).map_err(|e| format!("{e:?}"))?;
    let checkpoint_directory = directory.join("checkpoints");
    let checkpoint_identity = SnapshotIdentity {
        store: identity,
        group,
    };
    let mut snapshots = if checkpoint_directory.join("MANIFEST").exists() {
        NativeSnapshotStore::recover(
            FileSnapshotIo::open(&checkpoint_directory)?,
            checkpoint_identity,
            SnapshotLimits::default(),
        )?
    } else {
        NativeSnapshotStore::create(
            FileSnapshotIo::create(&checkpoint_directory)?,
            checkpoint_identity,
            SnapshotLimits::default(),
        )?
    };
    let (core, restored) = recover_replica(node, group, &store, &mut snapshots, &mut application)
        .map_err(|e| format!("{e:?}"))?;
    Ok(Replica {
        core,
        store,
        application,
        receipts: Vec::new(),
        read_value: None,
        snapshots,
        restored_checkpoint: restored.checkpoint_index,
    })
}
fn open_demo(root: &std::path::Path) -> Result<Demo, Failure> {
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let stores: BTreeMap<_, _> = (1..=3)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                StoreIdentity {
                    id: StoreId::new(n as u128).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect();
    let policy = Policy::new(
        Tree::Majority(stores.keys().map(|n| Tree::Voter(*n)).collect()),
        Limits::default(),
    )
    .map_err(|e| format!("{e:?}"))?;
    let bootstrap = Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        policy,
        voter_stores: stores.clone(),
    };
    let mut demo = Demo {
        replicas: BTreeMap::new(),
        messages: VecDeque::new(),
    };
    for (node, identity) in stores {
        demo.replicas
            .insert(node, open_replica(root, node, identity, &bootstrap)?);
    }
    Ok(demo)
}
