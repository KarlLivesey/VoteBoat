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
//! This is a composition demo, not a network daemon or linearizable-read API.
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
};
use voteboat::{application::*, identity::*, log::*, native::log_store::*, quorum::*, raft::*};

struct Replica {
    core: Raft,
    store: NativeLogStore<FileLogIo>,
    application: Counter,
    receipts: Vec<CounterReceipt>,
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
        if state.bootstrap != bootstrap {
            return Err("demo configuration differs from recovered configuration".into());
        }
        let core = Raft::recover(node, store.binding(), state, store.limits())
            .map_err(|e| format!("{e:?}"))?;
        let mut application = Counter::new(10000).map_err(|e| format!("{e:?}"))?;
        application
            .apply_batch(core.replay_committed())
            .map_err(|e| format!("{e:?}"))?;
        demo.replicas.insert(
            node,
            Replica {
                core,
                store,
                application,
                receipts: Vec::new(),
            },
        );
    }
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
    for (node, r) in &demo.replicas {
        println!(
            "replica={} committed={} applied={} value={}",
            node.get(),
            r.core.state().commit_index,
            r.application.applied_index(),
            r.application
                .read_applied(r.core.state().commit_index)
                .map_err(|e| format!("{e:?}"))?
        );
    }
    Ok(())
}
