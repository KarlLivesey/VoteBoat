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
//! Public storage seam checks for request-scoped replication. These histories
//! use static logs; dynamic activation is covered separately by internal tests.
mod support;
use support::*;
use voteboat::{identity::*, log::*, raft::*};
fn request(configuration: u64, term: u64) -> Message {
    let sender = HostLogStore::new(1).binding();
    Message {
        group: group(1),
        configuration: ConfigurationId::new(configuration).unwrap(),
        from: node(1),
        sender,
        to: node(2),
        term,
        context: RequestContext {
            origin: sender,
            sequence: term,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term,
                payload: EntryPayload::Noop,
            }],
            leader_commit: 1,
        },
    }
}
fn create<S: LogStore>(store: &mut S) -> Raft {
    append(store, vec![LogMutation::Create(bootstrap(1, 3))]);
    Raft::recover(
        node(2),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap()
}
fn reply(effects: Vec<Effect>) -> Message {
    effects
        .into_iter()
        .find_map(|e| {
            if let Effect::Send(m) = e {
                Some(m)
            } else {
                None
            }
        })
        .unwrap()
}
fn conformance<S: LogStore>(mut store: S) {
    let mut core = create(&mut store);
    let message = request(9, 1);
    let effects = core.step(Event::Receive(message.clone())).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("expected durability dependency")
    };
    let tickets = store
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    core.admitted(tickets[0]).unwrap();
    assert_eq!(store.state(group(1)).unwrap().last_index(), 0);
    assert_eq!(core.state().last_index(), 0);
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    assert_eq!(
        core.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let completion = store.barrier(&tickets).unwrap();
    let ack = reply(core.complete(&completion).unwrap());
    assert_eq!(ack.configuration, message.configuration);
    assert_eq!(ack.context, message.context);
    assert_eq!(ack.term, 1);
    assert!(matches!(
        ack.rpc,
        Rpc::Appended {
            success: true,
            matching_index: 1
        }
    ));
    assert_eq!(core.membership().id(), ConfigurationId::new(1).unwrap());
    assert_eq!(
        reply(core.step(Event::Receive(message.clone())).unwrap()),
        ack
    );
    // A failed prefix match also echoes its scope without changing durable data.
    let mut mismatch = message.clone();
    mismatch.configuration = ConfigurationId::new(7).unwrap();
    mismatch.rpc = Rpc::Append {
        previous_index: 2,
        previous_term: 1,
        entries: vec![],
        leader_commit: 2,
    };
    let rejection = reply(core.step(Event::Receive(mismatch.clone())).unwrap());
    assert_eq!(rejection.configuration, mismatch.configuration);
    assert!(matches!(
        rejection.rpc,
        Rpc::Appended {
            success: false,
            matching_index: 1
        }
    ));
    let before = core.state().clone();
    mismatch.rpc = Rpc::ReadProbe;
    assert_eq!(
        core.step(Event::Receive(mismatch.clone())),
        Err(RaftError::WrongIdentity)
    );
    mismatch.rpc = message.rpc;
    mismatch.sender.identity = identity(99);
    mismatch.context.origin = mismatch.sender;
    assert_eq!(
        core.step(Event::Receive(mismatch)),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(core.state(), &before);
}
fn vote_request(configuration: u64, term: u64) -> Message {
    let mut message = request(configuration, term);
    message.rpc = Rpc::Vote {
        last_index: 0,
        last_term: 0,
    };
    message
}
fn vote_conformance<S: LogStore>(mut store: S) {
    let mut core = create(&mut store);
    let message = vote_request(9, 1);
    let effects = core.step(Event::Receive(message.clone())).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("vote reply requires durable promise")
    };
    let tickets = store
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    core.admitted(tickets[0]).unwrap();
    assert_eq!(core.state().hard_state.voted_for, None);
    assert_eq!(store.state(group(1)).unwrap().hard_state.voted_for, None);
    assert_eq!(core.step(Event::Heartbeat), Err(RaftError::Busy));
    assert_eq!(
        core.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let completion = store.barrier(&tickets).unwrap();
    let ack = reply(core.complete(&completion).unwrap());
    assert_eq!(ack.configuration, message.configuration);
    assert_eq!(ack.context, message.context);
    assert!(matches!(ack.rpc, Rpc::Voted { granted: true }));
    assert_eq!(core.state().hard_state.voted_for, Some(node(1)));
    let origin = core.state().ballot_origin.unwrap();
    assert_eq!(origin.configuration, ConfigurationId::new(1).unwrap());
    assert_eq!(origin.candidate_store, message.sender.identity);
    assert_eq!(core.membership().id(), ConfigurationId::new(1).unwrap());
    // Correlation scope cannot manufacture another vote or replace its origin.
    let mut retry = vote_request(10, 1);
    retry.context.sequence = 2;
    let ack = reply(core.step(Event::Receive(retry.clone())).unwrap());
    assert_eq!(ack.context, retry.context);
    assert_eq!(ack.configuration, retry.configuration);
    assert!(matches!(ack.rpc, Rpc::Voted { granted: true }));
    assert_eq!(core.state().ballot_origin, Some(origin));
    let mut other = vote_request(11, 1);
    other.from = node(3);
    other.sender = HostLogStore::new(3).binding();
    other.context.origin = other.sender;
    assert!(matches!(
        reply(core.step(Event::Receive(other.clone())).unwrap()).rpc,
        Rpc::Voted { granted: false }
    ));
    other.sender.identity = identity(99);
    other.context.origin = other.sender;
    assert_eq!(
        core.step(Event::Receive(other)),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(core.state().ballot_origin, Some(origin));
    // A newer request scope/term cannot hide a stale candidate log. Observing
    // that term is still durable before the negative reply escapes.
    let effects = core.step(Event::Receive(request(12, 2))).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    assert!(matches!(
        reply(persist_effect(&mut core, &mut store, update.clone()).unwrap()).rpc,
        Rpc::Appended { success: true, .. }
    ));
    let stale = vote_request(13, 3);
    let effects = core.step(Event::Receive(stale.clone())).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    assert_eq!(core.state().hard_state.term, 2);
    let denied = reply(persist_effect(&mut core, &mut store, update.clone()).unwrap());
    assert_eq!(denied.context, stale.context);
    assert_eq!(denied.configuration, stale.configuration);
    assert!(matches!(denied.rpc, Rpc::Voted { granted: false }));
    assert_eq!(core.state().hard_state.term, 3);
    assert_eq!(core.state().hard_state.voted_for, None);
    assert_eq!(core.state().ballot_origin, None);
    assert_eq!(core.membership().id(), ConfigurationId::new(1).unwrap());
}
#[test]
fn host_vote_scope_preserves_durable_local_origin_and_single_vote() {
    vote_conformance(HostLogStore::new(2));
}
#[test]
fn host_store_replication_scopes_never_replace_durability_or_current_authority() {
    conformance(HostLogStore::new(2));
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::contracts::StorageError;
    use voteboat::{
        native::{log_store::*, wire::NativeWireCodec},
        wire::*,
    };
    #[test]
    fn native_vote_scope_preserves_durable_local_origin_and_single_vote() {
        vote_conformance(
            NativeLogStore::create(ModelIo::default(), identity(2), LogLimits::default()).unwrap(),
        );
    }
    #[test]
    fn native_store_replication_scopes_never_replace_durability_or_current_authority() {
        conformance(
            NativeLogStore::create(ModelIo::default(), identity(2), LogLimits::default()).unwrap(),
        );
    }
    #[test]
    fn failed_scope_bridge_barrier_has_no_reply_and_restarts_from_durable_state() {
        for voting in [false, true] {
            for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
                let io = ModelIo::default();
                let mut store =
                    NativeLogStore::create(io.clone(), identity(2), LogLimits::default()).unwrap();
                let mut core = create(&mut store);
                let old = core.state().clone();
                let message = if voting {
                    vote_request(9, 1)
                } else {
                    request(9, 1)
                };
                let effects = core.step(Event::Receive(message.clone())).unwrap();
                let [Effect::Persist(update)] = effects.as_slice() else {
                    panic!()
                };
                io.0.borrow_mut().fault = fault;
                assert!(matches!(
                    persist_effect(&mut core, &mut store, update.clone()),
                    Err(RaftError::Storage(StorageError::Uncertain(_)))
                ));
                assert_eq!(core.state(), &old);
                assert_eq!(
                    core.step(Event::Receive(message.clone())),
                    Err(RaftError::Fenced)
                );
                drop(store);
                io.0.borrow_mut().power_loss();
                let mut store =
                    NativeLogStore::recover(io, identity(2), LogLimits::default()).unwrap();
                let recovered = store.state(group(1)).unwrap();
                assert!([0, 1].contains(&recovered.last_index()));
                let mut core =
                    Raft::recover(node(2), store.binding(), recovered, store.limits()).unwrap();
                let mut effects = core.step(Event::Receive(message.clone())).unwrap();
                if let [Effect::Persist(update)] = effects.as_slice() {
                    effects = persist_effect(&mut core, &mut store, update.clone()).unwrap();
                }
                let ack = reply(effects);
                assert_eq!(ack.configuration, message.configuration);
                if voting {
                    assert!(matches!(ack.rpc, Rpc::Voted { granted: true }));
                    let state = store.state(group(1)).unwrap();
                    assert_eq!(state.hard_state.voted_for, Some(node(1)));
                    assert_eq!(
                        state.ballot_origin.unwrap().configuration,
                        ConfigurationId::new(1).unwrap()
                    );
                    assert_eq!(state.commit_index, 0);
                } else {
                    assert!(matches!(
                        ack.rpc,
                        Rpc::Appended {
                            success: true,
                            matching_index: 1
                        }
                    ));
                    assert_eq!(store.state(group(1)).unwrap().commit_index, 1);
                }
            }
        }
    }
    #[test]
    fn native_file_vote_scope_survives_reopen_without_changing_local_membership() {
        let path = std::env::temp_dir().join(format!("voteboat-vote-scope-{}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        let mut store = NativeLogStore::create(
            FileLogIo::create(&path).unwrap(),
            identity(2),
            LogLimits::default(),
        )
        .unwrap();
        let mut core = create(&mut store);
        let message = vote_request(9, 1);
        let effects = core.step(Event::Receive(message.clone())).unwrap();
        let [Effect::Persist(update)] = effects.as_slice() else {
            panic!()
        };
        let ack = reply(persist_effect(&mut core, &mut store, update.clone()).unwrap());
        assert!(matches!(ack.rpc, Rpc::Voted { granted: true }));
        let expected = core.state().clone();
        drop(store);
        let store = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(2),
            LogLimits::default(),
        )
        .unwrap();
        let recovered = store.state(group(1)).unwrap();
        assert_eq!(recovered, expected);
        let mut core = Raft::recover(node(2), store.binding(), recovered, store.limits()).unwrap();
        let mut other = vote_request(10, 1);
        other.from = node(3);
        other.sender = HostLogStore::new(3).binding();
        other.context.origin = other.sender;
        assert!(matches!(
            reply(core.step(Event::Receive(other)).unwrap()).rpc,
            Rpc::Voted { granted: false }
        ));
        let ack = reply(core.step(Event::Receive(message)).unwrap());
        assert_eq!(ack.configuration, ConfigurationId::new(9).unwrap());
        assert!(matches!(ack.rpc, Rpc::Voted { granted: true }));
        assert_eq!(core.membership().id(), ConfigurationId::new(1).unwrap());
        assert_eq!(
            core.state().ballot_origin.unwrap().configuration,
            ConfigurationId::new(1).unwrap()
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn native_wire_preserves_request_scope_separately_from_sender_and_snapshot_base() {
        let codec = NativeWireCodec::with_membership(WireLimits::default()).unwrap();
        let mut message = request(9, 1);
        for rpc in [
            message.rpc.clone(),
            Rpc::Vote {
                last_index: 1,
                last_term: 1,
            },
            Rpc::Snapshot {
                snapshot: Box::new(voteboat::snapshot::Snapshot {
                    metadata: voteboat::snapshot::SnapshotMetadata {
                        bootstrap: bootstrap(1, 3),
                        membership: None,
                        index: 1,
                        term: 1,
                        application_schema: 1,
                    },
                    application: vec![7],
                }),
            },
        ] {
            message.rpc = rpc;
            let scope = WireScope {
                from: message.from,
                sender: message.sender,
                to: message.to,
            };
            let bytes = codec
                .encode_batch(scope, std::slice::from_ref(&message))
                .unwrap();
            assert_eq!(
                codec.decode_batch(scope, &bytes).unwrap(),
                [message.clone()]
            );
        }
        message.from = node(2);
        message.to = node(1);
        message.sender = HostLogStore::new(2).binding();
        for rpc in [
            Rpc::Appended {
                success: true,
                matching_index: 1,
            },
            Rpc::SnapshotAck { index: 1 },
            Rpc::Compacted { index: 1, term: 1 },
        ] {
            message.rpc = rpc;
            let scope = WireScope {
                from: message.from,
                sender: message.sender,
                to: message.to,
            };
            let bytes = codec
                .encode_batch(scope, std::slice::from_ref(&message))
                .unwrap();
            assert_eq!(
                codec.decode_batch(scope, &bytes).unwrap(),
                [message.clone()]
            );
        }
    }
}
