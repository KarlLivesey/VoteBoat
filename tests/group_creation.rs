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
mod support;
use support::*;
use voteboat::{
    contracts::StorageError, directory::*, group_creation::*, identity::*, log::*,
    routing::ApplicationAdapter,
};
impl CreationLogStore for HostLogStore {
    fn creation_state(&self, group: GroupIdentity) -> Result<Option<GroupLog>, StorageError> {
        if !self.pending.is_empty() {
            return Err(StorageError::Rejected("pending host write"));
        }
        Ok(self.durable.get(&group).cloned())
    }
}
struct Authority {
    accepted: GroupCreationStatus,
}
impl CreationAuthority for Authority {
    fn verify(&self, status: &GroupCreationStatus) -> Result<(), CreationBootstrapError> {
        if status == &self.accepted {
            Ok(())
        } else {
            Err(CreationBootstrapError::Rejected("untrusted status"))
        }
    }
}
fn status() -> GroupCreationStatus {
    GroupCreationStatus {
        operation: OperationId::new(10).unwrap(),
        index: 7,
        intent: GroupCreationIntent {
            authority: group(9),
            parent: ResponsibilityIdentity {
                id: ResponsibilityId::new(1).unwrap(),
                incarnation: ResponsibilityIncarnation::new(1).unwrap(),
            },
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(2).unwrap(),
                incarnation: ResponsibilityIncarnation::new(1).unwrap(),
            },
            expected: RouteGeneration::new(1).unwrap(),
            bootstrap: bootstrap(100, 3),
            application: ApplicationAdapter {
                id: ApplicationAdapterId::new(1).unwrap(),
                version: 1,
            },
            mode: GroupCreationMode::Empty,
        },
    }
}
fn verified(status: GroupCreationStatus) -> VerifiedGroupCreation {
    let authority = Authority {
        accepted: status.clone(),
    };
    let application = status.intent.application;
    VerifiedGroupCreation::verify(&authority, status, node(1), identity(1), application).unwrap()
}
#[derive(Default)]
struct HostBindings {
    bytes: Option<Vec<u8>>,
    fail: u8,
    publishes: usize,
}
impl CreationBindings for HostBindings {
    fn load(&mut self) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.bytes.clone())
    }
    fn publish_exact(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        self.publishes += 1;
        if self.fail == 1 {
            return Err(StorageError::Uncertain("before publication".into()));
        }
        if self.bytes.as_ref().is_some_and(|old| old != bytes) {
            return Err(StorageError::Rejected("conflict"));
        }
        self.bytes = Some(bytes.to_vec());
        if self.fail == 2 {
            return Err(StorageError::Uncertain("after publication".into()));
        }
        Ok(())
    }
}
#[test]
fn host_authority_assignment_and_application_are_checked_before_mutation() {
    let source = status();
    let authority = Authority {
        accepted: source.clone(),
    };
    let adapter = source.intent.application;
    let mut altered = source.clone();
    altered.index += 1;
    assert!(
        VerifiedGroupCreation::verify(&authority, altered, node(1), identity(1), adapter).is_err()
    );
    assert!(VerifiedGroupCreation::verify(
        &authority,
        source.clone(),
        node(1),
        identity(2),
        adapter
    )
    .is_err());
    assert!(VerifiedGroupCreation::verify(
        &authority,
        source.clone(),
        node(4),
        identity(4),
        adapter
    )
    .is_err());
    let mut wrong_app = adapter;
    wrong_app.version += 1;
    assert!(VerifiedGroupCreation::verify(
        &authority,
        source.clone(),
        node(1),
        identity(1),
        wrong_app
    )
    .is_err());
    let mut zero = source;
    zero.index = 0;
    assert!(
        VerifiedGroupCreation::verify(&authority, zero, node(1), identity(1), adapter).is_err()
    );
    let verified = verified(status());
    let mut log = HostLogStore::new(2);
    let mut bindings = HostBindings::default();
    assert!(establish_created_group(&verified, &mut log, &mut bindings).is_err());
    assert!(log.durable.is_empty());
    assert_eq!(bindings.publishes, 0);
}
#[test]
fn immutable_binding_precedes_bootstrap_and_uncertain_publication_resumes_exactly() {
    for failure in [1, 2] {
        let verified = verified(status());
        let mut log = HostLogStore::new(1);
        let mut bindings = HostBindings {
            fail: failure,
            ..Default::default()
        };
        assert!(establish_created_group(&verified, &mut log, &mut bindings).is_err());
        assert!(log.durable.is_empty());
        assert!(log.pending.is_empty());
        bindings.fail = 0;
        let receipt = establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        assert_eq!(receipt.binding(), log.binding());
        assert_eq!(receipt.group(), group(100));
        assert_eq!(receipt.operation(), status().operation);
        assert_eq!(receipt.metadata_index(), status().index);
        let state = log.state(group(100)).unwrap();
        assert_eq!(state.bootstrap, status().intent.bootstrap);
        assert_eq!(state.hard_state.term, 0);
        assert!(state.entries.is_empty());
        assert_eq!(
            establish_created_group(&verified, &mut log, &mut bindings).unwrap(),
            receipt
        );
        assert_eq!(log.state(group(100)).unwrap(), state);
    }
}
#[test]
fn existing_unbound_or_conflicting_creation_never_resets_a_group() {
    let original = status();
    let verified = verified(original.clone());
    let mut log = HostLogStore::new(1);
    let mut bindings = HostBindings::default();
    append(
        &mut log,
        vec![LogMutation::Create(original.intent.bootstrap.clone())],
    );
    let before = log.state(group(100)).unwrap();
    assert!(establish_created_group(&verified, &mut log, &mut bindings).is_err());
    assert!(bindings.bytes.is_none());
    assert_eq!(log.state(group(100)).unwrap(), before);
    let mut log = HostLogStore::new(1);
    establish_created_group(&verified, &mut log, &mut bindings).unwrap();
    let mut changed = original;
    changed.intent.mode = GroupCreationMode::Staging;
    let conflicting = self::verified(changed);
    assert!(establish_created_group(&conflicting, &mut log, &mut bindings).is_err());
    assert_eq!(log.state(group(100)).unwrap(), before);
    let state = log.state(group(100)).unwrap();
    append(
        &mut log,
        vec![update(
            &state,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 1,
                    payload: EntryPayload::Noop,
                }],
            }),
        )],
    );
    let progressed = log.state(group(100)).unwrap();
    establish_created_group(&verified, &mut log, &mut bindings).unwrap();
    assert_eq!(log.state(group(100)).unwrap(), progressed);
}
#[cfg(feature = "native")]
#[test]
fn native_bootstrap_every_torn_frame_and_sync_publication_fault_resumes_after_reopen() {
    use voteboat::native::log_store::*;
    use voteboat::raft::*;
    let verified = verified(status());
    let mutation = LogMutation::Create(status().intent.bootstrap);
    let frame = NativeLogCodec
        .encode_batch(1, &[mutation], LogLimits::default())
        .unwrap();
    let mut saw_absent = false;
    let mut saw_complete = false;
    for fault in (0..=frame.len()).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        let mut bindings = HostBindings::default();
        io.0.borrow_mut().fault = fault;
        assert!(establish_created_group(&verified, &mut log, &mut bindings).is_err());
        assert!(bindings.bytes.is_some());
        drop(log);
        io.0.borrow_mut().power_loss();
        let mut log = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        if log.creation_state(group(100)).unwrap().is_some() {
            saw_complete = true;
        } else {
            saw_absent = true;
        }
        let receipt = establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        assert_eq!(receipt.binding(), log.binding());
        let mut core = Raft::recover(
            node(1),
            log.binding(),
            log.state(group(100)).unwrap(),
            log.limits(),
        )
        .unwrap();
        assert_eq!(core.role(), Role::Follower);
        let campaign = core.step(Event::Campaign).unwrap();
        let [Effect::Persist(update)] = campaign.as_slice() else {
            panic!("durable ballot first")
        };
        let effects = persist_effect(&mut core, &mut log, update.clone()).unwrap();
        assert!(effects.iter().any(
            |e| matches!(e, Effect::Send(message) if matches!(message.rpc, Rpc::Vote { .. }))
        ));
        assert_eq!(core.role(), Role::Candidate);
    }
    assert!(saw_absent && saw_complete);
}
