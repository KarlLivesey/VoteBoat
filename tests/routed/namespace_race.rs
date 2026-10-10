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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Decision {
    Publish,
    Cancel,
}
impl Decision {
    fn other(self) -> Self {
        match self {
            Self::Publish => Self::Cancel,
            Self::Cancel => Self::Publish,
        }
    }
    fn operation(self) -> u128 {
        match self {
            Self::Publish => 10003,
            Self::Cancel => 10004,
        }
    }
    fn bytes(self, s: &Service, ready: NamespaceStatus) -> Vec<u8> {
        match self {
            Self::Publish => NamespacePublication::from_status(&s.plan, ready)
                .unwrap()
                .encode(MAX_NAMESPACE_PUBLICATION_BYTES)
                .unwrap(),
            Self::Cancel => CancelGroupCreation::from_status(&s.plan.creation)
                .encode(GROUP_CREATION_CANCELLATION_BYTES)
                .unwrap(),
        }
    }
    fn applied(self, app: &Directory, creation: OperationId) -> bool {
        match self {
            Self::Publish => app.namespace_publication_at(0, creation).unwrap().is_some(),
            Self::Cancel => app
                .group_creation_cancellation_at(0, creation)
                .unwrap()
                .is_some(),
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Schedule {
    first: Decision,
    committed: bool,
    checkpoint: bool,
}
type DecisionState = (
    Option<NamespacePublicationStatus>,
    Option<GroupCreationCancellationStatus>,
);
fn decision_state(s: &Service) -> DecisionState {
    let app = &s.parents[0].local().applications[&group(1)];
    (
        app.namespace_publication_at(0, s.plan.creation.operation)
            .unwrap(),
        app.group_creation_cancellation_at(0, s.plan.creation.operation)
            .unwrap(),
    )
}
fn directory() -> Directory {
    namespace_directory()
        .with_creation_cancellation()
        .unwrap_or_else(|_| panic!("schema16"))
}
fn interrupt_first(
    s: &mut Service,
    ready: NamespaceStatus,
    plan: Schedule,
) -> Option<DecisionState> {
    let leader = s.parents[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .local_node();
    let operation = OperationId::new(plan.first.operation()).unwrap();
    let before = s.parents[0]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .state()
        .commit_index;
    let bytes = plan.first.bytes(s, ready);
    s.parents[0]
        .propose(ClientRequest {
            group: group(1),
            operation,
            bytes,
        })
        .unwrap();
    if plan.committed {
        drive(&mut s.parents[..2], &s.clock, |nodes| {
            nodes.iter().all(|n| {
                plan.first.applied(
                    &n.local().applications[&group(1)],
                    s.plan.creation.operation,
                )
            })
        });
        assert!(!plan.first.applied(
            &s.parents[2].local().applications[&group(1)],
            s.plan.creation.operation
        ));
    }
    assert_eq!(s.parents[0].local().clients.usage().requests, 1);
    let original = plan.committed.then(|| decision_state(s));
    let logs = abandon(std::mem::take(&mut s.parents), 1);
    let contains = |log: &GroupLog| {
        log.entries.iter().any(
            |e| matches!(e.payload, EntryPayload::Command { operation: id, .. } if id == operation),
        )
    };
    if plan.committed {
        assert!(
            logs.values()
                .filter(|log| log.commit_index > before && contains(log))
                .count()
                >= 2
        );
    } else {
        assert!(logs
            .values()
            .all(|log| log.commit_index == before && !contains(log)));
    }
    s.parents = open(
        configuration(&s.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &s.clock,
        s.protocol,
        directory,
    );
    // Prefer a different, freshest voter: the prior leader's memory and client
    // receipt cannot determine the recovered outcome.
    s.parents.sort_by_key(|n| {
        let core = n.local().owner.core(group(1)).unwrap();
        (
            core.local_node() != leader,
            core.state().last_term(),
            core.state().last_index(),
        )
    });
    s.parents.reverse();
    assert_ne!(
        s.parents[0]
            .local()
            .owner
            .core(group(1))
            .unwrap()
            .local_node(),
        leader
    );
    campaign(&mut s.parents, &s.clock, 1);
    original
}
fn check_decision(s: &Service, winner: Decision) {
    for n in &s.parents {
        let app = &n.local().applications[&group(1)];
        assert!(winner.applied(app, s.plan.creation.operation));
        assert!(!winner.other().applied(app, s.plan.creation.operation));
        assert_eq!(app.manifest(responsibility(1)), Some(&s.parent_manifest));
        assert_eq!(
            app.manifest(responsibility(50)).is_some(),
            winner == Decision::Publish
        );
        assert_eq!(app.reserved_publication_bytes(), 0);
    }
}
fn checkpoint_and_reopen(s: &mut Service) {
    for n in &mut s.parents {
        n.control(group(1), NodeControl::Checkpoint).unwrap();
    }
    drive(&mut s.parents, &s.clock, |nodes| {
        nodes.iter().all(|n| {
            n.local().owner.core(group(1)).unwrap().state().base_index()
                == n.local().applications[&group(1)].applied_index()
        })
    });
    close(std::mem::take(&mut s.parents), &s.clock, 1, || {});
    s.parents = open(
        configuration(&s.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &s.clock,
        s.protocol,
        directory,
    );
    campaign(&mut s.parents, &s.clock, 1);
}
fn finish(mut s: Service, ready: NamespaceStatus, winner: Decision, checkpoint: bool) {
    let original = decision_state(&s);
    if checkpoint {
        checkpoint_and_reopen(&mut s);
    }
    assert_eq!(decision_state(&s), original);
    check_decision(&s, winner);
    let bytes = winner.bytes(&s, ready);
    assert!(propose(&mut s.parents, &s.clock, 1, winner.operation(), bytes).duplicate);
    assert_eq!(decision_state(&s), original);
    let rejected = ClientRequest {
        group: group(1),
        operation: OperationId::new(winner.other().operation()).unwrap(),
        bytes: winner.other().bytes(&s, ready),
    };
    assert!(s.parents[0].propose(rejected).is_err());
    check_decision(&s, winner);
    // A stale ready receipt cannot activate a canceled namespace. Only a
    // recovered committed publication can enable the target.
    if winner == Decision::Publish {
        let status = s.parents[0].local().applications[&group(1)]
            .namespace_publication_at(0, s.plan.creation.operation)
            .unwrap()
            .unwrap();
        let logs = s.stop_metadata();
        let activation = s.activate(&status);
        s.exercise();
        s.recover_service(activation);
        s.verify_metadata(logs);
    } else {
        s.reopen_ready(ready);
        assert_eq!(
            read_recovering(&mut s.nodes, &s.clock, 100, query()),
            NamespaceRead::NotActive
        );
        assert!(s.nodes[0]
            .propose(ClientRequest {
                group: group(100),
                operation: OperationId::new(20000).unwrap(),
                bytes: data(),
            })
            .is_err());
        close(std::mem::take(&mut s.parents), &s.clock, 1, || {});
        close(std::mem::take(&mut s.nodes), &s.clock, 100, || {});
    }
    std::fs::remove_dir_all(s.root).unwrap();
}
pub(crate) fn run(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    for first in [Decision::Publish, Decision::Cancel] {
        for committed in [false, true] {
            for checkpoint in [false, true] {
                let plan = Schedule {
                    first,
                    committed,
                    checkpoint,
                };
                eprintln!("namespace race {protocol:?}: {plan:?}");
                let mut s = Service::new_in(protocol, true, directory);
                let ready = s.prepare_ready();
                s.reopen_ready(ready);
                campaign(&mut s.parents, &s.clock, 1);
                let original = interrupt_first(&mut s, ready, plan);
                if let Some(original) = original {
                    assert_eq!(decision_state(&s), original);
                }
                let winner = if committed { first } else { first.other() };
                let bytes = winner.bytes(&s, ready);
                let receipt = propose(&mut s.parents, &s.clock, 1, winner.operation(), bytes);
                assert_eq!(receipt.duplicate, committed);
                check_decision(&s, winner);
                finish(s, ready, winner, plan.checkpoint);
            }
        }
    }
}
