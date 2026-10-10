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
use voteboat::{
    bucket_counter::{encode_add, BucketOutcome},
    transfer::*,
    transfer_publication::*,
    transfer_source::*,
    transfer_target::*,
};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Intent,
    Stage(u128),
    Fence,
    Import(u128),
    Publish,
    Activate(u128),
    Done,
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Observed {
    intent: Option<TransferIntentStatus>,
    publication: Option<TransferPublicationStatus>,
    source: Option<SourceFreezeStatus>,
    targets: [TargetStatus; 2],
}
fn metadata() -> LifecycleDirectory {
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(group(1), vec![source_fixture::grant()]).unwrap(),
            DirectoryLimits {
                operations: 3,
                history_bytes: 65536,
            },
        )
        .unwrap(),
    )
}
pub(super) fn observe<A>(
    nodes: &mut [Node<A>],
    clock: &Instant,
    g: u128,
    query: A::Query,
) -> A::ReadResult
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
    A::Query: Clone,
{
    campaign(nodes, clock, g);
    read_recovering(nodes, clock, g, query)
}
pub(super) fn compact<A>(nodes: &mut [Node<A>], clock: &Instant, g: u128)
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    drive(nodes, clock, |ns| {
        ns.iter().all(|n| {
            n.local().applications[&group(g)].applied_index()
                == n.local().owner.core(group(g)).unwrap().state().commit_index
        })
    });
    let boundaries = nodes
        .iter()
        .map(|n| n.local().applications[&group(g)].applied_index())
        .collect::<Vec<_>>();
    for node in nodes.iter_mut() {
        let state = node.local().owner.core(group(g)).unwrap().state();
        if state.commit_index > state.base_index() {
            node.control(group(g), NodeControl::Checkpoint).unwrap();
        }
    }
    drive(nodes, clock, |ns| {
        ns.iter()
            .zip(&boundaries)
            .all(|(n, b)| n.local().owner.core(group(g)).unwrap().state().base_index() >= *b)
    });
}
struct Split {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    metadata: fn() -> LifecycleDirectory,
    parent: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<source_fixture::Source>>,
    targets: [Vec<Node<target_fixture::Target>>; 2],
}
impl Split {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        Self::with_metadata(protocol, checkpoint, metadata)
    }
    fn with_metadata(
        protocol: NativePeerProtocol,
        checkpoint: bool,
        metadata: fn() -> LifecycleDirectory,
    ) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-split-resume-{}-{protocol:?}-{checkpoint}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut result = Self {
            parent: open(
                configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                metadata,
            ),
            source: open(
                configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
                &clock,
                protocol,
                source_fixture::fresh,
            ),
            targets: [
                open(
                    configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Create),
                    &clock,
                    protocol,
                    target_fixture::fresh,
                ),
                open(
                    configuration(&root, 22, &[1, 2, 3], NativeOpenMode::Create),
                    &clock,
                    protocol,
                    || target_fixture::fresh_for(22),
                ),
            ],
            root,
            clock,
            protocol,
            checkpoint,
            metadata,
        };
        campaign(&mut result.parent, &result.clock, 1);
        propose_recovering(
            &mut result.parent,
            &result.clock,
            1,
            1000,
            metadata().directory().bootstrap_command(65536).unwrap(),
        );
        propose_recovering(
            &mut result.parent,
            &result.clock,
            1,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: source_fixture::grant(),
            }
            .encode(32768)
            .unwrap(),
        );
        campaign(&mut result.source, &result.clock, 20);
        propose_recovering(
            &mut result.source,
            &result.clock,
            20,
            100,
            source_fixture::fresh().bootstrap_command(65536).unwrap(),
        );
        propose_recovering(
            &mut result.source,
            &result.clock,
            20,
            1,
            source_fixture::data(1, 7),
        );
        propose_recovering(
            &mut result.source,
            &result.clock,
            20,
            2,
            source_fixture::data(200, 11),
        );
        result
    }
    fn observed(&mut self) -> Observed {
        let DirectoryRead::Transfer(intent) = observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Transfer(OperationId::new(200).unwrap()),
        ) else {
            panic!("intent observation")
        };
        let DirectoryRead::Publication(publication) = observe(
            &mut self.parent,
            &self.clock,
            1,
            DirectoryQuery::Publication(OperationId::new(200).unwrap()),
        ) else {
            panic!("publication observation")
        };
        let SourceRead::Freeze(source) =
            observe(&mut self.source, &self.clock, 20, SourceQuery::Freeze)
        else {
            panic!("source observation")
        };
        let targets = std::array::from_fn(|i| {
            let TargetRead::Status(status) = observe(
                &mut self.targets[i],
                &self.clock,
                21 + i as u128,
                TargetQuery::Status,
            ) else {
                panic!("target observation")
            };
            status
        });
        Observed {
            intent,
            publication,
            source,
            targets,
        }
    }
    // Exercise the public operator, not a second test-only decision machine.
    fn resume_one(&mut self) -> Phase {
        operator::resume_one(self)
    }
    fn close_all(&mut self) {
        if self.checkpoint {
            compact(&mut self.parent, &self.clock, 1);
            compact(&mut self.source, &self.clock, 20);
            compact(&mut self.targets[0], &self.clock, 21);
            compact(&mut self.targets[1], &self.clock, 22);
        }
        close(std::mem::take(&mut self.parent), &self.clock, 1, || {
            drive(&mut self.source, &self.clock, |_| true);
            for target in &mut self.targets {
                drive(target, &self.clock, |_| true);
            }
        });
        close(std::mem::take(&mut self.source), &self.clock, 20, || {
            for target in &mut self.targets {
                drive(target, &self.clock, |_| true);
            }
        });
        let left = std::mem::take(&mut self.targets[0]);
        close(left, &self.clock, 21, || {
            drive(&mut self.targets[1], &self.clock, |_| true);
        });
        close(std::mem::take(&mut self.targets[1]), &self.clock, 22, || {});
    }
    fn restart(&mut self) {
        self.close_all();
        self.parent = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            self.metadata,
        );
        self.source = open(
            configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            source_fixture::fresh,
        );
        self.targets = std::array::from_fn(|i| {
            open(
                configuration(
                    &self.root,
                    21 + i as u128,
                    &[1, 2, 3],
                    NativeOpenMode::Recover,
                ),
                &self.clock,
                self.protocol,
                || target_fixture::fresh_for(21 + i as u128),
            )
        });
    }
    fn serving(&mut self, state: &Observed) {
        let source = observe(
            &mut self.source,
            &self.clock,
            20,
            SourceQuery::Data(RoutedQuery {
                hint: source_fixture::hint(1),
                key: vec![1],
                query: vec![1],
            }),
        );
        if state.source.is_some() {
            assert_eq!(
                source,
                SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
            );
            assert!(self.source[0]
                .propose(ClientRequest {
                    group: group(20),
                    operation: OperationId::new(900).unwrap(),
                    bytes: source_fixture::data(1, 1)
                })
                .is_err());
        } else {
            assert_eq!(source, SourceRead::Data(RoutedRead::Served(7)));
        }
        for (i, key, value) in [(0, 1u8, 7), (1, 200u8, 11)] {
            let g = 21 + i as u128;
            let result = observe(
                &mut self.targets[i],
                &self.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: hint(key),
                    key: vec![key],
                    query: vec![key],
                }),
            );
            if state.targets[i].activated.is_some() {
                assert!(state.source.is_some() && state.publication.is_some());
                assert_eq!(result, TargetRead::Data(value));
            } else {
                assert_eq!(result, TargetRead::NotActive);
                assert!(self.targets[i][0]
                    .propose(ClientRequest {
                        group: group(g),
                        operation: OperationId::new(900).unwrap(),
                        bytes: data(key, 1)
                    })
                    .is_err());
            }
        }
    }
}

#[path = "split_membership.rs"]
mod membership;
#[path = "split_operator.rs"]
mod operator;
#[path = "repeat.rs"]
mod repeat;
fn hint(key: u8) -> RouteHint {
    let mut h = source_fixture::hint(key);
    h.group = group(if key < 128 { 21 } else { 22 });
    h.scope = if key < 128 {
        source_fixture::range(0, 128)
    } else {
        source_fixture::range(128, 256)
    };
    h.epoch = OwnershipEpoch::new(2).unwrap();
    h.generation = RouteGeneration::new(2).unwrap();
    h
}
fn data(key: u8, delta: i64) -> Vec<u8> {
    voteboat::routed::encode_routed(
        hint(key),
        &[key],
        &encode_add(&[key], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn write_independent_children(rig: &mut Split) {
    // Both target scopes remain usable with all ancestor/source workers stopped.
    close(std::mem::take(&mut rig.parent), &rig.clock, 1, || {
        for target in &mut rig.targets {
            drive(target, &rig.clock, |_| true);
        }
    });
    close(std::mem::take(&mut rig.source), &rig.clock, 20, || {
        for target in &mut rig.targets {
            drive(target, &rig.clock, |_| true);
        }
    });
    for (i, key, old_op, value) in [(0, 1u8, 1, 7), (1, 200u8, 2, 11)] {
        let g = 21 + i as u128;
        campaign(&mut rig.targets[i], &rig.clock, g);
        let TargetOutcome::Applied(retry) =
            propose_recovering(&mut rig.targets[i], &rig.clock, g, old_op, data(key, value))
                .outcome
        else {
            panic!("imported retry")
        };
        assert!(retry.duplicate);
        assert_eq!(retry.outcome, BucketOutcome::Value(value));
        let TargetOutcome::Applied(write) = propose_recovering(
            &mut rig.targets[i],
            &rig.clock,
            g,
            100 + old_op,
            data(key, 2),
        )
        .outcome
        else {
            panic!("write")
        };
        assert_eq!(write.outcome, BucketOutcome::Value(value + 2));
    }
}
fn interrupted(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Split::new(protocol, checkpoint);
    let expected = [
        Phase::Intent,
        Phase::Stage(21),
        Phase::Stage(22),
        Phase::Fence,
        Phase::Import(21),
        Phase::Import(22),
        Phase::Publish,
        Phase::Activate(21),
        Phase::Activate(22),
    ];
    for phase in expected {
        eprintln!("split {protocol:?} checkpoint={checkpoint} phase={phase:?}");
        assert_eq!(rig.resume_one(), phase);
        let before = rig.observed(); // Oracle only; not supplied to resume_one.
        rig.serving(&before);
        rig.restart();
        let recovered = rig.observed();
        assert_eq!(recovered, before, "recovered {phase:?}");
        rig.serving(&recovered);
    }
    assert_eq!(rig.resume_one(), Phase::Done);
    let original = rig.observed();
    write_independent_children(&mut rig);
    // Reopen once more after child writes. No phase can be performed twice.
    rig.restart();
    assert_eq!(rig.resume_one(), Phase::Done);
    assert_eq!(rig.observed(), original);
    for (i, key, value) in [(0, 1u8, 9), (1, 200u8, 13)] {
        let g = 21 + i as u128;
        assert_eq!(
            observe(
                &mut rig.targets[i],
                &rig.clock,
                g,
                TargetQuery::Data(RoutedQuery {
                    hint: hint(key),
                    key: vec![key],
                    query: vec![key]
                })
            ),
            TargetRead::Data(value)
        );
        assert!(rig.targets[i]
            .iter()
            .all(|n| n.local().applications[&group(g)]
                .application()
                .outbox()
                .count()
                == 2));
        let old = observe(
            &mut rig.targets[i],
            &rig.clock,
            g,
            TargetQuery::Data(RoutedQuery {
                hint: source_fixture::hint(key),
                key: vec![key],
                query: vec![key],
            }),
        );
        assert!(matches!(old, TargetRead::Rejected(_)));
    }
    let source = observe(
        &mut rig.source,
        &rig.clock,
        20,
        SourceQuery::Data(RoutedQuery {
            hint: source_fixture::hint(200),
            key: vec![200],
            query: vec![200],
        }),
    );
    assert_eq!(
        source,
        SourceRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    rig.close_all();
    std::fs::remove_dir_all(rig.root).unwrap();
}
#[test]
fn tcp_split_resumes_every_phase_from_wal_status_without_dual_owners() {
    interrupted(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_split_resumes_every_phase_from_checkpoint_status_without_dual_owners() {
    interrupted(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_split_resumes_every_phase_from_wal_status_without_dual_owners() {
    interrupted(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_split_resumes_every_phase_from_checkpoint_status_without_dual_owners() {
    interrupted(NativePeerProtocol::Quic, true);
}
