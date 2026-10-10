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
type Namespace = CreatedNamespace<Counter, HostPolicy>;
struct Service {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    interrupted: bool,
    plan: NamespacePlan,
    parent_manifest: ResponsibilityManifest,
    parents: Vec<Node<Directory>>,
    nodes: Vec<Node<Namespace>>,
}
fn fresh(plan: &NamespacePlan) -> Namespace {
    CreatedNamespace::new(
        plan.clone(),
        Counter::new(64).unwrap(),
        HostPolicy,
        limits(),
    )
    .unwrap_or_else(|_| panic!("created namespace"))
}
fn hint() -> RouteHint {
    let mut hint = super::super::super::hint(4);
    hint.responsibility = responsibility(50);
    hint.group = group(100);
    hint
}
fn query() -> NamespaceQuery<()> {
    NamespaceQuery::Data(RoutedQuery {
        hint: hint(),
        key: vec![4],
        query: (),
    })
}
fn data() -> Vec<u8> {
    encode_routed(hint(), &[4], &7i64.to_le_bytes(), 1024).unwrap()
}
fn reserve(
    parents: &mut [Node<Directory>],
    clock: &Instant,
    intent: &GroupCreationIntent,
) -> (NamespacePlan, ResponsibilityManifest) {
    campaign(parents, clock, 1);
    let boot = namespace_directory()
        .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap();
    assert_eq!(
        propose(parents, clock, 1, 10000, boot).outcome,
        DirectoryOutcome::Initialized
    );
    let parent_manifest = manifests()[0].clone();
    propose(
        parents,
        clock,
        1,
        10001,
        DirectoryCommand {
            expected: None,
            manifest: parent_manifest.clone(),
        }
        .encode(MAX_DIRECTORY_COMMAND_BYTES)
        .unwrap(),
    );
    assert_eq!(
        propose(
            parents,
            clock,
            1,
            10002,
            intent.encode(MAX_GROUP_CREATION_BYTES).unwrap()
        )
        .outcome,
        DirectoryOutcome::CreationReserved
    );
    let creation = parents[0].local().applications[&group(1)]
        .group_creation_at(0, group(100))
        .unwrap()
        .unwrap();
    let mut m = grant().into_input();
    m.responsibility = responsibility(50);
    m.parent = None;
    m.execution = ExecutionMode::Single(group(100));
    let plan = NamespacePlan {
        creation: creation.clone(),
        manifest: ResponsibilityManifest::new(m).unwrap(),
    };
    (plan, parent_manifest)
}
fn establish(
    root: &Path,
    parents: &[Node<Directory>],
    targets: &[NativeStartup],
    intent: &GroupCreationIntent,
    plan: &NamespacePlan,
) {
    let creation = &plan.creation;
    std::fs::create_dir_all(root.join("100")).unwrap();
    for c in targets {
        let verified = VerifiedGroupCreation::verify(
            &LocalCreationAuthority {
                core: parents[0].local().owner.core(group(1)).unwrap(),
                directory: &parents[0].local().applications[&group(1)],
            },
            creation.clone(),
            c.node,
            c.store,
            intent.application,
        )
        .unwrap();
        let mut log = NativeLogStore::create(
            FileLogIo::create(&c.directory).unwrap(),
            c.store,
            LogLimits::default(),
        )
        .unwrap();
        let mut bindings = FileCreationBindings::open(&c.directory).unwrap();
        establish_created_group(&verified, &mut log, &mut bindings).unwrap();
        NativeSnapshotStore::create(
            FileSnapshotIo::create(c.directory.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: c.store,
                group: group(100),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
}
impl Service {
    fn new(protocol: NativePeerProtocol, interrupted: bool) -> Self {
        let clock = Instant::now();
        let root = std::env::temp_dir().join(format!(
            "voteboat-namespace-service-{}-{protocol:?}-{interrupted}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let targets = configuration(&root, 100, &[1, 2, 3], NativeOpenMode::Recover);
        let intent = GroupCreationIntent {
            authority: group(1),
            parent: responsibility(1),
            expected: RouteGeneration::new(1).unwrap(),
            responsibility: responsibility(50),
            bootstrap: targets[0].bootstrap.clone(),
            application: grant().input().application,
            mode: GroupCreationMode::Empty,
        };
        let mut parents = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            namespace_directory,
        );
        let (plan, parent_manifest) = reserve(&mut parents, &clock, &intent);
        establish(&root, &parents, &targets, &intent, &plan);
        let nodes = open(targets, &clock, protocol, || fresh(&plan));
        Self {
            root,
            clock,
            protocol,
            interrupted,
            plan,
            parent_manifest,
            parents,
            nodes,
        }
    }
    fn prepare_ready(&mut self) -> NamespaceStatus {
        campaign(&mut self.nodes, &self.clock, 100);
        let bytes = data();
        assert!(self.nodes[0].local().applications[&group(100)]
            .validate_proposal(OperationId::new(20000).unwrap(), &bytes, std::iter::empty())
            .is_err());
        assert_eq!(
            read(&mut self.nodes, &self.clock, 100, query()),
            NamespaceRead::NotActive
        );
        let init = fresh(&self.plan)
            .initialization_command(MAX_ROUTED_COMMAND_BYTES)
            .unwrap();
        if self.interrupted {
            let ticket = self.nodes[0]
                .propose(ClientRequest {
                    group: group(100),
                    operation: self.plan.creation.operation,
                    bytes: init,
                })
                .unwrap();
            drive(&mut self.nodes, &self.clock, |ns| {
                ns.iter().all(|n| {
                    n.local().applications[&group(100)]
                        .status()
                        .ready_index
                        .is_some()
                })
            });
            assert_eq!(ticket.operation, self.plan.creation.operation);
            assert_eq!(self.nodes[0].local().clients.usage().requests, 1);
        } else {
            assert_eq!(
                propose(&mut self.nodes, &self.clock, 100, 10002, init).outcome,
                NamespaceOutcome::Ready
            );
        }
        let NamespaceRead::Status(ready) =
            read(&mut self.nodes, &self.clock, 100, NamespaceQuery::Status)
        else {
            panic!("ready status")
        };
        ready
    }
    fn reopen_ready(&mut self, ready: NamespaceStatus) {
        if !self.interrupted {
            for n in &mut self.nodes {
                n.control(group(100), NodeControl::Checkpoint).unwrap();
            }
            drive(&mut self.nodes, &self.clock, |ns| {
                ns.iter().all(|n| {
                    n.local()
                        .owner
                        .core(group(100))
                        .unwrap()
                        .state()
                        .base_index()
                        == n.local().applications[&group(100)].applied_index()
                })
            });
        }
        if self.interrupted {
            let logs = abandon(std::mem::take(&mut self.nodes), 100);
            assert!(logs.values().all(|s| s.base_index() == 0));
            assert!(logs
                .values()
                .all(|s| s.commit_index >= ready.ready_index.unwrap()));
        } else {
            close(std::mem::take(&mut self.nodes), &self.clock, 100, || {
                drive(&mut self.parents, &self.clock, |_| true);
            });
        }
        self.nodes = open(
            configuration(&self.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || fresh(&self.plan),
        );
        campaign(&mut self.nodes, &self.clock, 100);
        assert!(self
            .nodes
            .iter()
            .all(|n| n.local().applications[&group(100)].status() == ready));
        assert_eq!(
            read(&mut self.nodes, &self.clock, 100, query()),
            NamespaceRead::NotActive
        );
    }
    fn publish(
        &mut self,
        publication: NamespacePublication,
    ) -> (NamespacePublicationStatus, Vec<u8>) {
        campaign(&mut self.parents, &self.clock, 1);
        let pub_bytes = publication.encode(MAX_NAMESPACE_PUBLICATION_BYTES).unwrap();
        if self.interrupted {
            let ticket = self.parents[0]
                .propose(ClientRequest {
                    group: group(1),
                    operation: OperationId::new(10003).unwrap(),
                    bytes: pub_bytes.clone(),
                })
                .unwrap();
            drive(&mut self.parents[..2], &self.clock, |ns| {
                ns.iter().all(|n| {
                    n.local().applications[&group(1)]
                        .namespace_publication_at(0, self.plan.creation.operation)
                        .unwrap()
                        .is_some()
                })
            });
            assert_eq!(ticket.operation, OperationId::new(10003).unwrap());
            assert_eq!(self.parents[0].local().clients.usage().requests, 1);
            assert!(self.parents[2].local().applications[&group(1)]
                .manifest(responsibility(50))
                .is_none());
        } else {
            assert_eq!(
                propose(&mut self.parents, &self.clock, 1, 10003, pub_bytes.clone()).outcome,
                DirectoryOutcome::NamespacePublished(RouteGeneration::new(1).unwrap())
            );
        }
        let status = self.parents[0].local().applications[&group(1)]
            .namespace_publication_at(0, self.plan.creation.operation)
            .unwrap()
            .unwrap();
        (status, pub_bytes)
    }
    fn close_publication(&mut self, status: &NamespacePublicationStatus) {
        if !self.interrupted {
            for n in &mut self.parents {
                n.control(group(1), NodeControl::Checkpoint).unwrap();
            }
            drive(&mut self.parents, &self.clock, |ns| {
                ns.iter().all(|n| {
                    n.local().owner.core(group(1)).unwrap().state().base_index()
                        == n.local().applications[&group(1)].applied_index()
                })
            });
        }
        if self.interrupted {
            let quorum = self.parents[..2]
                .iter()
                .map(|n| n.local().owner.core(group(1)).unwrap().local_node())
                .collect::<Vec<_>>();
            let lagging = self.parents[2]
                .local()
                .owner
                .core(group(1))
                .unwrap()
                .local_node();
            let logs = abandon(std::mem::take(&mut self.parents), 1);
            assert!(logs.values().all(|s| s.base_index() == 0));
            assert!(quorum.iter().all(|n| logs[n].commit_index >= status.index));
            assert!(logs[&lagging].commit_index < status.index);
        } else {
            close(std::mem::take(&mut self.parents), &self.clock, 1, || {
                drive(&mut self.nodes, &self.clock, |_| true);
            });
        }
    }
    fn recover_publication(&mut self, status: &NamespacePublicationStatus, pub_bytes: Vec<u8>) {
        self.parents = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            namespace_directory,
        );
        campaign(&mut self.parents, &self.clock, 1);
        assert!(propose(&mut self.parents, &self.clock, 1, 10003, pub_bytes).duplicate);
        assert_eq!(
            self.parents[0].local().applications[&group(1)]
                .namespace_publication_at(0, self.plan.creation.operation)
                .unwrap(),
            Some(status.clone())
        );
        assert!(self
            .parents
            .iter()
            .all(|n| n.local().applications[&group(1)]
                .namespace_publication_at(0, self.plan.creation.operation)
                .unwrap()
                == Some(status.clone())));
        assert_eq!(
            self.parents[0].local().applications[&group(1)].manifest(responsibility(1)),
            Some(&self.parent_manifest)
        );
        assert_eq!(
            self.parents[0].local().applications[&group(1)].manifest(responsibility(50)),
            Some(&self.plan.manifest)
        );
    }
    fn stop_metadata(&mut self) -> BTreeMap<NodeId, GroupLog> {
        let observed = read(&mut self.parents, &self.clock, 1, responsibility(50)).unwrap();
        let mut cache = NativeManifestCache::new(ManifestCacheLimits {
            manifests: 1,
            bytes: 4096,
        })
        .unwrap();
        cache.admit(observed).unwrap();
        assert_eq!(
            resolve(&cache, &HostPolicy, responsibility(50), &[4], 1).unwrap(),
            hint()
        );
        let parent_ids = self
            .parents
            .iter()
            .map(|n| n.local().owner.core(group(1)).unwrap().local_node())
            .collect::<Vec<_>>();
        parent_ids
            .into_iter()
            .zip(close(
                std::mem::take(&mut self.parents),
                &self.clock,
                1,
                || {
                    drive(&mut self.nodes, &self.clock, |_| true);
                },
            ))
            .collect::<BTreeMap<_, _>>()
    }
    fn activate(&mut self, status: &NamespacePublicationStatus) -> Vec<u8> {
        campaign(&mut self.nodes, &self.clock, 100);
        assert_eq!(
            read(&mut self.nodes, &self.clock, 100, query()),
            NamespaceRead::NotActive
        );
        let activation = self.nodes[0].local().applications[&group(100)]
            .activation_command(status, 2000)
            .unwrap();
        if self.interrupted {
            let ticket = self.nodes[0]
                .propose(ClientRequest {
                    group: group(100),
                    operation: status.operation,
                    bytes: activation.clone(),
                })
                .unwrap();
            drive(&mut self.nodes[..2], &self.clock, |ns| {
                ns.iter().all(|n| {
                    n.local().applications[&group(100)]
                        .status()
                        .activation_index
                        .is_some()
                })
            });
            let original = self.nodes[0].local().applications[&group(100)].status();
            assert_eq!(ticket.operation, status.operation);
            assert_eq!(self.nodes[0].local().clients.usage().requests, 1);
            assert!(self.nodes[2].local().applications[&group(100)]
                .status()
                .activation_index
                .is_none());
            assert_eq!(
                self.nodes[2].local().applications[&group(100)]
                    .read_at(
                        self.nodes[2].local().applications[&group(100)].applied_index(),
                        query()
                    )
                    .unwrap(),
                NamespaceRead::NotActive
            );
            let quorum = self.nodes[..2]
                .iter()
                .map(|n| n.local().owner.core(group(100)).unwrap().local_node())
                .collect::<Vec<_>>();
            let lagging = self.nodes[2]
                .local()
                .owner
                .core(group(100))
                .unwrap()
                .local_node();
            let logs = abandon(std::mem::take(&mut self.nodes), 100);
            assert!(logs.values().all(|s| s.base_index() == 0));
            assert!(quorum
                .iter()
                .all(|n| logs[n].commit_index >= original.activation_index.unwrap()));
            assert!(logs[&lagging].commit_index < original.activation_index.unwrap());
            self.nodes = open(
                configuration(&self.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
                &self.clock,
                self.protocol,
                || fresh(&self.plan),
            );
            campaign(&mut self.nodes, &self.clock, 100);
            assert_eq!(
                propose(&mut self.nodes, &self.clock, 100, 10003, activation.clone()).outcome,
                NamespaceOutcome::Activated
            );
            assert!(self
                .nodes
                .iter()
                .all(|n| n.local().applications[&group(100)].status() == original));
        } else {
            assert_eq!(
                propose(&mut self.nodes, &self.clock, 100, 10003, activation.clone()).outcome,
                NamespaceOutcome::Activated
            );
        }
        activation
    }
    fn exercise(&mut self) {
        let bytes = data();
        let original = propose(&mut self.nodes, &self.clock, 100, 20000, bytes.clone());
        assert!(matches!(
            original.outcome,
            NamespaceOutcome::Data(RoutedReceipt {
                outcome: RoutedOutcome::Applied(CounterReceipt {
                    outcome: CounterOutcome::Value(7),
                    duplicate: false,
                    ..
                }),
                ..
            })
        ));
        assert_eq!(
            read(&mut self.nodes, &self.clock, 100, query()),
            NamespaceRead::Data(RoutedRead::Served(7))
        );
        for n in &mut self.nodes {
            n.control(group(100), NodeControl::Checkpoint).unwrap();
        }
        drive(&mut self.nodes, &self.clock, |ns| {
            ns.iter().all(|n| {
                n.local()
                    .owner
                    .core(group(100))
                    .unwrap()
                    .state()
                    .base_index()
                    == n.local().applications[&group(100)].applied_index()
            })
        });
        close(std::mem::take(&mut self.nodes), &self.clock, 100, || {});
    }
    fn recover_service(&mut self, activation: Vec<u8>) {
        let bytes = data();
        self.nodes = open(
            configuration(&self.root, 100, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || fresh(&self.plan),
        );
        campaign(&mut self.nodes, &self.clock, 100);
        assert_eq!(
            propose(&mut self.nodes, &self.clock, 100, 10003, activation).outcome,
            NamespaceOutcome::Activated
        );
        let retry = propose(&mut self.nodes, &self.clock, 100, 20000, bytes);
        assert!(matches!(
            retry.outcome,
            NamespaceOutcome::Data(RoutedReceipt {
                outcome: RoutedOutcome::Applied(CounterReceipt {
                    outcome: CounterOutcome::Value(7),
                    duplicate: true,
                    ..
                }),
                ..
            })
        ));
        assert_eq!(
            read(&mut self.nodes, &self.clock, 100, query()),
            NamespaceRead::Data(RoutedRead::Served(7))
        );
        close(std::mem::take(&mut self.nodes), &self.clock, 100, || {});
    }
    fn verify_metadata(&mut self, parent_logs: BTreeMap<NodeId, GroupLog>) {
        for n in 1..=3 {
            let log = NativeLogStore::recover(
                FileLogIo::open(self.root.join(format!("1/{n}"))).unwrap(),
                support::identity(n),
                LogLimits::default(),
            )
            .unwrap();
            assert_eq!(
                parent_logs[&support::node(n as u64)],
                log.state(group(1)).unwrap()
            );
        }
    }
}
pub(super) fn run(protocol: NativePeerProtocol, interrupted: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut service = Service::new(protocol, interrupted);
    let ready = service.prepare_ready();
    let publication = NamespacePublication::from_status(&service.plan, ready).unwrap();
    service.reopen_ready(ready);
    let (status, bytes) = service.publish(publication);
    service.close_publication(&status);
    service.recover_publication(&status, bytes);
    let parent_logs = service.stop_metadata();
    let activation = service.activate(&status);
    service.exercise();
    service.recover_service(activation);
    service.verify_metadata(parent_logs);
    std::fs::remove_dir_all(service.root).unwrap();
}
