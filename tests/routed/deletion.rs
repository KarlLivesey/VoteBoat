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
use super::super::{creation, creation_source::durable_files};
use super::*;
use voteboat::{bucket_counter::BucketCounter, deletion::*};
type Owner = RoutedControlReads<BucketCounter<source_fixture::Policy>, source_fixture::Policy>;
fn manifests() -> (ResponsibilityManifest, ResponsibilityManifest) {
    let mut c = source_fixture::grant().into_input();
    c.responsibility = fixture::id(11);
    c.parent = Some(ParentAuthority {
        responsibility: fixture::id(10),
        group: group(1),
    });
    c.authority = group(2);
    c.scope = source_fixture::range(0, 128);
    c.execution = ExecutionMode::Single(group(21));
    let child = ResponsibilityManifest::new(c).unwrap();
    let mut p = source_fixture::grant().into_input();
    p.execution = ExecutionMode::Delegated(vec![
        RouteEntry {
            scope: child.input().scope,
            target: RouteTarget::Child(ChildAuthority {
                responsibility: child.input().responsibility,
                group: group(2),
                epoch: child.input().epoch,
            }),
        },
        RouteEntry {
            scope: source_fixture::range(128, 256),
            target: RouteTarget::Group(group(20)),
        },
    ]);
    (ResponsibilityManifest::new(p).unwrap(), child)
}
fn metadata(child: bool) -> LifecycleDirectory {
    let (p, c) = manifests();
    let m = if child { c } else { p };
    LifecycleDirectory::new(
        Directory::new(
            DirectoryPlan::new(m.input().authority, vec![m]).unwrap(),
            DirectoryLimits {
                operations: 8,
                history_bytes: 200000,
            },
        )
        .unwrap()
        .with_namespace_deletion()
        .unwrap_or_else(|_| panic!("schema9")),
    )
}
fn owner(child: bool) -> Owner {
    let (p, c) = manifests();
    let (m, g, scope) = if child {
        (c, 21, source_fixture::range(0, 128))
    } else {
        (p, 20, source_fixture::range(128, 256))
    };
    RoutedControlReads::new(
        RoutedApplication::new(
            group(g),
            m,
            BucketCounter::new(
                scope,
                source_fixture::Policy,
                source_fixture::bucket_limits(),
            )
            .unwrap(),
            source_fixture::Policy,
            RoutedLimits {
                operations: 32,
                semantic_bytes: 8192,
                payload_bytes: 1024,
                inner_checkpoint_bytes: source_fixture::bucket_limits().checkpoint_bound().unwrap(),
            },
        )
        .unwrap_or_else(|e| panic!("{:?}", e.error)),
    )
}
fn hint(child: bool) -> RouteHint {
    let mut h = source_fixture::hint(if child { 1 } else { 200 });
    h.scope = if child {
        source_fixture::range(0, 128)
    } else {
        source_fixture::range(128, 256)
    };
    if child {
        h.responsibility = fixture::id(11);
        h.group = group(21);
    }
    h
}
fn data(child: bool, delta: i64) -> Vec<u8> {
    let k = if child { 1 } else { 200 };
    encode_routed(
        hint(child),
        &[k],
        &encode_add(&[k], delta, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap()
}
fn read(nodes: &mut [Node<Owner>], clock: &Instant, child: bool) -> RoutedControlRead<i64> {
    let k = if child { 1 } else { 200 };
    observe(
        nodes,
        clock,
        if child { 21 } else { 20 },
        RoutedControlQuery::Data(RoutedQuery {
            hint: hint(child),
            key: vec![k],
            query: vec![k],
        }),
    )
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct MetadataFacts {
    manifest: ResponsibilityManifest,
    intent: Option<DeletionIntentStatus>,
    deleted: Option<DeletionStatus>,
}
fn metadata_facts(
    nodes: &mut [Node<LifecycleDirectory>],
    clock: &Instant,
    child: bool,
) -> MetadataFacts {
    let (p, c) = manifests();
    let (m, g, op) = if child { (c, 2, 210) } else { (p, 1, 200) };
    let DirectoryRead::Manifest(Some(manifest)) = observe(
        nodes,
        clock,
        g,
        DirectoryQuery::Manifest(m.input().responsibility),
    ) else {
        panic!("manifest")
    };
    let DirectoryRead::DeletionIntent(intent) = observe(
        nodes,
        clock,
        g,
        DirectoryQuery::DeletionIntent(source_fixture::op(op)),
    ) else {
        panic!("intent")
    };
    let DirectoryRead::Deletion(deleted) = observe(
        nodes,
        clock,
        g,
        DirectoryQuery::Deletion(source_fixture::op(op)),
    ) else {
        panic!("deleted")
    };
    MetadataFacts {
        manifest,
        intent,
        deleted,
    }
}
fn full_fence(nodes: &mut [Node<Owner>], clock: &Instant, child: bool) -> Option<OwnershipFence> {
    let RoutedControlRead::Fence(f) = observe(
        nodes,
        clock,
        if child { 21 } else { 20 },
        RoutedControlQuery::Fence,
    ) else {
        panic!("F")
    };
    f
}
#[derive(Clone, Debug, Eq, PartialEq)]
struct Facts {
    parent: MetadataFacts,
    child: MetadataFacts,
    parent_fence: Option<OwnershipFence>,
    child_fence: Option<OwnershipFence>,
}
struct Rig {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    checkpoint: bool,
    parent: Vec<Node<LifecycleDirectory>>,
    child: Vec<Node<LifecycleDirectory>>,
    parent_owner: Vec<Node<Owner>>,
    child_owner: Vec<Node<Owner>>,
}
impl Rig {
    fn new(protocol: NativePeerProtocol, checkpoint: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-delete-native-{}-{protocol:?}-{checkpoint}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let clock = Instant::now();
        let mut parent = open(
            configuration(&root, 1, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            || metadata(false),
        );
        let mut child = open(
            configuration(&root, 2, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            || metadata(true),
        );
        let (p, c) = manifests();
        initialize(&mut parent, &clock, 1, p);
        initialize(&mut child, &clock, 2, c);
        let mut parent_owner = open(
            configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            || owner(false),
        );
        let mut child_owner = open(
            configuration(&root, 21, &[1, 2, 3], NativeOpenMode::Create),
            &clock,
            protocol,
            || owner(true),
        );
        campaign(&mut parent_owner, &clock, 20);
        propose_recovering(
            &mut parent_owner,
            &clock,
            20,
            100,
            owner(false).routed().bootstrap_command(200000).unwrap(),
        );
        propose_recovering(&mut parent_owner, &clock, 20, 2, data(false, 11));
        campaign(&mut child_owner, &clock, 21);
        propose_recovering(
            &mut child_owner,
            &clock,
            21,
            100,
            owner(true).routed().bootstrap_command(200000).unwrap(),
        );
        propose_recovering(&mut child_owner, &clock, 21, 1, data(true, 7));
        assert_eq!(
            read(&mut parent_owner, &clock, false),
            RoutedControlRead::Data(RoutedRead::Served(11))
        );
        assert_eq!(
            read(&mut child_owner, &clock, true),
            RoutedControlRead::Data(RoutedRead::Served(7))
        );
        Self {
            root,
            clock,
            protocol,
            checkpoint,
            parent,
            child,
            parent_owner,
            child_owner,
        }
    }
    fn facts(&mut self) -> Facts {
        Facts {
            parent: metadata_facts(&mut self.parent, &self.clock, false),
            child: metadata_facts(&mut self.child, &self.clock, true),
            parent_fence: full_fence(&mut self.parent_owner, &self.clock, false),
            child_fence: full_fence(&mut self.child_owner, &self.clock, true),
        }
    }
    fn reopen(&mut self) {
        if self.checkpoint {
            compact(&mut self.parent, &self.clock, 1);
            compact(&mut self.child, &self.clock, 2);
            compact(&mut self.parent_owner, &self.clock, 20);
            compact(&mut self.child_owner, &self.clock, 21);
        }
        creation::abandon(std::mem::take(&mut self.parent), 1);
        creation::abandon(std::mem::take(&mut self.child), 2);
        creation::abandon(std::mem::take(&mut self.parent_owner), 20);
        creation::abandon(std::mem::take(&mut self.child_owner), 21);
        self.parent = open(
            configuration(&self.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || metadata(false),
        );
        self.child = open(
            configuration(&self.root, 2, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || metadata(true),
        );
        self.parent_owner = open(
            configuration(&self.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || owner(false),
        );
        self.child_owner = open(
            configuration(&self.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
            &self.clock,
            self.protocol,
            || owner(true),
        );
    }
    fn phase(&mut self, g: u128, op: u128, b: Vec<u8>) {
        match g {
            1 => {
                campaign(&mut self.parent, &self.clock, g);
                phase_write(true, &mut self.parent, &self.clock, g, op, b.clone());
            }
            2 => {
                campaign(&mut self.child, &self.clock, g);
                phase_write(true, &mut self.child, &self.clock, g, op, b.clone());
            }
            20 => {
                campaign(&mut self.parent_owner, &self.clock, g);
                phase_write(true, &mut self.parent_owner, &self.clock, g, op, b.clone());
            }
            21 => {
                campaign(&mut self.child_owner, &self.clock, g);
                phase_write(true, &mut self.child_owner, &self.clock, g, op, b.clone());
            }
            _ => unreachable!(),
        }
        let original = self.facts();
        self.reopen();
        assert_eq!(self.facts(), original);
        match g {
            1 => {
                campaign(&mut self.parent, &self.clock, g);
                assert!(propose_recovering(&mut self.parent, &self.clock, g, op, b).duplicate);
            }
            2 => {
                campaign(&mut self.child, &self.clock, g);
                assert!(propose_recovering(&mut self.child, &self.clock, g, op, b).duplicate);
            }
            20 => {
                campaign(&mut self.parent_owner, &self.clock, g);
                assert!(
                    matches!(propose_recovering(&mut self.parent_owner,&self.clock,g,op,b).outcome,RoutedOutcome::Fenced(f) if Some(f)==original.parent_fence)
                );
            }
            21 => {
                campaign(&mut self.child_owner, &self.clock, g);
                assert!(
                    matches!(propose_recovering(&mut self.child_owner,&self.clock,g,op,b).outcome,RoutedOutcome::Fenced(f) if Some(f)==original.child_fence)
                );
            }
            _ => unreachable!(),
        }
        assert_eq!(self.facts(), original);
    }
}
fn configuration_id<A>(nodes: &[Node<A>], g: u128) -> ConfigurationId
where
    A: ProposalAdmission + BoundedReadableStateMachine + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    nodes[0]
        .local()
        .owner
        .core(group(g))
        .unwrap()
        .membership()
        .id()
}
// Diagnostic cloned-provider replay verifies retained retry/outbox contents; it
// does not serve a request or alter the fenced native owner.
fn retained_data(nodes: &[Node<Owner>], child: bool) {
    let (g, k, op, value) = if child {
        (21, 1, 1, 7)
    } else {
        (20, 200, 2, 11)
    };
    for n in nodes {
        let original = n.local().applications[&group(g)].routed().application();
        assert_eq!(original.value(&[k]).unwrap(), value);
        assert_eq!(original.outbox().count(), 1);
        let mut copy = original.clone();
        let receipt = copy
            .apply_batch(&[source_fixture::entry(
                copy.applied_index() + 1,
                op,
                encode_add(&[k], value, b"effect", 1024).unwrap(),
            )])
            .unwrap()
            .remove(0);
        assert!(receipt.duplicate);
        assert_eq!(receipt.outcome, BucketOutcome::Value(value));
        assert_eq!(copy.outbox().count(), 1);
    }
}
fn complete_deletion(r: &mut Rig) -> Facts {
    let (p, c) = manifests();
    r.phase(
        1,
        200,
        DeletionIntent { before: p.clone() }.encode(200000).unwrap(),
    );
    r.phase(
        2,
        210,
        DeletionIntent { before: c.clone() }.encode(200000).unwrap(),
    );
    r.phase(21, 210, encode_fence(c.input().epoch));
    assert_eq!(
        read(&mut r.child_owner, &r.clock, true),
        RoutedControlRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(
        read(&mut r.parent_owner, &r.clock, false),
        RoutedControlRead::Data(RoutedRead::Served(11))
    );
    let facts = r.facts();
    let child_completion = DeletionCompletion {
        intent: facts.child.intent.unwrap(),
        fences: vec![DeletionFenceEvidence {
            configuration: configuration_id(&r.child_owner, 21),
            fence: facts.child_fence.unwrap(),
        }],
        children: vec![],
    };
    r.phase(2, 211, child_completion.encode(200000).unwrap());
    r.phase(20, 200, encode_fence(p.input().epoch));
    let original = r.facts();
    let parent_completion = DeletionCompletion {
        intent: original.parent.intent.clone().unwrap(),
        fences: vec![DeletionFenceEvidence {
            configuration: configuration_id(&r.parent_owner, 20),
            fence: original.parent_fence.unwrap(),
        }],
        children: vec![ChildDeletionEvidence::from_status(
            configuration_id(&r.child, 2),
            original.child.deleted.as_ref().unwrap(),
        )
        .unwrap()],
    };
    let mut missing = parent_completion.clone();
    missing.children.clear();
    assert!(missing.encode(200000).is_err());
    assert!(original.parent.deleted.is_none());
    r.phase(1, 201, parent_completion.encode(200000).unwrap());
    let deleted = r.facts();
    assert_eq!(
        deleted.parent.manifest,
        parent_completion.intent.intent.tombstone().unwrap()
    );
    assert_eq!(
        deleted.child.manifest,
        child_completion.intent.intent.tombstone().unwrap()
    );
    assert!(deleted.parent.deleted.is_some() && deleted.child.deleted.is_some());
    deleted
}
fn run(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut r = Rig::new(protocol, checkpoint);
    let deleted = complete_deletion(&mut r);
    retained_data(&r.parent_owner, false);
    retained_data(&r.child_owner, true);
    let parent_logs = creation::abandon(std::mem::take(&mut r.parent), 1);
    let child_logs = creation::abandon(std::mem::take(&mut r.child), 2);
    let parent_files = durable_files(&r.root.join("1"));
    let child_files = durable_files(&r.root.join("2"));
    for child in [false, true] {
        let (nodes, g) = if child {
            (&mut r.child_owner, 21)
        } else {
            (&mut r.parent_owner, 20)
        };
        assert_eq!(
            read(nodes, &r.clock, child),
            RoutedControlRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
        );
        assert!(nodes[0]
            .propose(ClientRequest {
                group: group(g),
                operation: source_fixture::op(999),
                bytes: data(child, 99)
            })
            .is_err());
        assert_eq!(nodes[0].local().clients.usage().requests, 0);
        assert!(nodes.iter().all(|n| n.local().applications[&group(g)]
            .routed()
            .application()
            .outbox()
            .count()
            == 1));
    }
    if checkpoint {
        compact(&mut r.parent_owner, &r.clock, 20);
        compact(&mut r.child_owner, &r.clock, 21);
    }
    creation::abandon(std::mem::take(&mut r.parent_owner), 20);
    creation::abandon(std::mem::take(&mut r.child_owner), 21);
    r.parent_owner = open(
        configuration(&r.root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &r.clock,
        protocol,
        || owner(false),
    );
    r.child_owner = open(
        configuration(&r.root, 21, &[1, 2, 3], NativeOpenMode::Recover),
        &r.clock,
        protocol,
        || owner(true),
    );
    assert_eq!(
        full_fence(&mut r.parent_owner, &r.clock, false),
        deleted.parent_fence
    );
    assert_eq!(
        full_fence(&mut r.child_owner, &r.clock, true),
        deleted.child_fence
    );
    assert_eq!(
        read(&mut r.parent_owner, &r.clock, false),
        RoutedControlRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    assert_eq!(
        read(&mut r.child_owner, &r.clock, true),
        RoutedControlRead::Data(RoutedRead::Rejected(RoutingError::Fenced))
    );
    retained_data(&r.parent_owner, false);
    retained_data(&r.child_owner, true);
    for (g, logs, files) in [(1, parent_logs, parent_files), (2, child_logs, child_files)] {
        assert_eq!(durable_files(&r.root.join(g.to_string())), files);
        for c in configuration(&r.root, g, &[1, 2, 3], NativeOpenMode::Recover) {
            let log = NativeLogStore::recover(
                FileLogIo::open(&c.directory).unwrap(),
                c.store,
                LogLimits::default(),
            )
            .unwrap();
            assert_eq!(log.state(group(g)).unwrap(), logs[&c.node]);
        }
    }
    r.parent = open(
        configuration(&r.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &r.clock,
        protocol,
        || metadata(false),
    );
    r.child = open(
        configuration(&r.root, 2, &[1, 2, 3], NativeOpenMode::Recover),
        &r.clock,
        protocol,
        || metadata(true),
    );
    assert_eq!(r.facts(), deleted);
    creation::abandon(std::mem::take(&mut r.parent), 1);
    creation::abandon(std::mem::take(&mut r.child), 2);
    creation::abandon(std::mem::take(&mut r.parent_owner), 20);
    creation::abandon(std::mem::take(&mut r.child_owner), 21);
    std::fs::remove_dir_all(&r.root).unwrap();
}
#[test]
fn tcp_recursive_deletion_recovers_unread_phases_from_wal() {
    run(NativePeerProtocol::TcpTls, false)
}
#[test]
fn tcp_recursive_deletion_recovers_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::TcpTls, true)
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_deletion_recovers_unread_phases_from_wal() {
    run(NativePeerProtocol::Quic, false)
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_deletion_recovers_unread_phases_from_checkpoint() {
    run(NativePeerProtocol::Quic, true)
}
