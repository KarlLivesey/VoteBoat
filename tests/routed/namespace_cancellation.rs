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

fn directory() -> Directory {
    namespace_directory()
        .with_creation_cancellation()
        .unwrap_or_else(|_| panic!("schema16"))
}

fn cancel_unread(s: &mut Service, bytes: &[u8]) -> GroupCreationCancellationStatus {
    let creation = s.plan.creation.operation;
    s.parents[0]
        .propose(ClientRequest {
            group: group(1),
            operation: OperationId::new(10003).unwrap(),
            bytes: bytes.to_vec(),
        })
        .unwrap();
    drive(&mut s.parents[..2], &s.clock, |ns| {
        ns.iter().all(|n| {
            n.local().applications[&group(1)]
                .group_creation_cancellation_at(0, creation)
                .unwrap()
                .is_some()
        })
    });
    let status = s.parents[0].local().applications[&group(1)]
        .group_creation_cancellation_at(0, creation)
        .unwrap()
        .unwrap();
    assert_eq!(s.parents[0].local().clients.usage().requests, 1);
    assert!(s.parents[2].local().applications[&group(1)]
        .group_creation_cancellation_at(0, creation)
        .unwrap()
        .is_none());
    let lagging = s.parents[2]
        .local()
        .owner
        .core(group(1))
        .unwrap()
        .local_node();
    let logs = abandon(std::mem::take(&mut s.parents), 1);
    assert!(logs[&lagging].commit_index < status.index);
    assert!(logs
        .iter()
        .filter(|(id, _)| **id != lagging)
        .all(|(_, log)| log.commit_index >= status.index));
    s.parents = open(
        configuration(&s.root, 1, &[1, 2, 3], NativeOpenMode::Recover),
        &s.clock,
        s.protocol,
        directory,
    );
    campaign(&mut s.parents, &s.clock, 1);
    assert!(propose(&mut s.parents, &s.clock, 1, 10003, bytes.to_vec()).duplicate);
    assert!(s.parents.iter().all(|n| n.local().applications[&group(1)]
        .group_creation_cancellation_at(0, creation)
        .unwrap()
        == Some(status)));
    status
}

fn checkpoint_cancellation(s: &mut Service, status: GroupCreationCancellationStatus) {
    for n in &mut s.parents {
        n.control(group(1), NodeControl::Checkpoint).unwrap();
    }
    drive(&mut s.parents, &s.clock, |ns| {
        ns.iter().all(|n| {
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
    assert!(s.parents.iter().all(|n| n.local().applications[&group(1)]
        .group_creation_cancellation_at(0, s.plan.creation.operation)
        .unwrap()
        == Some(status)));
}

pub(crate) fn run(protocol: NativePeerProtocol) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut s = Service::new_in(protocol, true, directory);
    let ready = s.prepare_ready();
    s.reopen_ready(ready); // unread readiness survives target WAL recovery
    let publication = NamespacePublication::from_status(&s.plan, ready).unwrap();
    let bytes = CancelGroupCreation::from_status(&s.plan.creation)
        .encode(56)
        .unwrap();
    let status = cancel_unread(&mut s, &bytes);
    checkpoint_cancellation(&mut s, status);
    for parent in &mut s.parents {
        assert!(parent
            .propose(ClientRequest {
                group: group(1),
                operation: OperationId::new(10004).unwrap(),
                bytes: publication.encode(1024).unwrap(),
            })
            .is_err());
        let app = &parent.local().applications[&group(1)];
        assert!(app.manifest(responsibility(50)).is_none());
        assert_eq!(app.manifest(responsibility(1)), Some(&s.parent_manifest));
        assert!(app.group_creation_at(0, group(100)).unwrap().is_none());
        assert!(LocalCreationAuthority {
            core: parent.local().owner.core(group(1)).unwrap(),
            directory: app,
        }
        .verify(&s.plan.creation)
        .is_err());
    }
    // Metadata recovery pauses target polling; establish fresh target leadership
    // before asking for a quorum read rather than trusting the old leader.
    campaign(&mut s.nodes, &s.clock, 100);
    assert_eq!(
        read(&mut s.nodes, &s.clock, 100, query()),
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
    std::fs::remove_dir_all(s.root).unwrap();
}
