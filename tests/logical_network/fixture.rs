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
use super::{
    aliases::Bridge,
    claim,
    cluster::{Cluster, HISTORY},
    parent::Parent,
    setup, support,
};
use std::time::{Duration, Instant};
use voteboat::{
    identity::*, logical_voter::*, native::connect::NativePeerProtocol, outbound::*, raft::*,
    runtime::*, secure::*,
};
pub fn history(
    control: impl FnOnce(&mut [Bridge<'_>], &mut [Parent], &[LogicalPeerDescriptor], &[Instant]),
) {
    let _serial = HISTORY.lock().unwrap();
    let mut clusters = (0..3)
        .map(|slot| Cluster::new(NativePeerProtocol::TcpTls, slot))
        .collect::<Vec<_>>();
    let clocks = clusters.iter().map(|c| c.clock).collect::<Vec<_>>();
    let mut bridges = clusters.iter_mut().map(claim::bridge).collect::<Vec<_>>();
    let images = bridges
        .iter_mut()
        .enumerate()
        .map(|(i, b)| claim::claim(b, clocks[i], 9500 + i as u128))
        .collect::<Vec<_>>();
    let descriptors = images
        .iter()
        .enumerate()
        .map(|(i, v)| LogicalPeerDescriptor::from_verified(v, &setup::app(i)).unwrap())
        .collect::<Vec<_>>();
    let mut parents = images.iter().map(Parent::new).collect::<Vec<_>>();
    control(&mut bridges, &mut parents, &descriptors, &clocks);
    for b in &mut bridges {
        b.close();
        assert!(b.is_drained());
    }
    drop(bridges);
    for parent in parents {
        assert!(parent.is_drained());
        parent.close();
    }
    for mut cluster in clusters {
        // The selected representative remains the local owner; independently
        // retained physical peers still participate in checkpoint/cold cleanup.
        cluster.park_quorum_peers();
        cluster.checkpoint();
        cluster.finish();
    }
}
pub fn plan(descriptors: &[LogicalPeerDescriptor], local: usize) -> LogicalPeerPlan {
    LogicalPeerPlan::new(
        descriptors[local].clone(),
        descriptors
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != local)
            .map(|(_, d)| d.clone())
            .collect(),
    )
    .unwrap()
}
pub fn binding(
    descriptors: &[LogicalPeerDescriptor],
    local: usize,
    peer: usize,
    generation: u64,
) -> SessionBinding {
    SessionBinding {
        local: descriptors[local].physical(),
        peer: descriptors[peer].physical(),
        generation: SecureSessionGeneration::new(generation).unwrap(),
        wire_version: LOGICAL_NETWORK_VERSION,
    }
}
pub fn original_send(
    bridge: &mut Bridge<'_>,
    parent: &mut Parent,
    clock: Instant,
) -> LogicalNetworkBatch {
    let sender = StoreBinding {
        identity: support::identity(102),
        session: StoreSession::new(1).unwrap(),
    };
    parent
        .owner
        .admit(
            parent.mapping.parent,
            Event::Receive(Message {
                group: parent.mapping.parent,
                configuration: ConfigurationId::new(1).unwrap(),
                from: support::node(12),
                sender,
                to: parent.mapping.voter,
                term: 1,
                context: RequestContext {
                    origin: sender,
                    sequence: 1,
                },
                rpc: Rpc::AuthorityRequest {
                    candidate: PeerIdentity {
                        node: support::node(13),
                        store: support::identity(103),
                    },
                    configuration: ConfigurationId::new(2).unwrap(),
                },
            }),
        )
        .unwrap();
    let end = Instant::now() + Duration::from_secs(15);
    loop {
        parent.poll();
        bridge.pump_release(parent.driver.releases()).unwrap();
        bridge
            .advance(
                MonoTime(clock.elapsed().as_millis() as u64),
                LogicalBridgeBudget {
                    node_polls: 3,
                    completions: 4,
                    transitions: 1,
                    node: NodePollBudget::default(),
                },
            )
            .unwrap();
        parent.poll();
        if parent.outbound.usage().batches == 1
            && bridge.is_drained()
            && parent.driver.release_usage().pending == 0
        {
            break;
        }
        assert!(
            Instant::now() < end,
            "actual original child capsule output timeout"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut sends = parent.outbound.poll_logical(1);
    assert_eq!(sends.len(), 1);
    let send = sends.remove(0);
    assert_eq!(
        send.send().provenance().receipt().outcome,
        LogicalOutcome::CapsuleAuthorized
    );
    assert_ne!(send.send().message().from, claim::representative(0));
    send
}
