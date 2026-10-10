// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    authorization::CredentialGeneration, connect::*, secure::*, transport::ConnectTicket,
};

struct Controlled {
    inner: Connector,
    generation: CredentialGeneration,
    material: Vec<u8>,
}
impl PeerConnector for Controlled {
    type Endpoint = ();
    type Session = <Connector as PeerConnector>::Session;
    fn local(&self) -> LocalIdentity {
        self.inner.local()
    }
    fn limits(&self) -> ConnectLimits {
        self.inner.limits()
    }
    fn usage(&self) -> ConnectUsage {
        self.inner.usage()
    }
    fn next_deadline(&self) -> Option<MonoTime> {
        self.inner.next_deadline()
    }
    fn supports_peer(&self, peer: PeerIdentity) -> bool {
        self.inner.supports_peer(peer)
    }
    fn submit(&mut self, r: ConnectRequest<()>, n: MonoTime) -> Result<(), ConnectRejected<()>> {
        self.inner.submit(r, n)
    }
    fn cancel(&mut self, t: ConnectTicket) -> bool {
        self.inner.cancel(t)
    }
    fn poll(
        &mut self,
        n: MonoTime,
        b: ConnectPollBudget,
    ) -> Result<Vec<ConnectCompletion<Self::Session>>, ConnectError> {
        self.inner.poll(n, b)
    }
    fn close(&mut self) {
        self.inner.close();
    }
}
impl PeerCredentialControl for Controlled {
    type Credentials = Vec<u8>;
    fn credential_generation(&self) -> Option<CredentialGeneration> {
        Some(self.generation)
    }
    fn replace_peer_credentials(
        &mut self,
        expected: CredentialGeneration,
        next: CredentialGeneration,
        material: Vec<u8>,
    ) -> Result<Vec<u8>, (ConnectError, Vec<u8>)> {
        if expected != self.generation || next <= expected {
            return Err((ConnectError::InvalidRequest, material));
        }
        self.generation = next;
        Ok(std::mem::replace(&mut self.material, material))
    }
}
#[test]
fn node_forwards_prepared_peer_credentials_without_changing_consensus_and_refuses_after_shutdown() {
    let original = parts(3, true);
    let PeerParts {
        connector,
        factory,
        roster,
        ingress,
        routes,
        admission_routes,
    } = original.peers.unwrap();
    let gen = |n| CredentialGeneration::new(n).unwrap();
    let mut node = Node::from_parts(
        NodeParts {
            local: original.local,
            peers: Some(PeerParts {
                connector: Controlled {
                    inner: connector,
                    generation: gen(1),
                    material: vec![1],
                },
                factory,
                roster,
                ingress,
                routes,
                admission_routes,
            }),
        },
        NodeLimits::default(),
        MonoTime(0),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.reason));
    let before = format!("{:?}", node.local().owner.core(group(1)).unwrap().state());
    let local = node.local().owner.identity();
    assert_eq!(
        node.replace_peer_credentials(gen(1), gen(2), vec![2]),
        Ok(vec![1])
    );
    assert_eq!(node.local().owner.identity(), local);
    assert_eq!(
        format!("{:?}", node.local().owner.core(group(1)).unwrap().state()),
        before
    );
    node.begin_shutdown();
    let material = vec![3];
    let pointer = material.as_ptr();
    let (error, returned) = node
        .replace_peer_credentials(gen(2), gen(3), material)
        .unwrap_err();
    assert_eq!(error, ConnectError::Closed);
    assert_eq!(pointer, returned.as_ptr());
}
