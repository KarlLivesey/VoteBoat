// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) fn transient<R>(outcome: &ReadOutcome<R>) -> bool {
    matches!(
        outcome,
        ReadOutcome::Unavailable(ReadUnavailable::LeadershipChanged)
            | ReadOutcome::NotRead(voteboat::raft::RaftError::NotLeader)
            | ReadOutcome::NotRead(voteboat::raft::RaftError::ReadNotReady)
    )
}
// Discovery intentionally maps read failures to Unavailable. Preserve the narrow
// original cause for the fixture's positive retry path; inactive reads stay fatal.
pub(super) struct AttemptSource<S> {
    source: S,
    pub(super) retryable: bool,
}
impl<S> AttemptSource<S> {
    pub(super) fn new(source: S) -> Self {
        Self {
            source,
            retryable: false,
        }
    }
}
impl<N, M> AttemptSource<MappedManifestReadSource<N, M>> {
    pub(super) fn node_mut(&mut self) -> &mut N {
        self.source.node_mut()
    }
    pub(super) fn into_node(self) -> N {
        self.source.into_node()
    }
}
impl<S: ManifestReadSource> ManifestReadSource for AttemptSource<S> {
    type ReadResult = S::ReadResult;
    fn binding(&self) -> ReadInvocationBinding {
        self.source.binding()
    }
    fn pending_reads(&self) -> usize {
        self.source.pending_reads()
    }
    fn submit(
        &mut self,
        request: ManifestLookup,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<ResponsibilityIdentity>> {
        let result = self.source.submit(request);
        self.retryable = matches!(
            &result,
            Err(ReadInvocationRejected {
                reason: ReadInvocationError::Consensus(
                    voteboat::raft::RaftError::NotLeader | voteboat::raft::RaftError::ReadNotReady
                ),
                ..
            })
        );
        result
    }
    fn poll_result(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<
        Option<ReadOutcome<Option<ResponsibilityManifest>>>,
        ReadCompletionRejected<Self::ReadResult>,
    > {
        let result = self.source.poll_result(ticket);
        if let Ok(Some(outcome)) = &result {
            self.retryable = transient(outcome);
            if self.retryable {
                eprintln!("metadata lookup lost leadership: {outcome:?}");
            }
        }
        result
    }
    fn cancel(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        self.source.cancel(ticket)
    }
}

fn controlled(protocol: NativePeerProtocol, wrong_ticket: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-metadata-read-cut-{}-{protocol:?}-{wrong_ticket}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Instant::now();
    let env = Environment {
        root: &root,
        clock: &clock,
        protocol,
        checkpoint: false,
    };
    let (mut nodes, _) = initialize_source(&env);
    if wrong_ticket {
        campaign(&mut nodes, &clock, 1);
        let source = AttemptSource::new(MappedManifestReadSource::<
            _,
            MetadataPublishingManifestRead,
        >::new(nodes.remove(0)));
        let mut driver = NativeManifestLookup::new(
            source,
            ManifestCacheLimits {
                manifests: 2,
                bytes: 65536,
            },
            60000,
            10000,
            1,
            now(&clock),
        )
        .unwrap_or_else(|_| panic!("controlled lookup construction"));
        assert!(rejected_completion(
            &mut driver,
            &mut nodes,
            &clock,
            request(1),
            Some(NodeControl::Campaign)
        ));
        assert!(driver.is_drained());
        assert_eq!(driver.source_mut().pending_reads(), 0);
        driver.close();
        nodes.insert(
            0,
            driver
                .into_source()
                .unwrap_or_else(|_| panic!("owned controlled read"))
                .into_node(),
        );
    } else {
        let (result, retryable) = lookup_once::<_, MetadataPublishingManifestRead>(
            &mut nodes,
            &clock,
            1,
            Some(NodeControl::Campaign),
        );
        assert_eq!(result, Err(ManifestDiscoveryError::Unavailable));
        assert!(retryable);
    }
    assert!(nodes
        .iter()
        .all(|node| node.local().reads.usage().requests == 0));
    assert_eq!(
        lookup::<_, MetadataPublishingManifestRead>(&mut nodes, &clock, 1).unwrap(),
        root_manifest()
    );
    assert!(nodes
        .iter()
        .all(|node| node.local().reads.usage().requests == 0));
    creation::abandon(nodes, 1);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn admitted_lookup_leadership_change_drains_before_fresh_authority() {
    controlled(NativePeerProtocol::TcpTls, false);
    #[cfg(feature = "quic")]
    controlled(NativePeerProtocol::Quic, false);
}
#[test]
fn wrong_ticket_preserves_original_leadership_change_and_cleanup() {
    controlled(NativePeerProtocol::TcpTls, true);
    #[cfg(feature = "quic")]
    controlled(NativePeerProtocol::Quic, true);
}
