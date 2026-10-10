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
use super::split::{compact, observe};
use super::*;
use voteboat::{
    bucket_counter::BucketOutcome, retirement::*, transfer::*, transfer_publication::*,
    transfer_source::*, transfer_target::*,
};
type Source = RetirementGuard<source_fixture::Source>;
fn fresh() -> Source {
    RetirementGuard::new(source_fixture::fresh()).unwrap_or_else(|e| panic!("{:?}", e.0))
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
fn status(nodes: &mut [Node<target_fixture::Target>], clock: &Instant, g: u128) -> TargetStatus {
    let TargetRead::Status(s) = observe(nodes, clock, g, TargetQuery::Status) else {
        panic!("target status")
    };
    s
}
struct RetirementCluster {
    parent: Vec<Node<LifecycleDirectory>>,
    source: Vec<Node<Source>>,
    targets: [Vec<Node<target_fixture::Target>>; 2],
}
impl RetirementCluster {
    fn new(root: &std::path::Path, clock: &Instant, protocol: NativePeerProtocol) -> Self {
        let parent = open(
            configuration(root, 1, &[1, 2, 3], NativeOpenMode::Create),
            clock,
            protocol,
            metadata,
        );
        let source = open(
            configuration(root, 20, &[1, 2, 3], NativeOpenMode::Create),
            clock,
            protocol,
            fresh,
        );
        let targets = [
            open(
                configuration(root, 21, &[1, 2, 3], NativeOpenMode::Create),
                clock,
                protocol,
                target_fixture::fresh,
            ),
            open(
                configuration(root, 22, &[1, 2, 3], NativeOpenMode::Create),
                clock,
                protocol,
                || target_fixture::fresh_for(22),
            ),
        ];
        Self {
            parent,
            source,
            targets,
        }
    }
    fn initialize(&mut self, clock: &Instant) -> SourceFreezeStatus {
        campaign(&mut self.parent, clock, 1);
        propose_recovering(
            &mut self.parent,
            clock,
            1,
            1000,
            metadata().directory().bootstrap_command(65536).unwrap(),
        );
        propose_recovering(
            &mut self.parent,
            clock,
            1,
            1001,
            DirectoryCommand {
                expected: None,
                manifest: source_fixture::grant(),
            }
            .encode(32768)
            .unwrap(),
        );
        propose_recovering(
            &mut self.parent,
            clock,
            1,
            200,
            source_fixture::intent().encode(32768).unwrap(),
        );
        campaign(&mut self.source, clock, 20);
        for (id, bytes) in [
            (
                100,
                source_fixture::fresh().bootstrap_command(65536).unwrap(),
            ),
            (1, source_fixture::data(1, 7)),
            (2, source_fixture::data(200, 11)),
            (200, source_fixture::freeze()),
        ] {
            propose_recovering(&mut self.source, clock, 20, id, bytes);
        }
        let RetirementRead::Freeze(Some(frozen)) =
            observe(&mut self.source, clock, 20, RetirementQuery::Freeze)
        else {
            panic!("source freeze")
        };
        frozen
    }
    fn import_targets(
        &mut self,
        clock: &Instant,
        configuration: ConfigurationId,
    ) -> Vec<TargetReadyEvidence> {
        let mut ready = Vec::new();
        for (i, ts) in self.targets.iter_mut().enumerate() {
            let g = 21 + i as u128;
            campaign(ts, clock, g);
            propose_recovering(
                ts,
                clock,
                g,
                200,
                target_fixture::fresh_for(g)
                    .bootstrap_command(65536)
                    .unwrap(),
            );
            let import = target_fixture::from_source(
                self.source[0].local().applications[&group(20)]
                    .owner()
                    .unwrap(),
                g,
                configuration,
            );
            let bytes = ts[0].local().applications[&group(g)]
                .import_command(&import, 65536)
                .unwrap();
            propose_recovering(ts, clock, g, 200, bytes);
            let s = status(ts, clock, g);
            assert!(TargetActivationEvidence::from_status(configuration, s.clone()).is_err());
            ready.push(
                TargetReadyEvidence::from_status(configuration, s)
                    .unwrap_or_else(|e| panic!("{:?}", e.0)),
            );
        }
        ready
    }
    fn retirement_proof(
        &mut self,
        clock: &Instant,
        frozen: &SourceFreezeStatus,
    ) -> RetirementProof {
        let configuration = ConfigurationId::new(1).unwrap();
        let ready = self.import_targets(clock, configuration);
        let publication = TransferPublication::new(
            OperationId::new(200).unwrap(),
            source_fixture::intent(),
            vec![
                SourceFenceEvidence::from_status(configuration, frozen.clone())
                    .unwrap_or_else(|e| panic!("{:?}", e.0)),
            ],
            ready,
        )
        .unwrap_or_else(|e| panic!("{:?}", e.0));
        campaign(&mut self.parent, clock, 1);
        propose_recovering(
            &mut self.parent,
            clock,
            1,
            201,
            publication.encode(65536).unwrap(),
        );
        let DirectoryRead::Publication(Some(decision)) = observe(
            &mut self.parent,
            clock,
            1,
            DirectoryQuery::Publication(OperationId::new(200).unwrap()),
        ) else {
            panic!("publication")
        };
        let mut activations = Vec::new();
        for (i, ts) in self.targets.iter_mut().enumerate() {
            let g = 21 + i as u128;
            let bytes = ts[0].local().applications[&group(g)]
                .activation_command(
                    &TargetActivation {
                        metadata_configuration: configuration,
                        decision: decision.clone(),
                    },
                    65536,
                )
                .unwrap();
            campaign(ts, clock, g);
            propose_recovering(ts, clock, g, 200, bytes);
            activations.push(
                TargetActivationEvidence::from_status(configuration, status(ts, clock, g))
                    .unwrap_or_else(|e| panic!("{:?}", e.0)),
            );
        }
        RetirementProof {
            metadata_configuration: configuration,
            decision,
            targets: activations,
            release: RetentionRelease {
                source: group(20),
                operation: OperationId::new(200).unwrap(),
                fence_index: frozen.fence.index,
                release: OperationId::new(900).unwrap(),
            },
        }
    }
}
fn reclaim_retired_source(source: &mut [Node<Source>], clock: &Instant) {
    compact(source, clock, 20);
    let requests = source
        .iter_mut()
        .map(|n| n.reclaim(LogLimits::default().max_wal_bytes).unwrap())
        .collect::<Vec<_>>();
    let mut completed = [false; 3];
    drive(source, clock, |ns| {
        for (i, n) in ns.iter_mut().enumerate() {
            if let Some(event) = n.poll_reclaim() {
                assert_eq!(event.request, requests[i]);
                let report = event.result.unwrap();
                assert!(report.after_bytes < report.before_bytes);
                completed[i] = true;
            }
        }
        completed.iter().all(|x| *x)
    });
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let root = std::env::temp_dir().join(format!(
        "voteboat-native-retirement-{}-{protocol:?}-{checkpoint}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let clock = Instant::now();
    let mut rig = RetirementCluster::new(&root, &clock, protocol);
    let frozen = rig.initialize(&clock);
    let proof = rig.retirement_proof(&clock, &frozen);
    let RetirementCluster {
        parent,
        mut source,
        mut targets,
    } = rig;
    let bytes = source[0].local().applications[&group(20)]
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    campaign(&mut source, &clock, 20);
    // Discard the client completion; reconstruct the result from quorum status.
    let _ = propose_recovering(&mut source, &clock, 20, 200, bytes.clone());
    let RetirementRead::Status(Some(retired)) =
        observe(&mut source, &clock, 20, RetirementQuery::Status)
    else {
        panic!("retired status")
    };
    drive(&mut source, &clock, |ns| {
        ns.iter()
            .all(|n| n.local().applications[&group(20)].status() == Some(retired))
    });
    if checkpoint {
        reclaim_retired_source(&mut source, &clock);
    }
    let old_logs = close(source, &clock, 20, || {});
    for log in old_logs {
        if checkpoint {
            assert!(log.base_index() >= retired.index);
        } else {
            assert!(log.entries.iter().any(|e| e.index == retired.index));
        }
    }
    // The new owner serves retained request IDs with the old source offline.
    campaign(&mut targets[0], &clock, 21);
    let mut hint = source_fixture::hint(1);
    hint.group = group(21);
    hint.scope = source_fixture::range(0, 128);
    hint.epoch = OwnershipEpoch::new(2).unwrap();
    hint.generation = RouteGeneration::new(2).unwrap();
    let data = encode_routed(
        hint,
        &[1],
        &voteboat::bucket_counter::encode_add(&[1], 7, b"effect", 1024).unwrap(),
        4096,
    )
    .unwrap();
    let TargetOutcome::Applied(retry) =
        propose_recovering(&mut targets[0], &clock, 21, 1, data).outcome
    else {
        panic!("target retry")
    };
    assert!(retry.duplicate);
    assert_eq!(retry.outcome, BucketOutcome::Value(7));
    let mut source = open(
        super::configuration(&root, 20, &[1, 2, 3], NativeOpenMode::Recover),
        &clock,
        protocol,
        fresh,
    );
    for n in &source {
        let app = &n.local().applications[&group(20)];
        assert!(app.owner().is_none());
        assert_eq!(app.status(), Some(retired));
        assert_eq!(app.freeze_status().unwrap(), Some(frozen.clone()));
        assert!(app.export_target(group(21), 65536).is_err());
    }
    let RetirementRead::Status(Some(observed)) =
        observe(&mut source, &clock, 20, RetirementQuery::Status)
    else {
        panic!("recovered status")
    };
    assert_eq!(observed, retired);
    let retry = propose_recovering(&mut source, &clock, 20, 200, bytes);
    assert_eq!(retry.outcome, RetirementOutcome::Retired(retired));
    assert_eq!(
        observe(
            &mut source,
            &clock,
            20,
            RetirementQuery::Owner(SourceQuery::Freeze)
        ),
        RetirementRead::Retired
    );
    close(source, &clock, 20, || {});
    close(parent, &clock, 1, || {});
    for (i, ts) in targets.into_iter().enumerate() {
        close(ts, &clock, 21 + i as u128, || {});
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_retirement_recovers_from_wal_after_lost_receipt() {
    history(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_retirement_recovers_after_checkpoint_and_reclamation() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_retirement_recovers_from_wal_after_lost_receipt() {
    history(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_retirement_recovers_after_checkpoint_and_reclamation() {
    history(NativePeerProtocol::Quic, true);
}
