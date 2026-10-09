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
use voteboat::{identity::*, routing::*, runtime::*};
#[derive(Debug)]
struct HostReads {
    binding: ReadInvocationBinding,
    pending: Option<ReadInvocationTicket>,
    started: u64,
    cancelled: usize,
    ready: bool,
}
impl HostReads {
    fn new() -> Self {
        Self {
            binding: ReadInvocationBinding {
                owner: RuntimeOwner {
                    store: StoreBinding {
                        identity: support::identity(1),
                        session: StoreSession::new(1).unwrap(),
                    },
                    lane: ExecutionLaneId::new(1).unwrap(),
                    generation: RuntimeGeneration::new(1).unwrap(),
                },
                generation: ReadInvocationGeneration::new(1).unwrap(),
            },
            pending: None,
            started: 0,
            cancelled: 0,
            ready: false,
        }
    }
}
fn query() -> ManifestLookup {
    ManifestLookup {
        locator: AuthorityLocator {
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(1).unwrap(),
                incarnation: ResponsibilityIncarnation::new(1).unwrap(),
            },
            authority: support::group(1),
        },
        minimum_epoch: None,
        minimum_generation: None,
    }
}
impl ManifestReadSource for HostReads {
    fn binding(&self) -> ReadInvocationBinding {
        self.binding
    }
    fn pending_reads(&self) -> usize {
        usize::from(self.pending.is_some())
    }
    fn submit(
        &mut self,
        request: ManifestLookup,
    ) -> Result<ReadInvocationTicket, ReadInvocationRejected<ResponsibilityIdentity>> {
        if self.pending.is_some() {
            return Err(ReadInvocationRejected {
                reason: ReadInvocationError::Overloaded,
                group: request.locator.authority,
                query: request.locator.responsibility,
            });
        }
        self.started += 1;
        let ticket = ReadInvocationTicket {
            binding: self.binding,
            sequence: self.started,
            group: request.locator.authority,
            request: ReadRequestId::new(self.started).unwrap(),
        };
        self.pending = Some(ticket);
        Ok(ticket)
    }
    fn poll_result(
        &mut self,
        ticket: ReadInvocationTicket,
    ) -> Result<
        Option<ReadOutcome<Option<ResponsibilityManifest>>>,
        ReadCompletionRejected<Option<ResponsibilityManifest>>,
    > {
        assert_eq!(self.pending, Some(ticket));
        if !self.ready {
            return Ok(None);
        }
        self.pending = None;
        Ok(Some(ReadOutcome::Unavailable(ReadUnavailable::Cancelled)))
    }
    fn cancel(&mut self, ticket: ReadInvocationTicket) -> Result<(), ReadInvocationError> {
        assert_eq!(self.pending, Some(ticket));
        self.cancelled += 1;
        Ok(())
    }
}
#[test]
fn downstream_original_read_source_contract_compiles_without_native() {
    let mut source = HostReads::new();
    let ticket = source.submit(query()).unwrap();
    assert_eq!(ticket.binding, source.binding());
    assert_eq!(source.pending_reads(), 1);
    source.cancel(ticket).unwrap();
    assert_eq!(source.pending_reads(), 1);
    source.ready = true;
    assert!(matches!(
        source.poll_result(ticket).unwrap(),
        Some(ReadOutcome::Unavailable(ReadUnavailable::Cancelled))
    ));
    assert_eq!(source.pending_reads(), 0);
}
#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::lookup_discovery::*;
    fn driver() -> NativeManifestLookup<HostReads> {
        NativeManifestLookup::new(
            HostReads::new(),
            ManifestCacheLimits {
                manifests: 1,
                bytes: MAX_MANIFEST_BYTES,
            },
            100,
            10,
            5,
            MonoTime(0),
        )
        .unwrap()
    }
    #[test]
    fn deadline_retains_accepted_source_work_until_actual_terminal_result() {
        let mut d = driver();
        assert_eq!(
            d.lookup(query(), MonoTime(0)).err(),
            Some(ManifestDiscoveryError::Unavailable)
        );
        let original = d.pending().unwrap();
        assert_eq!(d.next_deadline(), Some(MonoTime(10)));
        assert!(!d.poll(MonoTime(10)).unwrap());
        assert!(!d.is_drained());
        assert_eq!(d.pending().unwrap().ticket, original.ticket);
        assert_eq!(d.source_mut().cancelled, 1);
        assert!(!d.poll(MonoTime(11)).unwrap());
        assert_eq!(d.source_mut().cancelled, 1);
        d.source_mut().ready = true;
        assert!(matches!(
            d.poll(MonoTime(12)),
            Err(ManifestLookupPollError::Discovery(
                ManifestDiscoveryError::Expired
            ))
        ));
        assert!(d.is_drained());
        assert_eq!(
            d.lookup(query(), MonoTime(13)).err(),
            Some(ManifestDiscoveryError::Expired)
        );
        assert_eq!(d.source_mut().started, 1);
        assert_eq!(
            d.lookup(query(), MonoTime(17)).err(),
            Some(ManifestDiscoveryError::Unavailable)
        );
        assert_eq!(d.source_mut().started, 2);
        d.close();
        assert_eq!(
            d.lookup(query(), MonoTime(17)).err(),
            Some(ManifestDiscoveryError::Closed)
        );
        assert!(!d.is_drained());
        assert!(matches!(
            d.poll(MonoTime(17)),
            Err(ManifestLookupPollError::Discovery(
                ManifestDiscoveryError::Closed
            ))
        ));
        let source = d.into_source().unwrap_or_else(|_| panic!("source held"));
        assert_eq!(source.pending_reads(), 0);
    }
    #[test]
    fn construction_and_recovery_preserve_original_owned_source_and_ticket() {
        let mut source = HostReads::new();
        let accepted = source.submit(query()).unwrap();
        let (error, source) = NativeManifestLookup::new(
            source,
            ManifestCacheLimits {
                manifests: 1,
                bytes: MAX_MANIFEST_BYTES,
            },
            100,
            10,
            5,
            MonoTime(0),
        )
        .err()
        .unwrap();
        assert_eq!(error, ManifestDiscoveryError::InvalidLimits);
        assert_eq!(source.pending, Some(accepted));
        let mut d = driver();
        d.lookup(query(), MonoTime(0)).unwrap_err();
        let original = d.pending().unwrap();
        assert!(matches!(d.poll(MonoTime(0)), Ok(false)));
        assert_eq!(
            d.lookup(query(), MonoTime(0)).err(),
            Some(ManifestDiscoveryError::Unavailable)
        );
        assert_eq!(d.source_mut().started, 1);
        assert!(matches!(d.poll(MonoTime(1)), Ok(false)));
        assert!(matches!(
            d.poll(MonoTime(0)),
            Err(ManifestLookupPollError::Discovery(
                ManifestDiscoveryError::TimeWentBack
            ))
        ));
        d.source_mut().binding.generation = ReadInvocationGeneration::new(2).unwrap();
        assert!(matches!(
            d.poll(MonoTime(1)),
            Err(ManifestLookupPollError::Discovery(
                ManifestDiscoveryError::WrongAuthority
            ))
        ));
        let (source, pending) = d.into_recovery();
        assert_eq!(pending.unwrap().ticket, original.ticket);
        assert_eq!(source.pending, Some(original.ticket));
    }
}
