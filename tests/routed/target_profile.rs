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
use voteboat::retirement::*;

type Owner = target_fixture::Target;
type Receipt = <Owner as StateMachine>::Receipt;

// Select the checkpoint profile before bootstrap. This only shares test setup;
// retirement assertions must use the guard's own typed outcomes directly.
pub(super) trait TargetProfile {
    type Application: ProposalAdmission
        + BoundedReadableStateMachine
        + CheckpointStateMachine
        + StateMachine<Receipt: ApplicationReceipt>
        + ReadableStateMachine<Query: Clone>;
    fn wrap(owner: Owner) -> Self::Application;
    fn owner(application: &Self::Application) -> &Owner;
    fn query(query: TargetQuery<Vec<u8>>) -> <Self::Application as ReadableStateMachine>::Query;
    fn read(result: <Self::Application as ReadableStateMachine>::ReadResult) -> TargetRead<i64>;
    fn receipt(result: <Self::Application as StateMachine>::Receipt) -> Receipt;
}
pub(super) struct Raw;
impl TargetProfile for Raw {
    type Application = Owner;
    fn wrap(owner: Owner) -> Owner {
        owner
    }
    fn owner(application: &Owner) -> &Owner {
        application
    }
    fn query(query: TargetQuery<Vec<u8>>) -> TargetQuery<Vec<u8>> {
        query
    }
    fn read(result: TargetRead<i64>) -> TargetRead<i64> {
        result
    }
    fn receipt(result: Receipt) -> Receipt {
        result
    }
}
pub(super) struct Guarded;
impl TargetProfile for Guarded {
    type Application = RetirementGuard<Owner>;
    fn wrap(owner: Owner) -> Self::Application {
        RetirementGuard::new(owner).unwrap_or_else(|e| panic!("guard profile: {:?}", e.0))
    }
    fn owner(application: &Self::Application) -> &Owner {
        application.owner().expect("live owner required by setup")
    }
    fn query(query: TargetQuery<Vec<u8>>) -> RetirementQuery<TargetQuery<Vec<u8>>> {
        RetirementQuery::Owner(query)
    }
    fn read(result: RetirementRead<TargetRead<i64>>) -> TargetRead<i64> {
        let RetirementRead::Owner(result) = result else {
            panic!("expected live owner read")
        };
        result
    }
    fn receipt(result: RetirementReceipt<Receipt>) -> Receipt {
        let RetirementOutcome::Owner(result) = result.outcome else {
            panic!("expected live owner receipt")
        };
        result
    }
}
pub(super) fn profile_target<P: TargetProfile>(intent: &TransferIntent, g: u128) -> P::Application {
    P::wrap(target(intent, g))
}
pub(super) fn observe_target<P: TargetProfile>(
    nodes: &mut [Node<P::Application>],
    clock: &Instant,
    g: u128,
    query: TargetQuery<Vec<u8>>,
) -> TargetRead<i64> {
    P::read(split::observe(nodes, clock, g, P::query(query)))
}
pub(super) fn propose_target<P: TargetProfile>(
    nodes: &mut [Node<P::Application>],
    clock: &Instant,
    g: u128,
    operation: u128,
    bytes: Vec<u8>,
) -> Receipt {
    P::receipt(propose_recovering(nodes, clock, g, operation, bytes))
}
