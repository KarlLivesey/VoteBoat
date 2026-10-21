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
//! Readiness inner profiles: a wrapper declares the schemas of the
//! applications inside it, and readiness compares them layer for layer.
use super::*;
use voteboat::{
    identity::{GroupId, GroupIdentity, GroupIncarnation},
    maintenance::Maintenance,
    raft::{
        ApplicationFamily, InnerLayer, InnerProfile, LayerIdentity, ReadinessRequirements,
        MAX_INNER_PROFILE_LAYERS,
    },
};

fn u(schema: u64) -> InnerLayer {
    InnerLayer::unregistered(schema)
}
fn counter_layer(counter: &Counter) -> InnerLayer {
    InnerLayer::new(
        counter.schema_version(),
        LayerIdentity::family(ApplicationFamily::COUNTER),
    )
}

fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}

#[test]
fn inner_profiles_are_exact_bounded_and_refuse_overflow() {
    assert_eq!(InnerProfile::LEAF.layers(), Some(&[][..]));
    let counter = Counter::new(4).unwrap();
    assert_eq!(
        counter.deployment_requirements().unwrap().inner_profile,
        InnerProfile::LEAF
    );
    let wrapped = Maintenance::new(group(), 1, 4, counter.clone()).unwrap();
    let declared = wrapped.deployment_requirements().unwrap();
    assert_eq!(
        declared.inner_profile.layers(),
        Some(&[counter_layer(&counter)][..])
    );
    assert_eq!(declared.inner_profile, InnerProfile::of(&counter));
    // A profile that differs in any layer, or in depth, is refused.
    for other in [
        InnerProfile::LEAF,
        InnerProfile::from_layers(&[counter_layer(&Counter::new(5).unwrap())]).unwrap(),
        InnerProfile::from_layers(&[counter_layer(&counter), u(9)]).unwrap(),
        // The same schema under another family, or with no family.
        InnerProfile::from_layers(&[u(counter.schema_version())]).unwrap(),
        InnerProfile::from_layers(&[InnerLayer::new(
            counter.schema_version(),
            LayerIdentity::family(ApplicationFamily::BUCKET_COUNTER),
        )])
        .unwrap(),
    ] {
        assert_eq!(
            wrapped.validate_deployment_requirements(ReadinessRequirements {
                inner_profile: other,
                ..declared
            }),
            Err(ApplicationError::UnsupportedSchema)
        );
        assert_eq!(
            counter.validate_readiness_requirements(ReadinessRequirements {
                inner_profile: other,
                ..counter.readiness_requirements()
            }),
            if other == InnerProfile::LEAF {
                Ok(())
            } else {
                Err(ApplicationError::UnsupportedSchema)
            }
        );
    }
    assert_eq!(wrapped.validate_deployment_requirements(declared), Ok(()));
    // Explicit profiles: at most seven non-zero layers.
    let seven = [u(7); MAX_INNER_PROFILE_LAYERS];
    let full = InnerProfile::from_layers(&seven).unwrap();
    assert_eq!(full.layers(), Some(&seven[..]));
    assert_eq!(
        InnerProfile::from_layers(&[u(7); MAX_INNER_PROFILE_LAYERS + 1]),
        None
    );
    assert_eq!(InnerProfile::from_layers(&[u(7), u(0)]), None);
    // Wrapping a seven-layer stack overflows; readiness refuses an overflowed
    // profile even when the declaration matches it.
    let deep = ReadinessRequirements {
        application_schema: 3,
        identity: voteboat::raft::LayerIdentity::UNREGISTERED,
        inner_profile: full,
        command_bytes: 8,
        snapshot_bytes: 8,
    };
    let overflowed = InnerProfile::wrapping(deep);
    assert_eq!(overflowed.layers(), None);
    assert_eq!(
        InnerProfile::wrapping(ReadinessRequirements {
            inner_profile: overflowed,
            ..deep
        }),
        overflowed
    );
    let six = InnerProfile::from_layers(&seven[1..]).unwrap();
    assert_eq!(
        InnerProfile::wrapping(ReadinessRequirements {
            inner_profile: six,
            ..deep
        })
        .schemas(),
        Some(vec![3, 7, 7, 7, 7, 7, 7])
    );
    let app = Deep(deep);
    assert_eq!(
        app.validate_deployment_requirements(app.deployment_requirements().unwrap()),
        Err(ApplicationError::UnsupportedSchema)
    );
}

/// An application whose declared stack is deeper than a profile records.
struct Deep(ReadinessRequirements);
impl StateMachine for Deep {
    type Receipt = ();
    fn retention(&self) -> Retention {
        Retention::Stateless
    }
    fn deployment_requirements(&self) -> Option<ReadinessRequirements> {
        Some(ReadinessRequirements {
            inner_profile: InnerProfile::wrapping(self.0),
            ..self.0
        })
    }
    fn applied_index(&self) -> u64 {
        0
    }
    fn apply_batch(&mut self, _: &[LogEntry]) -> Result<Vec<()>, ApplicationError> {
        Ok(Vec::new())
    }
}
