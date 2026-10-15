// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Compile and exercise the service's actual Counter/Maintenance wrapper.
use super::*;
#[path = "../../../src/bin/support/counter_application.rs"]
mod application;
mod setup {
    pub type Failure = Box<dyn std::error::Error>;
    pub fn checked<T, E: std::fmt::Debug>(value: Result<T, E>) -> Result<T, Failure> {
        value.map_err(|error| format!("{error:?}").into())
    }
    pub fn group() -> voteboat::identity::GroupIdentity {
        use voteboat::identity::*;
        GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        }
    }
}

#[test]
fn executable_plain_counter_forwards_general_and_ready_with_full_receipt_bound() {
    let app = application::Application::new(Counter::new(2).unwrap(), false).unwrap();
    assert!(app.maintenance().is_err());
    assert!(app.supports_ready_batch());
    let bytes = app.data(1).unwrap();
    let bound = std::mem::size_of::<application::Receipt>();
    let pending = [(operation(1), &[0][..])];
    for mut prepared in [
        app.prepare_proposal_admission(pending.into_iter()).unwrap(),
        app.prepare_ready_admission(pending.into_iter()).unwrap(),
    ] {
        assert_eq!(prepared(operation(2), &bytes), Ok(bound));
        assert_eq!(prepared(operation(1), &bytes), Ok(bound));
        assert_eq!(
            prepared(operation(3), &bytes),
            Err(ApplicationError::DedupCapacity)
        );
    }
    assert_eq!(
        app.validate_proposal(operation(2), &bytes, pending.into_iter()),
        Ok(bound)
    );
    assert_eq!(app.applied_index(), 0);
}

#[test]
fn executable_maintenance_profile_retains_both_fallbacks() {
    let app = application::Application::new(Counter::new(2).unwrap(), true).unwrap();
    assert!(app.maintenance().is_ok());
    assert!(!app.supports_ready_batch());
    let bytes = app.data(1).unwrap();
    assert!(app.prepare_proposal_admission(std::iter::empty()).is_none());
    assert!(app.prepare_ready_admission(std::iter::empty()).is_none());
    assert!(app
        .validate_proposal(operation(1), &bytes, std::iter::empty())
        .is_ok());
}
