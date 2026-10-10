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
use voteboat::{
    application::ApplicationError, identity::*, metadata_transfer::*, routing::*, transfer::*,
};
fn id() -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(1).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
#[test]
fn default_mappings_accept_only_current_manifest_branches() {
    assert_eq!(
        LifecycleManifestRead::query(id()),
        DirectoryQuery::Manifest(id())
    );
    assert_eq!(
        MetadataSourceManifestRead::query(id()),
        MetadataSourceQuery::Directory(DirectoryQuery::Manifest(id()))
    );
    assert_eq!(
        MetadataPublishingManifestRead::query(id()),
        MetadataPublishingQuery::Source(MetadataSourceManifestRead::query(id()))
    );
    assert_eq!(
        MetadataServingManifestRead::query(id()),
        MetadataServingQuery::Directory(DirectoryQuery::Manifest(id()))
    );
    assert_eq!(
        LifecycleManifestRead::manifest(DirectoryRead::Manifest(None)),
        Ok(None)
    );
    assert_eq!(
        MetadataSourceManifestRead::manifest(MetadataSourceRead::Directory(
            DirectoryRead::Manifest(None)
        )),
        Ok(None)
    );
    assert_eq!(
        MetadataSourceManifestRead::manifest(MetadataSourceRead::Serving(
            MetadataServingRead::Directory(DirectoryRead::Manifest(None))
        )),
        Ok(None)
    );
    assert_eq!(
        MetadataPublishingManifestRead::manifest(MetadataPublishingRead::Source(
            MetadataSourceRead::Serving(MetadataServingRead::Directory(DirectoryRead::Manifest(
                None
            )))
        )),
        Ok(None)
    );
    assert_eq!(
        MetadataServingManifestRead::manifest(MetadataServingRead::Directory(
            DirectoryRead::Manifest(None)
        )),
        Ok(None)
    );
    assert_eq!(
        MetadataSourceManifestRead::manifest(MetadataSourceRead::Fenced),
        Err(ApplicationError::NotApplied)
    );
    assert_eq!(
        MetadataServingManifestRead::manifest(MetadataServingRead::NotActive),
        Err(ApplicationError::NotApplied)
    );
    assert_eq!(
        MetadataPublishingManifestRead::manifest(MetadataPublishingRead::Source(
            MetadataSourceRead::Fenced
        )),
        Err(ApplicationError::NotApplied)
    );
    assert_eq!(
        MetadataSourceManifestRead::manifest(MetadataSourceRead::Serving(
            MetadataServingRead::NotActive
        )),
        Err(ApplicationError::NotApplied)
    );
    assert_eq!(
        LifecycleManifestRead::manifest(DirectoryRead::Transfer(None)),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(
        MetadataPublishingManifestRead::manifest(MetadataPublishingRead::Publication(None)),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(
        MetadataSourceManifestRead::manifest(MetadataSourceRead::Status(None)),
        Err(ApplicationError::InvalidCommand)
    );
    let historical = MetadataServingRead::Historical {
        source: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        through: 7,
        value: DirectoryRead::Manifest(None),
    };
    assert_eq!(
        MetadataServingManifestRead::manifest(historical.clone()),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(
        MetadataSourceManifestRead::manifest(MetadataSourceRead::Serving(historical)),
        Err(ApplicationError::InvalidCommand)
    );
}
struct HostMapping;
impl ManifestReadMapping for HostMapping {
    type Query = (u8, ResponsibilityIdentity);
    type ReadResult = Result<Option<ResponsibilityManifest>, u8>;
    fn query(r: ResponsibilityIdentity) -> Self::Query {
        (17, r)
    }
    fn manifest(r: Self::ReadResult) -> Result<Option<ResponsibilityManifest>, ApplicationError> {
        r.map_err(|_| ApplicationError::NotApplied)
    }
}
#[test]
fn host_mapping_uses_the_public_contract_without_native_features() {
    assert_eq!(HostMapping::query(id()), (17, id()));
    assert_eq!(HostMapping::manifest(Ok(None)), Ok(None));
    assert_eq!(
        HostMapping::manifest(Err(1)),
        Err(ApplicationError::NotApplied)
    );
}
