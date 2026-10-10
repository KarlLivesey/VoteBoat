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
use crate::credential_worker::{journal, prepare, Pending, Rejected};
use std::thread;
use std::{
    fs,
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc,
};
use voteboat::credential_reload::*;
use voteboat::identity::{NodeId, StoreId, StoreIdentity, StoreIncarnation};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    credentials: Credentials,
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-reload-worker-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let path = root.join("access");
        fs::write(&path, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
        let tls = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let access = Access::load(&path, &tls, 1).unwrap();
        let owner = PeerIdentity {
            node: NodeId::new(1).unwrap(),
            store: StoreIdentity {
                id: StoreId::new(1).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        };
        let reload = reload(&root, &path, &tls, 1, owner, &access).unwrap();
        let active = ActiveAccess::new(access.policy.generation(), access);
        Self {
            root,
            credentials: Credentials {
                reload: Some(reload),
                access: Some(active),
            },
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}
fn request() -> CredentialReloadRequest {
    CredentialReloadRequest {
        sequence: 1,
        expected: CredentialGeneration::new(1).unwrap(),
        replacement: CredentialGeneration::new(2).unwrap(),
    }
}
#[test]
fn held_preparation_is_single_flight_and_shutdown_joins_before_publication_returns() {
    let mut f = Fixture::new();
    fs::write(
        &f.credentials.reload.as_mut().unwrap().paths.source.access,
        "voteboat-service-access-v1 2\n3 admin 1 1\n",
    )
    .unwrap();
    let paths = f.credentials.reload.as_mut().unwrap().paths.clone();
    let (release, held) = mpsc::channel();
    f.credentials.reload.as_mut().unwrap().pending = Some(Pending {
        request: request(),
        handle: thread::spawn(move || {
            held.recv().unwrap();
            prepare(paths, request())
        }),
    });
    assert!(!f.credentials.poll());
    assert_eq!(
        f.credentials.access.as_mut().unwrap().generation(),
        Some(request().expected)
    );
    assert_eq!(
        journal(&f.credentials.reload.as_mut().unwrap().paths)
            .unwrap()
            .latest()
            .unwrap(),
        None
    );
    assert!(f
        .credentials
        .reload
        .as_mut()
        .unwrap()
        .submit(request(), request().expected)
        .unwrap()
        .contains("pending=true"));
    let mut other = request();
    other.sequence = 2;
    assert_eq!(
        f.credentials
            .reload
            .as_mut()
            .unwrap()
            .submit(other, request().expected),
        Err("credential reload busy".into())
    );
    release.send(()).unwrap();
    f.credentials.finish().unwrap();
    assert!(f.credentials.reload.as_mut().unwrap().pending.is_none());
    assert_eq!(
        f.credentials.access.as_mut().unwrap().generation(),
        Some(request().replacement)
    );
    assert_eq!(
        journal(&f.credentials.reload.as_mut().unwrap().paths)
            .unwrap()
            .latest()
            .unwrap(),
        f.credentials.reload.as_mut().unwrap().latest
    );
}
#[test]
fn uncertain_worker_result_fences_current_credentials_and_drains_the_handle() {
    let mut f = Fixture::new();
    let lease = f.credentials.access.as_mut().unwrap().lease().unwrap();
    f.credentials.reload.as_mut().unwrap().pending = Some(Pending {
        request: request(),
        handle: thread::spawn(|| {
            Err(Rejected {
                message: "sync outcome uncertain".into(),
                uncertain: true,
            })
        }),
    });
    assert!(f.credentials.finish().is_err());
    assert!(f
        .credentials
        .access
        .as_mut()
        .unwrap()
        .generation()
        .is_none());
    assert!(voteboat::secure::SessionValidity::validate(&lease).is_err());
    assert!(f.credentials.reload.as_mut().unwrap().pending.is_none());
    assert!(f
        .credentials
        .reload
        .as_mut()
        .unwrap()
        .submit(request(), request().expected)
        .unwrap_err()
        .contains("fenced"));
}
