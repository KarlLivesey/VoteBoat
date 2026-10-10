// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Same ownership histories for independent host and native admission providers.
use super::support::{admission::HostPolicy, identity, node};
use std::{collections::BTreeMap, sync::mpsc, time::Duration};
use voteboat::{admission::*, identity::*, outbound::*};

trait Observed: AdmissionPolicy + Clone {
    fn credits(&self) -> OutboundUsage;
}
impl Observed for HostPolicy {
    fn credits(&self) -> OutboundUsage {
        self.usage()
    }
}
#[cfg(feature = "native")]
impl Observed for voteboat::native::admission::NativeAdmissionPolicy {
    fn credits(&self) -> OutboundUsage {
        self.usage()
    }
}
fn limits() -> OutboundUsage {
    OutboundUsage {
        batches: 3,
        messages: 7,
        bytes: 4096,
    }
}
fn request(step: u64) -> AdmissionRequest {
    let id = 1 + step % 2;
    AdmissionRequest {
        owner: OutboundBinding {
            node: node(id),
            store: StoreBinding {
                identity: identity(id.into()),
                session: StoreSession::new(1 + step % 3).unwrap(),
            },
            generation: OutboundGeneration::new(1 + step % 5).unwrap(),
        },
        peer: node(3),
        class: if step.is_multiple_of(2) {
            MessageClass::Data
        } else {
            MessageClass::Background
        },
        cost: OutboundUsage {
            batches: 1,
            messages: 1 + step as usize % 5,
            bytes: 1024 * (1 + step as usize % 3),
        },
        node_usage: OutboundUsage::default(),
        peer_usage: OutboundUsage::default(),
    }
}
#[derive(Clone)]
struct Held {
    id: u64,
    cost: OutboundUsage,
    _lease: AdmissionLease,
}
fn live_cost(held: &[Held]) -> OutboundUsage {
    let distinct: BTreeMap<_, _> = held.iter().map(|h| (h.id, h.cost)).collect();
    OutboundUsage {
        batches: distinct.values().map(|c| c.batches).sum(),
        messages: distinct.values().map(|c| c.messages).sum(),
        bytes: distinct.values().map(|c| c.bytes).sum(),
    }
}
fn reserve<P: Observed>(view: &P, closed: bool, held: &mut Vec<Held>, id: u64) {
    let before = live_cost(held);
    let request = request(id);
    let fits = before.batches + request.cost.batches <= limits().batches
        && before.messages + request.cost.messages <= limits().messages
        && before.bytes + request.cost.bytes <= limits().bytes;
    match view.reserve(request) {
        Ok(lease) => {
            assert!(!closed && fits, "reservation exceeded model capacity");
            held.push(Held {
                id,
                cost: request.cost,
                _lease: lease,
            });
        }
        Err(error) => assert_eq!(
            error,
            if closed {
                AdmissionError::Closed
            } else {
                assert!(!fits, "uncontended declared capacity was refused");
                AdmissionError::Overloaded
            }
        ),
    }
}
fn refused_dimension<P: Observed>(view: &P, step: u64) {
    let mut request = request(step);
    match step % 3 {
        0 => request.cost.messages = limits().messages + 1,
        1 => request.cost.bytes = limits().bytes + 1,
        _ => request.cost.messages = usize::MAX,
    }
    let before = view.credits();
    assert!(matches!(
        view.reserve(request),
        Err(AdmissionError::Overloaded)
    ));
    assert_eq!(view.credits(), before, "refusal retained partial credits");
}
fn batch_ceiling<P: Observed>(view: &P) {
    let leases: Vec<_> = (0..limits().batches)
        .map(|_| view.reserve(request(0)).unwrap())
        .collect();
    assert_eq!(
        view.credits(),
        OutboundUsage {
            batches: 3,
            messages: 3,
            bytes: 3072
        }
    );
    // Another request would still fit the message and byte limits.
    assert!(matches!(
        view.reserve(request(0)),
        Err(AdmissionError::Overloaded)
    ));
    drop(leases);
    assert_eq!(view.credits(), OutboundUsage::default());
}
fn history<P: Observed>(root: P, mut seed: u64) {
    let mut views = [root.clone(), root.clone(), root.clone(), root.clone()];
    let mut closed = [false; 4];
    let mut held = Vec::new();
    reserve(&root, false, &mut held, 0);
    assert_eq!(root.credits(), live_cost(&held), "reservation ownership");
    for step in 1..=256 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let action = (seed >> 32) as usize;
        let view = action % views.len();
        match (action / views.len()) % 7 {
            0 | 1 => reserve(&views[view], closed[view], &mut held, step),
            2 if !held.is_empty() && held.len() < 32 => {
                held.push(held[action % held.len()].clone());
            }
            3 if !held.is_empty() => {
                held.swap_remove(action % held.len());
            }
            4 => {
                views[view].close();
                closed[view] = true;
            }
            5 => {
                views[view] = root.clone();
                closed[view] = false;
            }
            6 => refused_dimension(&root, step),
            _ => (),
        }
        assert_eq!(
            root.credits(),
            live_cost(&held),
            "reservation ownership at {step}"
        );
    }
    for view in &mut views {
        view.close();
    }
    drop(views);
    assert_eq!(root.credits(), live_cost(&held));
    held.clear();
    assert_eq!(root.credits(), OutboundUsage::default());
    drop(root.reserve(request(0)).unwrap());
    assert_eq!(root.credits(), OutboundUsage::default());
}
fn cross_thread_release<P: Observed>(mut view: P) {
    let observer = view.clone();
    let mut request = request(0);
    request.cost = OutboundUsage {
        batches: 1,
        messages: 7,
        bytes: 4096,
    };
    let lease = view.reserve(request).unwrap();
    let last = lease.clone();
    let (ready, started) = mpsc::sync_channel(0);
    let (release, waiting) = mpsc::sync_channel(0);
    let thread = std::thread::spawn(move || {
        ready.send(()).unwrap();
        waiting.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(last);
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    view.close();
    assert!(matches!(view.reserve(request), Err(AdmissionError::Closed)));
    drop(view);
    drop(lease);
    assert_eq!(observer.credits(), request.cost);
    assert!(matches!(
        observer.reserve(request),
        Err(AdmissionError::Overloaded)
    ));
    release.send(()).unwrap();
    thread.join().unwrap();
    assert_eq!(observer.credits(), OutboundUsage::default());
    drop(observer.reserve(request).unwrap());
    assert_eq!(observer.credits(), OutboundUsage::default());
}
#[test]
fn host_admission_generated_ownership_and_refusal_history() {
    batch_ceiling(&HostPolicy::new(limits()));
    for seed in 1..=32 {
        history(HostPolicy::new(limits()), seed);
    }
}
#[test]
fn host_admission_final_credit_release_crosses_threads_and_view_close() {
    cross_thread_release(HostPolicy::new(limits()));
}
#[cfg(feature = "native")]
#[test]
fn native_admission_generated_ownership_and_refusal_history() {
    batch_ceiling(&voteboat::native::admission::NativeAdmissionPolicy::new(limits()).unwrap());
    for seed in 1..=32 {
        history(
            voteboat::native::admission::NativeAdmissionPolicy::new(limits()).unwrap(),
            seed,
        );
    }
}
#[cfg(feature = "native")]
#[test]
fn native_admission_final_credit_release_crosses_threads_and_view_close() {
    cross_thread_release(
        voteboat::native::admission::NativeAdmissionPolicy::new(limits()).unwrap(),
    );
}
#[test]
fn opaque_lease_destroys_its_provider_token_once_after_all_thread_owners() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Token(Arc<AtomicUsize>);
    impl Drop for Token {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let lease = AdmissionLease::new(Token(drops.clone()));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let copy = lease.clone();
            std::thread::spawn(move || drop(copy))
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(lease);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct EarlyRelease(HostPolicy);
impl Observed for EarlyRelease {
    fn credits(&self) -> OutboundUsage {
        self.0.usage()
    }
}
impl AdmissionPolicy for EarlyRelease {
    fn reserve(&self, request: AdmissionRequest) -> Result<AdmissionLease, AdmissionError> {
        drop(self.0.reserve(request)?);
        Ok(AdmissionLease::new(()))
    }
    fn close(&mut self) {
        self.0.close();
    }
}
#[test]
#[should_panic(expected = "reservation ownership")]
fn shared_history_detects_a_provider_releasing_accepted_credits_early() {
    history(EarlyRelease(HostPolicy::new(limits())), 1);
}
