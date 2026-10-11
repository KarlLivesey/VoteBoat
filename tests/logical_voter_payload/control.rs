// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{model::Model, oracle, types::*};
#[derive(Default)]
pub struct Trace {
    pub actions: Vec<Action>,
}
impl Trace {
    pub fn take(&mut self, model: &mut Model, action: Action, weak: Weakening) {
        assert!(
            model.step(action, weak),
            "refused {action:?} at child{}",
            model.applied.0
        );
        self.actions.push(action);
    }
    pub fn settle(&mut self, model: &mut Model, kind: Kind, weak: Weakening) {
        self.take(model, Action::Submit(kind), weak);
        for physical in 0..3 {
            while model.stores[physical].base() + model.stores[physical].written.len()
                < model.admitted.len()
            {
                self.take(model, Action::Write(physical), weak);
            }
            while model.stores[physical].end() < model.admitted.len() {
                self.take(model, Action::Sync(physical), weak);
            }
        }
        self.take(model, Action::Commit, weak);
        self.take(model, Action::Apply, weak);
        self.take(model, Action::Observe, weak);
    }
    pub fn recover(&mut self, model: &mut Model, physical: usize, weak: Weakening) {
        self.take(model, Action::Quorum, weak);
        self.take(model, Action::Recover(physical), weak);
    }
    pub fn checkpoint(&mut self, model: &mut Model, weak: Weakening) {
        for p in 0..3 {
            self.take(model, Action::CheckpointStage(p), weak);
            self.take(model, Action::CheckpointSync(p), weak);
            self.take(model, Action::CheckpointAnchor(p), weak);
        }
    }
    pub fn replay_failure(&self, original: &Model, weak: Weakening, failure: &str) {
        let mut replayed = Model::initial();
        for action in &self.actions {
            assert!(replayed.step(*action, weak), "replay refused {action:?}");
        }
        assert_eq!(&replayed, original);
        assert!(!replayed.step(Action::Recover(1), weak));
        assert_eq!(&replayed, original);
        assert_eq!(oracle::check(&replayed).unwrap_err(), failure);
    }
}
#[test]
fn missing_body_counterexample_and_safe_dependency_progress() {
    let mut broken = Model::initial();
    let mut trace = Trace::default();
    for kind in [Kind::Claim1, Kind::Promise, Kind::AppendX, Kind::Commit1] {
        trace.settle(&mut broken, kind, Weakening::MissingPayload);
    }
    trace.take(&mut broken, Action::Ack(0), Weakening::MissingPayload);
    trace.take(&mut broken, Action::Power, Weakening::MissingPayload);
    trace.take(&mut broken, Action::Quorum, Weakening::MissingPayload);
    assert!(!broken.step(Action::Recover(1), Weakening::MissingPayload));
    let failure = oracle::check(&broken).unwrap_err();
    trace.replay_failure(&broken, Weakening::MissingPayload, &failure);
    assert!(failure.contains("recoverable exact-byte"));
    assert_eq!(
        oracle::own_bytes(&broken.stores[1], broken.applied).unwrap()[0],
        None
    );
    println!(
        "B missing-payload counterexample actions{}: {:?}; oracle={failure}",
        trace.actions.len(),
        trace.actions
    );
    let mut safe = Model::initial();
    let mut safe_trace = Trace::default();
    for kind in [Kind::Claim1, Kind::Promise] {
        safe_trace.settle(&mut safe, kind, Weakening::None);
    }
    safe_trace.take(&mut safe, Action::Reference, Weakening::None);
    for p in 0..3 {
        safe_trace.take(&mut safe, Action::Write(p), Weakening::None);
        safe_trace.take(&mut safe, Action::Sync(p), Weakening::None);
    }
    safe_trace.take(&mut safe, Action::Commit, Weakening::None);
    safe_trace.take(&mut safe, Action::Apply, Weakening::None);
    safe_trace.take(&mut safe, Action::Observe, Weakening::None);
    assert!(!safe.step(Action::Submit(Kind::Commit1), Weakening::None));
    assert!(!safe.step(Action::Ack(0), Weakening::None));
    safe_trace.settle(&mut safe, Kind::FillX, Weakening::None);
    safe_trace.settle(&mut safe, Kind::Commit1, Weakening::None);
    safe_trace.take(&mut safe, Action::Ack(0), Weakening::None);
    oracle::check(&safe).unwrap();
    println!(
        "B safe missing dependency refusal then actual FillX progress actions{}",
        safe_trace.actions.len()
    );
}
fn through_metadata(weak: Weakening) -> (Model, Trace) {
    let mut model = Model::initial();
    let mut trace = Trace::default();
    for kind in PROGRAM.into_iter().take(12) {
        trace.settle(&mut model, kind, weak);
        if kind == Kind::Commit1 {
            trace.take(&mut model, Action::Ack(0), weak);
        }
        if kind == Kind::Commit2 {
            trace.take(&mut model, Action::Ack(1), weak);
        }
    }
    (model, trace)
}
#[test]
fn early_anchor_counterexample_and_safe_pin_progress() {
    let (mut broken, mut trace) = through_metadata(Weakening::EarlyAnchor);
    trace.settle(&mut broken, Kind::Anchor1, Weakening::EarlyAnchor);
    trace.settle(&mut broken, Kind::Reclaim0, Weakening::EarlyAnchor);
    trace.checkpoint(&mut broken, Weakening::EarlyAnchor);
    trace.take(&mut broken, Action::Power, Weakening::EarlyAnchor);
    trace.take(&mut broken, Action::Quorum, Weakening::EarlyAnchor);
    assert!(!broken.step(Action::Recover(1), Weakening::EarlyAnchor));
    assert!(oracle::own_bytes(&broken.stores[1], broken.applied).is_none());
    let failure = oracle::check(&broken).unwrap_err();
    trace.replay_failure(&broken, Weakening::EarlyAnchor, &failure);
    println!(
        "B early-anchor counterexample actions{}: {:?}; oracle={failure}",
        trace.actions.len(),
        trace.actions
    );
    let (mut safe, mut safe_trace) = through_metadata(Weakening::None);
    assert!(!safe.step(Action::Submit(Kind::Anchor1), Weakening::None));
    assert!(!safe.step(Action::Submit(Kind::Reclaim0), Weakening::None));
    assert!(safe.image.pins[0]);
    safe_trace.checkpoint(&mut safe, Weakening::None);
    safe_trace.take(&mut safe, Action::Power, Weakening::None);
    safe_trace.recover(&mut safe, 1, Weakening::None);
    oracle::check(&safe).unwrap();
    for kind in [Kind::Bytes1, Kind::Publish1, Kind::Anchor1, Kind::Reclaim0] {
        safe_trace.settle(&mut safe, kind, Weakening::None);
    }
    assert!(!safe.image.pins[0]);
    assert!(safe.image.pins[1]);
    safe_trace.recover(&mut safe, 1, Weakening::None);
    safe_trace.settle(&mut safe, Kind::Claim2, Weakening::None);
    safe_trace.recover(&mut safe, 1, Weakening::None);
    safe_trace.settle(&mut safe, Kind::Serve2, Weakening::None);
    oracle::check(&safe).unwrap();
    assert!(safe.served);
    println!(
        "B safe checkpoint refusal then full bytes/publish/anchor/pin progress actions{}",
        safe_trace.actions.len()
    );
}
#[test]
fn holes_stale_suffix_original_binding_and_unavailable_recovery_refuse() {
    let mut model = Model::initial();
    assert!(!model.step(Action::Recover(1), Weakening::None));
    assert!(!model.step(Action::Sync(0), Weakening::None));
    let mut trace = Trace::default();
    for kind in PROGRAM.into_iter().take(9) {
        trace.settle(&mut model, kind, Weakening::None);
        if kind == Kind::Commit1 {
            trace.take(&mut model, Action::Ack(0), Weakening::None);
        }
    }
    let original = model.observed.unwrap();
    trace.take(&mut model, Action::Submit(Kind::Replace), Weakening::None);
    for p in 0..3 {
        trace.take(&mut model, Action::Write(p), Weakening::None);
        trace.take(&mut model, Action::Sync(p), Weakening::None);
    }
    trace.take(&mut model, Action::Commit, Weakening::None);
    trace.take(&mut model, Action::Apply, Weakening::None);
    let replacement = model.output.unwrap();
    assert!(!model.consume(original));
    assert_eq!(model.output, Some(replacement));
    assert!(model.consume(replacement));
    let mut attempted = model.image.clone();
    let old = &model.admitted[8];
    assert!(!super::image::update(&mut attempted, old, Weakening::None));
    assert_eq!(attempted, model.image);
    trace.take(&mut model, Action::Power, Weakening::None);
    assert!(!model.step(Action::Recover(1), Weakening::None));
    trace.take(&mut model, Action::Disconnect(0), Weakening::None);
    trace.take(&mut model, Action::Disconnect(1), Weakening::None);
    assert!(!model.step(Action::Quorum, Weakening::None));
    trace.take(&mut model, Action::Reconnect(0), Weakening::None);
    trace.take(&mut model, Action::Reconnect(1), Weakening::None);

    // Real model link loss removes quorum evidence without deleting durable bytes.
    trace.recover(&mut model, 1, Weakening::None);
    oracle::check(&model).unwrap();
}

#[test]
fn missing_own_prefix_and_inner_quorum_refuse_then_catchup_recovers() {
    let mut model = Model::initial();
    let mut trace = Trace::default();
    for kind in [Kind::Claim1, Kind::Promise, Kind::AppendX] {
        trace.take(&mut model, Action::Submit(kind), Weakening::None);
        for p in [0, 2] {
            trace.take(&mut model, Action::Write(p), Weakening::None);
            trace.take(&mut model, Action::Sync(p), Weakening::None);
        }
        trace.take(&mut model, Action::Commit, Weakening::None);
        trace.take(&mut model, Action::Apply, Weakening::None);
        trace.take(&mut model, Action::Observe, Weakening::None);
    }
    trace.take(&mut model, Action::Disconnect(0), Weakening::None);
    assert!(!model.step(Action::Quorum, Weakening::None));
    assert!(!model.step(Action::Recover(1), Weakening::None));
    trace.take(&mut model, Action::Reconnect(0), Weakening::None);
    trace.take(&mut model, Action::Quorum, Weakening::None);
    assert!(!model.step(Action::Recover(1), Weakening::None));
    while model.stores[1].written.len() < model.admitted.len() {
        trace.take(&mut model, Action::Write(1), Weakening::None);
        trace.take(&mut model, Action::Sync(1), Weakening::None);
    }
    trace.take(&mut model, Action::Recover(1), Weakening::None);
    oracle::check(&model).unwrap();
    assert_eq!(
        model.recovered[1].as_ref().unwrap().entries[0],
        Some(Entry::exact(Tag::X))
    );
    println!("B own-prefix/quorum refusal -> link repair/actual ordered catchup recovery actions{}: {:?}",trace.actions.len(),trace.actions);
}
