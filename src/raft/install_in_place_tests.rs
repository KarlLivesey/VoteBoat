// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! V3f: the pending transition is installed where it lies. A failed install
//! leaves it intact (the fence then discards it, as before); a successful one
//! empties the slot before anything else runs.
use super::*;
use crate::log::transition::LogMetadata;
use crate::membership::ConfigurationChange;
use membership_tests::{accept, configuration, core, durable, node};

/// Every observable part of the pending transition.
#[derive(Debug, PartialEq)]
struct PendingView {
    update: LogUpdate,
    metadata: LogMetadata,
    commit: bool,
    full: Option<GroupLog>,
    membership: Option<Membership>,
    ticket: Option<LogTicket>,
    after: String,
    reply: Option<Message>,
    planned: Option<LogStamp>,
}
fn pending(core: &Raft) -> PendingView {
    let p = core.pending.as_ref().expect("a pending transition");
    PendingView {
        update: p.next.update(),
        metadata: p.next.metadata(),
        commit: p.next.is_commit(),
        full: p.next.full().cloned(),
        membership: p.membership.as_deref().cloned(),
        ticket: p.ticket,
        after: format!("{:?}", p.after),
        reply: p.reply.clone(),
        planned: p.planned,
    }
}
/// The core's log as the install sees it.
#[derive(Debug, PartialEq)]
struct LogView {
    state: GroupLog,
    summary: LogSummary,
    stamp: Option<LogStamp>,
}
fn log_view(core: &Raft) -> LogView {
    LogView {
        state: core.state().clone(),
        summary: *core.durable.summary(),
        stamp: core.durable.stamp(),
    }
}
const MISMATCH: &str = "transition plan predecessor or update mismatch";

/// Run the failing install on `core` and check it changed nothing: the same
/// error, the pending transition intact, the log, summary and stamp as they
/// were, the core not yet fenced.
fn refused_in_place(core: &mut Raft) {
    let staged = pending(core);
    let log = log_view(core);
    let Err(error) = core.install_pending_plan() else {
        panic!("the install must be refused")
    };
    assert!(
        matches!(error, RaftError::Storage(StorageError::Rejected(MISMATCH))),
        "{error:?}"
    );
    assert_eq!(pending(core), staged, "the pending transition is intact");
    assert_eq!(log_view(core), log, "the log is unchanged");
    assert!(!core.is_fenced());
}
/// Then the public completion fences exactly as before: the same error, no
/// pending dependency, the log unchanged.
fn fenced_as_before(core: &mut Raft, result: Result<(), RaftError>, log: &LogView) {
    assert!(
        matches!(
            result,
            Err(RaftError::Storage(StorageError::Rejected(MISMATCH)))
        ),
        "{result:?}"
    );
    assert!(core.is_fenced());
    assert!(!core.has_pending_dependency());
    assert!(core.pending.is_none());
    assert_eq!(&log_view(core), log);
}

fn volatile_follower() -> (Raft, VolatileLog) {
    let mut core = core();
    core.volatile = true;
    core.incarnation = Raft::next_volatile_incarnation().unwrap();
    core.role = Role::Follower;
    let log = VolatileLog::new(core.binding, core.limits).unwrap();
    (core, log)
}
fn command(index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::from(index)).unwrap(),
            bytes: vec![index as u8].into(),
        },
    }
}
fn token(effects: &[Effect]) -> VolatilePersist {
    let [Effect::PersistVolatile(token)] = effects else {
        panic!("one payload-free persistence token, got {effects:?}")
    };
    *token
}
/// A follower append of `from..=to` with a reply to send once it is durable.
fn stage_append(core: &mut Raft, from: u64, to: u64) -> VolatilePersist {
    let context = core.context().unwrap();
    let reply = core.message(
        node(2),
        context,
        Rpc::Vote {
            last_index: to,
            last_term: 1,
        },
    );
    let effects = core
        .persist(
            core.state().hard_state,
            core.state().commit_index,
            Some(Suffix {
                from,
                entries: (from..=to).map(command).collect::<Vec<_>>().into(),
            }),
            After::Reply,
            Some(reply),
        )
        .unwrap()
        .into_owned();
    assert!(core.pending.as_ref().unwrap().next.is_append());
    token(&effects)
}
fn stage_commit(core: &mut Raft, commit: u64) -> VolatilePersist {
    let effects = core
        .persist(core.state().hard_state, commit, None, After::Commit, None)
        .unwrap()
        .into_owned();
    assert!(core.pending.as_ref().unwrap().next.is_commit());
    token(&effects)
}
/// Change the log behind a staged plan: its stamp moves and its predecessor
/// no longer matches.
fn move_log(core: &mut Raft) {
    let before = core.state().clone();
    core.durable
        .edit(|log| log.hard_state.voted_for = Some(node(3)));
    assert_ne!(core.state(), &before);
}
fn persisted(
    core: &mut Raft,
    log: &VolatileLog,
    persist: VolatilePersist,
) -> Result<(), RaftError> {
    persist_volatile(core, log, persist).map(|_| ())
}

#[test]
fn a_failed_install_after_a_predecessor_mismatch_leaves_the_plan_intact() {
    // An append (with its reply) and a commit advance.
    for commit in [false, true] {
        let (mut core, log) = volatile_follower();
        let mut persist = stage_append(&mut core, 2, 3);
        if commit {
            persisted(&mut core, &log, persist).unwrap();
            persist = stage_commit(&mut core, 3);
        }
        move_log(&mut core);
        refused_in_place(&mut core);
        let moved = log_view(&core);
        let result = persisted(&mut core, &log, persist);
        fenced_as_before(&mut core, result, &moved);
    }
}

/// A plan moved to another core at the same version: the stamp is foreign,
/// so the install compares it and refuses it (the other log is longer).
#[test]
fn a_failed_install_of_a_foreign_plan_leaves_the_plan_intact() {
    let (mut short, _) = volatile_follower();
    stage_append(&mut short, 2, 3);
    let (mut grown, grown_log) = volatile_follower();
    let persist = stage_append(&mut grown, 2, 4);
    persisted(&mut grown, &grown_log, persist).unwrap();
    let mut other = Raft::recover_volatile(grown.node, &grown_log, grown.state().clone()).unwrap();
    let version = |core: &Raft| core.durable.stamp().map(LogStamp::version);
    assert_eq!((version(&short), version(&other)), (Some(0), Some(0)));
    other.pending = short.pending.take();
    refused_in_place(&mut other);
    // The public completion path: the moved plan's token is not this
    // incarnation's, so drive the install the completions use.
    let log = log_view(&other);
    let mut out = EffectQueue::new();
    let result = other.install_pending_into(&mut out).map(|_| ());
    fenced_as_before(&mut other, result, &log);
    assert!(out.is_empty());
}

/// A cold full plan (a configuration entry on a durable core) refused by the
/// `NextLog` check, the same validation a store applies, after its log moved.
#[test]
fn a_failed_install_of_a_refused_full_plan_leaves_the_plan_intact() {
    let mut leader = core();
    let effects = accept(
        &mut leader,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
    );
    let [Effect::Persist(update)] = &effects[..] else {
        panic!("one durable persistence effect")
    };
    assert!(leader.pending.as_ref().unwrap().next.full().is_some());
    assert!(leader.pending.as_ref().unwrap().membership.is_some());
    leader.validate_persist_effect(update).unwrap();
    let next = leader.pending.as_ref().unwrap().next.metadata();
    let ticket = LogTicket {
        binding: leader.binding,
        batch: leader.last_batch + 1,
        group: next.group,
        revision: next.revision,
        generation: next.generation,
        last_index: next.last_index,
        term: next.hard_state.term,
    };
    leader.admitted(ticket).unwrap();
    move_log(&mut leader);
    refused_in_place(&mut leader);
    let moved = log_view(&leader);
    let result = leader
        .complete(&DurableLog {
            tickets: vec![ticket],
        })
        .map(|_| ());
    fenced_as_before(&mut leader, result, &moved);
}

/// A successful install empties the slot before anything else runs, and
/// returns the parts the core still uses.
#[test]
fn a_successful_install_empties_the_slot() {
    let (mut core, log) = volatile_follower();
    stage_append(&mut core, 2, 3);
    let staged = pending(&core);
    let installed = core.install_pending_plan().unwrap();
    assert!(
        core.pending.is_none(),
        "the spent plan was left in the slot"
    );
    assert_eq!(format!("{:?}", installed.after), staged.after);
    assert_eq!(installed.reply, staged.reply);
    assert!(installed.membership.is_none());
    assert!(installed.preserves_checkpoint_prefix);
    assert_eq!(core.state().last_index(), 3);
    assert_eq!(
        *core.durable.summary(),
        LogSummary::from_validated(core.state())
    );
    // Through the public path: a commit round stages nothing after it.
    let persist = stage_commit(&mut core, 3);
    persisted(&mut core, &log, persist).unwrap();
    assert!(
        core.pending.is_none(),
        "the spent plan was left in the slot"
    );
    assert!(!core.has_pending_dependency());
    assert_eq!(core.state().commit_index, 3);
    // A durable configuration: full plan, membership taken from the slot.
    let mut leader = membership_tests::core();
    let effects = accept(
        &mut leader,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
    );
    durable(&mut leader, effects);
    assert!(
        leader.pending.is_none(),
        "the spent plan was left in the slot"
    );
    assert_eq!(leader.membership().id(), membership_tests::cid(2));
    assert!(!leader.is_fenced());
}
